"""Paper & live signal runner with bounded memory, STALE/GAP safety guard, and JSONL journal."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Literal

from bq_backtest.fill import FillResult, FillSimulator, OrderBookView
from bq_backtest.strategy import (
    BaseStrategy,
    BtcSpotBboRule,
    OrderIntent,
    StrategyAccountView,
    ThreeStageQuantStrategy,
)
from bq_feature.engine import FeatureEngine, FeatureState
from bq_feed.events import (
    BookTickerEvent,
    DepthUpdateEvent,
    FeedEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    StreamHealthEvent,
    event_to_dict,
)
from bq_feed.live import GatewayLiveFeed
from bq_feed.parity import check_strategy_capabilities
from bq_model.bundle import ModelBundle

from .config import DeployConfig


@dataclass(frozen=True, slots=True)
class PaperJournalEntry:
    journal_seq: int
    config_hash: str
    strategy_id: str
    model_id: str | None
    schema_hash: str | None
    mode: str
    health_ok: bool
    blocked_reason: str | None
    event: dict[str, Any]
    intent: dict[str, Any] | None
    fill: dict[str, Any] | None
    cash_after: str
    position_after: str


@dataclass(frozen=True, slots=True)
class RunnerSummary:
    mode: str
    processed_events: int
    intents_emitted: int
    fills_count: int
    blocked_by_health_count: int
    final_cash_quote: str
    final_position_qty: str
    journal_path: str | None


def build_strategy(cfg: DeployConfig) -> tuple[BaseStrategy, str | None]:
    if cfg.strategy_kind == "three_stage":
        bundle = ModelBundle.load(Path(cfg.model_dir)) if cfg.model_dir else None
        schema_hash = bundle.schema_hash if bundle is not None else None
        return (
            ThreeStageQuantStrategy(
                market=cfg.market,
                symbol=cfg.symbol,
                required_streams=((cfg.market, cfg.symbol, cfg.stream),),
                model_bundle=bundle,
                order_qty=cfg.order_qty,
                max_pos_qty=cfg.max_pos_qty,
                max_spread_bps=cfg.max_spread_bps,
            ),
            schema_hash,
        )
    return (
        BtcSpotBboRule(
            symbol=cfg.symbol,
            market=cfg.market,
            order_qty=cfg.order_qty,
            max_pos_qty=cfg.max_pos_qty,
            buy_imbalance_threshold=cfg.buy_imbalance_threshold,
            sell_imbalance_threshold=cfg.sell_imbalance_threshold,
            max_spread_bps=cfg.max_spread_bps,
        ),
        None,
    )


class SignalRunner:
    """Executes strategy in --mode paper (writes paper_journal.jsonl) or --mode live (OrderIntent only)."""

    def __init__(
        self,
        config: DeployConfig,
        *,
        mode: Literal["paper", "live"] = "paper",
        live_feed: GatewayLiveFeed | None = None,
        intent_sink: Callable[[OrderIntent], None] | None = None,
    ) -> None:
        self._cfg = config
        self._mode = mode
        self._live_feed = live_feed
        self._intent_sink = intent_sink
        self._strategy, self._schema_hash = build_strategy(config)
        self._feat_eng = FeatureEngine()
        self._feat_state = FeatureState()
        self._sim = FillSimulator(mode="F1", taker_fee_bps=config.taker_fee_bps)

        self._cash = config.initial_cash_quote
        self._pos = Decimal(0)
        self._filters = config.to_filters()
        self._book_ver = 0
        self._best_bid: tuple[Decimal, Decimal] | None = None
        self._best_ask: tuple[Decimal, Decimal] | None = None
        self._health_ok = True
        self._health_reason = "OK"
        self._last_trade_ts = 0
        self._journal_seq = 0
        self._intents_count = 0
        self._fills_count = 0
        self._blocked_count = 0

    def run_events(
        self,
        events: Iterable[FeedEvent],
        *,
        journal_path: Path | None = None,
    ) -> RunnerSummary:
        # Step 1: capability parity & live preflight Fail-Fast
        check_strategy_capabilities(self._strategy.required_streams, mode=self._mode)
        if self._live_feed is not None:
            self._live_feed.preflight_capabilities(self._strategy.required_streams)

        out_path = journal_path if journal_path is not None else (
            Path(self._cfg.journal_path) if self._mode == "paper" else None
        )
        jf = None
        if self._mode == "paper" and out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            jf = out_path.open("w", encoding="utf-8")

        try:
            for ev in events:
                self._process_one(ev, jf)
                if self._live_feed is not None:
                    for stale_ev in self._live_feed.poll_watchdog():
                        self._process_one(stale_ev, jf)
        finally:
            if jf is not None:
                jf.flush()
                jf.close()

        return RunnerSummary(
            mode=self._mode,
            processed_events=self._journal_seq,
            intents_emitted=self._intents_count,
            fills_count=self._fills_count,
            blocked_by_health_count=self._blocked_count,
            final_cash_quote=str(self._cash),
            final_position_qty=str(self._pos),
            journal_path=str(out_path) if out_path else None,
        )

    def _process_one(self, ev: FeedEvent, jf: Any | None) -> None:
        self._journal_seq += 1
        intent_obj: OrderIntent | None = None
        fill_obj: FillResult | None = None
        blocked_reason: str | None = None

        if isinstance(ev, StreamHealthEvent):
            self._feat_eng.on_health(self._feat_state, ev)
            if ev.status in ("GAP", "STALE", "DEGRADED", "CLOSED", "STOPPED"):
                self._health_ok = False
                self._health_reason = f"{ev.status}:{ev.reason}"
            elif ev.status == "LIVE":
                self._health_ok = True
                self._health_reason = "OK"
        else:
            if isinstance(ev, ReferenceObservationEvent) and ev.source_kind == "exchange_info":
                self._filters = self._cfg.to_filters().__class__(
                    tick_size=ev.tick_size,
                    step_size=ev.step_size,
                    min_notional=ev.min_notional,
                    is_known=True,
                )
            elif isinstance(ev, BookTickerEvent):
                self._best_bid = (ev.best_bid_price, ev.best_bid_quantity)
                self._best_ask = (ev.best_ask_price, ev.best_ask_quantity)
                self._book_ver += 1
                if self._health_reason.startswith("STALE:"):
                    self._health_ok = True
                    self._health_reason = "OK"
            elif isinstance(ev, OrderBookSnapshotEvent):
                if ev.bids and ev.asks:
                    self._best_bid = max(ev.bids, key=lambda x: x[0])
                    self._best_ask = min(ev.asks, key=lambda x: x[0])
                    self._book_ver += 1
                    self._health_ok = True
                    self._health_reason = "OK"

            vec = self._feat_eng.update(self._feat_state, ev)
            if vec is not None:
                acct = StrategyAccountView(
                    position_qty=self._pos,
                    cash_quote=self._cash,
                    last_trade_ts_ns=self._last_trade_ts,
                    intent_counter=self._intents_count,
                )
                if not self._health_ok or not vec.health_valid:
                    blocked_reason = self._health_reason if not self._health_ok else "FEATURE_HEALTH_INVALID"
                    self._blocked_count += 1
                else:
                    intent_obj = self._strategy.on_feature(vec, acct)

                if intent_obj is not None:
                    self._intents_count += 1
                    if self._mode == "live":
                        if self._intent_sink is not None:
                            self._intent_sink(intent_obj)
                    elif self._best_bid is not None and self._best_ask is not None:
                        book = OrderBookView(
                            symbol=self._cfg.symbol,
                            book_version=self._book_ver,
                            bids=(self._best_bid,),
                            asks=(self._best_ask,),
                            health_valid=self._health_ok,
                            crossed=self._best_bid[0] >= self._best_ask[0],
                        )
                        fill_obj = self._sim.execute(intent_obj, book, self._filters)
                        if fill_obj.status in ("FILLED", "PARTIAL"):
                            self._fills_count += 1
                            if intent_obj.side == "BUY":
                                self._pos += fill_obj.filled_qty
                                self._cash -= fill_obj.notional_quote + fill_obj.fee_quote
                            else:
                                self._pos -= fill_obj.filled_qty
                                self._cash += fill_obj.notional_quote - fill_obj.fee_quote
                            self._last_trade_ts = intent_obj.decision_ts_ns

        if jf is not None:
            entry = PaperJournalEntry(
                journal_seq=self._journal_seq,
                config_hash=self._cfg.config_hash,
                strategy_id=self._strategy.strategy_id,
                model_id=self._cfg.model_id,
                schema_hash=self._schema_hash,
                mode=self._mode,
                health_ok=self._health_ok,
                blocked_reason=blocked_reason,
                event=event_to_dict(ev),
                intent={
                    "intent_id": intent_obj.intent_id,
                    "side": intent_obj.side,
                    "quantity": str(intent_obj.quantity),
                    "decision_seq": intent_obj.decision_seq,
                    "decision_ts_ns": intent_obj.decision_ts_ns,
                    "reason": intent_obj.reason,
                }
                if intent_obj
                else None,
                fill={
                    "status": fill_obj.status,
                    "filled_qty": str(fill_obj.filled_qty),
                    "avg_fill_price": str(fill_obj.avg_fill_price),
                    "fee_quote": str(fill_obj.fee_quote),
                    "reason": fill_obj.reason,
                }
                if fill_obj
                else None,
                cash_after=str(self._cash),
                position_after=str(self._pos),
            )
            jf.write(json.dumps(asdict(entry), sort_keys=True, separators=(",", ":")) + "\n")
