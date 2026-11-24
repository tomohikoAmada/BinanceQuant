"""Paper & live signal runner driven by shared CausalTradingCore, append-safe JSONL journal & STALE safety."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from bq_backtest.engine import CausalTradingCore, LedgerEntry
from bq_backtest.fill import FillResult
from bq_backtest.strategy import (
    BaseStrategy,
    BtcSpotBboRule,
    OrderIntent,
    ThreeStageQuantStrategy,
)
from bq_feed.events import FeedEvent, event_to_dict
from bq_feed.live import GatewayLiveFeed
from bq_feed.parity import check_strategy_capabilities
from bq_model.bundle import ModelBundle

from .config import DeployConfig


@dataclass(frozen=True, slots=True)
class PaperJournalEntry:
    journal_seq: int
    run_id: str
    config_hash: str
    strategy_id: str
    model_id: str | None
    schema_hash: str | None
    model_digest: str | None
    mode: str
    health_ok: bool
    blocked_reason: str | None
    event: dict[str, Any]
    intent: dict[str, Any] | None
    fill: dict[str, Any] | None
    settled_fills: list[dict[str, Any]]
    cash_after: str
    position_after: str
    pending_count: int


@dataclass(frozen=True, slots=True)
class RunnerSummary:
    mode: str
    run_id: str
    processed_events: int
    intents_emitted: int
    fills_count: int
    blocked_by_health_count: int
    pending_at_end_count: int
    final_cash_quote: str
    final_position_qty: str
    journal_path: str | None


def build_strategy(cfg: DeployConfig) -> tuple[BaseStrategy, str | None, str | None]:
    infer_ns = int((cfg.model_infer_ms or 0.0) * 1_000_000)
    if cfg.strategy_kind == "three_stage":
        bundle = ModelBundle.load(Path(cfg.model_dir)) if cfg.model_dir else None
        schema_hash = bundle.schema_hash if bundle is not None else None
        model_digest = bundle.metadata.model_digest if bundle is not None else None
        return (
            ThreeStageQuantStrategy(
                market=cfg.market,
                symbol=cfg.symbol,
                required_streams=((cfg.market, cfg.symbol, cfg.stream),),
                model_bundle=bundle,
                order_qty=cfg.order_qty,
                max_pos_qty=cfg.max_pos_qty,
                max_spread_bps=cfg.max_spread_bps,
                infer_ns=infer_ns,
            ),
            schema_hash,
            model_digest,
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
            infer_ns=infer_ns,
        ),
        None,
        None,
    )


def _serialize_settled_fill(intent: OrderIntent, fill: FillResult, entry: LedgerEntry) -> dict[str, Any]:
    return {
        "intent_id": intent.intent_id,
        "side": intent.side,
        "status": fill.status,
        "filled_qty": str(fill.filled_qty),
        "avg_fill_price": str(fill.avg_fill_price),
        "fee_quote": str(fill.fee_quote),
        "send_ts_ns": entry.send_ts_ns,
        "arrival_ts_ns": entry.arrival_ts_ns,
        "cash_after": entry.cash_after,
        "position_after": entry.position_after,
        "reason": fill.reason,
    }


class SignalRunner:
    """Executes strategy in --mode paper (append-safe paper_journal.jsonl) or --mode live (OrderIntent sink)."""

    def __init__(
        self,
        config: DeployConfig,
        *,
        mode: Literal["paper", "live"] = "paper",
        live_feed: GatewayLiveFeed | None = None,
        intent_sink: Callable[[OrderIntent], None] | None = None,
        diagnostic: bool = False,
        run_id: str | None = None,
    ) -> None:
        if mode == "live" and intent_sink is None and not diagnostic:
            raise ValueError(
                "live mode requires an explicit intent_sink (pass diagnostic=True only for sinkless dry-runs)"
            )
        self._cfg = config
        self._mode = mode
        self._live_feed = live_feed
        self._intent_sink = intent_sink
        self._run_id = run_id or f"bq-{uuid4().hex[:12]}"
        self._strategy, self._schema_hash, self._model_digest = build_strategy(config)
        self._journal_seq = 0
        self._processed_this_run = 0
        self._core = CausalTradingCore(
            self._strategy,
            initial_cash_quote=config.initial_cash_quote,
            fill_mode="F1",
            taker_fee_bps=config.taker_fee_bps,
            model_infer_ms=config.model_infer_ms,
            network_rtt_ms=config.network_rtt_ms,
            default_filters=config.to_filters(),
            simulate_fills=(mode == "paper"),
            intent_sink=intent_sink,
            enforce_model_cutoff=True,
            strict_receive_time=config.strict_receive_time,
        )

    def _restore_prior_journal(self, path: Path) -> None:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not lines:
            return
        last_row = json.loads(lines[-1])
        prior_hash = last_row.get("config_hash")
        if prior_hash is not None and prior_hash != self._cfg.config_hash:
            raise ValueError(
                f"Cannot append to existing journal with mismatched config_hash: {prior_hash} != {self._cfg.config_hash}"
            )
        last_seq = int(last_row.get("journal_seq", len(lines)))
        last_cash = Decimal(str(last_row["cash_after"]))
        last_pos = Decimal(str(last_row["position_after"]))
        self._journal_seq = last_seq
        self._core.cash = last_cash
        self._core.pos = last_pos

    def run_events(
        self,
        events: Iterable[FeedEvent],
        *,
        journal_path: Path | None = None,
    ) -> RunnerSummary:
        check_strategy_capabilities(self._strategy.required_streams, mode=self._mode)
        if self._live_feed is not None:
            self._live_feed.preflight_capabilities(self._strategy.required_streams)

        out_path = journal_path if journal_path is not None else (
            Path(self._cfg.journal_path) if self._mode == "paper" else None
        )
        jf = None
        if self._mode == "paper" and out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path.exists() and out_path.stat().st_size > 0:
                self._restore_prior_journal(out_path)
            jf = out_path.open("a", encoding="utf-8")

        try:
            for ev in events:
                if self._live_feed is not None:
                    for pre_stale in self._live_feed.drain_pending_watchdog():
                        self._process_one(pre_stale, jf)
                self._process_one(ev, jf)
                if self._live_feed is not None:
                    for post_stale in self._live_feed.poll_watchdog():
                        self._process_one(post_stale, jf)
        finally:
            if jf is not None:
                jf.flush()
                jf.close()

        return RunnerSummary(
            mode=self._mode,
            run_id=self._run_id,
            processed_events=self._processed_this_run,
            intents_emitted=len(self._core.intents),
            fills_count=self._core.fills_count,
            blocked_by_health_count=self._core.blocked_count,
            pending_at_end_count=len(self._core.pending),
            final_cash_quote=str(self._core.cash),
            final_position_qty=str(self._core.pos),
            journal_path=str(out_path) if out_path else None,
        )

    def _process_one(self, ev: FeedEvent, jf: Any | None) -> None:
        self._journal_seq += 1
        self._processed_this_run += 1
        outcome = self._core.step(ev)

        if jf is not None:
            settled_payload = [
                _serialize_settled_fill(intent, fill, entry)
                for intent, fill, entry in outcome.settled_fills
            ]
            primary_fill = settled_payload[0] if settled_payload else None
            intent_obj = outcome.emitted_intent
            entry = PaperJournalEntry(
                journal_seq=self._journal_seq,
                run_id=self._run_id,
                config_hash=self._cfg.config_hash,
                strategy_id=self._strategy.strategy_id,
                model_id=self._cfg.model_id,
                schema_hash=self._schema_hash,
                model_digest=self._model_digest,
                mode=self._mode,
                health_ok=outcome.health_ok,
                blocked_reason=outcome.blocked_reason,
                event=event_to_dict(ev),
                intent={
                    "intent_id": intent_obj.intent_id,
                    "side": intent_obj.side,
                    "quantity": str(intent_obj.quantity),
                    "decision_seq": intent_obj.decision_seq,
                    "decision_ts_ns": intent_obj.decision_ts_ns,
                    "decision_infer_ns": intent_obj.decision_infer_ns,
                    "reason": intent_obj.reason,
                }
                if intent_obj
                else None,
                fill=primary_fill,
                settled_fills=settled_payload,
                cash_after=str(outcome.cash_after),
                position_after=str(outcome.position_after),
                pending_count=len(self._core.pending),
            )
            jf.write(json.dumps(asdict(entry), sort_keys=True, separators=(",", ":")) + "\n")
            jf.flush()
