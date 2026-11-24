"""Causal event-driven backtest engine, sensitivity matrix, causality test, and run persistence."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Sequence

from bq_feature.engine import FeatureEngine, FeatureState, FeatureVector, _event_ts_ns
from bq_feed.events import (
    BookTickerEvent,
    DepthUpdateEvent,
    FeedEvent,
    Market,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    StreamHealthEvent,
    event_from_dict,
    event_to_dict,
)

from .fill import ExchangeFilters, FillResult, FillSimulator, OrderBookView
from .strategy import BaseStrategy, OrderIntent, StrategyAccountView


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    run_id: str = "run-default"
    initial_cash_quote: Decimal = Decimal("100000")
    fill_mode: Literal["F1", "F2"] = "F1"
    taker_fee_bps: Decimal = Decimal("10")
    model_infer_ms: float | None = None
    network_rtt_ms: float = 0.0
    default_filters: ExchangeFilters = field(default_factory=ExchangeFilters)
    enforce_model_cutoff: bool = True
    strict_receive_time: bool = False


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    seq: int
    decision_ts_ns: int
    send_ts_ns: int
    arrival_ts_ns: int
    intent_id: str
    side: str
    requested_qty: str
    status: str
    filled_qty: str
    avg_fill_price: str
    fee_quote: str
    funding_delta_quote: str
    cash_after: str
    position_after: str
    realized_equity: str


@dataclass(slots=True)
class BacktestResult:
    run_id: str
    metrics: dict[str, Any]
    intents: list[OrderIntent]
    ledger: list[LedgerEntry]
    feature_history: list[FeatureVector]
    semantic_digest: str


@dataclass(slots=True)
class _PendingOrder:
    intent: OrderIntent
    send_ts_ns: int
    arrival_ts_ns: int
    enqueue_seq: int
    reserved_cash_quote: Decimal = Decimal(0)
    reserved_buy_qty: Decimal = Decimal(0)
    reserved_sell_qty: Decimal = Decimal(0)


@dataclass(frozen=True, slots=True)
class StepOutcome:
    event: FeedEvent
    ts_ns: int | None
    health_ok: bool
    blocked_reason: str | None
    emitted_intent: OrderIntent | None
    settled_fills: tuple[tuple[OrderIntent, FillResult, LedgerEntry], ...]
    cash_after: Decimal
    position_after: Decimal


def _sorted_book_sides(
    bids_map: dict[Decimal, Decimal], asks_map: dict[Decimal, Decimal]
) -> tuple[tuple[tuple[Decimal, Decimal], ...], tuple[tuple[Decimal, Decimal], ...]]:
    bids = tuple(sorted(((p, q) for p, q in bids_map.items() if q > 0), key=lambda x: x[0], reverse=True))
    asks = tuple(sorted(((p, q) for p, q in asks_map.items() if q > 0), key=lambda x: x[0]))
    return bids, asks


class CausalTradingCore:
    """Single shared event-driven F1/F2 execution, reservation, and MTM drawdown engine."""

    def __init__(
        self,
        strategy: BaseStrategy,
        *,
        initial_cash_quote: Decimal = Decimal("100000"),
        initial_position_qty: Decimal = Decimal(0),
        initial_last_trade_ts_ns: int = 0,
        fill_mode: Literal["F1", "F2"] = "F1",
        taker_fee_bps: Decimal = Decimal("10"),
        model_infer_ms: float | None = None,
        network_rtt_ms: float = 0.0,
        default_filters: ExchangeFilters | None = None,
        simulate_fills: bool = True,
        intent_sink: Callable[[OrderIntent], None] | None = None,
        enforce_model_cutoff: bool = True,
        strict_receive_time: bool = False,
        retain_history: bool = True,
        feature_engine: FeatureEngine | None = None,
    ) -> None:
        self.strategy = strategy
        self.feat_eng = feature_engine or FeatureEngine(strict_receive_time=strict_receive_time)
        self.feat_state = FeatureState()
        self.sim = FillSimulator(mode=fill_mode, taker_fee_bps=taker_fee_bps)
        self.initial_cash_quote = initial_cash_quote
        self.cash = initial_cash_quote
        self.pos = initial_position_qty
        self.reserved_cash_quote = Decimal(0)
        self.reserved_buy_qty = Decimal(0)
        self.reserved_sell_qty = Decimal(0)
        self.last_trade_ts = initial_last_trade_ts_ns
        self.simulate_fills = simulate_fills
        self.intent_sink = intent_sink
        self.enforce_model_cutoff = enforce_model_cutoff
        self.strict_receive_time = strict_receive_time
        self.retain_history = retain_history
        self.default_filters = default_filters or ExchangeFilters()

        self.infer_ns_cfg = int(model_infer_ms * 1_000_000) if model_infer_ms is not None else None
        self.rtt_ns_cfg = int(network_rtt_ms * 1_000_000)
        self.taker_fee_bps = taker_fee_bps

        self.allowed_products: set[tuple[Market, str]] = {
            (m, s) for m, s, _ in strategy.required_streams
        }
        self.max_pos_qty: Decimal | None = getattr(strategy, "_max_pos", None)
        self.allow_short: bool = bool(getattr(strategy, "_allow_short", False))
        self.model_bundle: Any = getattr(strategy, "_model", None)

        self.filters_by_key: dict[tuple[Market, str], ExchangeFilters] = {}
        self.bids_l2: dict[tuple[Market, str], dict[Decimal, Decimal]] = {}
        self.asks_l2: dict[tuple[Market, str], dict[Decimal, Decimal]] = {}
        self.bbo_books: dict[tuple[Market, str], tuple[tuple[Decimal, Decimal], tuple[Decimal, Decimal]]] = {}
        self.book_ver: dict[tuple[Market, str], int] = {}
        self.health_by_key: dict[tuple[Market, str], bool] = {}
        self.health_reason_by_key: dict[tuple[Market, str], str] = {}
        self.mark_px_by_key: dict[tuple[Market, str], Decimal] = {}
        self.last_mark_px = Decimal(0)
        self.settled_funding_keys: set[tuple[Market, str, int]] = set()

        self.pending: list[_PendingOrder] = []
        self._enqueue_seq = 0
        self.intents_count = 0
        self.intents: list[OrderIntent] = []
        self.ledger: list[LedgerEntry] = []
        self.feat_hist: list[FeatureVector] = []

        self.total_fees = Decimal(0)
        self.total_funding = Decimal(0)
        self.fills_count = 0
        self.skips_gap = 0
        self.blocked_count = 0
        self.foreign_events_ignored_count = 0
        self.skipped_missing_receive_time_count = 0
        self.last_ts_ns = 0

        # O(1) incremental mark-to-market drawdown state
        self.equity_peak = float(initial_cash_quote + initial_position_qty * self.last_mark_px)
        self.max_drawdown_ratio = 0.0

    def _update_drawdown(self, mark_px: Decimal | None = None) -> Decimal:
        px = mark_px if (mark_px is not None and mark_px > 0) else self.last_mark_px
        eq = self.cash + self.pos * px
        eq_f = float(eq)
        if eq_f > self.equity_peak:
            self.equity_peak = eq_f
        elif self.equity_peak > 0.0:
            dd = (self.equity_peak - eq_f) / self.equity_peak
            if dd > self.max_drawdown_ratio:
                self.max_drawdown_ratio = dd
        return eq

    def _current_book(self, key: tuple[Market, str]) -> OrderBookView:
        _, sym = key
        hv = self.health_by_key.get(key, True)
        ver = self.book_ver.get(key, 0)
        if key in self.bbo_books and key not in self.bids_l2:
            b, a = self.bbo_books[key]
            crossed = b[0] >= a[0]
            return OrderBookView(sym, ver, (b,), (a,), health_valid=hv, crossed=crossed)
        b_side, a_side = _sorted_book_sides(self.bids_l2.get(key, {}), self.asks_l2.get(key, {}))
        if (not b_side or not a_side) and key in self.bbo_books:
            b, a = self.bbo_books[key]
            b_side = b_side or (b,)
            a_side = a_side or (a,)
        crossed = bool(b_side and a_side and b_side[0][0] >= a_side[0][0])
        return OrderBookView(sym, ver, b_side, a_side, health_valid=hv, crossed=crossed)

    def _append_rejected_entry(
        self,
        intent: OrderIntent,
        send_ts_ns: int,
        arrival_ts_ns: int,
        status: Literal[
            "FILLED",
            "PARTIAL",
            "SKIPPED_GAP",
            "REJECTED_UNKNOWN_FILTERS",
            "REJECTED_MIN_NOTIONAL",
            "REJECTED_ZERO_QTY",
            "REJECTED_INSUFFICIENT_FUNDS",
            "REJECTED_POSITION_LIMIT",
            "UNFILLED_EMPTY",
        ],
        reason: str,
    ) -> tuple[OrderIntent, FillResult, LedgerEntry]:
        res = FillResult(
            status=status,
            filled_qty=Decimal(0),
            avg_fill_price=Decimal(0),
            notional_quote=Decimal(0),
            fee_quote=Decimal(0),
            fee_asset="USDT",
            levels_consumed=0,
            reason=reason,
        )
        eq = self._update_drawdown()
        entry = LedgerEntry(
            seq=intent.decision_seq,
            decision_ts_ns=intent.decision_ts_ns,
            send_ts_ns=send_ts_ns,
            arrival_ts_ns=arrival_ts_ns,
            intent_id=intent.intent_id,
            side=intent.side,
            requested_qty=str(intent.quantity),
            status=res.status,
            filled_qty="0",
            avg_fill_price="0",
            fee_quote="0",
            funding_delta_quote="0",
            cash_after=str(self.cash),
            position_after=str(self.pos),
            realized_equity=str(eq),
        )
        if self.retain_history:
            self.ledger.append(entry)
        return (intent, res, entry)

    def _process_pending(self, up_to_ns: int) -> list[tuple[OrderIntent, FillResult, LedgerEntry]]:
        if not self.pending:
            return []
        ordered = sorted(self.pending, key=lambda o: (o.arrival_ts_ns, o.enqueue_seq))
        still_waiting: list[_PendingOrder] = []
        settled: list[tuple[OrderIntent, FillResult, LedgerEntry]] = []
        for p in ordered:
            if p.arrival_ts_ns > up_to_ns:
                still_waiting.append(p)
                continue
            # Release pre-flight reservations prior to settlement
            self.reserved_cash_quote -= p.reserved_cash_quote
            self.reserved_buy_qty -= p.reserved_buy_qty
            self.reserved_sell_qty -= p.reserved_sell_qty

            key = (p.intent.market, p.intent.symbol)
            book = self._current_book(key)
            flt = self.filters_by_key.get(key, self.default_filters)
            max_budget: Decimal | None = None
            if p.intent.market == Market.SPOT and p.intent.side == "BUY":
                max_budget = max(Decimal(0), self.cash - self.reserved_cash_quote)

            res: FillResult = self.sim.execute(
                p.intent, book, flt, max_buy_notional_plus_fee=max_budget
            )
            if res.status in ("FILLED", "PARTIAL"):
                self.fills_count += 1
                self.total_fees += res.fee_quote
                if p.intent.side == "BUY":
                    self.pos += res.filled_qty
                    self.cash -= res.notional_quote + res.fee_quote
                else:
                    self.pos -= res.filled_qty
                    self.cash += res.notional_quote - res.fee_quote
                self.last_trade_ts = p.arrival_ts_ns
            elif res.status == "SKIPPED_GAP":
                self.skips_gap += 1

            mark = self.mark_px_by_key.get(key) or self.last_mark_px
            if not mark and book.bids and book.asks:
                mark = (book.bids[0][0] + book.asks[0][0]) / Decimal(2)
            eq = self._update_drawdown(mark)
            entry = LedgerEntry(
                seq=p.intent.decision_seq,
                decision_ts_ns=p.intent.decision_ts_ns,
                send_ts_ns=p.send_ts_ns,
                arrival_ts_ns=p.arrival_ts_ns,
                intent_id=p.intent.intent_id,
                side=p.intent.side,
                requested_qty=str(p.intent.quantity),
                status=res.status,
                filled_qty=str(res.filled_qty),
                avg_fill_price=str(res.avg_fill_price),
                fee_quote=str(res.fee_quote),
                funding_delta_quote="0",
                cash_after=str(self.cash),
                position_after=str(self.pos),
                realized_equity=str(eq),
            )
            if self.retain_history:
                self.ledger.append(entry)
            settled.append((p.intent, res, entry))
        self.pending = still_waiting
        return settled

    def step(self, ev: FeedEvent) -> StepOutcome:
        is_health = isinstance(ev, StreamHealthEvent)
        if is_health:
            ts_ns: int | None = ev.observed_time_utc_ns
        else:
            ts_ns = _event_ts_ns(ev, strict_receive_time=self.strict_receive_time)
            if ts_ns is None:
                self.skipped_missing_receive_time_count += 1
                return StepOutcome(ev, None, True, "MISSING_RECEIVE_TIME", None, (), self.cash, self.pos)

        assert ts_ns is not None
        if ts_ns > self.last_ts_ns:
            self.last_ts_ns = ts_ns

        # 1. Strictly settle pending orders that arrive <= ts_ns BEFORE applying ev state updates
        settled_fills = self._process_pending(ts_ns)

        key = (ev.market, ev.symbol)
        if key not in self.allowed_products:
            self.foreign_events_ignored_count += 1
            return StepOutcome(ev, ts_ns, True, "FOREIGN_PRODUCT_IGNORED", None, tuple(settled_fills), self.cash, self.pos)

        # Enforce causal model cutoff against trained_until_ns
        if (
            self.enforce_model_cutoff
            and self.model_bundle is not None
            and not is_health
        ):
            cutoff = getattr(self.model_bundle.metadata, "trained_until_ns", None)
            if cutoff is not None and cutoff > 0 and ts_ns <= cutoff:
                raise ValueError(
                    f"Causal model leakage rejected: event ts_ns={ts_ns} <= model.trained_until_ns={cutoff}"
                )

        # 2. Apply event state changes strictly causally
        if isinstance(ev, StreamHealthEvent):
            self.feat_eng.on_health(self.feat_state, ev)
            if ev.status in ("GAP", "STALE", "DEGRADED", "CLOSED", "STOPPED"):
                self.health_by_key[key] = False
                self.health_reason_by_key[key] = f"{ev.status}:{ev.reason}"
            elif ev.status == "LIVE":
                self.health_by_key[key] = True
                self.health_reason_by_key[key] = "OK"
            hv = self.health_by_key.get(key, True)
            hr = None if hv else self.health_reason_by_key.get(key, ev.status)
            return StepOutcome(ev, ts_ns, hv, hr, None, tuple(settled_fills), self.cash, self.pos)

        if isinstance(ev, ReferenceObservationEvent):
            if ev.source_kind in ("exchange_info", "instrument") and ev.status == "VALID":
                if ev.tick_size is not None or ev.step_size is not None or ev.min_notional is not None:
                    self.filters_by_key[key] = ExchangeFilters(
                        tick_size=ev.tick_size,
                        step_size=ev.step_size,
                        min_notional=ev.min_notional,
                        is_known=True,
                    )
            elif (
                ev.source_kind == "funding_rate"
                and ev.market == Market.USDM
                and ev.funding_rate_settled is not None
                and self.pos != 0
            ):
                fkey = (ev.market, ev.symbol, ev.funding_time_ms)
                if ev.funding_time_ms is not None and fkey in self.settled_funding_keys:
                    pass
                else:
                    if ev.funding_time_ms is not None:
                        self.settled_funding_keys.add(fkey)
                    ref_px = self.mark_px_by_key.get(key) or self.last_mark_px
                    if ref_px <= 0 and key in self.bbo_books:
                        ref_px = (self.bbo_books[key][0][0] + self.bbo_books[key][1][0]) / Decimal(2)
                    funding_delta = -self.pos * ref_px * ev.funding_rate_settled
                    self.cash += funding_delta
                    self.total_funding += funding_delta
                    eq = self._update_drawdown(ref_px)
                    self.ledger.append(
                        LedgerEntry(
                            seq=ev.delivery_seq,
                            decision_ts_ns=ts_ns,
                            send_ts_ns=ts_ns,
                            arrival_ts_ns=ts_ns,
                            intent_id=f"funding-{ev.funding_time_ms}",
                            side="FUNDING",
                            requested_qty="0",
                            status="SETTLED",
                            filled_qty="0",
                            avg_fill_price=str(ref_px),
                            fee_quote="0",
                            funding_delta_quote=str(funding_delta),
                            cash_after=str(self.cash),
                            position_after=str(self.pos),
                            realized_equity=str(eq),
                        )
                    )
        elif isinstance(ev, BookTickerEvent):
            self.bbo_books[key] = (
                (ev.best_bid_price, ev.best_bid_quantity),
                (ev.best_ask_price, ev.best_ask_quantity),
            )
            self.book_ver[key] = self.book_ver.get(key, 0) + 1
            if self.health_reason_by_key.get(key, "").startswith("STALE:"):
                self.health_by_key[key] = True
                self.health_reason_by_key[key] = "OK"
            elif key not in self.health_by_key:
                self.health_by_key[key] = True
                self.health_reason_by_key[key] = "OK"
            mark = (ev.best_bid_price + ev.best_ask_price) / Decimal(2)
            self.mark_px_by_key[key] = mark
            self.last_mark_px = mark
            self._update_drawdown(mark)
        elif isinstance(ev, OrderBookSnapshotEvent):
            self.bids_l2[key] = {p: q for p, q in ev.bids if q > 0}
            self.asks_l2[key] = {p: q for p, q in ev.asks if q > 0}
            self.book_ver[key] = self.book_ver.get(key, 0) + 1
            self.health_by_key[key] = True
            self.health_reason_by_key[key] = "OK"
            if self.bids_l2[key] and self.asks_l2[key]:
                mark = (max(self.bids_l2[key]) + min(self.asks_l2[key])) / Decimal(2)
                self.mark_px_by_key[key] = mark
                self.last_mark_px = mark
                self._update_drawdown(mark)
        elif isinstance(ev, DepthUpdateEvent):
            bm = self.bids_l2.setdefault(key, {})
            am = self.asks_l2.setdefault(key, {})
            for p, q in ev.bids:
                bm.pop(p, None) if q == 0 else bm.__setitem__(p, q)
            for p, q in ev.asks:
                am.pop(p, None) if q == 0 else am.__setitem__(p, q)
            self.book_ver[key] = self.book_ver.get(key, 0) + 1
            if bm and am:
                mark = (max(bm) + min(am)) / Decimal(2)
                self.mark_px_by_key[key] = mark
                self.last_mark_px = mark
                self._update_drawdown(mark)
        elif isinstance(ev, MarkPriceEvent):
            self.mark_px_by_key[key] = ev.mark_price
            self.last_mark_px = ev.mark_price
            self._update_drawdown(ev.mark_price)

        # 3. Feature extraction & causal strategy decision
        vec = self.feat_eng.update(self.feat_state, ev)
        hv = self.health_by_key.get(key, True)
        blocked_reason: str | None = None
        emitted_intent: OrderIntent | None = None

        if vec is not None:
            if self.retain_history:
                self.feat_hist.append(vec)
            if not hv or not vec.health_valid:
                blocked_reason = self.health_reason_by_key.get(key, "FEATURE_HEALTH_INVALID") if not hv else "FEATURE_HEALTH_INVALID"
                self.blocked_count += 1
            else:
                acct = StrategyAccountView(
                    position_qty=self.pos,
                    cash_quote=max(Decimal(0), self.cash - self.reserved_cash_quote),
                    last_trade_ts_ns=self.last_trade_ts,
                    intent_counter=self.intents_count,
                    reserved_cash_quote=self.reserved_cash_quote,
                    reserved_buy_qty=self.reserved_buy_qty,
                    reserved_sell_qty=self.reserved_sell_qty,
                )
                emitted_intent = self.strategy.on_feature(vec, acct)

        if emitted_intent is not None:
            self.intents_count += 1
            if self.retain_history:
                self.intents.append(emitted_intent)
            infer_ns = self.infer_ns_cfg if self.infer_ns_cfg is not None else max(0, int(emitted_intent.decision_infer_ns))
            send_ns = emitted_intent.decision_ts_ns + infer_ns
            arrival_ns = send_ns + self.rtt_ns_cfg

            if not self.simulate_fills:
                if self.intent_sink is not None:
                    self.intent_sink(emitted_intent)
            else:
                # Admission check & pre-reservation BEFORE queueing
                flt = self.filters_by_key.get(key, self.default_filters)
                req_qty = flt.round_step_down(emitted_intent.quantity)
                res_cash = Decimal(0)
                res_buy = Decimal(0)
                res_sell = Decimal(0)
                rejected = False

                if req_qty <= 0:
                    settled_fills.append(
                        self._append_rejected_entry(emitted_intent, send_ns, arrival_ns, "REJECTED_ZERO_QTY", "below_step_size")
                    )
                    rejected = True
                elif emitted_intent.side == "BUY":
                    if self.max_pos_qty is not None and (self.pos + self.reserved_buy_qty + req_qty) > self.max_pos_qty:
                        settled_fills.append(
                            self._append_rejected_entry(emitted_intent, send_ns, arrival_ns, "REJECTED_POSITION_LIMIT", "max_pos_qty_exceeded")
                        )
                        rejected = True
                    elif emitted_intent.market == Market.SPOT:
                        book = self._current_book(key)
                        est_px = book.asks[0][0] if book.asks else self.mark_px_by_key.get(key, Decimal(0))
                        fee_mult = Decimal(1) + (self.taker_fee_bps / Decimal("10000"))
                        est_cost = req_qty * flt.round_tick(est_px) * fee_mult
                        free_cash = self.cash - self.reserved_cash_quote
                        if est_px <= 0 or est_cost > free_cash:
                            settled_fills.append(
                                self._append_rejected_entry(emitted_intent, send_ns, arrival_ns, "REJECTED_INSUFFICIENT_FUNDS", "insufficient_quote_cash")
                            )
                            rejected = True
                        else:
                            res_cash = est_cost
                            res_buy = req_qty
                    else:
                        res_buy = req_qty
                else:
                    if not self.allow_short and (self.pos - self.reserved_sell_qty - req_qty) < 0:
                        settled_fills.append(
                            self._append_rejected_entry(emitted_intent, send_ns, arrival_ns, "REJECTED_POSITION_LIMIT", "spot_short_disallowed")
                        )
                        rejected = True
                    elif self.max_pos_qty is not None and abs(self.pos - self.reserved_sell_qty - req_qty) > self.max_pos_qty:
                        settled_fills.append(
                            self._append_rejected_entry(emitted_intent, send_ns, arrival_ns, "REJECTED_POSITION_LIMIT", "max_pos_qty_exceeded")
                        )
                        rejected = True
                    else:
                        res_sell = req_qty

                if not rejected:
                    self.reserved_cash_quote += res_cash
                    self.reserved_buy_qty += res_buy
                    self.reserved_sell_qty += res_sell
                    self._enqueue_seq += 1
                    self.pending.append(
                        _PendingOrder(
                            intent=emitted_intent,
                            send_ts_ns=send_ns,
                            arrival_ts_ns=arrival_ns,
                            enqueue_seq=self._enqueue_seq,
                            reserved_cash_quote=res_cash,
                            reserved_buy_qty=res_buy,
                            reserved_sell_qty=res_sell,
                        )
                    )
                    if arrival_ns <= ts_ns:
                        settled_fills.extend(self._process_pending(ts_ns))

        return StepOutcome(
            event=ev,
            ts_ns=ts_ns,
            health_ok=hv,
            blocked_reason=blocked_reason,
            emitted_intent=emitted_intent,
            settled_fills=tuple(settled_fills),
            cash_after=self.cash,
            position_after=self.pos,
        )


class BacktestEngine:
    """Strictly causal event-loop backtester driving Strategy + CausalTradingCore."""

    def __init__(self, *, feature_engine: FeatureEngine | None = None) -> None:
        self._feat_eng = feature_engine

    def run(
        self,
        events: Iterable[FeedEvent],
        strategy: BaseStrategy,
        config: BacktestConfig,
        *,
        runs_root: Path | None = None,
    ) -> BacktestResult:
        feat_eng = self._feat_eng or FeatureEngine(strict_receive_time=config.strict_receive_time)
        core = CausalTradingCore(
            strategy,
            initial_cash_quote=config.initial_cash_quote,
            fill_mode=config.fill_mode,
            taker_fee_bps=config.taker_fee_bps,
            model_infer_ms=config.model_infer_ms,
            network_rtt_ms=config.network_rtt_ms,
            default_filters=config.default_filters,
            simulate_fills=True,
            enforce_model_cutoff=config.enforce_model_cutoff,
            strict_receive_time=config.strict_receive_time,
            feature_engine=feat_eng,
        )
        for ev in events:
            core.step(ev)

        final_eq = core.cash + core.pos * core.last_mark_px
        core._update_drawdown(core.last_mark_px)

        metrics = {
            "run_id": config.run_id,
            "strategy_id": strategy.strategy_id,
            "fill_mode": config.fill_mode,
            "model_infer_ms": config.model_infer_ms,
            "network_rtt_ms": config.network_rtt_ms,
            "taker_fee_bps": str(config.taker_fee_bps),
            "initial_cash_quote": str(config.initial_cash_quote),
            "final_cash_quote": str(core.cash),
            "final_position_qty": str(core.pos),
            "reserved_cash_at_end_quote": str(core.reserved_cash_quote),
            "final_equity_quote": str(final_eq),
            "valuation_basis": "mark_or_bbo_mid",
            "pnl_quote": str(final_eq - config.initial_cash_quote),
            "total_fees_quote": str(core.total_fees),
            "total_funding_quote": str(core.total_funding),
            "intents_count": len(core.intents),
            "fills_count": core.fills_count,
            "skips_gap_count": core.skips_gap,
            "pending_at_end_count": len(core.pending),
            "foreign_events_ignored_count": core.foreign_events_ignored_count,
            "skipped_missing_receive_time_count": core.skipped_missing_receive_time_count,
            "window_end_ts_ns": core.last_ts_ns,
            "max_drawdown_ratio": core.max_drawdown_ratio,
        }
        canonical_blob = json.dumps(
            {"metrics": metrics, "ledger": [asdict(x) for x in core.ledger]},
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical_blob.encode("utf-8")).hexdigest()
        metrics["semantic_digest"] = digest

        if runs_root is not None:
            run_dir = runs_root / config.run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "metrics.json").write_text(
                json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            (run_dir / "intents.jsonl").write_text(
                "".join(
                    json.dumps(asdict(i), default=str, sort_keys=True) + "\n"
                    for i in core.intents
                ),
                encoding="utf-8",
            )
            (run_dir / "ledger.jsonl").write_text(
                "".join(json.dumps(asdict(l), sort_keys=True) + "\n" for l in core.ledger),
                encoding="utf-8",
            )
        return BacktestResult(
            run_id=config.run_id,
            metrics=metrics,
            intents=core.intents,
            ledger=core.ledger,
            feature_history=core.feat_hist,
            semantic_digest=digest,
        )


def verify_causality_perturbation(
    events: Sequence[FeedEvent],
    strategy: BaseStrategy,
    split_index: int,
    *,
    config: BacktestConfig | None = None,
) -> bool:
    """Causality Perturbation Test: mutate/shuffle events[split_index+1:]; prefix [0..split_index] must be identical."""
    if split_index < 0 or split_index >= len(events) - 1:
        raise ValueError("split_index must allow a non-empty suffix to perturb")
    cfg = config or BacktestConfig(run_id="causality-check", network_rtt_ms=0.0)
    eng1 = BacktestEngine()
    res_orig = eng1.run(events, strategy, cfg)

    prefix = list(events[: split_index + 1])
    suffix = list(events[split_index + 1 :])
    # Mutate future prices dramatically AND reverse suffix order while preserving monotonic timestamps
    mutated_suffix: list[FeedEvent] = []
    for ev in reversed(suffix):
        d = event_to_dict(ev)
        if "best_bid_price" in d:
            d["best_bid_price"] = str(Decimal(d["best_bid_price"]) * Decimal("9.5"))
            d["best_ask_price"] = str(Decimal(d["best_ask_price"]) * Decimal("9.6"))
        if "price" in d:
            d["price"] = str(Decimal(d["price"]) * Decimal("9.5"))
        mutated_suffix.append(event_from_dict(d))

    cutoff_seq = getattr(prefix[-1], "delivery_seq", split_index + 1)
    cutoff_ts = max((f.timestamp_ns for f in res_orig.feature_history if f.delivery_seq <= cutoff_seq), default=0)

    eng2 = BacktestEngine()
    res_pert = eng2.run(prefix + mutated_suffix, strategy, cfg)

    f_orig = [f for f in res_orig.feature_history if f.delivery_seq <= cutoff_seq]
    f_pert = [f for f in res_pert.feature_history if f.delivery_seq <= cutoff_seq]
    if f_orig != f_pert:
        return False

    i_orig = [i for i in res_orig.intents if i.decision_seq <= cutoff_seq]
    i_pert = [i for i in res_pert.intents if i.decision_seq <= cutoff_seq]
    if i_orig != i_pert:
        return False

    l_orig = [l for l in res_orig.ledger if l.arrival_ts_ns <= cutoff_ts and l.seq <= cutoff_seq]
    l_pert = [l for l in res_pert.ledger if l.arrival_ts_ns <= cutoff_ts and l.seq <= cutoff_seq]
    return l_orig == l_pert


def run_sensitivity_matrix(
    events: Sequence[FeedEvent],
    strategy: BaseStrategy,
    *,
    rtt_ms_grid: Sequence[float] = (0.0, 2.0, 10.0),
    fee_bps_grid: Sequence[Decimal] = (Decimal("5"), Decimal("10"), Decimal("20")),
    base_config: BacktestConfig | None = None,
) -> list[dict[str, Any]]:
    """Evaluate strategy across latency x fee sensitivity scenarios."""
    base = base_config or BacktestConfig()
    rows: list[dict[str, Any]] = []
    eng = BacktestEngine()
    for rtt in rtt_ms_grid:
        for fee in fee_bps_grid:
            cfg = BacktestConfig(
                run_id=f"{base.run_id}-rtt{rtt}-fee{fee}",
                initial_cash_quote=base.initial_cash_quote,
                fill_mode=base.fill_mode,
                taker_fee_bps=fee,
                model_infer_ms=base.model_infer_ms,
                network_rtt_ms=rtt,
                default_filters=base.default_filters,
            )
            res = eng.run(events, strategy, cfg)
            rows.append(
                {
                    "network_rtt_ms": rtt,
                    "taker_fee_bps": str(fee),
                    "pnl_quote": res.metrics["pnl_quote"],
                    "fills_count": res.metrics["fills_count"],
                    "total_fees_quote": res.metrics["total_fees_quote"],
                    "max_drawdown_ratio": res.metrics["max_drawdown_ratio"],
                    "semantic_digest": res.semantic_digest,
                }
            )
    return rows
