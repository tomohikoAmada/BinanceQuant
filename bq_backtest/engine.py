"""Causal event-driven backtest engine, sensitivity matrix, causality test, and run persistence."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

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
    model_infer_ms: float = 0.0
    network_rtt_ms: float = 0.0
    default_filters: ExchangeFilters = field(default_factory=ExchangeFilters)


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


def _sorted_book_sides(
    bids_map: dict[Decimal, Decimal], asks_map: dict[Decimal, Decimal]
) -> tuple[tuple[tuple[Decimal, Decimal], ...], tuple[tuple[Decimal, Decimal], ...]]:
    bids = tuple(sorted(((p, q) for p, q in bids_map.items() if q > 0), key=lambda x: x[0], reverse=True))
    asks = tuple(sorted(((p, q) for p, q in asks_map.items() if q > 0), key=lambda x: x[0]))
    return bids, asks


class BacktestEngine:
    """Strictly causal event-loop backtester driving Strategy + F1/F2 FillSimulator."""

    def __init__(self, *, feature_engine: FeatureEngine | None = None) -> None:
        self._feat_eng = feature_engine or FeatureEngine()

    def run(
        self,
        events: Iterable[FeedEvent],
        strategy: BaseStrategy,
        config: BacktestConfig,
        *,
        runs_root: Path | None = None,
    ) -> BacktestResult:
        feat_state = FeatureState()
        sim = FillSimulator(mode=config.fill_mode, taker_fee_bps=config.taker_fee_bps)

        cash = config.initial_cash_quote
        pos = Decimal(0)
        last_trade_ts = 0
        last_mark_px = Decimal(0)
        filters_by_sym: dict[str, ExchangeFilters] = {}
        bids_l2: dict[str, dict[Decimal, Decimal]] = {}
        asks_l2: dict[str, dict[Decimal, Decimal]] = {}
        bbo_books: dict[str, tuple[tuple[Decimal, Decimal], tuple[Decimal, Decimal]]] = {}
        book_ver: dict[str, int] = {}
        health_by_sym: dict[str, bool] = {}

        pending: list[_PendingOrder] = []
        intents: list[OrderIntent] = []
        ledger: list[LedgerEntry] = []
        feat_hist: list[FeatureVector] = []
        equity_curve: list[float] = []

        infer_ns_cfg = int(config.model_infer_ms * 1_000_000)
        rtt_ns_cfg = int(config.network_rtt_ms * 1_000_000)
        total_fees = Decimal(0)
        total_funding = Decimal(0)
        fills_count = 0
        skips_gap = 0

        def _current_book(sym: str) -> OrderBookView:
            hv = health_by_sym.get(sym, True)
            ver = book_ver.get(sym, 0)
            if sym in bbo_books and sym not in bids_l2:
                b, a = bbo_books[sym]
                crossed = b[0] >= a[0]
                return OrderBookView(sym, ver, (b,), (a,), health_valid=hv, crossed=crossed)
            b_side, a_side = _sorted_book_sides(bids_l2.get(sym, {}), asks_l2.get(sym, {}))
            if (not b_side or not a_side) and sym in bbo_books:
                b, a = bbo_books[sym]
                b_side = b_side or (b,)
                a_side = a_side or (a,)
            crossed = bool(b_side and a_side and b_side[0][0] >= a_side[0][0])
            return OrderBookView(sym, ver, b_side, a_side, health_valid=hv, crossed=crossed)

        def _process_pending(up_to_ns: int) -> None:
            nonlocal cash, pos, total_fees, fills_count, skips_gap, last_trade_ts
            still_waiting: list[_PendingOrder] = []
            for p in pending:
                if p.arrival_ts_ns > up_to_ns:
                    still_waiting.append(p)
                    continue
                book = _current_book(p.intent.symbol)
                flt = filters_by_sym.get(p.intent.symbol, config.default_filters)
                res: FillResult = sim.execute(p.intent, book, flt)
                if res.status in ("FILLED", "PARTIAL"):
                    fills_count += 1
                    total_fees += res.fee_quote
                    if p.intent.side == "BUY":
                        pos += res.filled_qty
                        cash -= res.notional_quote + res.fee_quote
                    else:
                        pos -= res.filled_qty
                        cash += res.notional_quote - res.fee_quote
                    last_trade_ts = p.arrival_ts_ns
                elif res.status == "SKIPPED_GAP":
                    skips_gap += 1
                mark = last_mark_px if last_mark_px > 0 else (
                    book.bids[0][0] if book.bids else Decimal(0)
                )
                eq = cash + pos * mark
                equity_curve.append(float(eq))
                ledger.append(
                    LedgerEntry(
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
                        cash_after=str(cash),
                        position_after=str(pos),
                        realized_equity=str(eq),
                    )
                )
            pending[:] = still_waiting

        for ev in events:
            if isinstance(ev, StreamHealthEvent):
                self._feat_eng.on_health(feat_state, ev)
                if ev.status in ("GAP", "STALE", "DEGRADED", "CLOSED", "STOPPED"):
                    health_by_sym[ev.symbol] = False
                elif ev.status == "LIVE":
                    health_by_sym[ev.symbol] = True
                _process_pending(ev.observed_time_utc_ns)
                continue

            ts_ns = _event_ts_ns(ev)
            # First process any orders whose arrival time <= current market event ts
            _process_pending(ts_ns)

            # Update visible exchange filters / books / mark price strictly causally
            if isinstance(ev, ReferenceObservationEvent):
                if ev.source_kind == "exchange_info":
                    filters_by_sym[ev.symbol] = ExchangeFilters(
                        tick_size=ev.tick_size,
                        step_size=ev.step_size,
                        min_notional=ev.min_notional,
                        is_known=True,
                    )
                elif ev.source_kind == "funding_rate" and ev.funding_rate_settled is not None and pos != 0:
                    # USD-M settles at real fundingTime strictly based on current position & mark price
                    ref_px = last_mark_px
                    if ref_px <= 0 and ev.symbol in bbo_books:
                        ref_px = (bbo_books[ev.symbol][0][0] + bbo_books[ev.symbol][1][0]) / Decimal(2)
                    funding_delta = -pos * ref_px * ev.funding_rate_settled
                    cash += funding_delta
                    total_funding += funding_delta
                    eq = cash + pos * ref_px
                    equity_curve.append(float(eq))
                    ledger.append(
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
                            cash_after=str(cash),
                            position_after=str(pos),
                            realized_equity=str(eq),
                        )
                    )
            elif isinstance(ev, BookTickerEvent):
                bbo_books[ev.symbol] = (
                    (ev.best_bid_price, ev.best_bid_quantity),
                    (ev.best_ask_price, ev.best_ask_quantity),
                )
                book_ver[ev.symbol] = book_ver.get(ev.symbol, 0) + 1
                health_by_sym[ev.symbol] = True
                last_mark_px = (ev.best_bid_price + ev.best_ask_price) / Decimal(2)
            elif isinstance(ev, OrderBookSnapshotEvent):
                bids_l2[ev.symbol] = {p: q for p, q in ev.bids if q > 0}
                asks_l2[ev.symbol] = {p: q for p, q in ev.asks if q > 0}
                book_ver[ev.symbol] = book_ver.get(ev.symbol, 0) + 1
                health_by_sym[ev.symbol] = True
            elif isinstance(ev, DepthUpdateEvent):
                bm = bids_l2.setdefault(ev.symbol, {})
                am = asks_l2.setdefault(ev.symbol, {})
                for p, q in ev.bids:
                    bm.pop(p, None) if q == 0 else bm.__setitem__(p, q)
                for p, q in ev.asks:
                    am.pop(p, None) if q == 0 else am.__setitem__(p, q)
                book_ver[ev.symbol] = book_ver.get(ev.symbol, 0) + 1
            elif isinstance(ev, MarkPriceEvent):
                last_mark_px = ev.mark_price

            vec = self._feat_eng.update(feat_state, ev)
            if vec is not None:
                feat_hist.append(vec)
                acct = StrategyAccountView(
                    position_qty=pos,
                    cash_quote=cash,
                    last_trade_ts_ns=last_trade_ts,
                    intent_counter=len(intents),
                )
                intent = strategy.on_feature(vec, acct)
                if intent is not None:
                    intents.append(intent)
                    infer_ns = infer_ns_cfg if config.model_infer_ms > 0 else intent.decision_infer_ns
                    send_ns = intent.decision_ts_ns + (infer_ns if config.model_infer_ms > 0 else 0)
                    arrival_ns = send_ns + rtt_ns_cfg
                    pending.append(_PendingOrder(intent, send_ns, arrival_ns))
                    if arrival_ns <= ts_ns:
                        _process_pending(ts_ns)

        # Flush any trailing pending orders at end of stream if rtt=0
        if pending and rtt_ns_cfg == 0:
            _process_pending(10**19)

        final_eq = cash + pos * last_mark_px
        max_dd = 0.0
        peak = float(config.initial_cash_quote)
        for eq in equity_curve:
            peak = max(peak, eq)
            if peak > 0:
                max_dd = max(max_dd, (peak - eq) / peak)

        metrics = {
            "run_id": config.run_id,
            "strategy_id": strategy.strategy_id,
            "fill_mode": config.fill_mode,
            "network_rtt_ms": config.network_rtt_ms,
            "taker_fee_bps": str(config.taker_fee_bps),
            "initial_cash_quote": str(config.initial_cash_quote),
            "final_cash_quote": str(cash),
            "final_position_qty": str(pos),
            "final_equity_quote": str(final_eq),
            "pnl_quote": str(final_eq - config.initial_cash_quote),
            "total_fees_quote": str(total_fees),
            "total_funding_quote": str(total_funding),
            "intents_count": len(intents),
            "fills_count": fills_count,
            "skips_gap_count": skips_gap,
            "max_drawdown_ratio": max_dd,
        }
        canonical_blob = json.dumps(
            {"metrics": metrics, "ledger": [asdict(x) for x in ledger]},
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
                    for i in intents
                ),
                encoding="utf-8",
            )
            (run_dir / "ledger.jsonl").write_text(
                "".join(json.dumps(asdict(l), sort_keys=True) + "\n" for l in ledger),
                encoding="utf-8",
            )
        return BacktestResult(
            run_id=config.run_id,
            metrics=metrics,
            intents=intents,
            ledger=ledger,
            feature_history=feat_hist,
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
