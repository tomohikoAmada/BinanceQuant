"""Capability parity matrix and Recorder vs Gateway cross-collector parity tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import numpy as np

from .events import FeedEvent, Market, Stream, StreamHealthEvent, event_identity
from .live import CapabilityMismatchError


@dataclass(frozen=True, slots=True)
class CapabilitySpec:
    market: Market
    stream: Stream
    research_available: bool
    live_gateway_available: bool
    status: str  # "SUPPORTED", "FROZEN_D2", "RESEARCH_ONLY"
    notes: str = ""


LIVE_CAPABILITY_MATRIX: dict[tuple[Market, Stream], CapabilitySpec] = {
    (Market.SPOT, Stream.BOOK_TICKER): CapabilitySpec(Market.SPOT, Stream.BOOK_TICKER, True, True, "SUPPORTED", "Spot BBO omits exchange E/T; rely on receive_time_utc_ns & update_id"),
    (Market.SPOT, Stream.AGG_TRADE): CapabilitySpec(Market.SPOT, Stream.AGG_TRADE, True, True, "SUPPORTED"),
    (Market.SPOT, Stream.DIFF_DEPTH): CapabilitySpec(Market.SPOT, Stream.DIFF_DEPTH, True, True, "SUPPORTED"),
    (Market.SPOT, Stream.ORDERBOOK): CapabilitySpec(Market.SPOT, Stream.ORDERBOOK, True, True, "SUPPORTED"),
    (Market.SPOT, Stream.REFERENCE_DATA): CapabilitySpec(Market.SPOT, Stream.REFERENCE_DATA, True, True, "SUPPORTED", "Spot exchangeInfo filters"),
    (Market.USDM, Stream.BOOK_TICKER): CapabilitySpec(Market.USDM, Stream.BOOK_TICKER, True, True, "SUPPORTED"),
    (Market.USDM, Stream.AGG_TRADE): CapabilitySpec(Market.USDM, Stream.AGG_TRADE, True, True, "SUPPORTED", "Optional nq field"),
    (Market.USDM, Stream.DIFF_DEPTH): CapabilitySpec(Market.USDM, Stream.DIFF_DEPTH, True, True, "SUPPORTED"),
    (Market.USDM, Stream.ORDERBOOK): CapabilitySpec(Market.USDM, Stream.ORDERBOOK, True, True, "SUPPORTED"),
    (Market.USDM, Stream.MARK_PRICE): CapabilitySpec(Market.USDM, Stream.MARK_PRICE, True, True, "SUPPORTED"),
    (Market.USDM, Stream.LIQUIDATION): CapabilitySpec(Market.USDM, Stream.LIQUIDATION, True, True, "SUPPORTED", "1000ms throttled forceOrder sample"),
    (Market.USDM, Stream.REFERENCE_DATA): CapabilitySpec(Market.USDM, Stream.REFERENCE_DATA, True, True, "SUPPORTED"),
    # D2: COIN-M and Options frozen for real-time / new recording
    (Market.COINM, Stream.BOOK_TICKER): CapabilitySpec(Market.COINM, Stream.BOOK_TICKER, True, False, "FROZEN_D2", "Research-only per D2; no live Gateway"),
    (Market.OPTIONS, Stream.BOOK_TICKER): CapabilitySpec(Market.OPTIONS, Stream.BOOK_TICKER, True, False, "FROZEN_D2", "Research-only per D2; no live Gateway"),
}


def check_strategy_capabilities(
    required_streams: Iterable[tuple[Market, str, Stream]],
    *,
    mode: str = "paper",
) -> None:
    """Reject strategies requiring offline-only or D2-frozen sources before live/paper execution."""
    for market, symbol, stream in required_streams:
        spec = LIVE_CAPABILITY_MATRIX.get((market, stream))
        if spec is None:
            raise CapabilityMismatchError(
                f"Unsupported capability pair ({market.value}, {stream.value}) for symbol {symbol}"
            )
        if mode in ("paper", "live") and not spec.live_gateway_available:
            raise CapabilityMismatchError(
                f"Strategy input ({market.value}, {symbol}, {stream.value}) has status={spec.status}; cannot run in {mode} mode"
            )


@dataclass(slots=True)
class FieldMismatch:
    identity: tuple[str, str, str, str | None]
    field: str
    recorder_value: str
    gateway_value: str


@dataclass(slots=True)
class ParityReport:
    recorder_total: int
    gateway_total: int
    matched_count: int
    recorder_only_count: int
    gateway_only_count: int
    recorder_coverage: float | None
    gateway_coverage: float | None
    field_mismatches: list[FieldMismatch] = field(default_factory=list)
    latency_p50_ms: float | None = None
    latency_p95_ms: float | None = None
    latency_p99_ms: float | None = None
    recorder_gaps: int = 0
    gateway_gaps: int = 0
    window_start_ns: int | None = None
    window_end_ns: int | None = None
    recorder_duplicate_count: int = 0
    gateway_duplicate_count: int = 0
    status: str = "PASSED"  # PASSED | NO_DATA | INSUFFICIENT | MISMATCH


def _compare_common_fields(
    ident: tuple[str, str, str, str | None], rec: FeedEvent, gw: FeedEvent
) -> list[FieldMismatch]:
    diffs: list[FieldMismatch] = []
    for attr in (
        "best_bid_price",
        "best_bid_quantity",
        "best_ask_price",
        "best_ask_quantity",
        "price",
        "quantity",
        "buyer_is_maker",
        "mark_price",
        "funding_rate",
        "side",
    ):
        if hasattr(rec, attr) and hasattr(gw, attr):
            rv = getattr(rec, attr)
            gv = getattr(gw, attr)
            if rv != gv:
                diffs.append(FieldMismatch(ident, attr, str(rv), str(gv)))
    return diffs


def compare_overlapping_windows(
    recorder_events: Iterable[FeedEvent],
    gateway_events: Iterable[FeedEvent],
    *,
    window_start_ns: int | None = None,
    window_end_ns: int | None = None,
) -> ParityReport:
    """Cross-compare independent Recorder and Gateway streams over overlapping time window."""
    rec_list = list(recorder_events)
    gw_list = list(gateway_events)

    rec_gaps = sum(1 for e in rec_list if isinstance(e, StreamHealthEvent) and e.status in ("GAP", "STALE"))
    gw_gaps = sum(1 for e in gw_list if isinstance(e, StreamHealthEvent) and e.status in ("GAP", "STALE"))

    rec_market = [e for e in rec_list if not isinstance(e, StreamHealthEvent)]
    gw_market = [e for e in gw_list if not isinstance(e, StreamHealthEvent)]

    rec_times = [e.receive_time_utc_ns for e in rec_market if e.receive_time_utc_ns is not None]
    gw_times = [e.receive_time_utc_ns for e in gw_market if e.receive_time_utc_ns is not None]

    explicit_window = window_start_ns is not None or window_end_ns is not None
    win_start = window_start_ns
    win_end = window_end_ns
    if (win_start is None or win_end is None) and rec_times and gw_times:
        auto_start = max(min(rec_times), min(gw_times))
        auto_end = min(max(rec_times), max(gw_times))
        win_start = auto_start if win_start is None else win_start
        win_end = auto_end if win_end is None else win_end

    def _in_window(ev: FeedEvent) -> bool:
        t = getattr(ev, "receive_time_utc_ns", None)
        if t is None:
            return not explicit_window
        if win_start is not None and t < win_start:
            return False
        if win_end is not None and t > win_end:
            return False
        return True

    no_overlap = (
        not rec_market
        or not gw_market
        or (win_start is not None and win_end is not None and win_start > win_end)
    )

    rec_all: dict[tuple[str, str, str, str | None], FeedEvent] = {}
    rec_dups = 0
    for ev in rec_market:
        ident = event_identity(ev)
        if ident[3] is None:
            continue
        if explicit_window and not _in_window(ev) and ident in rec_all:
            continue
        if ident in rec_all:
            rec_dups += 1
        else:
            rec_all[ident] = ev

    gw_all: dict[tuple[str, str, str, str | None], FeedEvent] = {}
    gw_dups = 0
    for ev in gw_market:
        ident = event_identity(ev)
        if ident[3] is None:
            continue
        if ident in gw_all:
            gw_dups += 1
        else:
            gw_all[ident] = ev

    if no_overlap:
        rec_only_n = sum(1 for v in rec_all.values() if _in_window(v)) if (win_start is not None and win_end is not None and win_start <= win_end) else len(rec_all)
        gw_only_n = sum(1 for v in gw_all.values() if _in_window(v)) if (win_start is not None and win_end is not None and win_start <= win_end) else len(gw_all)
        st = "NO_DATA" if (not rec_market and not gw_market) else "INSUFFICIENT"
        return ParityReport(
            recorder_total=rec_only_n,
            gateway_total=gw_only_n,
            matched_count=0,
            recorder_only_count=rec_only_n,
            gateway_only_count=gw_only_n,
            recorder_coverage=None,
            gateway_coverage=None,
            field_mismatches=[],
            recorder_gaps=rec_gaps,
            gateway_gaps=gw_gaps,
            window_start_ns=win_start,
            window_end_ns=win_end,
            recorder_duplicate_count=rec_dups,
            gateway_duplicate_count=gw_dups,
            status=st,
        )

    # Matched identities survive single-sided edge arrival clock jitter;
    # under an explicit window, ignore pairs where BOTH sides lie outside the window.
    common_keys = {
        k
        for k in (set(rec_all) & set(gw_all))
        if (not explicit_window) or _in_window(rec_all[k]) or _in_window(gw_all[k])
    }
    rec_only_keys = {k for k, v in rec_all.items() if k not in common_keys and _in_window(v)}
    gw_only_keys = {k for k, v in gw_all.items() if k not in common_keys and _in_window(v)}

    mismatches: list[FieldMismatch] = []
    latencies_ms: list[float] = []
    for key in sorted(common_keys):
        r_ev = rec_all[key]
        g_ev = gw_all[key]
        mismatches.extend(_compare_common_fields(key, r_ev, g_ev))
        r_ns = getattr(r_ev, "receive_time_utc_ns", None)
        g_ns = getattr(g_ev, "receive_time_utc_ns", None)
        if r_ns is not None and g_ns is not None:
            latencies_ms.append((g_ns - r_ns) / 1_000_000.0)

    matched = len(common_keys)
    rec_n = matched + len(rec_only_keys)
    gw_n = matched + len(gw_only_keys)
    p50 = p95 = p99 = None
    if latencies_ms:
        arr = np.asarray(latencies_ms, dtype=np.float64)
        p50 = float(np.percentile(arr, 50))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))

    rec_cov = (matched / rec_n) if rec_n > 0 else None
    gw_cov = (matched / gw_n) if gw_n > 0 else None
    if rec_n == 0 and gw_n == 0:
        status = "NO_DATA"
    elif matched == 0 or rec_cov is None or gw_cov is None:
        status = "INSUFFICIENT"
    elif mismatches or rec_dups > 0 or gw_dups > 0:
        status = "MISMATCH"
    else:
        status = "PASSED"

    return ParityReport(
        recorder_total=rec_n,
        gateway_total=gw_n,
        matched_count=matched,
        recorder_only_count=len(rec_only_keys),
        gateway_only_count=len(gw_only_keys),
        recorder_coverage=rec_cov,
        gateway_coverage=gw_cov,
        field_mismatches=mismatches,
        latency_p50_ms=p50,
        latency_p95_ms=p95,
        latency_p99_ms=p99,
        recorder_gaps=rec_gaps,
        gateway_gaps=gw_gaps,
        window_start_ns=win_start,
        window_end_ns=win_end,
        recorder_duplicate_count=rec_dups,
        gateway_duplicate_count=gw_dups,
        status=status,
    )
