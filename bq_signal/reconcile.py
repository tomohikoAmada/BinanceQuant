"""Two-stage reconciliation: (1) Exact paper journal replay, (2) Independent Recorder cross-check."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
import json
from pathlib import Path

from bq_feed.events import FeedEvent, Market, StreamHealthEvent, event_from_dict
from bq_feed.parity import ParityReport, compare_overlapping_windows
from bq_feed.replay import RecorderReplayFeed

from .config import DeployConfig
from .runner import SignalRunner


@dataclass(slots=True)
class ReconcileReport:
    stage1_exact_replay_passed: bool
    stage1_entries_checked: int
    stage1_mismatches: list[str] = field(default_factory=list)
    stage2_parity: ParityReport | None = None
    gap_attribution: dict[str, int] = field(default_factory=dict)
    accepted: bool = False


def reconcile_paper_journal(
    journal_path: Path,
    config: DeployConfig,
    *,
    recorder_events: Iterable[FeedEvent] | None = None,
    recorder_root: Path | None = None,
    window_start_ns: int | None = None,
    window_end_ns: int | None = None,
) -> ReconcileReport:
    """Execute Stage 1 streaming exact journal replay & Stage 2 independent Recorder UID cross-reconciliation."""
    mismatches: list[str] = []
    entries_checked = 0
    runner = SignalRunner(config, mode="paper")
    need_stage2 = recorder_root is not None or recorder_events is not None
    recorded_events: list[FeedEvent] = []
    seen_markets: set[Market] = set()
    seen_symbols: set[str] = set()
    min_recv_ns: int | None = None
    max_recv_ns: int | None = None
    paper_gaps = 0
    seq_broken = False

    with journal_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            orig = json.loads(line)
            entries_checked += 1
            ev = event_from_dict(orig["event"])

            if not seq_broken and int(orig.get("journal_seq", -1)) != entries_checked:
                mismatches.append(
                    f"sequence discontinuity at index {entries_checked}: got journal_seq={orig.get('journal_seq')}"
                )
                seq_broken = True

            rep = asdict(runner._build_entry(ev))
            seq = orig.get("journal_seq")
            if orig.get("config_hash") != config.config_hash:
                mismatches.append(f"seq {seq}: config_hash mismatch")
            elif orig.get("strategy_id") != rep.get("strategy_id"):
                mismatches.append(f"seq {seq}: strategy_id mismatch")
            elif orig.get("mode") != rep.get("mode"):
                mismatches.append(f"seq {seq}: mode mismatch")
            elif bool(orig.get("health_ok", True)) != bool(rep.get("health_ok", True)):
                mismatches.append(f"seq {seq}: health_ok mismatch")
            elif orig.get("blocked_reason") != rep.get("blocked_reason"):
                mismatches.append(f"seq {seq}: blocked_reason mismatch")
            elif orig.get("intent") != rep.get("intent"):
                mismatches.append(f"seq {seq}: intent mismatch {orig.get('intent')} != {rep.get('intent')}")
            elif "settled_fills" in orig and orig["settled_fills"] != rep["settled_fills"]:
                mismatches.append(f"seq {seq}: settled_fills mismatch")
            elif "settled_fills" not in orig and orig.get("fill") != rep.get("fill"):
                mismatches.append(f"seq {seq}: fill mismatch {orig.get('fill')} != {rep.get('fill')}")
            elif orig.get("cash_after") != rep.get("cash_after") or orig.get("position_after") != rep.get("position_after"):
                mismatches.append(f"seq {seq}: ledger mismatch")

            if need_stage2:
                if isinstance(ev, StreamHealthEvent):
                    ev_ts = ev.observed_time_utc_ns
                else:
                    ev_ts = ev.receive_time_utc_ns
                    seen_markets.add(ev.market)
                    seen_symbols.add(ev.symbol)
                    if ev_ts is not None:
                        min_recv_ns = ev_ts if min_recv_ns is None else min(min_recv_ns, ev_ts)
                        max_recv_ns = ev_ts if max_recv_ns is None else max(max_recv_ns, ev_ts)
                in_win = True
                if ev_ts is not None:
                    if window_start_ns is not None and ev_ts < window_start_ns:
                        in_win = False
                    if window_end_ns is not None and ev_ts > window_end_ns:
                        in_win = False
                if in_win:
                    recorded_events.append(ev)
                    if isinstance(ev, StreamHealthEvent) and ev.status in ("GAP", "STALE"):
                        paper_gaps += 1

    if entries_checked == 0:
        mismatches.append("empty_journal: 0 entries")

    stage1_ok = entries_checked > 0 and len(mismatches) == 0

    # Stage 2: Independent Recorder cross-reconciliation
    parity: ParityReport | None = None
    gap_attr: dict[str, int] = {}
    if recorder_root is not None:
        pad_ns = 10_000_000_000  # +/-10s clock/chunk overlap pad
        eff_min = window_start_ns if window_start_ns is not None else min_recv_ns
        eff_max = window_end_ns if window_end_ns is not None else max_recv_ns
        min_ns = (eff_min - pad_ns) if eff_min is not None else None
        max_ns = (eff_max + pad_ns) if eff_max is not None else None
        mkt_filter = seen_markets or {config.market}
        sym_filter = seen_symbols or {config.symbol}
        rec_stream = list(
            RecorderReplayFeed.from_recorder_root(
                recorder_root,
                market_filter=mkt_filter,
                symbol_filter=sym_filter,
                stream_filter={config.stream},
                min_utc_ns=min_ns,
                max_utc_ns=max_ns,
            ).iter_events()
        )
    elif recorder_events is not None:
        rec_stream = list(recorder_events)
    else:
        rec_stream = None

    if rec_stream is not None:
        parity = compare_overlapping_windows(
            rec_stream,
            recorded_events,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        gap_attr = {
            "paper_health_gaps": paper_gaps,
            "recorder_health_gaps": parity.recorder_gaps,
            "recorder_only_missing_in_paper": parity.recorder_only_count,
            "paper_only_missing_in_recorder": parity.gateway_only_count,
        }

    accepted = stage1_ok and (parity is None or parity.status == "PASSED")
    return ReconcileReport(
        stage1_exact_replay_passed=stage1_ok,
        stage1_entries_checked=entries_checked,
        stage1_mismatches=mismatches,
        stage2_parity=parity,
        gap_attribution=gap_attr,
        accepted=accepted,
    )
