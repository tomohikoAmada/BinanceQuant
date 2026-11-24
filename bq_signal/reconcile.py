"""Two-stage reconciliation: (1) Exact paper journal replay, (2) Independent Recorder cross-check."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from bq_feed.events import FeedEvent, StreamHealthEvent, event_from_dict
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
    """Execute Stage 1 exact journal replay & Stage 2 independent Recorder UID cross-reconciliation."""
    raw_lines = [
        json.loads(line)
        for line in journal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    mismatches: list[str] = []
    if not raw_lines:
        mismatches.append("empty_journal: 0 entries")
        recorded_events: list[FeedEvent] = []
    else:
        recorded_events = [event_from_dict(row["event"]) for row in raw_lines]

        # Validate contiguous monotonic sequence numbers before replay
        for expected_seq, row in enumerate(raw_lines, start=1):
            if int(row.get("journal_seq", -1)) != expected_seq:
                mismatches.append(
                    f"sequence discontinuity at index {expected_seq}: got journal_seq={row.get('journal_seq')}"
                )
                break

        with TemporaryDirectory() as tmpdir:
            replay_journal = Path(tmpdir) / "replay.jsonl"
            runner = SignalRunner(config, mode="paper")
            runner.run_events(recorded_events, journal_path=replay_journal)
            replay_lines = [
                json.loads(line)
                for line in replay_journal.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        if len(replay_lines) != len(raw_lines):
            mismatches.append(
                f"entry count mismatch: orig={len(raw_lines)} vs replay={len(replay_lines)}"
            )
        else:
            for orig, rep in zip(raw_lines, replay_lines):
                seq = orig["journal_seq"]
                if orig.get("config_hash") != config.config_hash:
                    mismatches.append(f"seq {seq}: config_hash mismatch")
                    break
                if orig.get("strategy_id") != rep.get("strategy_id"):
                    mismatches.append(f"seq {seq}: strategy_id mismatch")
                if orig.get("mode") != rep.get("mode"):
                    mismatches.append(f"seq {seq}: mode mismatch")
                if bool(orig.get("health_ok", True)) != bool(rep.get("health_ok", True)):
                    mismatches.append(f"seq {seq}: health_ok mismatch")
                if orig.get("blocked_reason") != rep.get("blocked_reason"):
                    mismatches.append(f"seq {seq}: blocked_reason mismatch")
                if orig.get("intent") != rep.get("intent"):
                    mismatches.append(
                        f"seq {seq}: intent mismatch {orig.get('intent')} != {rep.get('intent')}"
                    )
                if "settled_fills" in orig and "settled_fills" in rep:
                    if orig["settled_fills"] != rep["settled_fills"]:
                        mismatches.append(f"seq {seq}: settled_fills mismatch")
                elif orig.get("fill") != rep.get("fill"):
                    mismatches.append(
                        f"seq {seq}: fill mismatch {orig.get('fill')} != {rep.get('fill')}"
                    )
                if orig.get("cash_after") != rep.get("cash_after") or orig.get("position_after") != rep.get("position_after"):
                    mismatches.append(f"seq {seq}: ledger mismatch")

    stage1_ok = bool(raw_lines) and len(mismatches) == 0

    # Stage 2: Independent Recorder cross-reconciliation
    parity: ParityReport | None = None
    gap_attr: dict[str, int] = {}
    if recorder_root is not None:
        ts_vals = [
            ev.receive_time_utc_ns
            for ev in recorded_events
            if not isinstance(ev, StreamHealthEvent) and ev.receive_time_utc_ns is not None
        ]
        pad_ns = 10_000_000_000  # +/-10s clock/chunk overlap pad
        min_ns = ((window_start_ns if window_start_ns is not None else min(ts_vals)) - pad_ns) if (ts_vals or window_start_ns is not None) else None
        max_ns = ((window_end_ns if window_end_ns is not None else max(ts_vals)) + pad_ns) if (ts_vals or window_end_ns is not None) else None
        mkt_filter = {ev.market for ev in recorded_events if not isinstance(ev, StreamHealthEvent)} or {config.market}
        sym_filter = {ev.symbol for ev in recorded_events if not isinstance(ev, StreamHealthEvent)} or {config.symbol}
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
        paper_gaps = sum(
            1 for e in recorded_events if isinstance(e, StreamHealthEvent) and e.status in ("GAP", "STALE")
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
        stage1_entries_checked=len(raw_lines),
        stage1_mismatches=mismatches,
        stage2_parity=parity,
        gap_attribution=gap_attr,
        accepted=accepted,
    )
