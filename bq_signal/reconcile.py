"""Two-stage reconciliation: (1) Exact paper journal replay, (2) Independent Recorder cross-check."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
import json
from tempfile import TemporaryDirectory
from typing import Any

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


def reconcile_paper_journal(
    journal_path: Path,
    config: DeployConfig,
    *,
    recorder_events: Iterable[FeedEvent] | None = None,
    recorder_root: Path | None = None,
) -> ReconcileReport:
    """Execute Stage 1 exact journal replay & Stage 2 independent Recorder UID cross-reconciliation."""
    raw_lines = [
        json.loads(line)
        for line in journal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    recorded_events = [event_from_dict(row["event"]) for row in raw_lines]

    # Stage 1: Replay exact journal input stream and verify intents, fills, fees, cash, pos
    mismatches: list[str] = []
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
        mismatches.append(f"entry count mismatch: orig={len(raw_lines)} vs replay={len(replay_lines)}")
    else:
        for orig, rep in zip(raw_lines, replay_lines):
            if orig["config_hash"] != config.config_hash:
                mismatches.append(f"seq {orig['journal_seq']}: config_hash mismatch")
                break
            if orig["intent"] != rep["intent"]:
                mismatches.append(f"seq {orig['journal_seq']}: intent mismatch {orig['intent']} != {rep['intent']}")
            if orig["fill"] != rep["fill"]:
                mismatches.append(f"seq {orig['journal_seq']}: fill mismatch {orig['fill']} != {rep['fill']}")
            if orig["cash_after"] != rep["cash_after"] or orig["position_after"] != rep["position_after"]:
                mismatches.append(f"seq {orig['journal_seq']}: ledger mismatch")

    stage1_ok = len(mismatches) == 0

    # Stage 2: Independent Recorder cross-reconciliation
    parity: ParityReport | None = None
    gap_attr: dict[str, int] = {}
    if recorder_root is not None:
        rec_stream = list(RecorderReplayFeed.from_recorder_root(recorder_root).iter_events())
    elif recorder_events is not None:
        rec_stream = list(recorder_events)
    else:
        rec_stream = None

    if rec_stream is not None:
        parity = compare_overlapping_windows(rec_stream, recorded_events)
        paper_gaps = sum(1 for e in recorded_events if isinstance(e, StreamHealthEvent) and e.status in ("GAP", "STALE"))
        gap_attr = {
            "paper_health_gaps": paper_gaps,
            "recorder_health_gaps": parity.recorder_gaps,
            "recorder_only_missing_in_paper": parity.recorder_only_count,
            "paper_only_missing_in_recorder": parity.gateway_only_count,
        }

    return ReconcileReport(
        stage1_exact_replay_passed=stage1_ok,
        stage1_entries_checked=len(raw_lines),
        stage1_mismatches=mismatches,
        stage2_parity=parity,
        gap_attribution=gap_attr,
    )
