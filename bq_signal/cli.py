"""CLI entrypoints for bq_signal run and reconcile_paper."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
from typing import Literal

from bq_feed.live import GatewayLiveFeed

from .config import load_deploy_config
from .reconcile import reconcile_paper_journal
from .runner import SignalRunner, build_strategy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bq-signal")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run paper or live signal consumer against BMDG")
    p_run.add_argument("--config", type=Path, required=True)
    p_run.add_argument("--mode", choices=["paper", "live"], default="paper")
    p_run.add_argument("--max-events", type=int, default=None)
    p_run.add_argument(
        "--allow-no-sink",
        action="store_true",
        help="Explicit diagnostic override allowing --mode live without an execution sink.",
    )

    p_rec = sub.add_parser("reconcile", help="Reconcile paper_journal.jsonl (Stage 1 replay + Stage 2 Recorder)")
    p_rec.add_argument("--config", type=Path, required=True)
    p_rec.add_argument("--journal", type=Path, required=True)
    p_rec.add_argument("--recorder-root", type=Path, default=None)

    args = parser.parse_args(argv)
    cfg = load_deploy_config(args.config)

    if args.cmd == "run":
        mode: Literal["paper", "live"] = args.mode
        live = GatewayLiveFeed(target=cfg.gateway_target, stale_threshold_ns=cfg.stale_threshold_ns)
        try:
            runner = SignalRunner(cfg, mode=mode, live_feed=live, diagnostic=bool(args.allow_no_sink))
            stream_iter = live.subscribe_event_stream(cfg.market, cfg.symbol, cfg.stream, max_items=args.max_events)
            summary = runner.run_events(stream_iter)
            print(json.dumps(asdict(summary), indent=2, sort_keys=True))
        finally:
            live.close()
        return 0

    if args.cmd == "reconcile":
        report = reconcile_paper_journal(args.journal, cfg, recorder_root=args.recorder_root)
        print(json.dumps(asdict(report), indent=2, default=str, sort_keys=True))
        return 0 if report.accepted else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
