"""CLI entrypoints for bq_signal run and reconcile_paper."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from dataclasses import asdict
import json
from pathlib import Path
import signal
import threading
import time
from typing import Literal

from bq_feed.events import FeedEvent, StreamHealthEvent
from bq_feed.live import GatewayLiveFeed

from .config import load_deploy_config
from .reconcile import reconcile_paper_journal
from .runner import SignalRunner


def _stream_with_resilience(
    live: GatewayLiveFeed,
    *,
    market: object,
    symbol: str,
    stream: object,
    max_events: int | None,
    duration_seconds: float | None,
    reconnect: bool,
) -> Iterator[FeedEvent]:
    stop_ev = threading.Event()

    def _handle_sig(_signum: int, _frame: object) -> None:
        stop_ev.set()

    old_term = signal.signal(signal.SIGTERM, _handle_sig)
    old_int = signal.signal(signal.SIGINT, _handle_sig)
    deadline = (time.monotonic() + duration_seconds) if duration_seconds is not None else None
    yielded = 0
    backoff_s = 0.5
    try:
        while not stop_ev.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            rem = None if max_events is None else max(0, max_events - yielded)
            if rem == 0:
                break
            try:
                for ev in live.subscribe_event_stream(
                    market, symbol, stream, max_items=rem, stop_event=stop_ev  # type: ignore[arg-type]
                ):
                    yield ev
                    if not isinstance(ev, StreamHealthEvent) or ev.source != "gateway_watchdog":
                        backoff_s = 0.5
                    yielded += 1
                    if stop_ev.is_set():
                        break
                    if deadline is not None and time.monotonic() >= deadline:
                        stop_ev.set()
                        break
                    if max_events is not None and yielded >= max_events:
                        stop_ev.set()
                        break
                if not reconnect or stop_ev.is_set():
                    break
            except Exception as exc:
                if not reconnect or stop_ev.is_set():
                    raise
                yield StreamHealthEvent(
                    venue="BINANCE",
                    market=market,  # type: ignore[arg-type]
                    symbol=symbol,
                    stream=stream,  # type: ignore[arg-type]
                    status="DEGRADED",
                    reason=f"reconnect:{type(exc).__name__}:{exc}",
                    observed_time_utc_ns=time.time_ns(),
                    source="gateway_reconnect",
                )
            if deadline is not None and time.monotonic() >= deadline:
                break
            time.sleep(backoff_s)
            backoff_s = min(15.0, backoff_s * 2.0)
    finally:
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bq-signal")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run paper or live signal consumer against BMDG")
    p_run.add_argument("--config", type=Path, required=True)
    p_run.add_argument("--mode", choices=["paper", "live"], default="paper")
    p_run.add_argument("--max-events", type=int, default=None)
    p_run.add_argument("--duration-seconds", type=float, default=None)
    p_run.add_argument("--reconnect", action="store_true", help="Auto-reconnect on EOF/transient gRPC failure.")
    p_run.add_argument("--order-socket", type=Path, default=None, help="Unix socket for BQOrder IPC in --mode live.")
    p_run.add_argument(
        "--allow-no-sink",
        action="store_true",
        help="Explicit diagnostic override allowing --mode live without an execution sink.",
    )

    p_rec = sub.add_parser("reconcile", help="Reconcile paper_journal.jsonl (Stage 1 replay + Stage 2 Recorder)")
    p_rec.add_argument("--config", type=Path, required=True)
    p_rec.add_argument("--journal", type=Path, required=True)
    p_rec.add_argument("--recorder-root", type=Path, default=None)
    p_rec.add_argument("--window-start-ns", type=int, default=None)
    p_rec.add_argument("--window-end-ns", type=int, default=None)

    args = parser.parse_args(argv)
    cfg = load_deploy_config(args.config)

    if args.cmd == "run":
        mode: Literal["paper", "live"] = args.mode
        intent_sink = None
        if args.order_socket is not None:
            from bq_order.ipc import OrderIpcClient

            intent_sink = OrderIpcClient(args.order_socket)
        live = GatewayLiveFeed(target=cfg.gateway_target, stale_threshold_ns=cfg.stale_threshold_ns)
        try:
            runner = SignalRunner(
                cfg,
                mode=mode,
                live_feed=live,
                intent_sink=intent_sink,
                diagnostic=bool(args.allow_no_sink),
            )
            if args.reconnect or args.duration_seconds is not None:
                stream_iter = _stream_with_resilience(
                    live,
                    market=cfg.market,
                    symbol=cfg.symbol,
                    stream=cfg.stream,
                    max_events=args.max_events,
                    duration_seconds=args.duration_seconds,
                    reconnect=bool(args.reconnect),
                )
            else:
                stream_iter = live.subscribe_event_stream(cfg.market, cfg.symbol, cfg.stream, max_items=args.max_events)
            summary = runner.run_events(stream_iter)
            print(json.dumps(asdict(summary), indent=2, sort_keys=True))
        finally:
            live.close()
        return 0

    if args.cmd == "reconcile":
        report = reconcile_paper_journal(
            args.journal,
            cfg,
            recorder_root=args.recorder_root,
            window_start_ns=args.window_start_ns,
            window_end_ns=args.window_end_ns,
        )
        print(json.dumps(asdict(report), indent=2, default=str, sort_keys=True))
        return 0 if report.accepted else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
