"""Reproducible M-NT1 acceptance script: live BMDG Sandbox + 1h UTC Spot two-stage verification."""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
from pathlib import Path
import resource
import sys
import time
from typing import Any
import urllib.request

import grpc
from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

from bq_feed.events import BookTickerEvent, Market, Stream
from bq_feed.live import GatewayLiveFeed
from bq_nautilus import (
    BtcSpotBboStrategyConfig,
    hand_verify_spot_ledger,
    load_mcp_export_chunks,
    read_recorder_events,
    reconcile_spot_window,
    run_spot_backtest,
    run_spot_sandbox_live,
    spec_from_exchange_info_entry,
    spec_from_reference_event,
    verify_spot_causality_and_delay,
)


def _fetch_live_btcusdt_spot_instrument(target: str) -> Any:
    """Discover real contemporaneous BTCUSDT Spot instrument from Gateway REFERENCE_DATA or Binance REST."""
    chan = grpc.insecure_channel(target)
    try:
        stub = gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(chan)
        feed = GatewayLiveFeed(stub=stub, strict_capabilities=True)
        try:
            snap = feed.fetch_gateway_status()
            has_ref = any(
                m.market == enums_pb2.MARKET_SPOT
                and m.symbol == "BTCUSDT"
                and enums_pb2.STREAM_REFERENCE_DATA in list(m.enabled_streams.streams)
                for m in snap.markets
            )
            if has_ref:
                req = gateway_messages_pb2.EventSubscriptionRequest(
                    request_id="mnt1-inst-spec",
                    schema_version="event-subscription-request.v1",
                    delivery_mode=enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS,
                    supported_payload_schema_versions=["reference-observation.v1"],
                )
                sel = req.selectors.add()
                sel.venue = enums_pb2.VENUE_BINANCE
                sel.market = enums_pb2.MARKET_SPOT
                sel.symbol = "BTCUSDT"
                sel.stream = enums_pb2.STREAM_REFERENCE_DATA
                stream = stub.SubscribeEvents(req, timeout=8)
                try:
                    for env in stream:
                        if env.WhichOneof("payload") == "reference_observation":
                            ev = feed.translate_event_envelope(env)
                            if ev is not None and ev.source_kind == "instrument" and ev.status == "VALID":
                                return spec_from_reference_event(ev).to_nautilus_instrument()
                finally:
                    stream.cancel()
        except Exception:
            pass
    finally:
        chan.close()

    with urllib.request.urlopen(
        "https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT", timeout=10
    ) as resp:
        info = json.loads(resp.read().decode("utf-8"))
    obs_ns = time.time_ns()
    entry = info["symbols"][0]
    return spec_from_exchange_info_entry(
        entry,
        market=Market.SPOT,
        observe_time_utc_ns=obs_ns,
        source_time_ms=int(info.get("serverTime") or (obs_ns // 1_000_000)),
        source="binance_rest_exchange_info",
    ).to_nautilus_instrument()


def _proc_rss_bytes() -> int:
    proc_status = Path("/proc/self/status")
    if proc_status.is_file():
        for line in proc_status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                parts = line.split()
                return int(parts[1]) * 1024
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage * (1024 if sys.platform == "linux" else 1)


def _proc_io_write_bytes() -> int | None:
    io_path = Path("/proc/self/io")
    if not io_path.is_file():
        return None
    for line in io_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("write_bytes:"):
            return int(line.split(":")[1].strip())
    return None


def _recorder_status_tail(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines):
        if '"dropped_total"' in line:
            return json.loads(line)
    return None


def run_live_sandbox_gate(
    *,
    target: str,
    max_items: int,
    spot_status_log: Path | None,
    usdm_status_log: Path | None,
) -> dict[str, Any]:
    inst = _fetch_live_btcusdt_spot_instrument(target)
    spot_before = _recorder_status_tail(spot_status_log) if spot_status_log else None
    usdm_before = _recorder_status_tail(usdm_status_log) if usdm_status_log else None

    cpu_before = time.process_time()
    wall_before = time.monotonic()
    io_before = _proc_io_write_bytes()

    with ThreadPoolExecutor(max_workers=1) as pool:
        live_res = pool.submit(
            lambda: asyncio.run(
                run_spot_sandbox_live(
                    target=target,
                    instrument=inst,
                    market=Market.SPOT,
                    symbol="BTCUSDT",
                    max_items=max_items,
                    rule_mode="buy_then_sell",
                    order_qty="0.00020",
                    max_pos_qty="0.00040",
                    starting_usdt=Decimal("50"),
                    fee_rate=Decimal("0.001"),
                    timeout_secs=120.0,
                )
            )
        ).result()

    wall_s = time.monotonic() - wall_before
    cpu_s = time.process_time() - cpu_before
    io_after = _proc_io_write_bytes()
    rss_bytes = _proc_rss_bytes()

    # Stage 1 exact same-window replay + hand ledger check on the live Sandbox run
    from nautilus_trader.model import Price, Quantity, QuoteTick

    ticks = [
        QuoteTick(
            inst.id,
            Price.from_decimal_dp(Decimal(r["bid"]), inst.price_precision),
            Price.from_decimal_dp(Decimal(r["ask"]), inst.price_precision),
            Quantity.from_decimal_dp(Decimal(r["bid_size"]), inst.size_precision),
            Quantity.from_decimal_dp(Decimal(r["ask_size"]), inst.size_precision),
            int(r["ts_event"]),
            int(r["strategy_visible_ns"]),
        )
        for r in live_res["rows"]
    ]
    bt_replay = run_spot_backtest(
        instrument=inst,
        data=ticks,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="buy_then_sell",
            order_qty="0.00020",
            max_pos_qty="0.00040",
        ),
        starting_usdt=Decimal("50"),
        fee_rate=Decimal("0.001"),
    )
    hand = hand_verify_spot_ledger(
        starting_usdt=Decimal("50"),
        fee_rate=Decimal("0.001"),
        fills=bt_replay["fills"],
        reported_cash_usdt=bt_replay["cash_usdt"],
        reported_base_qty=bt_replay["base_qty"],
    )
    fills_match = [
        {"side": f["side"], "qty": f["qty"], "px": f["px"], "fee": f["fee"]}
        for f in bt_replay["fills"]
    ] == [
        {"side": f["side"], "qty": f["qty"], "px": f["px"], "fee": f["fee"]}
        for f in live_res["sandbox_fills"]
    ]

    spot_after = _recorder_status_tail(spot_status_log) if spot_status_log else None
    usdm_after = _recorder_status_tail(usdm_status_log) if usdm_status_log else None

    return {
        "instrument_id": str(inst.id),
        "instrument_info": inst.info,
        "accepted": live_res["accepted"],
        "callbacks_count": live_res["callbacks_count"],
        "unique_update_ids": live_res["unique_update_ids"],
        "sandbox_fills": live_res["sandbox_fills"],
        "same_window_replay_fills_match": fills_match,
        "hand_ledger_passed": hand["passed"],
        "hand_ledger": hand,
        "resources": {
            "wall_seconds": round(wall_s, 4),
            "cpu_seconds": round(cpu_s, 4),
            "cpu_pct_single_core": round((cpu_s / wall_s) * 100.0, 2) if wall_s > 0 else 0.0,
            "rss_bytes": rss_bytes,
            "rss_mib": round(rss_bytes / (1024 * 1024), 2),
            "disk_write_bytes": (io_after - io_before) if (io_before is not None and io_after is not None) else None,
            "dequeue_latency_p99_ns": live_res["dequeue_latency_p99_ns"],
            "dequeue_latency_max_ns": live_res["dequeue_latency_max_ns"],
        },
        "recorder_impact": {
            "spot_dropped_before": spot_before.get("dropped_total") if spot_before else None,
            "spot_dropped_after": spot_after.get("dropped_total") if spot_after else None,
            "usdm_dropped_before": usdm_before.get("dropped_total") if usdm_before else None,
            "usdm_dropped_after": usdm_after.get("dropped_total") if usdm_after else None,
        },
    }


def _seek_journal_near_ts(fh: Any, target_ns: int, file_size: int) -> int:
    """Binary-search monotonic journal byte offset slightly before target_ns."""
    if file_size < 32 * 1024 * 1024:
        return 0
    lo, hi = 0, file_size
    best_offset = 0
    for _ in range(32):
        if hi - lo <= 1024 * 1024:
            break
        mid = (lo + hi) // 2
        fh.seek(mid)
        if mid > 0:
            fh.readline()
        pos = fh.tell()
        line = fh.readline()
        if not line:
            hi = mid
            continue
        try:
            row = json.loads(line)
            ev = row.get("event") or {}
            ts = ev.get("receive_time_utc_ns") or ev.get("observed_time_utc_ns")
            if ts is None:
                lo = pos
                continue
            if int(ts) < target_ns:
                best_offset = pos
                lo = pos
            else:
                hi = mid
        except Exception:
            hi = mid
    return max(0, best_offset - 1024 * 1024)


def run_one_hour_spot_gate(
    *,
    target: str,
    journal_path: Path,
    mcp_export_root: Path,
    window_start_ns: int,
    window_end_ns: int,
    pad_ns: int = 10_000_000_000,
) -> dict[str, Any]:
    inst = _fetch_live_btcusdt_spot_instrument(target)

    # Stream-scan paper_journal.jsonl for Gateway BTCUSDT Spot book_ticker in [start-pad, end+pad)
    gw_rows: list[dict[str, Any]] = []
    scan_min = window_start_ns - pad_ns
    scan_max = window_end_ns + pad_ns
    gw_gaps_in_window = 0
    fsize = journal_path.stat().st_size
    with journal_path.open("r", encoding="utf-8") as fh:
        start_off = _seek_journal_near_ts(fh, scan_min - 60_000_000_000, fsize)
        fh.seek(start_off)
        if start_off > 0:
            fh.readline()
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            ev = row.get("event") or {}
            etype = ev.get("event_type")
            if etype == "StreamHealthEvent":
                obs_ns = int(ev.get("observed_time_utc_ns") or 0)
                if window_start_ns <= obs_ns < window_end_ns and ev.get("status") in ("GAP", "STALE"):
                    gw_gaps_in_window += 1
                if obs_ns > scan_max + 60_000_000_000:
                    break
                continue
            if etype != "BookTickerEvent":
                continue
            if ev.get("market") != "SPOT" or ev.get("symbol") != "BTCUSDT":
                continue
            recv_ns = ev.get("receive_time_utc_ns")
            if recv_ns is None:
                continue
            recv_ns = int(recv_ns)
            if recv_ns < scan_min:
                continue
            if recv_ns >= scan_max:
                if recv_ns >= scan_max + 60_000_000_000:
                    break
                continue
            gw_rows.append(
                {
                    "update_id": ev.get("update_id"),
                    "gateway_receive_ns": recv_ns,
                    "ts_event": recv_ns,
                    "strategy_visible_ns": recv_ns,
                    "bid": str(ev["best_bid_price"]),
                    "ask": str(ev["best_ask_price"]),
                    "bid_size": str(ev["best_bid_quantity"]),
                    "ask_size": str(ev["best_ask_quantity"]),
                }
            )

    # Read independent Recorder sealed chunks (verified SHA-256 + CRC32C)
    chunks = load_mcp_export_chunks(
        mcp_export_root,
        market_filter={Market.SPOT},
        symbol_filter={"BTCUSDT"},
        stream_filter={Stream.BOOK_TICKER},
        min_utc_ns=scan_min,
        max_utc_ns=scan_max,
    )
    rec_events = read_recorder_events(
        chunks,
        market_filter={Market.SPOT},
        symbol_filter={"BTCUSDT"},
        stream_filter={Stream.BOOK_TICKER},
        min_utc_ns=scan_min,
        max_utc_ns=scan_max,
        verify_hashes=True,
    )

    rec_res = reconcile_spot_window(
        instrument=inst,
        gateway_rows=gw_rows,
        recorder_events=rec_events,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns - 1,  # inclusive upper bound for compare_overlapping_windows
        rule_mode="imbalance",
        order_qty="0.00020",
        max_pos_qty="0.00060",
        starting_usdt=Decimal("50"),
        fee_rate=Decimal("0.001"),
        record_callbacks=False,
    )

    # Causality + 2s delay verification on real 1000-quote slice from the 1h Recorder window
    bbo_slice = [e for e in rec_events if isinstance(e, BookTickerEvent)][:1000]
    causality = verify_spot_causality_and_delay(
        instrument=inst,
        events=bbo_slice,
        split_index=min(200, len(bbo_slice) // 2),
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="imbalance",
            order_qty="0.00020",
            max_pos_qty="0.00060",
        ),
    )

    p = rec_res.stage2_parity
    return {
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "chunks_verified": len(chunks),
        "gateway_rows_with_pad": len(gw_rows),
        "recorder_events_with_pad": len(rec_events),
        "gateway_gaps_in_window": gw_gaps_in_window,
        "stage1_passed": rec_res.stage1_passed,
        "stage1_callbacks": rec_res.stage1_details["callbacks"],
        "stage1_callback_sha256": rec_res.stage1_details["callback_sha256"],
        "stage1_hand_ledger_passed": rec_res.stage1_details["hand_ledger"]["passed"],
        "stage1_fills_count": len(rec_res.stage1_details["hand_ledger"]["per_fill"]),
        "stage1_cash_usdt": rec_res.stage1_details["hand_ledger"]["reported_cash_usdt"],
        "stage1_base_qty": rec_res.stage1_details["hand_ledger"]["reported_base_qty"],
        "stage1_total_fee_usdt": rec_res.stage1_details["hand_ledger"]["total_fee_usdt"],
        "stage2_status": p.status,
        "stage2_recorder_total": p.recorder_total,
        "stage2_gateway_total": p.gateway_total,
        "stage2_matched_count": p.matched_count,
        "stage2_cross_boundary_paired": rec_res.cross_boundary_paired_count,
        "stage2_recorder_only_count": p.recorder_only_count,
        "stage2_gateway_only_count": p.gateway_only_count,
        "stage2_recorder_coverage": p.recorder_coverage,
        "stage2_gateway_coverage": p.gateway_coverage,
        "stage2_field_mismatches": len(p.field_mismatches),
        "stage2_recorder_gaps": p.recorder_gaps,
        "stage2_gateway_gaps": p.gateway_gaps,
        "stage2_latency_p50_ms": p.latency_p50_ms,
        "stage2_latency_p95_ms": p.latency_p95_ms,
        "stage2_latency_p99_ms": p.latency_p99_ms,
        "causality_and_2s_delay": causality,
        "accepted": rec_res.accepted and causality["passed"],
    }


def main() -> None:
    cli = argparse.ArgumentParser(description="M-NT1 VPS & 1h Spot Acceptance Runner")
    cli.add_argument("--target", default="127.0.0.1:50051")
    cli.add_argument("--live-items", type=int, default=200)
    cli.add_argument("--spot-status-log", type=Path, default=Path("/var/log/sbmdr/spot.status.jsonl"))
    cli.add_argument("--usdm-status-log", type=Path, default=Path("/var/log/sbmdr/usdm.status.jsonl"))
    cli.add_argument("--journal-path", type=Path, default=Path("/srv/recorder-data/bq-paper-72h/paper_journal.jsonl"))
    cli.add_argument("--mcp-export-root", type=Path, default=Path("/srv/recorder-data/S-BMDR/mcp-export"))
    cli.add_argument("--window-start-ns", type=int, default=None)
    cli.add_argument("--window-end-ns", type=int, default=None)
    cli.add_argument("--skip-live", action="store_true")
    cli.add_argument("--output", type=Path, required=True)
    args = cli.parse_args()

    report: dict[str, Any] = {}
    if not args.skip_live:
        report["live_sandbox_gate"] = run_live_sandbox_gate(
            target=args.target,
            max_items=args.live_items,
            spot_status_log=args.spot_status_log,
            usdm_status_log=args.usdm_status_log,
        )
    if args.window_start_ns is not None and args.window_end_ns is not None:
        report["one_hour_spot_gate"] = run_one_hour_spot_gate(
            target=args.target,
            journal_path=args.journal_path,
            mcp_export_root=args.mcp_export_root,
            window_start_ns=args.window_start_ns,
            window_end_ns=args.window_end_ns,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
