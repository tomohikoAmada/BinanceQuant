"""Isolated 5-symbol High-Frequency L2 + AggTrade Python grpc.aio -> Nautilus LiveNode Benchmark."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import resource
import statistics
import sys
import time
from typing import Any

import grpc

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.adapters.sandbox import (  # noqa: E402
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.common import Environment, LogLevel  # noqa: E402
from nautilus_trader.config import LoggerConfig  # noqa: E402
from nautilus_trader.execution import MakerTakerFeeModel  # noqa: E402
from nautilus_trader.live import LiveNode  # noqa: E402
from nautilus_trader.model import (  # noqa: E402
    AccountId,
    AccountType,
    BookType,
    Money,
    OmsType,
    TraderId,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2  # noqa: E402
from binance_market_data.gateway.v1 import (  # noqa: E402
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)
from bq_feed.events import Market, Stream  # noqa: E402
from bq_nautilus import (  # noqa: E402
    BmdDataClientFactory,
    BmdGatewayConfig,
    L2MicrostructureStrategy,
    L2MicrostructureStrategyConfig,
    spec_from_exchange_info_entry,
)

FIVE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
_BASE_PRICES = {
    "BTCUSDT": Decimal("65000.00"),
    "ETHUSDT": Decimal("3200.00"),
    "SOLUSDT": Decimal("180.00"),
    "BNBUSDT": Decimal("620.00"),
    "XRPUSDT": Decimal("1.40"),
}


def _read_rss_mib() -> float:
    proc_status = Path("/proc/self/status")
    if proc_status.is_file():
        for line in proc_status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                kb = int(line.split()[1])
                return kb / 1024.0
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS reports bytes.
    return rss / (1024.0 * 1024.0) if sys.platform == "darwin" else rss / 1024.0


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(q * (len(sorted_vals) - 1))))
    return float(sorted_vals[idx])


def _make_spot_instrument(symbol: str) -> Any:
    base = symbol[:-4]
    tick_size = "0.0001" if symbol == "XRPUSDT" else "0.01"
    step_size = "0.00001" if symbol == "BTCUSDT" else ("1.00000" if symbol == "XRPUSDT" else "0.00100")
    return spec_from_exchange_info_entry(
        {
            "symbol": symbol,
            "status": "TRADING",
            "baseAsset": base,
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": tick_size, "maxPrice": "1000000.00", "tickSize": tick_size},
                {"filterType": "LOT_SIZE", "minQty": step_size, "maxQty": "10000000.00", "stepSize": step_size},
                {"filterType": "NOTIONAL", "minNotional": "5.00"},
            ],
        },
        market=Market.SPOT,
        observe_time_utc_ns=1_795_420_800_000_000_000,
        source_time_ms=1_795_420_800_000,
        source="binance_exchange_info",
    ).to_nautilus_instrument()


class _SyntheticL2GatewayServicer(gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceServicer):
    """Isolated local gRPC server streaming prebuilt 5-symbol L2 + aggTrade protobuf envelopes."""

    def __init__(self, *, rate_per_symbol_stream: float, duration_secs: float, levels_per_update: int = 5) -> None:
        self._rate = rate_per_symbol_stream
        self._duration = duration_secs
        self._levels = levels_per_update

    async def GetGatewayStatus(self, request: Any, context: Any) -> Any:
        snap = gateway_messages_pb2.GatewayStatusSnapshot()
        for sym in FIVE_SYMBOLS:
            row = snap.markets.add()
            row.venue = enums_pb2.VENUE_BINANCE
            row.market = enums_pb2.MARKET_SPOT
            row.symbol = sym
            row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
            row.enabled_streams.streams.extend(
                [enums_pb2.STREAM_DIFF_DEPTH, enums_pb2.STREAM_AGG_TRADE]
            )
        return snap

    async def SubscribeEvents(self, request: Any, context: Any) -> Any:
        sel = request.selectors[0]
        sym = sel.symbol
        stream_enum = sel.stream

        acc = gateway_messages_pb2.GatewayEventEnvelope()
        acc.subscription_accepted.subscription_id = f"bench-{sym}-{stream_enum}"
        yield acc

        base_px = _BASE_PRICES[sym]
        tick = Decimal("0.0001") if sym == "XRPUSDT" else Decimal("0.01")
        qty_s = "10.00000" if sym == "XRPUSDT" else "0.50000"

        if stream_enum == enums_pb2.STREAM_DIFF_DEPTH:
            snap_env = gateway_messages_pb2.GatewayEventEnvelope()
            snap = snap_env.reference_observation.depth_baseline
            snap.venue = enums_pb2.VENUE_BINANCE
            snap.market = enums_pb2.MARKET_SPOT
            snap.symbol = sym
            snap.last_update_id = 1
            snap.receive_time_utc_ns = time.time_ns()
            for lv in range(self._levels):
                b = snap.bids.add()
                b.price = str(base_px - tick * (lv + 1))
                b.quantity = qty_s
                a = snap.asks.add()
                a.price = str(base_px + tick * (lv + 1))
                a.quantity = qty_s
            snap_env.reference_observation.venue = enums_pb2.VENUE_BINANCE
            snap_env.reference_observation.market = enums_pb2.MARKET_SPOT
            snap_env.reference_observation.symbol = sym
            snap_env.reference_observation.receive_time_utc_ns = snap.receive_time_utc_ns
            yield snap_env

        total_msgs = max(1, int(self._rate * self._duration))
        t_start = time.perf_counter()
        seq = 1
        # Send updates pacing smoothly in small batches to reflect real async gRPC bursts without busy spin.
        batch = 10 if self._rate > 400 else 1
        while seq <= total_msgs:
            for _ in range(batch):
                if seq > total_msgs:
                    break
                now_ns = time.time_ns()
                env = gateway_messages_pb2.GatewayEventEnvelope()
                if stream_enum == enums_pb2.STREAM_DIFF_DEPTH:
                    du = env.depth_update
                    du.metadata.venue = enums_pb2.VENUE_BINANCE
                    du.metadata.market = enums_pb2.MARKET_SPOT
                    du.metadata.symbol = sym
                    du.metadata.receive_time_utc_ns = now_ns
                    du.first_update_id = seq + 1
                    du.final_update_id = seq + 1
                    for lv in range(self._levels):
                        b = du.bids.add()
                        b.price = str(base_px - tick * (lv + 1))
                        b.quantity = str(Decimal(qty_s) + Decimal(seq % 5) * tick)
                else:
                    at = env.agg_trade
                    at.metadata.venue = enums_pb2.VENUE_BINANCE
                    at.metadata.market = enums_pb2.MARKET_SPOT
                    at.metadata.symbol = sym
                    at.metadata.receive_time_utc_ns = now_ns
                    at.aggregate_trade_id = seq
                    at.price = str(base_px)
                    at.quantity = qty_s
                    at.trade_time_ms = now_ns // 1_000_000
                    at.buyer_is_maker = bool(seq % 2)
                yield env
                seq += 1

            target_elapsed = seq / self._rate
            actual_elapsed = time.perf_counter() - t_start
            if target_elapsed > actual_elapsed:
                await asyncio.sleep(target_elapsed - actual_elapsed)
            else:
                await asyncio.sleep(0)


async def _serve_forever(rate_per_symbol_stream: float, duration_secs: float, levels_per_update: int) -> None:
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_BinanceMarketDataGatewayServiceServicer_to_server(
        _SyntheticL2GatewayServicer(
            rate_per_symbol_stream=rate_per_symbol_stream,
            duration_secs=duration_secs,
            levels_per_update=levels_per_update,
        ),
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    print(f"READY {port}", flush=True)
    await server.wait_for_termination()


async def _run_stage(
    *,
    name: str,
    rate_per_symbol_stream: float,
    duration_secs: float,
    levels_per_update: int = 5,
) -> dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        "--role",
        "server",
        "--rate",
        str(rate_per_symbol_stream),
        "--duration",
        str(duration_secs),
        "--levels",
        str(levels_per_update),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None
    ready_line = (await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)).decode("utf-8").strip()
    if not ready_line.startswith("READY "):
        err_bytes = await proc.stderr.read() if proc.stderr else b""
        raise RuntimeError(f"Benchmark server failed to start: {ready_line!r} stderr={err_bytes.decode('utf-8', 'ignore')}")
    port = int(ready_line.split()[1])

    instruments = {sym: _make_spot_instrument(sym) for sym in FIVE_SYMBOLS}
    inst_map = {inst.id: (Market.SPOT, sym, inst) for sym, inst in instruments.items()}
    req_streams = [
        (Market.SPOT, sym, st)
        for sym in FIVE_SYMBOLS
        for st in (Stream.DIFF_DEPTH, Stream.AGG_TRADE)
    ]
    stats: dict[str, Any] = {}
    cfg = BmdGatewayConfig(
        target=f"127.0.0.1:{port}",
        instruments=inst_map,
        required_streams=req_streams,
        stale_threshold_ns=15_000_000_000,
        record_rows=False,
        stats=stats,
    )
    node = (
        LiveNode.builder(f"MNT2-BENCH-{name.upper()}", TraderId("MNT2-BENCH"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        .with_timeout_connection(5)
        .with_timeout_disconnection_secs(2)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client("BMD", BmdDataClientFactory(), cfg)
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue("BINANCE"),
                account_id=AccountId("BINANCE-BENCH"),
                account_type=AccountType.CASH,
                oms_type=OmsType.NETTING,
                book_type=BookType.L2_MBP,
                starting_balances=[Money.from_str("100000.00 USDT")],
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
            ),
        )
        .build()
    )

    tels: dict[str, dict[str, Any]] = {}
    stop_strat: Any = None
    for sym, inst in instruments.items():
        tel: dict[str, Any] = {}
        tels[sym] = tel
        st = L2MicrostructureStrategy(
            L2MicrostructureStrategyConfig(
                instrument_id=inst.id,
                order_qty="0.001" if sym != "XRPUSDT" else "10",
                imbalance_threshold=2.0,  # disable orders to isolate pure data/L2 path
                data_client_id="BMD",
                exec_client_id="BINANCE",
                telemetry=tel,
            )
        )
        if stop_strat is None:
            stop_strat = st
        node.add_strategy(st)

    gc_pauses_ms: list[float] = []
    gc_t0: list[int] = [0]

    def _gc_cb(phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            gc_t0[0] = time.perf_counter_ns()
        elif phase == "stop" and gc_t0[0] > 0:
            gc_pauses_ms.append((time.perf_counter_ns() - gc_t0[0]) / 1_000_000.0)

    loop_lags_ms: list[float] = []
    stop_flag = False

    async def _loop_monitor() -> None:
        interval = 0.02
        expected = time.perf_counter() + interval
        while not stop_flag:
            await asyncio.sleep(interval)
            now = time.perf_counter()
            loop_lags_ms.append(max(0.0, (now - expected) * 1000.0))
            expected = now + interval

    expected_total = len(FIVE_SYMBOLS) * 2 * max(1, int(rate_per_symbol_stream * duration_secs))

    async def _stopper() -> None:
        deadline = time.perf_counter() + duration_secs + 10.0
        while time.perf_counter() < deadline:
            if int(stats.get("items_received", 0)) >= expected_total:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.1)
        if stop_strat is not None:
            stop_strat.shutdown_system("mnt2_l2_bench complete")

    gc.collect()
    gc.callbacks.append(_gc_cb)
    rss_start = _read_rss_mib()
    ru_start = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    mon_task = asyncio.create_task(_loop_monitor())
    stop_task = asyncio.create_task(_stopper())
    try:
        await asyncio.wait_for(node.run_async(), timeout=duration_secs + 15.0)
    finally:
        stop_flag = True
        mon_task.cancel()
        stop_task.cancel()
        if _gc_cb in gc.callbacks:
            gc.callbacks.remove(_gc_cb)
        node.dispose()
        node = None
        gc.collect()
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

    wall_s = max(1e-6, time.perf_counter() - t0)
    ru_end = resource.getrusage(resource.RUSAGE_SELF)
    rss_end = _read_rss_mib()
    cpu_s = (ru_end.ru_utime - ru_start.ru_utime) + (ru_end.ru_stime - ru_start.ru_stime)
    cpu_pct_single_core = (cpu_s / wall_s) * 100.0

    lat_ms = sorted(max(0.0, x / 1_000_000.0) for x in stats.get("dequeue_latencies_ns", []))
    gc_sorted = sorted(gc_pauses_ms)
    lag_sorted = sorted(loop_lags_ms)
    items = int(stats.get("items_received", 0))

    return {
        "stage": name,
        "symbols": list(FIVE_SYMBOLS),
        "target_rate_total_msg_s": round(rate_per_symbol_stream * len(FIVE_SYMBOLS) * 2, 1),
        "wall_secs": round(wall_s, 3),
        "items_received": items,
        "expected_items": expected_total,
        "achieved_msg_s": round(items / wall_s, 1),
        "deltas_events_total": sum(int(t["deltas_events"]) for t in tels.values()),
        "trade_events_total": sum(int(t["trade_events"]) for t in tels.values()),
        "cpu_secs_client_only": round(cpu_s, 3),
        "cpu_pct_one_core_client_only": round(cpu_pct_single_core, 2),
        "rss_start_mib": round(rss_start, 2),
        "rss_end_mib": round(rss_end, 2),
        "rss_delta_mib": round(rss_end - rss_start, 2),
        "dequeue_latency_ms": {
            "p50": round(_pct(lat_ms, 0.50), 3),
            "p95": round(_pct(lat_ms, 0.95), 3),
            "p99": round(_pct(lat_ms, 0.99), 3),
            "max": round(lat_ms[-1], 3) if lat_ms else 0.0,
        },
        "gc_pause_ms": {
            "count": len(gc_sorted),
            "p95": round(_pct(gc_sorted, 0.95), 3),
            "max": round(gc_sorted[-1], 3) if gc_sorted else 0.0,
        },
        "event_loop_lag_ms": {
            "p95": round(_pct(lag_sorted, 0.95), 3),
            "max": round(lag_sorted[-1], 3) if lag_sorted else 0.0,
        },
    }


def _evaluate_verdict(nominal: dict[str, Any], burst: dict[str, Any]) -> dict[str, Any]:
    pass_rate = burst["achieved_msg_s"] >= 2000.0
    pass_p99 = burst["dequeue_latency_ms"]["p99"] <= 15.0
    pass_gc = burst["gc_pause_ms"]["max"] <= 30.0
    pass_rss = burst["rss_delta_mib"] <= 150.0
    verdict = (
        "GO_KEEP_PYTHON_GRPC_AIO_WITH_RUST_CORE"
        if (pass_rate and pass_p99 and pass_gc and pass_rss)
        else "SWITCH_TO_RUST_TONIC_GRPC_OR_NATIVE_RUST_ADAPTER"
    )
    return {
        "verdict": verdict,
        "criteria": {
            "burst_throughput_ge_2000_msg_s": pass_rate,
            "burst_dequeue_p99_le_15ms": pass_p99,
            "burst_gc_max_pause_le_30ms": pass_gc,
            "burst_rss_delta_le_150mib": pass_rss,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="5-symbol High-Frequency L2 Python grpc.aio Benchmark")
    parser.add_argument("--role", choices=("client", "server"), default="client")
    parser.add_argument("--stage", choices=("nominal", "burst", "all"), default="all")
    parser.add_argument("--rate", type=float, default=50.0)
    parser.add_argument("--duration", type=float, default=8.0)
    parser.add_argument("--levels", type=int, default=5)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    if args.role == "server":
        asyncio.run(_serve_forever(args.rate, args.duration + 5.0, args.levels))
        return

    async def _run() -> dict[str, Any]:
        results: dict[str, Any] = {
            "host": os.uname().nodename,
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "note": "Synthetic grpc.aio server runs in an isolated subprocess so CPU/RSS/GC/loop-lag reflect Nautilus LiveNode + Python grpc.aio adapter only.",
        }
        if args.stage in ("nominal", "all"):
            # 50 msg/s * 2 streams * 5 symbols = 500 msg/s
            results["nominal_500_msg_s"] = await _run_stage(
                name="nominal",
                rate_per_symbol_stream=50.0,
                duration_secs=args.duration,
            )
        if args.stage in ("burst", "all"):
            # 300 msg/s * 2 streams * 5 symbols = 3,000 msg/s target burst
            results["burst_3000_msg_s"] = await _run_stage(
                name="burst",
                rate_per_symbol_stream=300.0,
                duration_secs=args.duration,
            )
        if "nominal_500_msg_s" in results and "burst_3000_msg_s" in results:
            results["architectural_decision"] = _evaluate_verdict(
                results["nominal_500_msg_s"],
                results["burst_3000_msg_s"],
            )
        return results

    payload = asyncio.run(_run())
    txt = json.dumps(payload, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(txt + "\n", encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
