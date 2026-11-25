"""M-NT2.5 Route A Benchmark: Rust Tonic + Prost DataClient against identical 5-symbol gRPC server."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import resource
import sys
import time
from typing import Any

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

from bq_feed.events import Market, Stream  # noqa: E402
from bq_nautilus import (  # noqa: E402
    L2MicrostructureStrategy,
    L2MicrostructureStrategyConfig,
)
from bq_nautilus.tonic_live import BmdTonicDataClientFactory, BmdTonicGatewayConfig  # noqa: E402
from scripts.mnt2_l2_bench import FIVE_SYMBOLS, _make_spot_instrument, _pct, _read_rss_mib  # noqa: E402


async def _run_stage(*, name: str, rate_per_symbol_stream: float, duration_secs: float) -> dict[str, Any]:
    # Spawn the exact same synthetic gRPC server subprocess from scripts/mnt2_l2_bench.py!
    bench_server_py = str(REPO_ROOT / "scripts" / "mnt2_l2_bench.py")
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        bench_server_py,
        "--role",
        "server",
        "--rate",
        str(rate_per_symbol_stream),
        "--duration",
        str(duration_secs),
        "--levels",
        "5",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None
    ready_line = (await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)).decode("utf-8").strip()
    if not ready_line.startswith("READY "):
        err_bytes = await proc.stderr.read() if proc.stderr else b""
        raise RuntimeError(f"gRPC server failed: {ready_line!r} {err_bytes.decode('utf-8', 'ignore')}")
    port = int(ready_line.split()[1])

    instruments = {sym: _make_spot_instrument(sym) for sym in FIVE_SYMBOLS}
    inst_map = {inst.id: (Market.SPOT, sym, inst) for sym, inst in instruments.items()}
    req_streams = [
        (Market.SPOT, sym, st)
        for sym in FIVE_SYMBOLS
        for st in (Stream.DIFF_DEPTH, Stream.AGG_TRADE)
    ]
    stats: dict[str, Any] = {}
    cfg = BmdTonicGatewayConfig(
        target=f"127.0.0.1:{port}",
        instruments=inst_map,
        required_streams=req_streams,
        stats=stats,
    )
    node = (
        LiveNode.builder(f"MNT25-ROUTEA-{name.upper()}", TraderId("MNT25-A"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        .with_timeout_connection(5)
        .with_timeout_disconnection_secs(2)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client("BMD", BmdTonicDataClientFactory(), cfg)
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue("BINANCE"),
                account_id=AccountId("BINANCE-ROUTEA"),
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
                imbalance_threshold=2.0,
                data_client_id="BMD",
                exec_client_id="BINANCE",
                telemetry=tel,
            )
        )
        if stop_strat is None:
            stop_strat = st
        node.add_strategy(st)

    gc_pauses_ms: list[float] = []
    gc_t0 = [0]

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
        deadline = time.perf_counter() + duration_secs + 12.0
        while time.perf_counter() < deadline:
            if int(stats.get("items_received", 0)) >= expected_total:
                break
            await asyncio.sleep(0.03)
        await asyncio.sleep(0.1)
        if stop_strat is not None:
            stop_strat.shutdown_system("route_a bench complete")

    gc.collect()
    gc.callbacks.append(_gc_cb)
    rss_start = _read_rss_mib()
    ru_start = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    mon_task = asyncio.create_task(_loop_monitor())
    stop_task = asyncio.create_task(_stopper())
    try:
        await asyncio.wait_for(node.run_async(), timeout=duration_secs + 18.0)
    finally:
        stop_flag = True
        mon_task.cancel()
        stop_task.cancel()
        if _gc_cb in gc.callbacks:
            gc.callbacks.remove(_gc_cb)
        node.dispose()
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
    cpu_pct = (cpu_s / wall_s) * 100.0

    r2p = sorted(x / 1_000_000.0 for x in stats.get("dequeue_latencies_ns", []))
    w2c = sorted(x / 1_000_000.0 for x in stats.get("wire_to_cb_latencies_ns", []))
    gc_s = sorted(gc_pauses_ms)
    lag_s = sorted(loop_lags_ms)
    items = int(stats.get("items_received", 0))

    return {
        "stage": name,
        "wall_secs": round(wall_s, 3),
        "items_received": items,
        "expected_items": expected_total,
        "achieved_msg_s": round(items / wall_s, 1),
        "deltas_events_total": sum(int(t["deltas_events"]) for t in tels.values()),
        "trade_events_total": sum(int(t["trade_events"]) for t in tels.values()),
        "batches": int(stats.get("batches", 0)),
        "cpu_secs_client_only": round(cpu_s, 3),
        "cpu_pct_one_core_client_only": round(cpu_pct, 2),
        "rss_start_mib": round(rss_start, 2),
        "rss_end_mib": round(rss_end, 2),
        "rss_delta_mib": round(rss_end - rss_start, 2),
        "rust_tonic_dequeue_ms": {
            "p50": round(_pct(r2p, 0.50), 3),
            "p95": round(_pct(r2p, 0.95), 3),
            "p99": round(_pct(r2p, 0.99), 3),
            "max": round(r2p[-1], 3) if r2p else 0.0,
        },
        "wire_to_py_batch_ms": {
            "p50": round(_pct(w2c, 0.50), 3),
            "p95": round(_pct(w2c, 0.95), 3),
            "p99": round(_pct(w2c, 0.99), 3),
            "max": round(w2c[-1], 3) if w2c else 0.0,
        },
        "gc_pause_ms": {
            "count": len(gc_s),
            "p95": round(_pct(gc_s, 0.95), 3),
            "max": round(gc_s[-1], 3) if gc_s else 0.0,
        },
        "event_loop_lag_ms": {
            "p95": round(_pct(lag_s, 0.95), 3),
            "max": round(lag_s[-1], 3) if lag_s else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="M-NT2.5 Route A Rust Tonic Synthetic Benchmark")
    parser.add_argument("--duration", type=float, default=8.0)
    parser.add_argument("--out", Path, default=None) if False else None
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    async def _run() -> dict[str, Any]:
        return {
            "host": os.uname().nodename,
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "adapter": "bq_nautilus.tonic_live.BmdTonicDataClient (Rust tonic+prost+PyO3)",
            "nominal_500_msg_s": await _run_stage(
                name="nominal",
                rate_per_symbol_stream=50.0,
                duration_secs=args.duration,
            ),
            "burst_3000_msg_s": await _run_stage(
                name="burst",
                rate_per_symbol_stream=300.0,
                duration_secs=args.duration,
            ),
        }

    payload = asyncio.run(_run())
    txt = json.dumps(payload, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(txt + "\n", encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
