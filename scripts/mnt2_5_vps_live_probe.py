"""M-NT2.5 VPS Real 5-Symbol Live Comparison: Route B/B+ (Native Rust Binance) vs Route A (Rust Tonic + BMDG)."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from typing import Any
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.adapters.binance import (  # noqa: E402
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceEnvironment,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
    BinanceSpotMarketDataMode,
)
from nautilus_trader.adapters.sandbox import (  # noqa: E402
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.common import Environment, LogLevel  # noqa: E402
from nautilus_trader.config import LoggerConfig, StrategyConfig  # noqa: E402
from nautilus_trader.execution import MakerTakerFeeModel  # noqa: E402
from nautilus_trader.live import LiveNode  # noqa: E402
from nautilus_trader.model import (  # noqa: E402
    AccountId,
    AccountType,
    BookAction,
    BookType,
    ClientId,
    FundingRateUpdate,
    InstrumentId,
    InstrumentStatus,
    MarkPriceUpdate,
    Money,
    OmsType,
    OrderBookDeltas,
    QuoteTick,
    TradeTick,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy  # noqa: E402

from bq_feed.events import Market, Stream  # noqa: E402
from bq_nautilus.instruments import spec_from_exchange_info_entry  # noqa: E402
from bq_nautilus.tonic_live import BmdTonicDataClientFactory, BmdTonicGatewayConfig  # noqa: E402

FIVE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")


def _read_rss_mib(pid: int | None = None) -> float:
    path = Path(f"/proc/{pid}/status") if pid is not None else Path("/proc/self/status")
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _read_proc_cpu_ticks(pid: int) -> int | None:
    p = Path(f"/proc/{pid}/stat")
    if not p.is_file():
        return None
    parts = p.read_text(encoding="utf-8").rsplit(")", 1)[-1].split()
    # fields after ')' start at index 0=state; utime=11, stime=12
    return int(parts[11]) + int(parts[12])


def _read_host_cpu() -> tuple[int, ...] | None:
    p = Path("/proc/stat")
    if not p.is_file():
        return None
    first = p.read_text(encoding="utf-8").splitlines()[0]
    vals = [int(x) for x in first.split()[1:9]]
    # user, nice, sys, idle, iowait, irq, softirq, steal
    return tuple(vals)


def _systemd_main_pid(unit: str) -> int | None:
    out = subprocess.check_output(
        ["systemctl", "show", unit, "-p", "MainPID", "--value"],
        text=True,
    ).strip()
    pid = int(out) if out.isdigit() else 0
    return pid if pid > 0 else None


def _count_tcp_established(pid: int | None) -> int:
    if pid is None:
        return 0
    try:
        out = subprocess.check_output(["ss", "-tnp"], text=True, stderr=subprocess.DEVNULL)
        marker = f"pid={pid},"
        return sum(1 for ln in out.splitlines() if "ESTAB" in ln and marker in ln)
    except Exception:
        return -1


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(q * (len(sorted_vals) - 1))))
    return float(sorted_vals[idx])


def _route_b_plus_slow_rest_probe() -> dict[str, Any]:
    """Measure Route B+ lightweight REST slow-reference poller for 5 USD-M symbols."""
    t0 = time.perf_counter()
    results = []
    endpoints = [
        ("open_interest", "https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}"),
        ("global_ls_account", "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period=5m&limit=1"),
        ("top_ls_account", "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={sym}&period=5m&limit=1"),
        ("top_ls_position", "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period=5m&limit=1"),
        ("taker_buy_sell", "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={sym}&period=5m&limit=1"),
    ]
    ok = 0
    for sym in FIVE_SYMBOLS:
        for name, tmpl in endpoints:
            url = tmpl.format(sym=sym)
            with urlopen(url, timeout=8.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                results.append({"symbol": sym, "kind": name, "non_empty": bool(data)})
                ok += 1
    dt_s = time.perf_counter() - t0
    return {
        "requests_succeeded": ok,
        "requests_total": len(endpoints) * len(FIVE_SYMBOLS),
        "poll_cycle_wall_secs": round(dt_s, 3),
        "estimated_weight_per_5min": len(endpoints) * len(FIVE_SYMBOLS),
        "sample": results[:5],
    }


def _fetch_exchange_instruments() -> dict[Any, tuple[Market, str, Any]]:
    spot_info = json.loads(urlopen("https://api.binance.com/api/v3/exchangeInfo", timeout=10.0).read())
    usdm_info = json.loads(urlopen("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=10.0).read())
    obs = time.time_ns()
    out: dict[Any, tuple[Market, str, Any]] = {}
    for row in spot_info["symbols"]:
        if row["symbol"] in FIVE_SYMBOLS:
            inst = spec_from_exchange_info_entry(
                row,
                market=Market.SPOT,
                observe_time_utc_ns=obs,
                source_time_ms=spot_info["serverTime"],
                source="binance_exchange_info",
            ).to_nautilus_instrument()
            out[inst.id] = (Market.SPOT, row["symbol"], inst)
    for row in usdm_info["symbols"]:
        if row["symbol"] in FIVE_SYMBOLS:
            inst = spec_from_exchange_info_entry(
                row,
                market=Market.USDM,
                observe_time_utc_ns=obs,
                source_time_ms=usdm_info["serverTime"],
                source="binance_exchange_info",
            ).to_nautilus_instrument()
            out[inst.id] = (Market.USDM, row["symbol"], inst)
    return out


class LiveProbeStrategyConfig(StrategyConfig):
    def __init__(
        self,
        *,
        spot_client: str | None,
        usdm_client: str | None,
        spot_ids: list[InstrumentId],
        usdm_ids: list[InstrumentId],
        telemetry: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.spot_client = spot_client
        self.usdm_client = usdm_client
        self.spot_ids = spot_ids
        self.usdm_ids = usdm_ids
        self.telemetry = telemetry


class LiveProbeStrategy(Strategy):
    def __init__(self, config: LiveProbeStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.tel = config.telemetry
        self.tel.setdefault("counts", {
            "quotes": 0,
            "trades": 0,
            "deltas": 0,
            "mark_prices": 0,
            "funding_rates": 0,
            "statuses": 0,
            "clears": 0,
        })
        self.tel.setdefault("rust_to_py_ms", [])
        self.tel.setdefault("sample_trades", [])
        self.tel.setdefault("status_events", [])
        self.tel["_start_mono"] = time.monotonic()

    def on_start(self) -> None:
        if self._cfg.spot_client:
            cid = ClientId(self._cfg.spot_client)
            for iid in self._cfg.spot_ids:
                self.subscribe_quotes(iid, client_id=cid)
                self.subscribe_trades(iid, client_id=cid)
                self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=cid)
                self.subscribe_instrument_status(iid, client_id=cid)
        if self._cfg.usdm_client:
            cid = ClientId(self._cfg.usdm_client)
            for iid in self._cfg.usdm_ids:
                self.subscribe_quotes(iid, client_id=cid)
                self.subscribe_trades(iid, client_id=cid)
                self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=cid)
                self.subscribe_mark_prices(iid, client_id=cid)
                self.subscribe_funding_rates(iid, client_id=cid)
                self.subscribe_instrument_status(iid, client_id=cid)

    def _rec_lat(self, obj: Any) -> None:
        cb_ns = time.time_ns()
        if len(self.tel["rust_to_py_ms"]) < 200_000:
            self.tel["rust_to_py_ms"].append(max(0.0, (cb_ns - int(obj.ts_init)) / 1_000_000.0))

    def on_quote(self, quote: QuoteTick) -> None:
        self.tel["counts"]["quotes"] += 1
        self._rec_lat(quote)

    def on_trade(self, trade: TradeTick) -> None:
        self.tel["counts"]["trades"] += 1
        self._rec_lat(trade)
        if time.monotonic() - self.tel.get("_start_mono", 0.0) >= 15.0 and len(self.tel["sample_trades"]) < 1000:
            self.tel["sample_trades"].append(
                {
                    "instrument_id": str(trade.instrument_id),
                    "trade_id": str(trade.trade_id),
                    "price": str(trade.price),
                    "size": str(trade.size),
                    "side": trade.aggressor_side.name,
                    "ts_event": int(trade.ts_event),
                    "ts_init": int(trade.ts_init),
                }
            )

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        self.tel["counts"]["deltas"] += 1
        self._rec_lat(deltas)
        if any(d.action == BookAction.CLEAR for d in deltas.deltas):
            self.tel["counts"]["clears"] += 1

    def on_mark_price(self, mp: MarkPriceUpdate) -> None:
        self.tel["counts"]["mark_prices"] += 1
        self._rec_lat(mp)

    def on_funding_rate(self, fr: FundingRateUpdate) -> None:
        self.tel["counts"]["funding_rates"] += 1

    def on_instrument_status(self, status: InstrumentStatus) -> None:
        self.tel["counts"]["statuses"] += 1
        self.tel["status_events"].append(
            {
                "instrument_id": str(status.instrument_id),
                "is_trading": bool(status.is_trading),
                "reason": str(status.reason),
                "ts_init": int(status.ts_init),
            }
        )


async def _run_live_probe(*, mode: str, duration_secs: float, load_ids_only: bool = False) -> dict[str, Any]:
    clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    bmdg_pid = _systemd_main_pid("bmdg")
    spot_rec_pid = _systemd_main_pid("sbmdr-recorder@spot")
    usdm_rec_pid = _systemd_main_pid("sbmdr-recorder@usdm")

    spot_ids = [InstrumentId.from_str(f"{s}.BINANCE") for s in FIVE_SYMBOLS]
    usdm_ids = [InstrumentId.from_str(f"{s}-PERP.BINANCE") for s in FIVE_SYMBOLS]
    spot_id_strs = [str(x) for x in spot_ids]
    usdm_id_strs = [str(x) for x in usdm_ids]

    builder = (
        LiveNode.builder(f"MNT25-LIVE-{mode.upper()}", TraderId(f"MNT25-{mode.upper()}"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
        .with_timeout_connection(15)
        .with_timeout_disconnection_secs(3)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
    )

    stats: dict[str, Any] = {}
    if mode == "route_b":
        spot_provider_cfg = (
            BinanceInstrumentProviderConfig(load_all=False, load_ids=spot_id_strs)
            if load_ids_only
            else BinanceInstrumentProviderConfig(load_all=True)
        )
        usdm_provider_cfg = (
            BinanceInstrumentProviderConfig(load_all=False, load_ids=usdm_id_strs)
            if load_ids_only
            else BinanceInstrumentProviderConfig(load_all=True)
        )
        builder = builder.add_data_client(
            "BINANCE-SPOT",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.SPOT,
                environment=BinanceEnvironment.LIVE,
                spot_market_data_mode=BinanceSpotMarketDataMode.Json,
                # rc9 calls SBE exchange_info() when us=False (Binance Spot returns HTTP 400 deprecated SBE schema);
                # us=True + Global URLs routes REST exchangeInfo/depth to JSON cleanly.
                us=True,
                base_url_http="https://api.binance.com",
                base_url_ws="wss://stream.binance.com:9443/ws",
                instrument_provider=spot_provider_cfg,
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        ).add_data_client(
            "BINANCE-USDM",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=usdm_provider_cfg,
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        spot_client_name, usdm_client_name = "BINANCE-SPOT", "BINANCE-USDM"
    else:
        inst_map = _fetch_exchange_instruments()
        req = [
            (mkt, sym, st)
            for (_, (mkt, sym, _)) in inst_map.items()
            for st in (Stream.BOOK_TICKER, Stream.AGG_TRADE, Stream.DIFF_DEPTH)
        ] + [
            (Market.USDM, sym, Stream.MARK_PRICE)
            for sym in FIVE_SYMBOLS
        ]
        builder = builder.add_data_client(
            "BMD",
            BmdTonicDataClientFactory(),
            BmdTonicGatewayConfig(
                target="127.0.0.1:50051",
                instruments=inst_map,
                required_streams=req,
                stats=stats,
            ),
        )
        spot_client_name, usdm_client_name = "BMD", "BMD"

    builder = builder.add_simulated_exec_client(
        "BINANCE",
        SandboxExecutionClientFactory(),
        SandboxExecutionClientConfig(
            venue=Venue("BINANCE"),
            account_id=AccountId("BINANCE-PROBE"),
            account_type=AccountType.MARGIN,
            oms_type=OmsType.NETTING,
            book_type=BookType.L2_MBP,
            starting_balances=[Money.from_str("10000.00 USDT")],
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
        ),
    )
    node = builder.build()
    tel: dict[str, Any] = {}
    strat = LiveProbeStrategy(
        LiveProbeStrategyConfig(
            spot_client=spot_client_name,
            usdm_client=usdm_client_name,
            spot_ids=spot_ids,
            usdm_ids=usdm_ids,
            telemetry=tel,
        )
    )
    node.add_strategy(strat)

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
        iv = 0.05
        exp = time.perf_counter() + iv
        while not stop_flag:
            await asyncio.sleep(iv)
            now = time.perf_counter()
            loop_lags_ms.append(max(0.0, (now - exp) * 1000.0))
            exp = now + iv

    async def _stopper() -> None:
        await asyncio.sleep(duration_secs)
        strat.shutdown_system(f"live probe {mode} complete")

    host_start = _read_host_cpu()
    bmdg_ticks_0 = _read_proc_cpu_ticks(bmdg_pid) if bmdg_pid else None
    spot_rec_ticks_0 = _read_proc_cpu_ticks(spot_rec_pid) if spot_rec_pid else None
    usdm_rec_ticks_0 = _read_proc_cpu_ticks(usdm_rec_pid) if usdm_rec_pid else None

    gc.collect()
    gc.callbacks.append(_gc_cb)
    rss_start = _read_rss_mib()
    ru_start = resource.getrusage(resource.RUSAGE_SELF)
    t_utc_start = time.time_ns()
    t0 = time.perf_counter()
    mon_task = asyncio.create_task(_loop_monitor())
    stop_task = asyncio.create_task(_stopper())
    tcp_estab_mid: dict[str, int] = {}

    async def _sample_tcp() -> None:
        await asyncio.sleep(min(15.0, duration_secs * 0.5))
        tcp_estab_mid["client"] = _count_tcp_established(os.getpid())
        tcp_estab_mid["bmdg"] = _count_tcp_established(bmdg_pid)
        tcp_estab_mid["spot_rec"] = _count_tcp_established(spot_rec_pid)
        tcp_estab_mid["usdm_rec"] = _count_tcp_established(usdm_rec_pid)

    tcp_task = asyncio.create_task(_sample_tcp())
    try:
        await asyncio.wait_for(node.run_async(), timeout=duration_secs + 30.0)
    finally:
        stop_flag = True
        mon_task.cancel()
        stop_task.cancel()
        tcp_task.cancel()
        if _gc_cb in gc.callbacks:
            gc.callbacks.remove(_gc_cb)
        wall_s = max(1e-6, time.perf_counter() - t0)
        t_utc_end = time.time_ns()
        ru_end = resource.getrusage(resource.RUSAGE_SELF)
        rss_end = _read_rss_mib()
        bmdg_rss = _read_rss_mib(bmdg_pid) if bmdg_pid else 0.0
        bmdg_ticks_1 = _read_proc_cpu_ticks(bmdg_pid) if bmdg_pid else None
        spot_rec_ticks_1 = _read_proc_cpu_ticks(spot_rec_pid) if spot_rec_pid else None
        usdm_rec_ticks_1 = _read_proc_cpu_ticks(usdm_rec_pid) if usdm_rec_pid else None
        host_end = _read_host_cpu()
        node.dispose()

    def _proc_pct(t_a: int | None, t_b: int | None) -> float:
        if t_a is None or t_b is None:
            return 0.0
        return round((((t_b - t_a) / clk_tck) / wall_s) * 100.0, 2)

    client_cpu_s = (ru_end.ru_utime - ru_start.ru_utime) + (ru_end.ru_stime - ru_start.ru_stime)
    client_cpu_pct = round((client_cpu_s / wall_s) * 100.0, 2)
    bmdg_cpu_pct = _proc_pct(bmdg_ticks_0, bmdg_ticks_1)

    host_stats: dict[str, float] = {}
    if host_start and host_end:
        diffs = [b - a for a, b in zip(host_start, host_end)]
        tot = max(1, sum(diffs))
        host_stats = {
            "user_pct": round((diffs[0] + diffs[1]) * 100.0 / tot, 2),
            "sys_pct": round(diffs[2] * 100.0 / tot, 2),
            "idle_pct": round(diffs[3] * 100.0 / tot, 2),
            "steal_pct": round(diffs[7] * 100.0 / tot, 2),
        }

    r2p = sorted(tel["rust_to_py_ms"])
    gc_s = sorted(gc_pauses_ms)
    lag_s = sorted(loop_lags_ms)
    counts = tel["counts"]
    total_ev = counts["quotes"] + counts["trades"] + counts["deltas"] + counts["mark_prices"]

    out: dict[str, Any] = {
        "mode": mode,
        "window_utc_ns": [t_utc_start, t_utc_end],
        "wall_secs": round(wall_s, 3),
        "counts": counts,
        "total_market_events": total_ev,
        "achieved_msg_s": round(total_ev / wall_s, 1),
        "client_cpu_pct_one_core": client_cpu_pct,
        "client_rss_start_mib": round(rss_start, 2),
        "client_rss_end_mib": round(rss_end, 2),
        "bmdg_cpu_pct_one_core": bmdg_cpu_pct,
        "bmdg_rss_mib": round(bmdg_rss, 2),
        "combined_client_plus_bmdg_cpu_pct_one_core": round(client_cpu_pct + bmdg_cpu_pct, 2),
        "combined_client_plus_bmdg_rss_mib": round(rss_end + bmdg_rss, 2),
        "recorder_spot_cpu_pct": _proc_pct(spot_rec_ticks_0, spot_rec_ticks_1),
        "recorder_usdm_cpu_pct": _proc_pct(usdm_rec_ticks_0, usdm_rec_ticks_1),
        "host_cpu_pct_all_cores": host_stats,
        "tcp_established_mid_window": tcp_estab_mid,
        "rust_to_py_dequeue_ms": {
            "p50": round(_pct(r2p, 0.50), 3),
            "p95": round(_pct(r2p, 0.95), 3),
            "p99": round(_pct(r2p, 0.99), 3),
            "max": round(r2p[-1], 3) if r2p else 0.0,
        },
        "gc_pause_ms": {
            "count": len(gc_s),
            "max": round(gc_s[-1], 3) if gc_s else 0.0,
        },
        "event_loop_lag_ms": {
            "p95": round(_pct(lag_s, 0.95), 3),
            "max": round(lag_s[-1], 3) if lag_s else 0.0,
        },
        "sample_trades_count": len(tel["sample_trades"]),
        "sample_trades": tel["sample_trades"],
        "status_events": tel["status_events"][:30],
    }
    if mode == "route_b":
        out["route_b_plus_slow_rest_probe"] = _route_b_plus_slow_rest_probe()
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("route_b", "route_a"), required=True)
    parser.add_argument("--duration", type=float, default=90.0)
    parser.add_argument("--load-ids-only", action="store_true")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    res = asyncio.run(
        _run_live_probe(
            mode=args.mode,
            duration_secs=args.duration,
            load_ids_only=bool(args.load_ids_only),
        )
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(res, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "sample_trades"}, indent=2))


if __name__ == "__main__":
    main()
