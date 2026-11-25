"""M-NT2.5 Route B Benchmark: Nautilus native Rust BinanceDataClientFactory synthetic + GAP + live probe."""

from __future__ import annotations

import argparse
import asyncio
import base64
from decimal import Decimal
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import struct
import sys
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

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
    InstrumentId,
    InstrumentStatus,
    Money,
    OmsType,
    OrderBookDeltas,
    TradeTick,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy  # noqa: E402

FIVE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
_BASE_PRICES = {
    "BTCUSDT": Decimal("65000.00"),
    "ETHUSDT": Decimal("3200.00"),
    "SOLUSDT": Decimal("180.00"),
    "BNBUSDT": Decimal("620.00"),
    "XRPUSDT": Decimal("1.4000"),
}
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _read_rss_mib() -> float:
    proc_status = Path("/proc/self/status")
    if proc_status.is_file():
        for line in proc_status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024.0 * 1024.0) if sys.platform == "darwin" else rss / 1024.0


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(q * (len(sorted_vals) - 1))))
    return float(sorted_vals[idx])


def _exchange_info_payload() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    syms = []
    for sym in FIVE_SYMBOLS:
        tick = "0.0001" if sym == "XRPUSDT" else "0.01"
        step = "0.001" if sym in ("BTCUSDT", "ETHUSDT") else ("1" if sym == "XRPUSDT" else "0.01")
        p_dp = 4 if sym == "XRPUSDT" else 2
        q_dp = 0 if sym == "XRPUSDT" else (3 if sym in ("BTCUSDT", "ETHUSDT") else 2)
        syms.append(
            {
                "symbol": sym,
                "pair": sym,
                "contractType": "PERPETUAL",
                "deliveryDate": 4133404800000,
                "onboardDate": 1569398400000,
                "status": "TRADING",
                "maintMarginPercent": "2.5000",
                "requiredMarginPercent": "5.0000",
                "baseAsset": sym[:-4],
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "pricePrecision": p_dp,
                "quantityPrecision": q_dp,
                "baseAssetPrecision": 8,
                "quotePrecision": 8,
                "underlyingType": "COIN",
                "underlyingSubType": [],
                "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"],
                "timeInForce": ["GTC", "IOC", "FOK", "GTX", "GTD"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": tick, "maxPrice": "1000000", "tickSize": tick},
                    {"filterType": "LOT_SIZE", "minQty": step, "maxQty": "1000000", "stepSize": step},
                    {"filterType": "MARKET_LOT_SIZE", "minQty": step, "maxQty": "1000000", "stepSize": step},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        )
    return {
        "timezone": "UTC",
        "serverTime": now_ms,
        "rateLimits": [
            {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
            {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1200},
        ],
        "exchangeFilters": [],
        "assets": [{"asset": "USDT", "marginAvailable": True, "autoAssetExchange": "-10000"}],
        "symbols": syms,
    }


def _pack_ws_text(payload: str | bytes) -> bytes:
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    n = len(data)
    if n < 126:
        return struct.pack("!BB", 0x81, n) + data
    if n < 65536:
        return struct.pack("!BBH", 0x81, 126, n) + data
    return struct.pack("!BBQ", 0x81, 127, n) + data


async def _read_ws_frame(reader: asyncio.StreamReader) -> tuple[int, bytes] | None:
    try:
        hdr = await reader.readexactly(2)
    except asyncio.IncompleteReadError:
        return None
    b1, b2 = hdr[0], hdr[1]
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = bytearray(await reader.readexactly(length))
    if masked:
        for i in range(length):
            payload[i] ^= mask[i % 4]
    return opcode, bytes(payload)


class _MockBinanceServer:
    """Minimal HTTP + RFC6455 WebSocket mock server for Binance USD-M Futures."""

    def __init__(
        self,
        *,
        rate_per_symbol_stream: float,
        duration_secs: float,
        levels_per_update: int = 5,
        gap_test: bool = False,
        snapshot_delay_ms: int = 0,
    ) -> None:
        self.rate = rate_per_symbol_stream
        self.duration = duration_secs
        self.levels = levels_per_update
        self.gap_test = gap_test
        self.snapshot_delay_ms = snapshot_delay_ms
        self._exchange_info_bytes = json.dumps(_exchange_info_payload()).encode("utf-8")
        self._snapshot_seq: dict[str, int] = {sym: 1 for sym in FIVE_SYMBOLS}
        self._depth_requests = 0
        self._all_subscribed: set[str] = set()

    def _build_snapshot_bytes(self, sym: str) -> bytes:
        base_px = _BASE_PRICES[sym]
        tick = Decimal("0.0001") if sym == "XRPUSDT" else Decimal("0.01")
        qty_s = "10" if sym == "XRPUSDT" else "0.500"
        uid = self._snapshot_seq.get(sym, 1)
        now_ms = int(time.time() * 1000)
        bids = [[str(base_px - tick * (lv + 1)), qty_s] for lv in range(self.levels)]
        asks = [[str(base_px + tick * (lv + 1)), qty_s] for lv in range(self.levels)]
        return json.dumps(
            {
                "lastUpdateId": uid,
                "E": now_ms,
                "T": now_ms,
                "bids": bids,
                "asks": asks,
            }
        ).encode("utf-8")

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw_hdr = await reader.readuntil(b"\r\n\r\n")
        except Exception:
            writer.close()
            return
        lines = raw_hdr.decode("utf-8", "ignore").split("\r\n")
        req_line = lines[0].split()
        if len(req_line) < 2:
            writer.close()
            return
        _, target = req_line[0], req_line[1]
        headers: dict[str, str] = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        if headers.get("upgrade", "").lower() == "websocket":
            await self._handle_ws(reader, writer, headers)
            return

        parsed = urlparse(target)
        path = parsed.path
        if path.endswith("/exchangeInfo"):
            body = self._exchange_info_bytes
        elif path.endswith("/depth"):
            qs = parse_qs(parsed.query)
            sym = qs.get("symbol", ["BTCUSDT"])[0].upper()
            self._depth_requests += 1
            if self.gap_test and self._depth_requests >= 2:
                # Bridge future GAP updates starting around uid 50
                self._snapshot_seq[sym] = 50
                if self.snapshot_delay_ms > 0:
                    await asyncio.sleep(self.snapshot_delay_ms / 1000.0)
            body = self._build_snapshot_bytes(sym)
        else:
            body = b"{}"
        resp = (
            f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n".encode(
                "ascii"
            )
            + body
        )
        writer.write(resp)
        await writer.drain()
        # Keep HTTP/1.1 connection alive for subsequent GETs
        await self.handle_conn(reader, writer)

    async def _handle_ws(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> None:
        key = headers.get("sec-websocket-key", "")
        accept = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()).decode("ascii")
        hs = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode("ascii")
        writer.write(hs)
        await writer.drain()

        subscribed: set[str] = set()
        writer_lock = asyncio.Lock()
        pump_task: asyncio.Task[None] | None = None
        stop_ws = False

        async def _send_text(txt: str) -> None:
            async with writer_lock:
                writer.write(_pack_ws_text(txt))

        async def _stream_pump() -> None:
            need_total = 1 if self.gap_test else (len(FIVE_SYMBOLS) * 2)
            while not stop_ws and (not subscribed or len(self._all_subscribed) < need_total):
                await asyncio.sleep(0.01)
            if stop_ws:
                return
            await asyncio.sleep(0.05)
            if self.gap_test:
                if any("depth" in s for s in subscribed):
                    await self._run_gap_sequence(_send_text, writer)
                return

            total_per_stream = max(1, int(self.rate * self.duration))
            last_uid = {s: 1 for s in FIVE_SYMBOLS}
            t0 = time.perf_counter()
            seq = 0
            agg_id = {s: 0 for s in FIVE_SYMBOLS}
            batch = 5 if self.rate > 250 else 1
            sub_lower = {s.lower() for s in subscribed}
            while not stop_ws and seq < total_per_stream:
                frames: list[bytes] = []
                for _ in range(batch):
                    if seq >= total_per_stream:
                        break
                    seq += 1
                    now_ms = int(time.time() * 1000)
                    for sym in FIVE_SYMBOLS:
                        base_px = _BASE_PRICES[sym]
                        tick = Decimal("0.0001") if sym == "XRPUSDT" else Decimal("0.01")
                        qty_s = "10" if sym == "XRPUSDT" else "0.500"
                        du_s = f"{sym.lower()}@depth@0ms"
                        tr_s = f"{sym.lower()}@aggtrade"
                        if du_s in sub_lower:
                            pu = last_uid[sym]
                            # First diff must satisfy U <= lastUpdateId(1) <= u to bridge Futures snapshot
                            first_u = 1 if seq == 1 else pu + 1
                            u = 2 if seq == 1 else pu + 1
                            last_uid[sym] = u
                            bids = [[str(base_px - tick * (lv + 1)), str(Decimal(qty_s) + Decimal(seq % 5) * (Decimal("1") if sym == "XRPUSDT" else Decimal("0.001")))] for lv in range(self.levels)]
                            frames.append(
                                _pack_ws_text(
                                    json.dumps(
                                        {
                                            "e": "depthUpdate",
                                            "E": now_ms,
                                            "T": now_ms,
                                            "s": sym,
                                            "U": first_u,
                                            "u": u,
                                            "pu": pu,
                                            "b": bids,
                                            "a": [],
                                        }
                                    )
                                )
                            )
                        if tr_s in sub_lower:
                            agg_id[sym] += 1
                            aid = agg_id[sym]
                            frames.append(
                                _pack_ws_text(
                                    json.dumps(
                                        {
                                            "e": "aggTrade",
                                            "E": now_ms,
                                            "s": sym,
                                            "a": aid,
                                            "p": str(base_px),
                                            "q": qty_s,
                                            "f": aid,
                                            "l": aid,
                                            "T": now_ms,
                                            "m": bool(aid % 2),
                                        }
                                    )
                                )
                            )
                if frames:
                    async with writer_lock:
                        writer.write(b"".join(frames))
                        await writer.drain()
                target_elapsed = seq / self.rate
                actual = time.perf_counter() - t0
                if target_elapsed > actual:
                    await asyncio.sleep(target_elapsed - actual)
                else:
                    await asyncio.sleep(0)

        pump_task = asyncio.create_task(_stream_pump())
        try:
            while True:
                frame = await _read_ws_frame(reader)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    async with writer_lock:
                        writer.write(struct.pack("!BB", 0x8A, len(payload)) + payload)
                        await writer.drain()
                    continue
                if opcode == 0x1:
                    msg = json.loads(payload.decode("utf-8"))
                    method = msg.get("method")
                    req_id = msg.get("id")
                    if method == "SUBSCRIBE":
                        for p in msg.get("params", []):
                            sp = str(p)
                            subscribed.add(sp)
                            self._all_subscribed.add(sp.lower())
                        await _send_text(json.dumps({"result": None, "id": req_id}))
                        async with writer_lock:
                            await writer.drain()
                    elif method == "UNSUBSCRIBE":
                        await _send_text(json.dumps({"result": None, "id": req_id}))
                        async with writer_lock:
                            await writer.drain()
        finally:
            stop_ws = True
            if pump_task:
                pump_task.cancel()
            writer.close()

    async def _run_gap_sequence(self, send_text: Any, writer: asyncio.StreamWriter) -> None:
        sym = "BTCUSDT"
        now_ms = int(time.time() * 1000)
        # 1. Valid seam update linking initial snapshot uid=1
        await send_text(
            json.dumps(
                {
                    "e": "depthUpdate",
                    "E": now_ms,
                    "T": now_ms,
                    "s": sym,
                    "U": 1,
                    "u": 2,
                    "pu": 1,
                    "b": [["64999.00", "1.000"]],
                    "a": [["65001.00", "1.000"]],
                }
            )
        )
        await writer.drain()
        await asyncio.sleep(0.05)
        # 2. Inject sequence GAP: pu=49 != previous u=2 -> triggers Recover + async REST snapshot!
        now_ms = int(time.time() * 1000)
        await send_text(
            json.dumps(
                {
                    "e": "depthUpdate",
                    "E": now_ms,
                    "T": now_ms,
                    "s": sym,
                    "U": 50,
                    "u": 52,
                    "pu": 49,
                    "b": [["65010.00", "2.000"]],
                    "a": [["65011.00", "2.000"]],
                }
            )
        )
        await writer.drain()
        await asyncio.sleep(max(0.1, (self.snapshot_delay_ms / 1000.0) + 0.15))
        # 3. Continue cleanly after recovery snapshot (lastUpdateId=50)
        now_ms = int(time.time() * 1000)
        await send_text(
            json.dumps(
                {
                    "e": "depthUpdate",
                    "E": now_ms,
                    "T": now_ms,
                    "s": sym,
                    "U": 53,
                    "u": 54,
                    "pu": 52,
                    "b": [["65010.50", "3.000"]],
                    "a": [["65012.00", "3.000"]],
                }
            )
        )
        await writer.drain()


class NativeL2BenchStrategyConfig(StrategyConfig):
    def __init__(
        self,
        *,
        instrument_ids: list[InstrumentId],
        telemetry: dict[str, Any] | None = None,
        gap_probe: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_ids = instrument_ids
        self.telemetry = telemetry if telemetry is not None else {}
        self.gap_probe = gap_probe


class NativeL2BenchStrategy(Strategy):
    def __init__(self, config: NativeL2BenchStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.tel = config.telemetry
        self.tel.setdefault("deltas_events", 0)
        self.tel.setdefault("trade_events", 0)
        self.tel.setdefault("rust_to_py_latencies_ms", [])
        self.tel.setdefault("wire_to_cb_latencies_ms", [])
        self.tel.setdefault("book_clears_observed", 0)
        self.tel.setdefault("status_halts_observed", 0)
        self.tel.setdefault("delta_timeline", [])

    def on_start(self) -> None:
        route = ClientId("BINANCE")
        for inst_id in self._cfg.instrument_ids:
            self.subscribe_book_deltas(inst_id, BookType.L2_MBP, managed=True, client_id=route)
            if not self._cfg.gap_probe:
                self.subscribe_trades(inst_id, client_id=route)
            self.subscribe_instrument_status(inst_id, client_id=route)

    def on_instrument_status(self, status: InstrumentStatus) -> None:
        if not status.is_trading:
            self.tel["status_halts_observed"] += 1

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        cb_ns = time.time_ns()
        self.tel["deltas_events"] += 1
        self.tel["rust_to_py_latencies_ms"].append(max(0.0, (cb_ns - int(deltas.ts_init)) / 1_000_000.0))
        self.tel["wire_to_cb_latencies_ms"].append(max(0.0, (cb_ns - int(deltas.ts_event)) / 1_000_000.0))
        has_clear = any(d.action == BookAction.CLEAR for d in deltas.deltas)
        if has_clear:
            self.tel["book_clears_observed"] += 1
        if self._cfg.gap_probe:
            self.tel["delta_timeline"].append(
                {
                    "cb_ns": cb_ns,
                    "ts_event": int(deltas.ts_event),
                    "has_clear": has_clear,
                    "num_deltas": len(deltas.deltas),
                    "sequence": int(deltas.deltas[-1].sequence) if deltas.deltas else 0,
                }
            )

    def on_trade(self, trade: TradeTick) -> None:
        cb_ns = time.time_ns()
        self.tel["trade_events"] += 1
        self.tel["rust_to_py_latencies_ms"].append(max(0.0, (cb_ns - int(trade.ts_init)) / 1_000_000.0))
        self.tel["wire_to_cb_latencies_ms"].append(max(0.0, (cb_ns - int(trade.ts_event)) / 1_000_000.0))


async def _serve_forever(rate: float, duration: float, levels: int, gap_test: bool, snapshot_delay_ms: int) -> None:
    srv = _MockBinanceServer(
        rate_per_symbol_stream=rate,
        duration_secs=duration,
        levels_per_update=levels,
        gap_test=gap_test,
        snapshot_delay_ms=snapshot_delay_ms,
    )
    server = await asyncio.start_server(srv.handle_conn, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    print(f"READY {port}", flush=True)
    async with server:
        await server.serve_forever()


async def _run_stage(
    *,
    name: str,
    rate_per_symbol_stream: float,
    duration_secs: float,
    levels_per_update: int = 5,
    gap_test: bool = False,
    snapshot_delay_ms: int = 0,
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
        "--snapshot-delay-ms",
        str(snapshot_delay_ms),
        *(["--gap-test"] if gap_test else []),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None
    ready_line = (await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)).decode("utf-8").strip()
    if not ready_line.startswith("READY "):
        err_bytes = await proc.stderr.read() if proc.stderr else b""
        raise RuntimeError(f"Mock Binance server failed: {ready_line!r} {err_bytes.decode('utf-8', 'ignore')}")
    port = int(ready_line.split()[1])

    inst_ids = [
        InstrumentId.from_str(f"{sym}-PERP.BINANCE")
        for sym in (["BTCUSDT"] if gap_test else FIVE_SYMBOLS)
    ]
    node = (
        LiveNode.builder(f"MNT25-ROUTEB-{name.upper()}", TraderId("MNT25-B"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
        .with_timeout_connection(5)
        .with_timeout_disconnection_secs(2)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client(
            "BINANCE",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                base_url_http=f"http://127.0.0.1:{port}",
                base_url_ws=f"ws://127.0.0.1:{port}/ws",
                instrument_provider=BinanceInstrumentProviderConfig(load_all=True),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue("BINANCE"),
                account_id=AccountId("BINANCE-ROUTEB"),
                account_type=AccountType.MARGIN,
                oms_type=OmsType.NETTING,
                book_type=BookType.L2_MBP,
                starting_balances=[Money.from_str("100000.00 USDT")],
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
            ),
        )
        .build()
    )
    tel: dict[str, Any] = {}
    strat = NativeL2BenchStrategy(
        NativeL2BenchStrategyConfig(
            instrument_ids=inst_ids,
            telemetry=tel,
            gap_probe=gap_test,
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
        interval = 0.02
        expected = time.perf_counter() + interval
        while not stop_flag:
            await asyncio.sleep(interval)
            now = time.perf_counter()
            loop_lags_ms.append(max(0.0, (now - expected) * 1000.0))
            expected = now + interval

    expected_total = 4 if gap_test else len(FIVE_SYMBOLS) * 2 * max(1, int(rate_per_symbol_stream * duration_secs))

    async def _stopper() -> None:
        deadline = time.perf_counter() + (5.0 if gap_test else duration_secs + 12.0)
        while time.perf_counter() < deadline:
            seen = int(tel.get("deltas_events", 0)) + int(tel.get("trade_events", 0))
            if seen >= expected_total:
                break
            await asyncio.sleep(0.03)
        await asyncio.sleep(0.1)
        strat.shutdown_system("route_b bench complete")

    gc.collect()
    gc.callbacks.append(_gc_cb)
    rss_start = _read_rss_mib()
    ru_start = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    mon_task = asyncio.create_task(_loop_monitor())
    stop_task = asyncio.create_task(_stopper())
    try:
        await asyncio.wait_for(node.run_async(), timeout=(8.0 if gap_test else duration_secs + 18.0))
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

    r2p = sorted(tel["rust_to_py_latencies_ms"])
    w2c = sorted(tel["wire_to_cb_latencies_ms"])
    gc_s = sorted(gc_pauses_ms)
    lag_s = sorted(loop_lags_ms)
    total_ev = int(tel["deltas_events"]) + int(tel["trade_events"])

    res: dict[str, Any] = {
        "stage": name,
        "wall_secs": round(wall_s, 3),
        "deltas_events": int(tel["deltas_events"]),
        "trade_events": int(tel["trade_events"]),
        "total_events": total_ev,
        "expected_items": expected_total,
        "achieved_msg_s": round(total_ev / wall_s, 1),
        "cpu_secs_client_only": round(cpu_s, 3),
        "cpu_pct_one_core_client_only": round(cpu_pct, 2),
        "rss_start_mib": round(rss_start, 2),
        "rss_end_mib": round(rss_end, 2),
        "rss_delta_mib": round(rss_end - rss_start, 2),
        "rust_to_py_dequeue_ms": {
            "p50": round(_pct(r2p, 0.50), 3),
            "p95": round(_pct(r2p, 0.95), 3),
            "p99": round(_pct(r2p, 0.99), 3),
            "max": round(r2p[-1], 3) if r2p else 0.0,
        },
        "wire_to_callback_ms": {
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
    if gap_test:
        res["gap_recovery_audit"] = {
            "status_halts_observed": int(tel["status_halts_observed"]),
            "book_clears_observed": int(tel["book_clears_observed"]),
            "snapshot_delay_injected_ms": snapshot_delay_ms,
            "timeline": tel["delta_timeline"],
        }
    return res


def main() -> None:
    parser = argparse.ArgumentParser(description="M-NT2.5 Route B Native Rust Binance Benchmark")
    parser.add_argument("--role", choices=("client", "server"), default="client")
    parser.add_argument("--stage", choices=("nominal", "burst", "gap", "all"), default="all")
    parser.add_argument("--rate", type=float, default=50.0)
    parser.add_argument("--duration", type=float, default=8.0)
    parser.add_argument("--levels", type=int, default=5)
    parser.add_argument("--snapshot-delay-ms", type=int, default=250)
    parser.add_argument("--gap-test", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    if args.role == "server":
        asyncio.run(_serve_forever(args.rate, args.duration + 6.0, args.levels, args.gap_test, args.snapshot_delay_ms))
        return

    async def _run() -> dict[str, Any]:
        results: dict[str, Any] = {
            "host": os.uname().nodename,
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "adapter": "nautilus_trader.adapters.binance.BinanceDataClientFactory (USD_M native Rust)",
        }
        if args.stage in ("nominal", "all"):
            results["nominal_500_msg_s"] = await _run_stage(
                name="nominal",
                rate_per_symbol_stream=50.0,
                duration_secs=args.duration,
            )
        if args.stage in ("burst", "all"):
            results["burst_3000_msg_s"] = await _run_stage(
                name="burst",
                rate_per_symbol_stream=300.0,
                duration_secs=args.duration,
            )
        if args.stage in ("gap", "all"):
            results["gap_recovery_probe"] = await _run_stage(
                name="gap",
                rate_per_symbol_stream=20.0,
                duration_secs=2.0,
                gap_test=True,
                snapshot_delay_ms=args.snapshot_delay_ms,
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
