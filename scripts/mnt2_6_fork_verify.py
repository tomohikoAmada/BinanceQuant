"""M-NT2.6 Step 2 verification for nautilus_trader 2.0.0rc9+bmd1 fork patches.

Covers:
1. --mode mock-gap: End-to-end 4-scenario GAP/reconnect test inside a real LiveNode
   with BinanceDataClientFactory(USD_M) -> stdlib mock HTTP/WS server:
   (a) Drop 1 diff packet (`pu` mismatch -> immediate CLEAR -> 0 opens during GAP -> resync)
   (b) Stale baseline snapshot (`lastUpdateId` too old -> retried until valid snapshot bridges)
   (c) WebSocket disconnect + stale frame post-reconnect (`reset_on_reconnect` CLEAR -> resync)
   (d) Consecutive double-gap (single immediate CLEAR -> suppressed second gap -> single resync)
   Proves `@aggTrade` keeps flowing during L2 lockout and `submit_protective_exit` still works.
2. --mode live-fork: Real Binance Global test via 2.0.0rc9+bmd1 without URL overrides for Spot:
   - Spot: `us=False`, `spot_market_data_mode=Json`, `use_spot_agg_trade=True`
   - USD-M: `futures_diff_depth_interval_ms=100`
"""

from __future__ import annotations

import argparse
import asyncio
import base64
from decimal import Decimal
import hashlib
import json
from pathlib import Path
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
    Money,
    OmsType,
    OrderBookDeltas,
    QuoteTick,
    TradeTick,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy  # noqa: E402

from bq_nautilus.patterns import (  # noqa: E402
    L2MicrostructureStrategy,
    L2MicrostructureStrategyConfig,
)

WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_encode_text(payload: str) -> bytes:
    data = payload.encode("utf-8")
    n = len(data)
    if n < 126:
        header = struct.pack("!BB", 0x81, n)
    elif n < 65536:
        header = struct.pack("!BBH", 0x81, 126, n)
    else:
        header = struct.pack("!BBQ", 0x81, 127, n)
    return header + data


async def _ws_read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes] | None:
    try:
        b1, b2 = await reader.readexactly(2)
    except asyncio.IncompleteReadError:
        return None
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
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


class _MockUsdmGapServer:
    """Minimal stdlib HTTP + WebSocket server driving 4 deterministic GAP/reconnect episodes."""

    def __init__(self) -> None:
        self.depth_call_count = 0
        self.scenario_log: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []
        self.ws_connections = 0
        self.subscribed_streams: list[str] = []
        self.depth_subscribed_event = asyncio.Event()
        self.trade_subscribed_event = asyncio.Event()
        self.second_conn_subscribed = asyncio.Event()
        self.public_writer: asyncio.StreamWriter | None = None
        self.market_writer: asyncio.StreamWriter | None = None
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0
        self._tasks: list[asyncio.Task[Any]] = []
        self._write_lock = asyncio.Lock()

    def _exchange_info_json(self) -> str:
        return json.dumps(
            {
                "timezone": "UTC",
                "serverTime": 1_700_000_000_000,
                "futuresType": "U_MARGINED",
                "rateLimits": [],
                "exchangeFilters": [],
                "assets": [{"asset": "USDT", "marginAvailable": True, "autoAssetExchange": "-10000"}],
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "pair": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "deliveryDate": 4133404800000,
                        "onboardDate": 1569398400000,
                        "status": "TRADING",
                        "maintMarginPercent": "2.5000",
                        "requiredMarginPercent": "5.0000",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "pricePrecision": 2,
                        "quantityPrecision": 3,
                        "baseAssetPrecision": 8,
                        "quotePrecision": 8,
                        "underlyingType": "COIN",
                        "settlePlan": 0,
                        "triggerProtect": "0.0500",
                        "liquidationFee": "0.012500",
                        "marketTakeBound": "0.05",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "maxPrice": "1000000", "minPrice": "0.10", "tickSize": "0.10"},
                            {"filterType": "LOT_SIZE", "maxQty": "1000", "minQty": "0.001", "stepSize": "0.001"},
                            {"filterType": "MARKET_LOT_SIZE", "maxQty": "1000", "minQty": "0.001", "stepSize": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                        "orderTypes": ["LIMIT", "MARKET"],
                        "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
                    }
                ],
            }
        )

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle_conn, "127.0.0.1", 0)
        sock = self.server.sockets[0]
        self.port = int(sock.getsockname()[1])

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self.public_writer is not None:
            self.public_writer.close()
        if self.market_writer is not None:
            self.market_writer.close()
        if self.server is not None:
            self.server.close()

    async def _send_ws(self, writer: asyncio.StreamWriter, obj: dict[str, Any]) -> None:
        async with self._write_lock:
            writer.write(_ws_encode_text(json.dumps(obj)))
            await writer.drain()

    async def send_public_diff(self, u_start: int, u_end: int, pu: int, bid_sz: str = "5.000") -> None:
        assert self.public_writer is not None
        ts_ms = int(time.time() * 1000)
        await self._send_ws(
            self.public_writer,
            {
                "e": "depthUpdate",
                "E": ts_ms,
                "T": ts_ms - 1,
                "s": "BTCUSDT",
                "U": u_start,
                "u": u_end,
                "pu": pu,
                "b": [["60000.00", bid_sz]],
                "a": [["60001.00", "1.000"]],
            },
        )

    async def send_market_agg_trade(self, aid: int, px: str = "60000.50") -> None:
        if self.market_writer is None:
            return
        ts_ms = int(time.time() * 1000)
        await self._send_ws(
            self.market_writer,
            {
                "e": "aggTrade",
                "E": ts_ms,
                "a": aid,
                "s": "BTCUSDT",
                "p": px,
                "q": "0.100",
                "f": aid * 10,
                "l": aid * 10,
                "T": ts_ms - 1,
                "m": False,
            },
        )

    async def drop_public_ws(self) -> None:
        if self.public_writer is not None:
            self.public_writer.close()
            try:
                await self.public_writer.wait_closed()
            except Exception:
                pass
            self.public_writer = None

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw_hdr = await reader.readuntil(b"\r\n\r\n")
        except Exception:
            writer.close()
            return
        lines = raw_hdr.decode("latin1").split("\r\n")
        req_line = lines[0].split(" ")
        target = req_line[1] if len(req_line) >= 2 else "/"
        headers: dict[str, str] = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        if headers.get("upgrade", "").lower() == "websocket":
            key = headers["sec-websocket-key"]
            accept = base64.b64encode(hashlib.sha1((key + WS_MAGIC).encode("ascii")).digest()).decode("ascii")
            handshake = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            writer.write(handshake.encode("ascii"))
            await writer.drain()
            await self._ws_loop(target, reader, writer)
            return

        parsed = urlparse(target)
        path = parsed.path
        _ = parse_qs(parsed.query)
        if path.endswith("/exchangeInfo"):
            body = self._exchange_info_json()
        elif path.endswith("/depth"):
            self.depth_call_count += 1
            if self.snapshots:
                snap = self.snapshots.pop(0)
            else:
                snap = {
                    "lastUpdateId": 100,
                    "E": int(time.time() * 1000),
                    "T": int(time.time() * 1000) - 1,
                    "bids": [["60000.00", "5.000"]],
                    "asks": [["60001.00", "1.000"]],
                }
            delay_s = float(snap.pop("_delay_s", 0.0))
            if delay_s > 0.0:
                await asyncio.sleep(delay_s)
            self.scenario_log.append({"event": "rest_depth_served", "call": self.depth_call_count, "lastUpdateId": snap["lastUpdateId"], "ts_ns": time.time_ns()})
            body = json.dumps(snap)
        else:
            body = "{}"

        resp = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: keep-alive\r\n\r\n"
            f"{body}"
        )
        writer.write(resp.encode("utf-8"))
        await writer.drain()
        await self._handle_conn(reader, writer)

    async def _ws_loop(self, target: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.ws_connections += 1
        is_public = "/public" in target or "depth" in target
        if is_public:
            self.public_writer = writer
        elif "/market" in target:
            self.market_writer = writer
        try:
            while True:
                frame = await _ws_read_frame(reader)
                if frame is None:
                    break
                opcode, data = frame
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    writer.write(b"\x8a" + bytes([len(data)]) + data)
                    await writer.drain()
                    continue
                if opcode != 0x1:
                    continue
                msg = json.loads(data.decode("utf-8"))
                method = msg.get("method")
                if method == "SUBSCRIBE":
                    params = list(msg.get("params") or [])
                    self.subscribed_streams.extend(params)
                    await self._send_ws(writer, {"result": None, "id": msg.get("id", 1)})
                    for p in params:
                        if "@depth" in p:
                            if not self.depth_subscribed_event.is_set():
                                self.public_writer = writer
                                self.depth_subscribed_event.set()
                            else:
                                self.public_writer = writer
                                self.second_conn_subscribed.set()
                        if "@aggTrade" in p:
                            self.market_writer = writer
                            self.trade_subscribed_event.set()
                elif method == "UNSUBSCRIBE":
                    await self._send_ws(writer, {"result": None, "id": msg.get("id", 1)})
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


async def run_mock_gap_4_scenarios() -> dict[str, Any]:
    srv = _MockUsdmGapServer()
    await srv.start()
    btc_perp = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    tel: dict[str, Any] = {}
    node = (
        LiveNode.builder("MNT26-GAP-VERIFY", TraderId("MNT26-GAP"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
        .with_timeout_connection(10)
        .with_timeout_disconnection_secs(2)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client(
            "BINANCE-USDM",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                base_url_http=f"http://127.0.0.1:{srv.port}",
                base_url_ws=f"ws://127.0.0.1:{srv.port}",
                futures_diff_depth_interval_ms=100,
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(btc_perp)]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
                retry_delay_initial_ms=50,
                retry_delay_max_ms=150,
            ),
        )
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue("BINANCE"),
                account_id=AccountId("BINANCE-GAP-SIM"),
                account_type=AccountType.MARGIN,
                oms_type=OmsType.NETTING,
                book_type=BookType.L2_MBP,
                starting_balances=[Money.from_str("100000.00 USDT")],
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
            ),
        )
        .build()
    )
    strat = L2MicrostructureStrategy(
        L2MicrostructureStrategyConfig(
            instrument_id=btc_perp,
            order_qty="0.010",
            imbalance_threshold=0.2,
            data_client_id="BINANCE-USDM",
            exec_client_id="BINANCE",
            telemetry=tel,
        )
    )
    node.add_strategy(strat)

    scenario_results: dict[str, Any] = {}

    async def _driver() -> None:
        print("[mock-gap] waiting for subscribe...", flush=True)
        await asyncio.wait_for(srv.depth_subscribed_event.wait(), timeout=8.0)
        await asyncio.wait_for(srv.trade_subscribed_event.wait(), timeout=8.0)
        print("[mock-gap] subscribed; running baseline", flush=True)

        # Seed trade price first so RiskEngine + SandboxExecutionClient can price initial MARKET entry
        await srv.send_market_agg_trade(9000, "60000.50")
        await asyncio.sleep(0.05)

        # Seed initial snapshot (lastUpdateId=100) + Scenario 2 test: first return stale snapshot(95) before valid snapshot(100)
        srv.snapshots.append({"lastUpdateId": 95, "E": 1700000000000, "T": 1700000000000, "bids": [["60000.00", "5.000"]], "asks": [["60001.00", "1.000"]]})
        srv.snapshots.append({"lastUpdateId": 100, "E": 1700000000010, "T": 1700000000010, "bids": [["60000.00", "5.000"]], "asks": [["60001.00", "1.000"]]})

        # Send initial bridging diff (U=99..u=102, pu=98)
        await srv.send_public_diff(99, 102, 98, bid_sz="5.000")
        for _ in range(100):
            if tel.get("orders_submitted", 0) >= 1 and strat.portfolio.is_net_long(btc_perp):
                break
            await asyncio.sleep(0.02)
        assert tel.get("orders_submitted", 0) >= 1, f"expected initial long entry; tel={tel}"
        scenario_results["baseline_synced_and_opened"] = {
            "orders_submitted": tel["orders_submitted"],
            "depth_calls_including_stale_retry": srv.depth_call_count,
            "is_net_long": strat.portfolio.is_net_long(btc_perp),
        }

        # Scenario 1: Drop 1 diff packet -> pu=99 != prev_u=102 -> immediate CLEAR + lockout
        t_gap1_inject = time.time_ns()
        orders_before_gap1 = tel["orders_submitted"]
        trades_before_gap1 = tel["trade_events"]
        halts_before_gap1 = tel["halt_events"]
        # Enqueue recovery snapshot(107) with 120ms network delay so we cleanly observe the degraded GAP window
        srv.snapshots.append({"_delay_s": 0.12, "lastUpdateId": 107, "E": int(time.time() * 1000) + 120, "T": int(time.time() * 1000) + 120, "bids": [["60000.00", "4.000"]], "asks": [["60001.00", "1.000"]]})
        await srv.send_public_diff(105, 108, 99, bid_sz="9.000")
        await asyncio.sleep(0.02)
        await srv.send_market_agg_trade(9001, "60000.20")
        for _ in range(50):
            if tel["halt_events"] > halts_before_gap1 and tel["trade_events"] > trades_before_gap1:
                break
            await asyncio.sleep(0.01)

        # Protective cancel/reduce-only exit MUST still work during GAP lockout!
        exited = strat.submit_protective_exit()
        await asyncio.sleep(0.02)
        orders_during_gap1 = tel["orders_submitted"] - orders_before_gap1
        # Wait for 120ms recovery snapshot(107) to arrive and restore book, then send linked diff
        for _ in range(80):
            if tel.get("recovery_events", 0) >= 1:
                break
            await asyncio.sleep(0.02)
        await srv.send_public_diff(109, 112, 108, bid_sz="4.000")
        for _ in range(80):
            if tel.get("recovery_events", 0) >= 1:
                break
            await asyncio.sleep(0.02)
        scenario_results["scenario_1_single_packet_gap"] = {
            "t_inject_ns": t_gap1_inject,
            "clear_ts_init_ns": tel.get("last_clear_ts_init_ns"),
            "strategy_halt_wall_ns": tel.get("last_halt_wall_ns"),
            "recovery_snapshot_ts_init_ns": tel.get("last_recovery_ts_init_ns"),
            "strategy_recovery_wall_ns": tel.get("last_recovery_wall_ns"),
            "rest_depth_events": [row for row in srv.scenario_log if row["lastUpdateId"] == 107],
            "halt_events": tel["halt_events"],
            "clear_events": tel["clear_events"],
            "trades_while_depth_bad": tel["trades_while_depth_bad"],
            "new_opens_during_gap": orders_during_gap1,
            "protective_exit_executed": exited,
            "protective_exits_submitted": tel["protective_exits_submitted"],
            "recovery_events": tel["recovery_events"],
        }
        assert tel["clear_events"] >= 1
        assert orders_during_gap1 == 0
        assert exited is True
        assert tel["trades_while_depth_bad"] >= 1
        assert tel["recovery_events"] >= 1

        # Scenario 4: Consecutive double-gap (`pu=90` gap1, followed immediately by `u=120..122, pu=119` before snapshot)
        clears_before_4 = tel["clear_events"]
        rec_before_4 = tel["recovery_events"]
        orders_before_4 = tel["orders_submitted"]
        # Make snapshot delay slightly: first send stale(112), then valid(121)
        srv.snapshots.append({"lastUpdateId": 112, "E": int(time.time() * 1000), "T": int(time.time() * 1000), "bids": [["60000.00", "3.000"]], "asks": [["60001.00", "1.000"]]})
        srv.snapshots.append({"lastUpdateId": 121, "E": int(time.time() * 1000), "T": int(time.time() * 1000), "bids": [["60000.00", "3.000"]], "asks": [["60001.00", "1.000"]]})
        await srv.send_public_diff(115, 118, 90, bid_sz="8.000")
        await asyncio.sleep(0.01)
        await srv.send_public_diff(119, 122, 118, bid_sz="8.000")
        await srv.send_market_agg_trade(9002, "60000.30")
        for _ in range(100):
            if tel["recovery_events"] > rec_before_4:
                break
            await asyncio.sleep(0.02)
        clears_emitted_double = tel["clear_events"] - clears_before_4
        scenario_results["scenario_4_double_gap_single_clear"] = {
            "clears_emitted": clears_emitted_double,
            "new_opens_during_double_gap_or_wait": tel["orders_submitted"] - orders_before_4,
            "recovery_events_after": tel["recovery_events"],
        }
        assert clears_emitted_double == 1, f"expected single CLEAR on double gap, got {clears_emitted_double}"

        # Scenario 3: WS disconnect + reconnect -> reset_on_reconnect emits CLEAR, old frame ignored/reset
        clears_before_3 = tel["clear_events"]
        rec_before_3 = tel["recovery_events"]
        await srv.drop_public_ws()
        await asyncio.wait_for(srv.second_conn_subscribed.wait(), timeout=8.0)
        srv.snapshots.append({"lastUpdateId": 130, "E": int(time.time() * 1000), "T": int(time.time() * 1000), "bids": [["60000.00", "2.000"]], "asks": [["60001.00", "1.000"]]})
        # Stale frame from pre-reconnect seq 120 must still require bridging snapshot(130)
        await srv.send_public_diff(128, 132, 127, bid_sz="2.000")
        for _ in range(100):
            if tel["recovery_events"] > rec_before_3:
                break
            await asyncio.sleep(0.02)
        scenario_results["scenario_3_ws_reconnect_clear_and_resync"] = {
            "clears_emitted_on_reconnect": tel["clear_events"] - clears_before_3,
            "recovery_events_after": tel["recovery_events"],
            "ws_connections_total": srv.ws_connections,
        }
        assert tel["clear_events"] - clears_before_3 >= 1
        assert tel["recovery_events"] > rec_before_3

        strat.shutdown_system("mock-gap 4-scenario verification passed")

    async def _driver_wrapped() -> None:
        try:
            await _driver()
        except Exception as exc:
            print(f"[mock-gap] driver error: {exc!r}", flush=True)
            strat.shutdown_system("driver exception")
            raise
        finally:
            await srv.stop()

    drv = asyncio.create_task(_driver_wrapped())
    try:
        await asyncio.wait_for(node.run_async(), timeout=20.0)
        if drv.done():
            await drv
    finally:
        if not drv.done():
            drv.cancel()
        node.dispose()
        await srv.stop()

    return {
        "mode": "mock_gap_4_scenarios",
        "subscribed_streams": srv.subscribed_streams,
        "telemetry": {k: v for k, v in tel.items() if k != "top_levels_seen"},
        "scenarios": scenario_results,
    }


class _LiveForkStratConfig(StrategyConfig):
    def __init__(self, *, spot_ids: list[InstrumentId], usdm_ids: list[InstrumentId], tel: dict[str, Any], **kw: Any) -> None:
        super().__init__(**kw)
        self.spot_ids = spot_ids
        self.usdm_ids = usdm_ids
        self.tel = tel


class _LiveForkStrat(Strategy):
    def __init__(self, cfg: _LiveForkStratConfig) -> None:
        super().__init__(cfg)
        self._cfg = cfg
        self.tel = cfg.tel
        self.tel.setdefault("spot_trades", 0)
        self.tel.setdefault("spot_quotes", 0)
        self.tel.setdefault("spot_deltas", 0)
        self.tel.setdefault("usdm_trades", 0)
        self.tel.setdefault("usdm_quotes", 0)
        self.tel.setdefault("usdm_deltas", 0)
        self.tel.setdefault("spot_trade_ids", [])

    def on_start(self) -> None:
        sc = ClientId("BINANCE-SPOT")
        for iid in self._cfg.spot_ids:
            self.subscribe_trades(iid, client_id=sc)
            self.subscribe_quotes(iid, client_id=sc)
            self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=sc)
        uc = ClientId("BINANCE-USDM")
        for iid in self._cfg.usdm_ids:
            self.subscribe_trades(iid, client_id=uc)
            self.subscribe_quotes(iid, client_id=uc)
            self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=uc)

    def on_trade(self, trade: TradeTick) -> None:
        if trade.instrument_id.symbol.value.endswith("-PERP"):
            self.tel["usdm_trades"] += 1
        else:
            self.tel["spot_trades"] += 1
            if len(self.tel["spot_trade_ids"]) < 10:
                self.tel["spot_trade_ids"].append({"sym": trade.instrument_id.symbol.value, "id": str(trade.trade_id), "px": str(trade.price)})

    def on_quote(self, quote: QuoteTick) -> None:
        if quote.instrument_id.symbol.value.endswith("-PERP"):
            self.tel["usdm_quotes"] += 1
        else:
            self.tel["spot_quotes"] += 1

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        if deltas.instrument_id.symbol.value.endswith("-PERP"):
            self.tel["usdm_deltas"] += 1
        else:
            self.tel["spot_deltas"] += 1


async def run_live_fork_smoke(duration_secs: float) -> dict[str, Any]:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
    spot_ids = [InstrumentId.from_str(f"{s}.BINANCE") for s in symbols]
    usdm_ids = [InstrumentId.from_str(f"{s}-PERP.BINANCE") for s in symbols]
    tel: dict[str, Any] = {}
    node = (
        LiveNode.builder("MNT26-LIVE-FORK", TraderId("MNT26-FORK"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
        .with_timeout_connection(15)
        .with_timeout_disconnection_secs(3)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client(
            "BINANCE-SPOT",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.SPOT,
                environment=BinanceEnvironment.LIVE,
                spot_market_data_mode=BinanceSpotMarketDataMode.Json,
                us=False,  # Global Spot JSON REST without us=True override!
                use_spot_agg_trade=True,  # Opt-in Spot @aggTrade!
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(x) for x in spot_ids]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        .add_data_client(
            "BINANCE-USDM",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                futures_diff_depth_interval_ms=100,  # @depth@100ms matching BMDR!
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(x) for x in usdm_ids]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue("BINANCE"),
                account_id=AccountId("BINANCE-FORK-SIM"),
                account_type=AccountType.MARGIN,
                oms_type=OmsType.NETTING,
                book_type=BookType.L2_MBP,
                starting_balances=[Money.from_str("10000.00 USDT")],
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
            ),
        )
        .build()
    )
    strat = _LiveForkStrat(_LiveForkStratConfig(spot_ids=spot_ids, usdm_ids=usdm_ids, tel=tel))
    node.add_strategy(strat)

    async def _stop() -> None:
        await asyncio.sleep(max(4.0, duration_secs * 0.45))
        sc = ClientId("BINANCE-SPOT")
        uc = ClientId("BINANCE-USDM")
        xrp_s, xrp_u = spot_ids[-1], usdm_ids[-1]
        strat.unsubscribe_trades(xrp_s, client_id=sc)
        strat.unsubscribe_quotes(xrp_s, client_id=sc)
        strat.unsubscribe_book_deltas(xrp_s, client_id=sc)
        strat.unsubscribe_trades(xrp_u, client_id=uc)
        strat.unsubscribe_quotes(xrp_u, client_id=uc)
        strat.unsubscribe_book_deltas(xrp_u, client_id=uc)
        await asyncio.sleep(1.5)
        strat.subscribe_trades(xrp_s, client_id=sc)
        strat.subscribe_quotes(xrp_s, client_id=sc)
        strat.subscribe_book_deltas(xrp_s, BookType.L2_MBP, managed=True, client_id=sc)
        strat.subscribe_trades(xrp_u, client_id=uc)
        strat.subscribe_quotes(xrp_u, client_id=uc)
        strat.subscribe_book_deltas(xrp_u, BookType.L2_MBP, managed=True, client_id=uc)
        tel["unsubscribe_resubscribe_exercised"] = True
        await asyncio.sleep(max(3.0, duration_secs * 0.4))
        strat.shutdown_system("live fork smoke complete")

    st = asyncio.create_task(_stop())
    try:
        await asyncio.wait_for(node.run_async(), timeout=duration_secs + 20.0)
    finally:
        st.cancel()
        node.dispose()

    return {
        "mode": "live_fork",
        "duration_secs": duration_secs,
        "telemetry": tel,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("mock-gap", "live-fork"), required=True)
    parser.add_argument("--duration", type=float, default=15.0)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    res = asyncio.run(run_mock_gap_4_scenarios() if args.mode == "mock-gap" else run_live_fork_smoke(args.duration))
    txt = json.dumps(res, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(txt + "\n", encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
