"""Route A: Rust Tonic + Prost gRPC MarketDataClient connecting BMD Gateway to Nautilus LiveNode."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import time
from typing import Any

from nautilus_trader.config import DataClientConfig
from nautilus_trader.live.clients import DataClientFactory, MarketDataClient
from nautilus_trader.model import (
    AggressorSide,
    BookAction,
    BookOrder,
    FundingRateUpdate,
    InstrumentStatus,
    MarketStatusAction,
    MarkPriceUpdate,
    OrderBookDelta,
    OrderBookDeltas,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    RecordFlag,
    TradeId,
    TradeTick,
    Venue,
)

from bq_feed.events import Market, Stream
from bq_feed.live import CapabilityMismatchError
from .custom_data import (
    BMD_STREAM_QUALITY_DATATYPE,
    BmdStreamQualityData,
    wrap_custom_data,
)

VENUE = Venue("BINANCE")

_MARKET_TO_PROTO_INT = {Market.SPOT: 1, Market.USDM: 2}
_STREAM_TO_PROTO_INT = {
    Stream.DIFF_DEPTH: 1,
    Stream.AGG_TRADE: 2,
    Stream.BOOK_TICKER: 3,
    Stream.MARK_PRICE: 5,
    Stream.LIQUIDATION: 6,
    Stream.REFERENCE_DATA: 7,
}
_PAYLOAD_SCHEMAS = {
    Stream.BOOK_TICKER: "book-ticker.v1",
    Stream.AGG_TRADE: "agg-trade.v1",
    Stream.DIFF_DEPTH: "depth-update.v1",
    Stream.MARK_PRICE: "mark-price.v1",
    Stream.LIQUIDATION: "liquidation.v1",
    Stream.REFERENCE_DATA: "reference-observation.v1",
}

_KIND_ACCEPTED = 1
_KIND_STATUS = 2
_KIND_GAP_CLEAR = 3
_KIND_BOOK_TICKER = 4
_KIND_AGG_TRADE = 5
_KIND_DEPTH_SNAPSHOT = 6
_KIND_DEPTH_UPDATE = 7
_KIND_MARK_PRICE = 8


class BmdTonicGatewayConfig(DataClientConfig):
    """Configuration for Route A Rust Tonic gRPC DataClient."""

    def __init__(
        self,
        *,
        target: str = "127.0.0.1:50051",
        instruments: dict[Any, tuple[Market, str, Any]] | None = None,
        required_streams: list[tuple[Market, str, Stream]] | None = None,
        strict_capabilities: bool = True,
        batch_wait_ms: int = 5,
        batch_max_items: int = 512,
        stats: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.target = target
        self.instruments = instruments or {}
        self.required_streams = required_streams
        self.strict_capabilities = strict_capabilities
        self.batch_wait_ms = batch_wait_ms
        self.batch_max_items = batch_max_items
        self.stats = stats if stats is not None else {}


class BmdTonicDataClient(MarketDataClient):
    """Nautilus MarketDataClient backed by Rust Tonic + Prost tokio client."""

    def __init__(
        self,
        *,
        name: str,
        config: BmdTonicGatewayConfig,
        cache: Any,
        clock: Any,
    ) -> None:
        super().__init__(name=name, config=config, cache=cache, clock=clock, venue=VENUE)
        from . import bmd_tonic_ext

        self._cfg = config
        self._ext = bmd_tonic_ext.TonicGatewayClient(config.target)
        self._inst_list: list[tuple[Market, str, Any]] = list(config.instruments.values())
        self._inst_idx: dict[Any, int] = {
            inst.id: idx for idx, (_, _, inst) in enumerate(self._inst_list)
        }
        self._caps: dict[tuple[int, str], set[int]] = {}
        self._started_streams: set[tuple[int, int]] = set()
        self._mark_wants: dict[int, dict[str, bool]] = {}
        self._stop = False
        self._pump_task: asyncio.Task[None] | None = None
        self._cfg.stats.setdefault("accepted", 0)
        self._cfg.stats.setdefault("items_received", 0)
        self._cfg.stats.setdefault("health_events", [])
        self._cfg.stats.setdefault("dequeue_latencies_ns", [])
        self._cfg.stats.setdefault("wire_to_cb_latencies_ns", [])
        self._cfg.stats.setdefault("batches", 0)

    async def _connect(self) -> None:
        req = self._cfg.required_streams or []
        req_tuples = [
            (_MARKET_TO_PROTO_INT[m], sym, _STREAM_TO_PROTO_INT[st])
            for m, sym, st in req
        ]
        try:
            caps_raw = await asyncio.to_thread(
                self._ext.preflight,
                self._cfg.strict_capabilities,
                req_tuples,
            )
        except Exception as exc:
            msg = str(exc)
            if "CapabilityMismatch" in msg:
                raise CapabilityMismatchError(msg) from exc
            raise
        self._caps = {(m, sym): set(streams) for m, sym, streams in caps_raw}
        for _, _, inst in self._inst_list:
            self._handle_instrument(inst)
        self._stop = False
        self._pump_task = self.create_task(self._pump_batches(), name="bmd-tonic-pump")

    async def _disconnect(self) -> None:
        self._stop = True
        self._ext.stop()
        if self._pump_task is not None:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except asyncio.CancelledError:
                pass
            self._pump_task = None

    async def _subscribe_instrument_status(self, command: Any) -> None:
        return None

    async def _unsubscribe_instrument_status(self, command: Any) -> None:
        return None

    def _start_sub(self, inst_id: Any, stream: Stream, schemas: list[str]) -> None:
        idx = self._inst_idx[inst_id]
        market, symbol, _ = self._inst_list[idx]
        mkt_i = _MARKET_TO_PROTO_INT[market]
        str_i = _STREAM_TO_PROTO_INT[stream]
        if (idx, str_i) in self._started_streams:
            return
        if self._cfg.strict_capabilities and self._caps:
            allowed = self._caps.get((mkt_i, symbol), set())
            if str_i not in allowed:
                raise CapabilityMismatchError(
                    f"Gateway disabled stream {stream.value} for {market.value}:{symbol}"
                )
        self._started_streams.add((idx, str_i))
        self._ext.subscribe_stream(idx, mkt_i, symbol, str_i, schemas)

    async def _subscribe_quotes(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            self._start_sub(inst_id, Stream.BOOK_TICKER, [_PAYLOAD_SCHEMAS[Stream.BOOK_TICKER]])

    async def _unsubscribe_quotes(self, command: Any) -> None:
        return None

    async def _subscribe_trades(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            self._start_sub(inst_id, Stream.AGG_TRADE, [_PAYLOAD_SCHEMAS[Stream.AGG_TRADE]])

    async def _unsubscribe_trades(self, command: Any) -> None:
        return None

    async def _subscribe_book_deltas(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            self._start_sub(
                inst_id,
                Stream.DIFF_DEPTH,
                [_PAYLOAD_SCHEMAS[Stream.DIFF_DEPTH], _PAYLOAD_SCHEMAS[Stream.REFERENCE_DATA]],
            )

    async def _unsubscribe_book_deltas(self, command: Any) -> None:
        return None

    async def _subscribe_mark_prices(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            idx = self._inst_idx[inst_id]
            self._mark_wants.setdefault(idx, {"mark": False, "funding": False})["mark"] = True
            self._start_sub(inst_id, Stream.MARK_PRICE, [_PAYLOAD_SCHEMAS[Stream.MARK_PRICE]])

    async def _unsubscribe_mark_prices(self, command: Any) -> None:
        return None

    async def _subscribe_funding_rates(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            idx = self._inst_idx[inst_id]
            self._mark_wants.setdefault(idx, {"mark": False, "funding": False})["funding"] = True
            self._start_sub(inst_id, Stream.MARK_PRICE, [_PAYLOAD_SCHEMAS[Stream.MARK_PRICE]])

    async def _unsubscribe_funding_rates(self, command: Any) -> None:
        return None

    def _emit_stream_quality(
        self,
        market: Market,
        symbol: str,
        inst_id: Any,
        stream_code: int,
        *,
        healthy: bool,
        reason: str,
        ts_ns: int,
    ) -> None:
        stream_name = {
            1: Stream.DIFF_DEPTH.value,
            2: Stream.AGG_TRADE.value,
            3: Stream.BOOK_TICKER.value,
            5: Stream.MARK_PRICE.value,
            6: Stream.LIQUIDATION.value,
            7: Stream.REFERENCE_DATA.value,
        }.get(stream_code, str(stream_code))
        status = "LIVE" if healthy else ("GAP" if "GAP" in reason else "DEGRADED")
        payload = BmdStreamQualityData(
            market=market.value,
            symbol=symbol,
            stream=stream_name,
            status=status,
            reason=reason,
            observed_time_utc_ns=ts_ns,
            ts_event=ts_ns,
            ts_init=ts_ns,
        )
        self._cfg.stats["health_events"].append(
            {
                "instrument_id": str(inst_id),
                "market": market.value,
                "symbol": symbol,
                "stream": stream_name,
                "status": status,
                "healthy": healthy,
                "reason": reason,
                "ts_ns": ts_ns,
            }
        )
        self._handle_data(wrap_custom_data(BMD_STREAM_QUALITY_DATATYPE, payload))

    async def _pump_batches(self) -> None:
        inst_list = self._inst_list
        wait_ms = self._cfg.batch_wait_ms
        max_items = self._cfg.batch_max_items
        while not self._stop:
            batch = await asyncio.to_thread(self._ext.drain_batch, max_items, wait_ms)
            if not batch:
                continue
            self._cfg.stats["batches"] += 1
            for ev in batch:
                kind = ev.kind
                market, symbol, inst = inst_list[ev.inst_idx]
                if kind == _KIND_ACCEPTED:
                    self._cfg.stats["accepted"] += 1
                    continue
                if kind == _KIND_GAP_CLEAR:
                    ts_ns = int(ev.ts_event) or int(ev.rust_read_ns)
                    clr = OrderBookDelta.clear(inst.id, 0, ts_ns, int(ev.rust_read_ns))
                    self._handle_data(OrderBookDeltas(inst.id, [clr]))
                    continue
                if kind == _KIND_STATUS:
                    ts_ns = int(ev.ts_event) or int(ev.rust_read_ns)
                    self._emit_stream_quality(
                        market,
                        symbol,
                        inst.id,
                        int(ev.stream_code),
                        healthy=bool(ev.b_flag),
                        reason=ev.reason,
                        ts_ns=ts_ns,
                    )
                    continue

                p_dp = inst.price_precision
                s_dp = inst.size_precision
                gw_recv = int(ev.gateway_recv_ns)
                rust_read = int(ev.rust_read_ns)
                py_now = time.time_ns()
                self._cfg.stats["items_received"] += 1
                self._cfg.stats["dequeue_latencies_ns"].append(max(0, rust_read - gw_recv))
                self._cfg.stats["wire_to_cb_latencies_ns"].append(max(0, py_now - gw_recv))

                if kind == _KIND_BOOK_TICKER:
                    tick = QuoteTick(
                        instrument_id=inst.id,
                        bid_price=Price.from_decimal_dp(Decimal(ev.p1), p_dp),
                        ask_price=Price.from_decimal_dp(Decimal(ev.p2), p_dp),
                        bid_size=Quantity.from_decimal_dp(Decimal(ev.q1), s_dp),
                        ask_size=Quantity.from_decimal_dp(Decimal(ev.q2), s_dp),
                        ts_event=int(ev.ts_event),
                        ts_init=rust_read,
                    )
                    self._handle_data(tick)
                elif kind == _KIND_AGG_TRADE:
                    tick = TradeTick(
                        instrument_id=inst.id,
                        price=Price.from_decimal_dp(Decimal(ev.p1), p_dp),
                        size=Quantity.from_decimal_dp(Decimal(ev.q1), s_dp),
                        aggressor_side=AggressorSide.SELL if ev.b_flag else AggressorSide.BUY,
                        trade_id=TradeId(str(ev.u1)),
                        ts_event=int(ev.ts_event),
                        ts_init=rust_read,
                    )
                    self._handle_data(tick)
                elif kind == _KIND_DEPTH_SNAPSHOT:
                    ts_ev = int(ev.ts_event)
                    uid = int(ev.u1)
                    deltas = [OrderBookDelta.clear(inst.id, uid, ts_ev, rust_read)]
                    raw_bids = [(OrderSide.BUY, Decimal(p), Decimal(q)) for p, q in ev.bids]
                    raw_asks = [(OrderSide.SELL, Decimal(p), Decimal(q)) for p, q in ev.asks]
                    levels = raw_bids + raw_asks
                    for i, (side, px, qty) in enumerate(levels):
                        if qty <= 0:
                            continue
                        flags = RecordFlag.F_SNAPSHOT.value | (
                            RecordFlag.F_LAST.value if i == len(levels) - 1 else 0
                        )
                        order = BookOrder(
                            side,
                            Price.from_decimal_dp(px, p_dp),
                            Quantity.from_decimal_dp(qty, s_dp),
                            0,
                        )
                        deltas.append(
                            OrderBookDelta(inst.id, BookAction.ADD, order, flags, uid, ts_ev, rust_read)
                        )
                    self._handle_data(OrderBookDeltas(inst.id, deltas))
                elif kind == _KIND_DEPTH_UPDATE:
                    ts_ev = int(ev.ts_event)
                    uid = int(ev.u1)
                    raw_levels = [(OrderSide.BUY, Decimal(p), Decimal(q)) for p, q in ev.bids] + [
                        (OrderSide.SELL, Decimal(p), Decimal(q)) for p, q in ev.asks
                    ]
                    if not raw_levels:
                        continue
                    deltas = []
                    for i, (side, px, qty) in enumerate(raw_levels):
                        is_last = i == len(raw_levels) - 1
                        action = BookAction.DELETE if qty == 0 else BookAction.UPDATE
                        flags = RecordFlag.F_LAST.value if is_last else 0
                        order = BookOrder(
                            side,
                            Price.from_decimal_dp(px, p_dp),
                            Quantity.from_decimal_dp(qty, s_dp),
                            0,
                        )
                        deltas.append(
                            OrderBookDelta(inst.id, action, order, flags, uid, ts_ev, rust_read)
                        )
                    self._handle_data(OrderBookDeltas(inst.id, deltas))
                elif kind == _KIND_MARK_PRICE:
                    wants = self._mark_wants.get(ev.inst_idx, {"mark": True, "funding": False})
                    ts_ev = int(ev.ts_event)
                    if wants.get("mark"):
                        self._handle_data(
                            MarkPriceUpdate(
                                instrument_id=inst.id,
                                value=Price.from_decimal_dp(Decimal(ev.p1), p_dp),
                                ts_event=ts_ev,
                                ts_init=rust_read,
                            )
                        )
                    if wants.get("funding"):
                        self._handle_data(
                            FundingRateUpdate(
                                instrument_id=inst.id,
                                rate=Decimal(ev.q1),
                                ts_event=ts_ev,
                                ts_init=rust_read,
                                interval=None,
                                next_funding_ns=int(ev.u1),
                            )
                        )


class BmdTonicDataClientFactory(DataClientFactory):
    """Factory creating Route A Rust Tonic BmdTonicDataClient instances."""

    def create(
        self,
        *,
        name: str,
        config: BmdTonicGatewayConfig,
        cache: Any,
        clock: Any,
    ) -> BmdTonicDataClient:
        return BmdTonicDataClient(name=name, config=config, cache=cache, clock=clock)


__all__ = [
    "BmdTonicDataClient",
    "BmdTonicDataClientFactory",
    "BmdTonicGatewayConfig",
]
