"""Live async gRPC MarketDataClient connecting BMD Gateway to Nautilus LiveNode."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import time
from typing import Any, Callable

import grpc
from nautilus_trader.live.clients import DataClientFactory, MarketDataClient
from nautilus_trader.config import DataClientConfig
from nautilus_trader.model import (
    InstrumentStatus,
    MarketStatusAction,
    Price,
    Quantity,
    QuoteTick,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

from bq_feed.events import Market, Stream
from bq_feed.live import CapabilityMismatchError

_MARKET_TO_PROTO = {
    Market.SPOT: enums_pb2.MARKET_SPOT,
    Market.USDM: enums_pb2.MARKET_USD_M_PERPETUAL,
}
_STREAM_TO_PROTO = {
    Stream.DIFF_DEPTH: enums_pb2.STREAM_DIFF_DEPTH,
    Stream.AGG_TRADE: enums_pb2.STREAM_AGG_TRADE,
    Stream.BOOK_TICKER: enums_pb2.STREAM_BOOK_TICKER,
    Stream.MARK_PRICE: enums_pb2.STREAM_MARK_PRICE,
    Stream.LIQUIDATION: enums_pb2.STREAM_LIQUIDATION,
    Stream.REFERENCE_DATA: enums_pb2.STREAM_REFERENCE_DATA,
}
_PAYLOAD_SCHEMAS = {
    Stream.BOOK_TICKER: "book-ticker.v1",
    Stream.AGG_TRADE: "agg-trade.v1",
    Stream.DIFF_DEPTH: "depth-update.v1",
    Stream.MARK_PRICE: "mark-price.v1",
    Stream.LIQUIDATION: "liquidation.v1",
    Stream.REFERENCE_DATA: "reference-observation.v1",
}
_LIFECYCLE_HEALTHY = {enums_pb2.STREAM_LIFECYCLE_STATE_LIVE}

VENUE = Venue("BINANCE")


class BmdGatewayConfig(DataClientConfig):
    """Configuration for the BMD Gateway async gRPC DataClient."""

    def __init__(
        self,
        *,
        target: str = "127.0.0.1:50051",
        instruments: dict[Any, tuple[Market, str, Any]] | None = None,
        stale_threshold_ns: int = 5_000_000_000,
        max_items: int | None = None,
        strict_capabilities: bool = True,
        rpc_timeout_secs: float | None = None,
        stats: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.target = target
        self.instruments = instruments or {}
        self.stale_threshold_ns = stale_threshold_ns
        self.max_items = max_items
        self.strict_capabilities = strict_capabilities
        self.rpc_timeout_secs = rpc_timeout_secs
        self.stats = stats if stats is not None else {}


class BmdDataClient(MarketDataClient):
    """Nautilus MarketDataClient consuming BMD Gateway gRPC with Fail-Fast & STALE/GAP gating."""

    def __init__(
        self,
        *,
        name: str,
        config: BmdGatewayConfig,
        cache: Any,
        clock: Any,
        stub: Any | None = None,
        monotonic_ns_fn: Callable[[], int] | None = None,
        utc_ns_fn: Callable[[], int] | None = None,
    ) -> None:
        super().__init__(name=name, config=config, cache=cache, clock=clock, venue=VENUE)
        self._cfg = config
        self._injected_stub = stub
        self._channel: grpc.aio.Channel | None = None
        self._stub: Any = stub
        self._mono_ns = monotonic_ns_fn or time.monotonic_ns
        self._utc_ns = utc_ns_fn or time.time_ns
        self._utc_anchor: tuple[int, int] | None = None
        self._last_activity_mono: dict[Any, int] = {}
        self._marked_stale: set[Any] = set()
        self._marked_gap: set[Any] = set()
        self._active_streams: list[Any] = []
        self._stop = False
        self._watchdog_task: asyncio.Task[None] | None = None
        self._cfg.stats.setdefault("accepted", 0)
        self._cfg.stats.setdefault("rows", [])
        self._cfg.stats.setdefault("health_events", [])
        self._cfg.stats.setdefault("other_payloads", [])
        self._cfg.stats.setdefault("dequeue_latencies_ns", [])

    def _anchored_utc_ns(self, mono_ns: int) -> int:
        if self._utc_anchor is not None:
            a_utc, a_mono = self._utc_anchor
            return a_utc + (mono_ns - a_mono)
        return self._utc_ns()

    def _emit_status(self, inst_id: Any, *, healthy: bool, reason: str, ts_ns: int) -> None:
        action = MarketStatusAction.TRADING if healthy else MarketStatusAction.HALT
        msg = InstrumentStatus(
            instrument_id=inst_id,
            action=action,
            ts_event=ts_ns,
            ts_init=ts_ns,
            reason=reason,
            is_trading=healthy,
            is_quoting=healthy,
        )
        self._cfg.stats["health_events"].append(
            {
                "instrument_id": str(inst_id),
                "healthy": healthy,
                "reason": reason,
                "ts_ns": ts_ns,
            }
        )
        self._handle_data(msg)

    async def _preflight(self, required: list[tuple[Market, str, Stream]]) -> None:
        req = gateway_messages_pb2.GatewayStatusRequest(
            request_id="bmd-nautilus-preflight",
            schema_version="gateway-status-request.v1",
        )
        snap = await self._stub.GetGatewayStatus(req, timeout=5.0)
        by_key = {(m.market, m.symbol): m for m in snap.markets}
        for market, symbol, stream in required:
            proto_mkt = _MARKET_TO_PROTO.get(market)
            if proto_mkt is None:
                raise CapabilityMismatchError(f"Unsupported live market {market.value}")
            row = by_key.get((proto_mkt, symbol))
            if row is None:
                raise CapabilityMismatchError(
                    f"Product ({market.value}, {symbol}) not configured in Gateway"
                )
            if not row.HasField("enabled_streams"):
                if self._cfg.strict_capabilities:
                    raise CapabilityMismatchError(
                        f"Gateway status missing enabled_streams for {market.value}:{symbol}"
                    )
                continue
            caps = row.enabled_streams
            if _STREAM_TO_PROTO[stream] not in list(caps.streams):
                raise CapabilityMismatchError(
                    f"Gateway disabled stream {stream.value} for {market.value}:{symbol}"
                )

    async def _connect(self) -> None:
        if self._injected_stub is None:
            self._channel = grpc.aio.insecure_channel(self._cfg.target)
            self._stub = gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(
                self._channel
            )
        required = [(mkt, sym, Stream.BOOK_TICKER) for _, (mkt, sym, _) in self._cfg.instruments.items()]
        try:
            if required:
                await self._preflight(required)
        except Exception as exc:
            self._cfg.stats["connect_error"] = exc
            raise
        for _, (_, _, inst) in self._cfg.instruments.items():
            self._handle_instrument(inst)
        self._stop = False
        self._watchdog_task = self.create_task(self._run_watchdog(), name="bmd-stale-watchdog")

    async def _disconnect(self) -> None:
        self._stop = True
        for st in self._active_streams:
            cancel = getattr(st, "cancel", None)
            if callable(cancel):
                cancel()
        self._active_streams.clear()
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def _run_watchdog(self) -> None:
        poll_s = min(0.5, max(0.05, self._cfg.stale_threshold_ns / 4_000_000_000.0))
        while not self._stop:
            await asyncio.sleep(poll_s)
            now_mono = self._mono_ns()
            for inst_id, last_mono in list(self._last_activity_mono.items()):
                if inst_id not in self._marked_stale and (now_mono - last_mono) > self._cfg.stale_threshold_ns:
                    self._marked_stale.add(inst_id)
                    self._emit_status(
                        inst_id,
                        healthy=False,
                        reason=f"STALE:watchdog_timeout>{self._cfg.stale_threshold_ns}ns",
                        ts_ns=self._anchored_utc_ns(now_mono),
                    )

    async def _subscribe_instrument_status(self, command: Any) -> None:
        # Status is emitted from the event streams and watchdog; register watched ids.
        now_mono = self._mono_ns()
        for inst_id in command.instrument_ids:
            self._last_activity_mono.setdefault(inst_id, now_mono)

    async def _unsubscribe_instrument_status(self, command: Any) -> None:
        return None

    async def _subscribe_quotes(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._last_activity_mono[inst_id] = self._mono_ns()
            self.create_task(
                self._consume_book_ticker(market, symbol, inst),
                name=f"bmd-bbo-{symbol}",
            )

    async def _unsubscribe_quotes(self, command: Any) -> None:
        return None

    async def _consume_book_ticker(self, market: Market, symbol: str, inst: Any) -> None:
        req = gateway_messages_pb2.EventSubscriptionRequest(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-bbo",
            schema_version="event-subscription-request.v1",
            delivery_mode=enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS,
            supported_payload_schema_versions=[_PAYLOAD_SCHEMAS[Stream.BOOK_TICKER]],
        )
        sel = req.selectors.add()
        sel.venue = enums_pb2.VENUE_BINANCE
        sel.market = _MARKET_TO_PROTO[market]
        sel.symbol = symbol
        sel.stream = enums_pb2.STREAM_BOOK_TICKER

        stream = self._stub.SubscribeEvents(req, timeout=self._cfg.rpc_timeout_secs)
        self._active_streams.append(stream)
        p_dp = inst.price_precision
        s_dp = inst.size_precision
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if name == "subscription_accepted":
                    self._cfg.stats["accepted"] += 1
                    self._last_activity_mono[inst.id] = now_mono
                    continue
                if name == "consumer_gap":
                    gap = env.consumer_gap
                    self._marked_gap.add(inst.id)
                    self._emit_status(
                        inst.id,
                        healthy=False,
                        reason=f"GAP:{gap.reason}",
                        ts_ns=int(gap.detected_time_utc_ns),
                    )
                    continue
                if name == "stream_status":
                    st = env.stream_status
                    healthy = st.state in _LIFECYCLE_HEALTHY
                    if healthy:
                        self._last_activity_mono[inst.id] = now_mono
                        self._marked_stale.discard(inst.id)
                        self._marked_gap.discard(inst.id)
                    else:
                        self._marked_gap.add(inst.id)
                    msg = st.message if st.HasField("message") else f"state:{st.state}"
                    self._emit_status(
                        inst.id,
                        healthy=healthy,
                        reason=f"{'LIVE' if healthy else 'DEGRADED'}:{msg}",
                        ts_ns=int(st.observed_time_utc_ns),
                    )
                    continue
                if name != "book_ticker":
                    self._cfg.stats["other_payloads"].append(name)
                    continue

                bt = env.book_ticker
                meta = bt.metadata
                read_ns = self._utc_ns()
                recv_ns = int(meta.receive_time_utc_ns)
                self._utc_anchor = (recv_ns, now_mono)
                was_stale_only = (inst.id in self._marked_stale) and (inst.id not in self._marked_gap)
                self._last_activity_mono[inst.id] = now_mono
                if was_stale_only:
                    self._marked_stale.discard(inst.id)
                    self._emit_status(
                        inst.id,
                        healthy=True,
                        reason="LIVE:recovered_on_quote",
                        ts_ns=read_ns,
                    )

                ts_event = (
                    int(meta.exchange_event_time_ms) * 1_000_000
                    if meta.HasField("exchange_event_time_ms")
                    else recv_ns
                )
                tick = QuoteTick(
                    instrument_id=inst.id,
                    bid_price=Price.from_decimal_dp(Decimal(bt.best_bid_price), p_dp),
                    ask_price=Price.from_decimal_dp(Decimal(bt.best_ask_price), p_dp),
                    bid_size=Quantity.from_decimal_dp(Decimal(bt.best_bid_quantity), s_dp),
                    ask_size=Quantity.from_decimal_dp(Decimal(bt.best_ask_quantity), s_dp),
                    ts_event=ts_event,
                    ts_init=read_ns,
                )
                row = {
                    "update_id": int(bt.update_id) if bt.HasField("update_id") else None,
                    "gateway_receive_ns": recv_ns,
                    "adapter_read_ns": read_ns,
                    "ts_event": ts_event,
                    "ts_event_source": "exchange_event_time_ms" if meta.HasField("exchange_event_time_ms") else "capture_receive_proxy",
                    "bid": bt.best_bid_price,
                    "ask": bt.best_ask_price,
                    "bid_size": bt.best_bid_quantity,
                    "ask_size": bt.best_ask_quantity,
                    "connection_generation": (
                        int(env.delivery_metadata.connection_generation)
                        if env.delivery_metadata.HasField("connection_generation")
                        else None
                    ),
                }
                self._cfg.stats["rows"].append(row)
                self._handle_data(tick)
                max_items = self._cfg.max_items
                if max_items is not None and len(self._cfg.stats["rows"]) >= max_items:
                    break
        finally:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                cancel()


class BmdDataClientFactory(DataClientFactory):
    """Factory creating BmdDataClient instances for Nautilus LiveNode."""

    def __init__(
        self,
        *,
        stub: Any | None = None,
        monotonic_ns_fn: Callable[[], int] | None = None,
        utc_ns_fn: Callable[[], int] | None = None,
    ) -> None:
        self._stub = stub
        self._mono_ns = monotonic_ns_fn
        self._utc_ns = utc_ns_fn

    def create(self, *, name: str, config: BmdGatewayConfig, cache: Any, clock: Any) -> BmdDataClient:
        return BmdDataClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            stub=self._stub,
            monotonic_ns_fn=self._mono_ns,
            utc_ns_fn=self._utc_ns,
        )


__all__ = [
    "BmdDataClient",
    "BmdDataClientFactory",
    "BmdGatewayConfig",
    "VENUE",
]
