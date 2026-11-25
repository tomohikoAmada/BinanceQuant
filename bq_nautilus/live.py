"""Live async gRPC MarketDataClient connecting BMD Gateway to Nautilus LiveNode."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from decimal import Decimal
import time
from typing import Any, Callable

import grpc
from nautilus_trader.live.clients import DataClientFactory, MarketDataClient
from nautilus_trader.config import DataClientConfig
from nautilus_trader.model import (
    AggressorSide,
    FundingRateUpdate,
    InstrumentStatus,
    MarketStatusAction,
    MarkPriceUpdate,
    Price,
    Quantity,
    QuoteTick,
    TradeId,
    TradeTick,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

from bq_feed.events import Market, Stream
from bq_feed.live import CapabilityMismatchError

from .custom_data import (
    BMD_LIQUIDATION_DATATYPE,
    BMD_LIQUIDATION_TYPE_NAME,
    BMD_REFERENCE_DATATYPE,
    BMD_REFERENCE_TYPE_NAME,
    BmdLiquidationData,
    BmdReferenceData,
    wrap_custom_data,
)
from .recorder import AggTradeDeduplicator, L2SequenceBridge

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
        required_streams: list[tuple[Market, str, Stream]] | None = None,
        stale_threshold_ns: int = 5_000_000_000,
        sparse_stale_threshold_ns: int | None = None,
        max_items: int | None = None,
        strict_capabilities: bool = True,
        rpc_timeout_secs: float | None = None,
        dedup_agg_trades: bool = True,
        record_rows: bool = True,
        l2_initial_snapshots: dict[Any, tuple[int, list[tuple[str, str]], list[tuple[str, str]]]] | None = None,
        stats: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.target = target
        self.instruments = instruments or {}
        self.required_streams = required_streams
        self.stale_threshold_ns = stale_threshold_ns
        self.sparse_stale_threshold_ns = sparse_stale_threshold_ns
        self.max_items = max_items
        self.strict_capabilities = strict_capabilities
        self.rpc_timeout_secs = rpc_timeout_secs
        self.dedup_agg_trades = dedup_agg_trades
        self.record_rows = record_rows
        self.l2_initial_snapshots = l2_initial_snapshots or {}
        self.stats = stats if stats is not None else {}


_SPARSE_STREAMS = frozenset({Stream.LIQUIDATION, Stream.REFERENCE_DATA})


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
        self._last_activity_mono: dict[tuple[Any, Stream], int] = {}
        self._stream_stale: dict[Any, set[Stream]] = defaultdict(set)
        self._stream_gap: dict[Any, set[Stream]] = defaultdict(set)
        self._gateway_caps: dict[tuple[Market, str], set[int]] = {}
        self._dedup = AggTradeDeduplicator()
        self._l2_bridges: dict[Any, L2SequenceBridge] = {}
        self._mark_wants: dict[Any, dict[str, bool]] = {}
        self._mark_tasks: set[Any] = set()
        self._custom_tasks: set[tuple[str, Any, Stream]] = set()
        self._active_streams: list[Any] = []
        self._stop = False
        self._watchdog_task: asyncio.Task[None] | None = None
        self._cfg.stats.setdefault("accepted", 0)
        self._cfg.stats.setdefault("items_received", 0)
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

    def _register_activity_key(self, inst_id: Any, stream: Stream, now_mono: int) -> None:
        if stream in _SPARSE_STREAMS and self._cfg.sparse_stale_threshold_ns is None:
            return
        self._last_activity_mono[(inst_id, stream)] = now_mono

    def _mark_healthy_activity(
        self,
        inst_id: Any,
        stream: Stream,
        now_mono: int,
        ts_ns: int,
        *,
        recovery_reason: str,
        allow_gap_clear: bool = False,
    ) -> None:
        self._register_activity_key(inst_id, stream, now_mono)
        if allow_gap_clear:
            self._stream_gap[inst_id].discard(stream)
        was_stale = stream in self._stream_stale[inst_id]
        self._stream_stale[inst_id].discard(stream)
        if was_stale and not self._stream_stale[inst_id] and not self._stream_gap[inst_id]:
            self._emit_status(inst_id, healthy=True, reason=recovery_reason, ts_ns=ts_ns)

    def _check_stream_cap(self, market: Market, symbol: str, stream: Stream) -> None:
        if not self._gateway_caps:
            return
        caps = self._gateway_caps.get((market, symbol))
        if caps is None:
            if self._cfg.strict_capabilities:
                raise CapabilityMismatchError(
                    f"Product ({market.value}, {symbol}) missing from Gateway status capabilities"
                )
            return
        if _STREAM_TO_PROTO[stream] not in caps:
            raise CapabilityMismatchError(
                f"Gateway disabled stream {stream.value} for {market.value}:{symbol}"
            )

    async def _preflight(self, required: list[tuple[Market, str, Stream]]) -> None:
        req = gateway_messages_pb2.GatewayStatusRequest(
            request_id="bmd-nautilus-preflight",
            schema_version="gateway-status-request.v1",
        )
        snap = await self._stub.GetGatewayStatus(req, timeout=5.0)
        by_key = {(m.market, m.symbol): m for m in snap.markets}
        self._gateway_caps.clear()
        for (proto_mkt, sym), row in by_key.items():
            mkt = Market.SPOT if proto_mkt == enums_pb2.MARKET_SPOT else Market.USDM
            if row.HasField("enabled_streams"):
                self._gateway_caps[(mkt, sym)] = set(row.enabled_streams.streams)
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
        if self._cfg.required_streams is not None:
            required = list(self._cfg.required_streams)
        else:
            required = [(mkt, sym, Stream.BOOK_TICKER) for _, (mkt, sym, _) in self._cfg.instruments.items()]
        try:
            if required or self._cfg.instruments:
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
            for (inst_id, stream), last_mono in list(self._last_activity_mono.items()):
                thresh = (
                    self._cfg.sparse_stale_threshold_ns
                    if stream in _SPARSE_STREAMS
                    else self._cfg.stale_threshold_ns
                )
                if thresh is None:
                    continue
                if stream not in self._stream_stale[inst_id] and (now_mono - last_mono) > thresh:
                    self._stream_stale[inst_id].add(stream)
                    self._emit_status(
                        inst_id,
                        healthy=False,
                        reason=f"STALE:{stream.value}:watchdog_timeout>{thresh}ns",
                        ts_ns=self._anchored_utc_ns(now_mono),
                    )

    async def _subscribe_instrument_status(self, command: Any) -> None:
        # Status is emitted from event streams and watchdog; nothing extra to open here.
        return None

    async def _unsubscribe_instrument_status(self, command: Any) -> None:
        return None

    def _open_gateway_stream(
        self,
        *,
        request_id: str,
        market: Market,
        symbol: str,
        stream: Stream,
        schemas: list[str],
    ) -> Any:
        req = gateway_messages_pb2.EventSubscriptionRequest(
            request_id=request_id,
            schema_version="event-subscription-request.v1",
            delivery_mode=enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS,
            supported_payload_schema_versions=schemas,
        )
        sel = req.selectors.add()
        sel.venue = enums_pb2.VENUE_BINANCE
        sel.market = _MARKET_TO_PROTO[market]
        sel.symbol = symbol
        sel.stream = _STREAM_TO_PROTO[stream]
        stream_call = self._stub.SubscribeEvents(req, timeout=self._cfg.rpc_timeout_secs)
        self._active_streams.append(stream_call)
        return stream_call

    def _handle_control_payload(
        self,
        *,
        name: str,
        env: Any,
        inst: Any,
        stream: Stream,
        now_mono: int,
    ) -> bool:
        """Handle subscription_accepted / consumer_gap / stream_status; returns True if consumed."""
        if name == "subscription_accepted":
            self._cfg.stats["accepted"] += 1
            self._register_activity_key(inst.id, stream, now_mono)
            return True
        if name == "consumer_gap":
            gap = env.consumer_gap
            ts_ns = int(gap.detected_time_utc_ns)
            self._stream_gap[inst.id].add(stream)
            if stream == Stream.DIFF_DEPTH and inst.id in self._l2_bridges:
                self._handle_data(self._l2_bridges[inst.id].on_gap(inst, ts_ns, ts_ns))
            self._emit_status(
                inst.id,
                healthy=False,
                reason=f"GAP:{stream.value}:{gap.reason}",
                ts_ns=ts_ns,
            )
            return True
        if name == "stream_status":
            st = env.stream_status
            healthy = st.state in _LIFECYCLE_HEALTHY
            ts_ns = int(st.observed_time_utc_ns)
            if healthy:
                self._register_activity_key(inst.id, stream, now_mono)
                self._stream_stale[inst.id].discard(stream)
                self._stream_gap[inst.id].discard(stream)
            else:
                self._stream_gap[inst.id].add(stream)
                if stream == Stream.DIFF_DEPTH and inst.id in self._l2_bridges:
                    self._handle_data(self._l2_bridges[inst.id].on_gap(inst, ts_ns, ts_ns))
            msg = st.message if st.HasField("message") else f"state:{st.state}"
            self._emit_status(
                inst.id,
                healthy=healthy,
                reason=f"{'LIVE' if healthy else 'DEGRADED'}:{stream.value}:{msg}",
                ts_ns=ts_ns,
            )
            return True
        return False

    def _reached_max_items(self) -> bool:
        max_items = self._cfg.max_items
        return max_items is not None and int(self._cfg.stats["items_received"]) >= max_items

    async def _subscribe_quotes(self, command: Any) -> None:
        now_mono = self._mono_ns()
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._check_stream_cap(market, symbol, Stream.BOOK_TICKER)
            self._register_activity_key(inst_id, Stream.BOOK_TICKER, now_mono)
            self.create_task(
                self._consume_book_ticker(market, symbol, inst),
                name=f"bmd-bbo-{symbol}",
            )

    async def _unsubscribe_quotes(self, command: Any) -> None:
        return None

    async def _subscribe_trades(self, command: Any) -> None:
        now_mono = self._mono_ns()
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._check_stream_cap(market, symbol, Stream.AGG_TRADE)
            self._register_activity_key(inst_id, Stream.AGG_TRADE, now_mono)
            self.create_task(
                self._consume_agg_trade(market, symbol, inst),
                name=f"bmd-aggtrade-{symbol}",
            )

    async def _unsubscribe_trades(self, command: Any) -> None:
        return None

    async def _subscribe_book_deltas(self, command: Any) -> None:
        now_mono = self._mono_ns()
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._check_stream_cap(market, symbol, Stream.DIFF_DEPTH)
            bridge = self._l2_bridges.setdefault(inst_id, L2SequenceBridge())
            if inst_id in self._cfg.l2_initial_snapshots:
                last_uid, bids, asks = self._cfg.l2_initial_snapshots[inst_id]
                ts_ns = self._utc_ns()
                self._handle_data(
                    bridge.apply_snapshot(
                        instrument=inst,
                        bids=[(Decimal(p), Decimal(q)) for p, q in bids],
                        asks=[(Decimal(p), Decimal(q)) for p, q in asks],
                        last_update_id=last_uid,
                        ts_event=ts_ns,
                        ts_init=ts_ns,
                    )
                )
            self._register_activity_key(inst_id, Stream.DIFF_DEPTH, now_mono)
            self.create_task(
                self._consume_diff_depth(market, symbol, inst, bridge),
                name=f"bmd-l2-{symbol}",
            )

    async def _unsubscribe_book_deltas(self, command: Any) -> None:
        return None

    def _ensure_mark_price_task(self, market: Market, symbol: str, inst: Any, key: str) -> None:
        wants = self._mark_wants.setdefault(inst.id, {"mark": False, "funding": False})
        wants[key] = True
        if inst.id in self._mark_tasks:
            return
        self._mark_tasks.add(inst.id)
        self._check_stream_cap(market, symbol, Stream.MARK_PRICE)
        self._register_activity_key(inst.id, Stream.MARK_PRICE, self._mono_ns())
        self.create_task(
            self._consume_mark_price(market, symbol, inst),
            name=f"bmd-mark-{symbol}",
        )

    async def _subscribe_mark_prices(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._ensure_mark_price_task(market, symbol, inst, "mark")

    async def _unsubscribe_mark_prices(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            if inst_id in self._mark_wants:
                self._mark_wants[inst_id]["mark"] = False

    async def _subscribe_funding_rates(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            entry = self._cfg.instruments.get(inst_id)
            if entry is None:
                raise KeyError(f"Unconfigured instrument_id in BmdGatewayConfig: {inst_id}")
            market, symbol, inst = entry
            self._ensure_mark_price_task(market, symbol, inst, "funding")

    async def _unsubscribe_funding_rates(self, command: Any) -> None:
        for inst_id in command.instrument_ids:
            if inst_id in self._mark_wants:
                self._mark_wants[inst_id]["funding"] = False

    async def _subscribe(self, command: Any) -> None:
        dtype = command.data_type
        type_name = getattr(dtype, "type_name", str(dtype))
        if type_name == BMD_LIQUIDATION_TYPE_NAME:
            stream_kind = Stream.LIQUIDATION
        elif type_name == BMD_REFERENCE_TYPE_NAME:
            stream_kind = Stream.REFERENCE_DATA
        else:
            return None

        params = getattr(command, "params", None) or {}
        targets = []
        if "market" in params and "symbol" in params:
            want_m = Market(str(params["market"]))
            want_s = str(params["symbol"])
            for m, s, inst in self._cfg.instruments.values():
                if m == want_m and s == want_s:
                    targets.append((m, s, inst))
        else:
            for m, s, inst in self._cfg.instruments.values():
                if stream_kind != Stream.LIQUIDATION or m == Market.USDM:
                    targets.append((m, s, inst))

        now_mono = self._mono_ns()
        for market, symbol, inst in targets:
            task_key = (type_name, inst.id, stream_kind)
            if task_key in self._custom_tasks:
                continue
            self._custom_tasks.add(task_key)
            self._check_stream_cap(market, symbol, stream_kind)
            # Note: sparse streams skip watchdog unless sparse_stale_threshold_ns is explicitly configured.
            self._register_activity_key(inst.id, stream_kind, now_mono)
            self.create_task(
                self._consume_custom_stream(market, symbol, inst, stream_kind),
                name=f"bmd-{stream_kind.value}-{symbol}",
            )

    async def _unsubscribe(self, command: Any) -> None:
        return None

    async def _consume_book_ticker(self, market: Market, symbol: str, inst: Any) -> None:
        stream = self._open_gateway_stream(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-bbo",
            market=market,
            symbol=symbol,
            stream=Stream.BOOK_TICKER,
            schemas=[_PAYLOAD_SCHEMAS[Stream.BOOK_TICKER]],
        )
        p_dp = inst.price_precision
        s_dp = inst.size_precision
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if self._handle_control_payload(
                    name=name,
                    env=env,
                    inst=inst,
                    stream=Stream.BOOK_TICKER,
                    now_mono=now_mono,
                ):
                    continue
                if name != "book_ticker":
                    self._cfg.stats["other_payloads"].append(name)
                    continue

                bt = env.book_ticker
                meta = bt.metadata
                read_ns = self._utc_ns()
                recv_ns = int(meta.receive_time_utc_ns)
                self._utc_anchor = (recv_ns, now_mono)
                self._mark_healthy_activity(
                    inst.id,
                    Stream.BOOK_TICKER,
                    now_mono,
                    read_ns,
                    recovery_reason="LIVE:recovered_on_quote",
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
                self._cfg.stats["items_received"] += 1
                self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                if self._cfg.record_rows:
                    self._cfg.stats["rows"].append(
                        {
                            "update_id": int(bt.update_id) if bt.HasField("update_id") else None,
                            "gateway_receive_ns": recv_ns,
                            "adapter_read_ns": read_ns,
                            "ts_event": ts_event,
                            "ts_event_source": (
                                "exchange_event_time_ms"
                                if meta.HasField("exchange_event_time_ms")
                                else "capture_receive_proxy"
                            ),
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
                    )
                self._handle_data(tick)
                if self._reached_max_items():
                    break
        finally:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                cancel()

    async def _consume_agg_trade(self, market: Market, symbol: str, inst: Any) -> None:
        stream = self._open_gateway_stream(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-aggtrade",
            market=market,
            symbol=symbol,
            stream=Stream.AGG_TRADE,
            schemas=[_PAYLOAD_SCHEMAS[Stream.AGG_TRADE]],
        )
        p_dp = inst.price_precision
        s_dp = inst.size_precision
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if self._handle_control_payload(
                    name=name,
                    env=env,
                    inst=inst,
                    stream=Stream.AGG_TRADE,
                    now_mono=now_mono,
                ):
                    continue
                if name != "agg_trade":
                    self._cfg.stats["other_payloads"].append(name)
                    continue
                at = env.agg_trade
                meta = at.metadata
                read_ns = self._utc_ns()
                recv_ns = int(meta.receive_time_utc_ns)
                self._utc_anchor = (recv_ns, now_mono)
                self._mark_healthy_activity(
                    inst.id,
                    Stream.AGG_TRADE,
                    now_mono,
                    read_ns,
                    recovery_reason="LIVE:recovered_on_trade",
                )
                agg_id = int(at.aggregate_trade_id)
                if self._cfg.dedup_agg_trades and not self._dedup.check_and_add(market, symbol, agg_id):
                    continue
                ts_event = int(at.trade_time_ms) * 1_000_000
                tick = TradeTick(
                    instrument_id=inst.id,
                    price=Price.from_decimal_dp(Decimal(at.price), p_dp),
                    size=Quantity.from_decimal_dp(Decimal(at.quantity), s_dp),
                    aggressor_side=AggressorSide.SELL if at.buyer_is_maker else AggressorSide.BUY,
                    trade_id=TradeId(str(agg_id)),
                    ts_event=ts_event,
                    ts_init=read_ns,
                )
                self._cfg.stats["items_received"] += 1
                self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                self._handle_data(tick)
                if self._reached_max_items():
                    break
        finally:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                cancel()

    async def _consume_diff_depth(
        self,
        market: Market,
        symbol: str,
        inst: Any,
        bridge: L2SequenceBridge,
    ) -> None:
        stream = self._open_gateway_stream(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-l2",
            market=market,
            symbol=symbol,
            stream=Stream.DIFF_DEPTH,
            schemas=[_PAYLOAD_SCHEMAS[Stream.DIFF_DEPTH], _PAYLOAD_SCHEMAS[Stream.REFERENCE_DATA]],
        )
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if self._handle_control_payload(
                    name=name,
                    env=env,
                    inst=inst,
                    stream=Stream.DIFF_DEPTH,
                    now_mono=now_mono,
                ):
                    continue
                read_ns = self._utc_ns()
                if name == "reference_observation" and env.reference_observation.HasField("depth_baseline"):
                    snap = env.reference_observation.depth_baseline
                    recv_ns = int(snap.receive_time_utc_ns)
                    self._utc_anchor = (recv_ns, now_mono)
                    ts_event = (
                        int(snap.exchange_event_time_ms) * 1_000_000
                        if snap.HasField("exchange_event_time_ms")
                        else recv_ns
                    )
                    deltas = bridge.apply_snapshot(
                        instrument=inst,
                        bids=[(Decimal(lvl.price), Decimal(lvl.quantity)) for lvl in snap.bids],
                        asks=[(Decimal(lvl.price), Decimal(lvl.quantity)) for lvl in snap.asks],
                        last_update_id=int(snap.last_update_id),
                        ts_event=ts_event,
                        ts_init=read_ns,
                    )
                    self._cfg.stats["items_received"] += 1
                    self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                    self._handle_data(deltas)
                    was_halted = bool(self._stream_gap[inst.id] or self._stream_stale[inst.id])
                    self._mark_healthy_activity(
                        inst.id,
                        Stream.DIFF_DEPTH,
                        now_mono,
                        read_ns,
                        recovery_reason="LIVE:l2_baseline_recovered",
                        allow_gap_clear=True,
                    )
                    if was_halted and not self._stream_gap[inst.id] and not self._stream_stale[inst.id]:
                        self._emit_status(
                            inst.id,
                            healthy=True,
                            reason="LIVE:l2_baseline_resync",
                            ts_ns=read_ns,
                        )
                    if self._reached_max_items():
                        break
                    continue

                if name != "depth_update":
                    self._cfg.stats["other_payloads"].append(name)
                    continue

                du = env.depth_update
                meta = du.metadata
                recv_ns = int(meta.receive_time_utc_ns)
                self._utc_anchor = (recv_ns, now_mono)
                ts_event = (
                    int(meta.exchange_event_time_ms) * 1_000_000
                    if meta.HasField("exchange_event_time_ms")
                    else recv_ns
                )
                self._mark_healthy_activity(
                    inst.id,
                    Stream.DIFF_DEPTH,
                    now_mono,
                    read_ns,
                    recovery_reason="LIVE:recovered_on_depth",
                )
                prev_uid = int(du.previous_final_update_id) if du.HasField("previous_final_update_id") else None
                deltas, gap_hit = bridge.apply_depth_update(
                    market=market,
                    instrument=inst,
                    first_update_id=int(du.first_update_id),
                    final_update_id=int(du.final_update_id),
                    previous_final_update_id=prev_uid,
                    bids=[(Decimal(lvl.price), Decimal(lvl.quantity)) for lvl in du.bids],
                    asks=[(Decimal(lvl.price), Decimal(lvl.quantity)) for lvl in du.asks],
                    ts_event=ts_event,
                    ts_init=read_ns,
                )
                self._cfg.stats["items_received"] += 1
                self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                if deltas is not None:
                    self._handle_data(deltas)
                if gap_hit:
                    self._stream_gap[inst.id].add(Stream.DIFF_DEPTH)
                    self._emit_status(
                        inst.id,
                        healthy=False,
                        reason="GAP:l2_sequence_gap",
                        ts_ns=read_ns,
                    )
                if self._reached_max_items():
                    break
        finally:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                cancel()

    async def _consume_mark_price(self, market: Market, symbol: str, inst: Any) -> None:
        stream = self._open_gateway_stream(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-mark",
            market=market,
            symbol=symbol,
            stream=Stream.MARK_PRICE,
            schemas=[_PAYLOAD_SCHEMAS[Stream.MARK_PRICE]],
        )
        p_dp = inst.price_precision
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if self._handle_control_payload(
                    name=name,
                    env=env,
                    inst=inst,
                    stream=Stream.MARK_PRICE,
                    now_mono=now_mono,
                ):
                    continue
                if name != "mark_price":
                    self._cfg.stats["other_payloads"].append(name)
                    continue
                mp = env.mark_price
                meta = mp.metadata
                read_ns = self._utc_ns()
                recv_ns = int(meta.receive_time_utc_ns)
                self._utc_anchor = (recv_ns, now_mono)
                self._mark_healthy_activity(
                    inst.id,
                    Stream.MARK_PRICE,
                    now_mono,
                    read_ns,
                    recovery_reason="LIVE:recovered_on_mark_price",
                )
                ts_event = (
                    int(meta.exchange_event_time_ms) * 1_000_000
                    if meta.HasField("exchange_event_time_ms")
                    else recv_ns
                )
                wants = self._mark_wants.get(inst.id, {"mark": True, "funding": False})
                self._cfg.stats["items_received"] += 1
                self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                if wants.get("mark"):
                    self._handle_data(
                        MarkPriceUpdate(
                            instrument_id=inst.id,
                            value=Price.from_decimal_dp(Decimal(mp.mark_price), p_dp),
                            ts_event=ts_event,
                            ts_init=read_ns,
                        )
                    )
                if wants.get("funding"):
                    self._handle_data(
                        FundingRateUpdate(
                            instrument_id=inst.id,
                            rate=Decimal(mp.funding_rate),
                            ts_event=ts_event,
                            ts_init=read_ns,
                            interval=480,
                            next_funding_ns=int(mp.next_funding_time_ms) * 1_000_000,
                        )
                    )
                if self._reached_max_items():
                    break
        finally:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                cancel()

    async def _consume_custom_stream(
        self,
        market: Market,
        symbol: str,
        inst: Any,
        stream_kind: Stream,
    ) -> None:
        stream = self._open_gateway_stream(
            request_id=f"bmd-nautilus-{market.value}-{symbol}-{stream_kind.value}",
            market=market,
            symbol=symbol,
            stream=stream_kind,
            schemas=[_PAYLOAD_SCHEMAS[stream_kind]],
        )
        try:
            async for env in stream:
                if self._stop:
                    break
                name = env.WhichOneof("payload")
                now_mono = self._mono_ns()
                if self._handle_control_payload(
                    name=name,
                    env=env,
                    inst=inst,
                    stream=stream_kind,
                    now_mono=now_mono,
                ):
                    continue
                read_ns = self._utc_ns()
                if stream_kind == Stream.LIQUIDATION and name == "liquidation":
                    lq = env.liquidation
                    recv_ns = int(lq.metadata.receive_time_utc_ns)
                    self._utc_anchor = (recv_ns, now_mono)
                    self._mark_healthy_activity(
                        inst.id,
                        Stream.LIQUIDATION,
                        now_mono,
                        read_ns,
                        recovery_reason="LIVE:recovered_on_liquidation",
                    )
                    payload = BmdLiquidationData(
                        market=market.value,
                        symbol=symbol,
                        side=lq.side,
                        order_type=lq.order_type,
                        time_in_force=lq.time_in_force,
                        original_quantity=lq.original_quantity,
                        price=lq.price,
                        average_price=lq.average_price if lq.HasField("average_price") else None,
                        order_status=lq.order_status,
                        last_filled_quantity=lq.last_filled_quantity,
                        accumulated_filled_quantity=lq.accumulated_filled_quantity,
                        order_trade_time_ms=int(lq.order_trade_time_ms),
                        ts_event=int(lq.order_trade_time_ms) * 1_000_000,
                        ts_init=read_ns,
                    )
                    self._cfg.stats["items_received"] += 1
                    self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                    self._handle_data(wrap_custom_data(BMD_LIQUIDATION_DATATYPE, payload))
                elif stream_kind == Stream.REFERENCE_DATA and name == "reference_observation":
                    ro = env.reference_observation
                    recv_ns = int(ro.receive_time_utc_ns)
                    self._utc_anchor = (recv_ns, now_mono)
                    self._mark_healthy_activity(
                        inst.id,
                        Stream.REFERENCE_DATA,
                        now_mono,
                        read_ns,
                        recovery_reason="LIVE:recovered_on_reference",
                    )
                    fr = ro.instrument.funding_rate_settled if ro.HasField("instrument") and ro.instrument.HasField("funding_rate_settled") else None
                    ft = int(ro.instrument.funding_time_ms) if ro.HasField("instrument") and ro.instrument.HasField("funding_time_ms") else None
                    payload = BmdReferenceData(
                        market=market.value,
                        symbol=symbol,
                        source_kind=str(ro.source_kind),
                        status=str(ro.status),
                        payload_json="{}",
                        source_time_ms=int(ro.source_time_ms) if ro.HasField("source_time_ms") else None,
                        funding_rate_settled=fr,
                        funding_time_ms=ft,
                        tick_size=None,
                        step_size=None,
                        min_notional=None,
                        ts_event=int(ro.source_time_ms) * 1_000_000 if ro.HasField("source_time_ms") else recv_ns,
                        ts_init=read_ns,
                    )
                    self._cfg.stats["items_received"] += 1
                    self._cfg.stats["dequeue_latencies_ns"].append(read_ns - recv_ns)
                    self._handle_data(wrap_custom_data(BMD_REFERENCE_DATATYPE, payload))
                else:
                    self._cfg.stats["other_payloads"].append(name)
                    continue
                if self._reached_max_items():
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
