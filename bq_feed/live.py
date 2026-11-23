"""Live BinanceMarketDataGateway gRPC consumer with Fail-Fast capabilities & STALE watchdog."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
import time
from typing import Any

import grpc

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

from .events import (
    AggTradeEvent,
    BookTickerEvent,
    DepthUpdateEvent,
    FeedEvent,
    LiquidationEvent,
    Market,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    Stream,
    StreamHealthEvent,
    to_decimal,
)


class CapabilityMismatchError(RuntimeError):
    """Raised immediately (Fail-Fast) when Gateway lacks required streams."""


_MARKET_TO_PROTO = {
    Market.SPOT: enums_pb2.MARKET_SPOT,
    Market.USDM: enums_pb2.MARKET_USD_M_PERPETUAL,
}
_PROTO_TO_MARKET = {
    enums_pb2.MARKET_SPOT: Market.SPOT,
    enums_pb2.MARKET_USD_M_PERPETUAL: Market.USDM,
}
_STREAM_TO_PROTO = {
    Stream.DIFF_DEPTH: enums_pb2.STREAM_DIFF_DEPTH,
    Stream.AGG_TRADE: enums_pb2.STREAM_AGG_TRADE,
    Stream.BOOK_TICKER: enums_pb2.STREAM_BOOK_TICKER,
    Stream.MARK_PRICE: enums_pb2.STREAM_MARK_PRICE,
    Stream.LIQUIDATION: enums_pb2.STREAM_LIQUIDATION,
    Stream.REFERENCE_DATA: enums_pb2.STREAM_REFERENCE_DATA,
}
_PROTO_TO_STREAM = {v: k for k, v in _STREAM_TO_PROTO.items()}
_PAYLOAD_SCHEMAS = {
    Stream.BOOK_TICKER: "book-ticker.v1",
    Stream.AGG_TRADE: "agg-trade.v1",
    Stream.DIFF_DEPTH: "depth-update.v1",
    Stream.MARK_PRICE: "mark-price.v1",
    Stream.LIQUIDATION: "liquidation.v1",
    Stream.REFERENCE_DATA: "reference-observation.v1",
}
_LIFECYCLE_MAP = {
    enums_pb2.STREAM_LIFECYCLE_STATE_LIVE: "LIVE",
    enums_pb2.STREAM_LIFECYCLE_STATE_DEGRADED: "DEGRADED",
    enums_pb2.STREAM_LIFECYCLE_STATE_RESYNC_IN_PROGRESS: "GAP",
    enums_pb2.STREAM_LIFECYCLE_STATE_CLOSING: "CLOSED",
    enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED: "CLOSED",
    enums_pb2.STREAM_LIFECYCLE_STATE_ACCEPTED: "DEGRADED",
    enums_pb2.STREAM_LIFECYCLE_STATE_SNAPSHOT_PENDING: "DEGRADED",
}


def _meta_opt_int(meta: Any, field_name: str) -> int | None:
    if meta.HasField(field_name):
        return int(getattr(meta, field_name))
    return None


def _levels_from_proto(levels: Iterable[Any]) -> tuple[tuple[Any, Any], ...]:
    return tuple((to_decimal(lvl.price), to_decimal(lvl.quantity)) for lvl in levels)


class GatewayLiveFeed:
    """Consumes real-time streams from BMDG with capability preflight and STALE watchdog."""

    def __init__(
        self,
        *,
        target: str = "127.0.0.1:50051",
        stub: Any | None = None,
        stale_threshold_ns: int = 5_000_000_000,
        clock_ns: Callable[[], int] | None = None,
        strict_capabilities: bool = True,
    ) -> None:
        self._target = target
        self._channel: grpc.Channel | None = None
        if stub is None:
            self._channel = grpc.insecure_channel(target)
            self._stub = gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(self._channel)
        else:
            self._stub = stub
        self._stale_threshold_ns = stale_threshold_ns
        self._clock_ns = clock_ns or time.monotonic_ns
        self._strict_capabilities = strict_capabilities
        self._delivery_seq = 0
        self._last_activity_ns: dict[tuple[Market, str, Stream], int] = {}
        self._marked_stale: set[tuple[Market, str, Stream]] = set()
        self._last_status_snapshot: Any = None

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def _next_seq(self) -> int:
        self._delivery_seq += 1
        return self._delivery_seq

    def fetch_gateway_status(self, request_id: str = "bq-preflight") -> Any:
        req = gateway_messages_pb2.GatewayStatusRequest()
        req.request_id = request_id
        req.schema_version = "gateway-status-request.v1"
        snap = self._stub.GetGatewayStatus(req, timeout=5.0)
        self._last_status_snapshot = snap
        return snap

    def preflight_capabilities(
        self, required_streams: Iterable[tuple[Market, str, Stream]]
    ) -> Any:
        """Fail-Fast check verifying GetGatewayStatus.enabled_streams satisfies strategy inputs."""
        reqs = list(required_streams)
        snap = self.fetch_gateway_status()
        by_market: dict[tuple[int, str], Any] = {}
        for m in snap.markets:
            by_market[(m.market, m.symbol)] = m

        for market, symbol, stream in reqs:
            proto_mkt = _MARKET_TO_PROTO.get(market)
            if proto_mkt is None:
                raise CapabilityMismatchError(
                    f"Market {market.value} is not supported by live Gateway (D2 frozen)"
                )
            row = by_market.get((proto_mkt, symbol))
            if row is None:
                raise CapabilityMismatchError(
                    f"Missing configured product ({market.value}, {symbol}) in Gateway status"
                )
            if not row.HasField("enabled_streams"):
                if self._strict_capabilities:
                    raise CapabilityMismatchError(
                        f"Gateway status missing enabled_streams capability field for {market.value}:{symbol}"
                    )
                continue
            caps = row.enabled_streams
            if stream == Stream.ORDERBOOK:
                if not caps.orderbook_rpc_enabled:
                    raise CapabilityMismatchError(
                        f"Gateway disabled SubscribeOrderBook for {market.value}:{symbol}"
                    )
            else:
                proto_stream = _STREAM_TO_PROTO.get(stream)
                if proto_stream not in list(caps.streams):
                    raise CapabilityMismatchError(
                        f"Gateway disabled stream {stream.value} for {market.value}:{symbol}"
                    )
        return snap

    def register_watched_stream(
        self, market: Market, symbol: str, stream: Stream, now_ns: int | None = None
    ) -> None:
        t = self._clock_ns() if now_ns is None else now_ns
        key = (market, symbol, stream)
        self._last_activity_ns[key] = t
        self._marked_stale.discard(key)

    def poll_watchdog(self, now_ns: int | None = None) -> list[StreamHealthEvent]:
        """Emit STALE health events for any watched stream exceeding stale_threshold_ns."""
        t = self._clock_ns() if now_ns is None else now_ns
        events: list[StreamHealthEvent] = []
        for key, last_t in sorted(self._last_activity_ns.items(), key=lambda kv: (kv[0][0].value, kv[0][1], kv[0][2].value)):
            if key not in self._marked_stale and (t - last_t) > self._stale_threshold_ns:
                self._marked_stale.add(key)
                market, symbol, stream = key
                events.append(
                    StreamHealthEvent(
                        venue="BINANCE",
                        market=market,
                        symbol=symbol,
                        stream=stream,
                        status="STALE",
                        reason=f"watchdog_timeout>{self._stale_threshold_ns}ns",
                        observed_time_utc_ns=t,
                        delivery_seq=self._next_seq(),
                        source="gateway_watchdog",
                    )
                )
        return events

    def translate_event_envelope(
        self,
        env: Any,
        *,
        watched_key: tuple[Market, str, Stream] | None = None,
        now_ns: int | None = None,
    ) -> FeedEvent | None:
        t_mono = self._clock_ns() if now_ns is None else now_ns
        payload_name = env.WhichOneof("payload")
        if payload_name == "subscription_accepted":
            if watched_key is not None:
                self.register_watched_stream(*watched_key, now_ns=t_mono)
            return None

        if payload_name == "consumer_gap":
            gap = env.consumer_gap
            mkt = _PROTO_TO_MARKET.get(gap.market, watched_key[0] if watched_key else Market.SPOT)
            sym = gap.symbol or (watched_key[1] if watched_key else "")
            strm = _PROTO_TO_STREAM.get(gap.stream, watched_key[2] if watched_key else Stream.BOOK_TICKER)
            return StreamHealthEvent(
                venue="BINANCE",
                market=mkt,
                symbol=sym,
                stream=strm,
                status="GAP",
                reason=f"consumer_gap:{gap.reason}",
                observed_time_utc_ns=int(gap.detected_time_utc_ns),
                delivery_seq=self._next_seq(),
                gap_start_seq=gap.last_delivered_session_sequence if gap.HasField("last_delivered_session_sequence") else None,
                gap_end_seq=gap.next_available_session_sequence if gap.HasField("next_available_session_sequence") else None,
                connection_generation=env.delivery_metadata.connection_generation if env.delivery_metadata.HasField("connection_generation") else None,
                source="gateway_grpc",
            )

        if payload_name == "stream_status":
            st = env.stream_status
            mapped = _LIFECYCLE_MAP.get(st.state, "DEGRADED")
            mkt, sym, strm = watched_key if watched_key else (Market.SPOT, "", Stream.BOOK_TICKER)
            if mapped == "LIVE":
                self._last_activity_ns[(mkt, sym, strm)] = t_mono
                self._marked_stale.discard((mkt, sym, strm))
            return StreamHealthEvent(
                venue="BINANCE",
                market=mkt,
                symbol=sym,
                stream=strm,
                status=mapped,
                reason=st.message if st.HasField("message") else f"state:{st.state}",
                observed_time_utc_ns=int(st.observed_time_utc_ns),
                delivery_seq=self._next_seq(),
                connection_generation=env.delivery_metadata.connection_generation if env.delivery_metadata.HasField("connection_generation") else None,
                source="gateway_grpc",
            )

        seq = self._next_seq()
        if payload_name == "book_ticker":
            bt = env.book_ticker
            meta = bt.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            self._last_activity_ns[(mkt, meta.symbol, Stream.BOOK_TICKER)] = t_mono
            self._marked_stale.discard((mkt, meta.symbol, Stream.BOOK_TICKER))
            return BookTickerEvent(
                venue="BINANCE",
                market=mkt,
                symbol=meta.symbol,
                update_id=int(bt.update_id) if bt.HasField("update_id") else None,
                best_bid_price=to_decimal(bt.best_bid_price),
                best_bid_quantity=to_decimal(bt.best_bid_quantity),
                best_ask_price=to_decimal(bt.best_ask_price),
                best_ask_quantity=to_decimal(bt.best_ask_quantity),
                exchange_event_time_ms=_meta_opt_int(meta, "exchange_event_time_ms"),
                exchange_transaction_time_ms=_meta_opt_int(meta, "exchange_transaction_time_ms"),
                receive_time_utc_ns=int(meta.receive_time_utc_ns),
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "agg_trade":
            at = env.agg_trade
            meta = at.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            self._last_activity_ns[(mkt, meta.symbol, Stream.AGG_TRADE)] = t_mono
            self._marked_stale.discard((mkt, meta.symbol, Stream.AGG_TRADE))
            return AggTradeEvent(
                venue="BINANCE",
                market=mkt,
                symbol=meta.symbol,
                aggregate_trade_id=int(at.aggregate_trade_id),
                price=to_decimal(at.price),
                quantity=to_decimal(at.quantity),
                first_trade_id=int(at.first_trade_id),
                last_trade_id=int(at.last_trade_id),
                trade_time_ms=int(at.trade_time_ms),
                buyer_is_maker=bool(at.buyer_is_maker),
                normal_quantity=to_decimal(at.normal_quantity) if at.HasField("normal_quantity") else None,
                exchange_event_time_ms=_meta_opt_int(meta, "exchange_event_time_ms"),
                receive_time_utc_ns=int(meta.receive_time_utc_ns),
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "depth_update":
            du = env.depth_update
            meta = du.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            self._last_activity_ns[(mkt, meta.symbol, Stream.DIFF_DEPTH)] = t_mono
            self._marked_stale.discard((mkt, meta.symbol, Stream.DIFF_DEPTH))
            return DepthUpdateEvent(
                venue="BINANCE",
                market=mkt,
                symbol=meta.symbol,
                first_update_id=int(du.first_update_id),
                final_update_id=int(du.final_update_id),
                previous_final_update_id=int(du.previous_final_update_id) if du.HasField("previous_final_update_id") else None,
                bids=_levels_from_proto(du.bids),
                asks=_levels_from_proto(du.asks),
                exchange_event_time_ms=_meta_opt_int(meta, "exchange_event_time_ms"),
                exchange_transaction_time_ms=_meta_opt_int(meta, "exchange_transaction_time_ms"),
                receive_time_utc_ns=int(meta.receive_time_utc_ns),
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "mark_price":
            mp = env.mark_price
            meta = mp.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            self._last_activity_ns[(mkt, meta.symbol, Stream.MARK_PRICE)] = t_mono
            self._marked_stale.discard((mkt, meta.symbol, Stream.MARK_PRICE))
            return MarkPriceEvent(
                venue="BINANCE",
                market=mkt,
                symbol=meta.symbol,
                mark_price=to_decimal(mp.mark_price),
                index_price=to_decimal(mp.index_price),
                estimated_settle_price=to_decimal(mp.estimated_settle_price),
                funding_rate=to_decimal(mp.funding_rate),
                next_funding_time_ms=int(mp.next_funding_time_ms),
                mark_price_moving_average=to_decimal(mp.mark_price_moving_average) if mp.HasField("mark_price_moving_average") else None,
                exchange_event_time_ms=_meta_opt_int(meta, "exchange_event_time_ms"),
                receive_time_utc_ns=int(meta.receive_time_utc_ns),
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "liquidation":
            lq = env.liquidation
            meta = lq.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            self._last_activity_ns[(mkt, meta.symbol, Stream.LIQUIDATION)] = t_mono
            self._marked_stale.discard((mkt, meta.symbol, Stream.LIQUIDATION))
            return LiquidationEvent(
                venue="BINANCE",
                market=mkt,
                symbol=meta.symbol,
                side=lq.side,
                order_type=lq.order_type,
                time_in_force=lq.time_in_force,
                original_quantity=to_decimal(lq.original_quantity),
                price=to_decimal(lq.price),
                average_price=to_decimal(lq.average_price) if lq.HasField("average_price") else None,
                order_status=lq.order_status,
                last_filled_quantity=to_decimal(lq.last_filled_quantity),
                accumulated_filled_quantity=to_decimal(lq.accumulated_filled_quantity),
                order_trade_time_ms=int(lq.order_trade_time_ms),
                exchange_event_time_ms=_meta_opt_int(meta, "exchange_event_time_ms"),
                receive_time_utc_ns=int(meta.receive_time_utc_ns),
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "reference_observation":
            ro = env.reference_observation
            mkt = _PROTO_TO_MARKET[ro.market]
            self._last_activity_ns[(mkt, ro.symbol, Stream.REFERENCE_DATA)] = t_mono
            self._marked_stale.discard((mkt, ro.symbol, Stream.REFERENCE_DATA))
            return ReferenceObservationEvent(
                venue="BINANCE",
                market=mkt,
                symbol=ro.symbol,
                source_kind=str(ro.source_kind),
                status=str(ro.status),
                payload_json=ro.payload_json or "{}",
                source_time_ms=int(ro.source_time_ms) if ro.HasField("source_time_ms") else None,
                observe_time_utc_ns=int(ro.observe_time_utc_ns) if ro.HasField("observe_time_utc_ns") else None,
                receive_time_utc_ns=int(ro.observe_time_utc_ns) if ro.HasField("observe_time_utc_ns") else None,
                source="gateway_grpc",
                delivery_seq=seq,
            )
        return None

    def subscribe_event_stream(
        self,
        market: Market,
        symbol: str,
        stream: Stream,
        *,
        request_id: str = "bq-live-ev",
        preflight: bool = True,
        max_items: int | None = None,
    ) -> Iterator[FeedEvent]:
        if preflight:
            self.preflight_capabilities([(market, symbol, stream)])
        req = gateway_messages_pb2.EventSubscriptionRequest()
        req.request_id = request_id
        req.schema_version = "event-subscription-request.v1"
        sel = req.selectors.add()
        sel.venue = enums_pb2.VENUE_BINANCE
        sel.market = _MARKET_TO_PROTO[market]
        sel.symbol = symbol
        sel.stream = _STREAM_TO_PROTO[stream]
        req.delivery_mode = enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS
        req.supported_payload_schema_versions.append(_PAYLOAD_SCHEMAS[stream])

        key = (market, symbol, stream)
        self.register_watched_stream(*key)
        count = 0
        rpc_stream = self._stub.SubscribeEvents(req)
        try:
            for env in rpc_stream:
                ev = self.translate_event_envelope(env, watched_key=key)
                if ev is not None:
                    yield ev
                    count += 1
                    if max_items is not None and count >= max_items:
                        break
        finally:
            cancel = getattr(rpc_stream, "cancel", None)
            if callable(cancel):
                cancel()
