"""Live BinanceMarketDataGateway gRPC consumer with Fail-Fast capabilities & STALE watchdog."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
import json
import queue
import threading
import time
from typing import Any

import grpc
from google.protobuf.json_format import MessageToDict

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
    _opt_decimal,
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

_REF_SOURCE_KIND_MAP = {
    enums_pb2.REFERENCE_SOURCE_KIND_UNSPECIFIED: "unspecified",
    enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST: "open_interest",
    enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO: "top_long_short_account_ratio",
    enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO: "top_long_short_position_ratio",
    enums_pb2.REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO: "global_long_short_account_ratio",
    enums_pb2.REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME: "taker_buy_sell_volume",
    enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT: "instrument",
    enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE: "depth_baseline",
}

_REF_STATUS_MAP = {
    enums_pb2.REFERENCE_OBSERVATION_STATUS_UNSPECIFIED: "UNSPECIFIED",
    enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID: "VALID",
    enums_pb2.REFERENCE_OBSERVATION_STATUS_EMPTY: "EMPTY",
    enums_pb2.REFERENCE_OBSERVATION_STATUS_ERROR: "ERROR",
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
        utc_clock_ns: Callable[[], int] | None = None,
        strict_capabilities: bool = True,
        async_watchdog: bool | None = None,
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
        self._utc_clock_ns = utc_clock_ns
        self._async_watchdog = (self._channel is not None) if async_watchdog is None else async_watchdog
        self._utc_anchor: tuple[int, int] | None = None
        self._strict_capabilities = strict_capabilities
        self._delivery_seq = 0
        self._last_activity_ns: dict[tuple[Market, str, Stream], int] = {}
        self._marked_stale: set[tuple[Market, str, Stream]] = set()
        self._pending_watchdog: deque[StreamHealthEvent] = deque()
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

    def _current_utc_ns(self, mono_ns: int) -> int:
        if self._utc_clock_ns is not None:
            return int(self._utc_clock_ns())
        if self._utc_anchor is not None:
            anchor_utc, anchor_mono = self._utc_anchor
            return anchor_utc + (mono_ns - anchor_mono)
        return time.time_ns()

    def _calibrate_utc(self, utc_ns: int | None, mono_ns: int) -> None:
        if utc_ns is not None and utc_ns > 0:
            self._utc_anchor = (int(utc_ns), mono_ns)

    def _check_silence_before_activity(self, key: tuple[Market, str, Stream], t_mono: int) -> None:
        last_t = self._last_activity_ns.get(key)
        if last_t is not None and key not in self._marked_stale and (t_mono - last_t) > self._stale_threshold_ns:
            self._marked_stale.add(key)
            market, symbol, stream = key
            self._pending_watchdog.append(
                StreamHealthEvent(
                    venue="BINANCE",
                    market=market,
                    symbol=symbol,
                    stream=stream,
                    status="STALE",
                    reason=f"watchdog_timeout>{self._stale_threshold_ns}ns",
                    observed_time_utc_ns=self._current_utc_ns(t_mono),
                    delivery_seq=self._next_seq(),
                    source="gateway_watchdog",
                )
            )

    def _record_activity(
        self,
        key: tuple[Market, str, Stream],
        t_mono: int,
        *,
        event_utc_ns: int | None = None,
    ) -> None:
        self._check_silence_before_activity(key, t_mono)
        self._calibrate_utc(event_utc_ns, t_mono)
        self._last_activity_ns[key] = t_mono
        self._marked_stale.discard(key)

    def register_watched_stream(
        self, market: Market, symbol: str, stream: Stream, now_ns: int | None = None
    ) -> None:
        t = self._clock_ns() if now_ns is None else now_ns
        key = (market, symbol, stream)
        self._last_activity_ns[key] = t
        self._marked_stale.discard(key)

    def drain_pending_watchdog(self) -> list[StreamHealthEvent]:
        out: list[StreamHealthEvent] = []
        while self._pending_watchdog:
            out.append(self._pending_watchdog.popleft())
        return out

    def poll_watchdog(self, now_ns: int | None = None) -> list[StreamHealthEvent]:
        """Emit STALE health events for any watched stream exceeding stale_threshold_ns."""
        t_mono = self._clock_ns() if now_ns is None else now_ns
        events: list[StreamHealthEvent] = self.drain_pending_watchdog()
        utc_now = self._current_utc_ns(t_mono)
        for key, last_t in sorted(self._last_activity_ns.items(), key=lambda kv: (kv[0][0].value, kv[0][1], kv[0][2].value)):
            if key not in self._marked_stale and (t_mono - last_t) > self._stale_threshold_ns:
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
                        observed_time_utc_ns=utc_now,
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

        if payload_name == "book_ticker":
            bt = env.book_ticker
            meta = bt.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            recv_utc = int(meta.receive_time_utc_ns)
            self._record_activity((mkt, meta.symbol, Stream.BOOK_TICKER), t_mono, event_utc_ns=recv_utc)
            seq = self._next_seq()
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
                receive_time_utc_ns=recv_utc,
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "agg_trade":
            at = env.agg_trade
            meta = at.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            recv_utc = int(meta.receive_time_utc_ns)
            self._record_activity((mkt, meta.symbol, Stream.AGG_TRADE), t_mono, event_utc_ns=recv_utc)
            seq = self._next_seq()
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
                receive_time_utc_ns=recv_utc,
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "depth_update":
            du = env.depth_update
            meta = du.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            recv_utc = int(meta.receive_time_utc_ns)
            self._record_activity((mkt, meta.symbol, Stream.DIFF_DEPTH), t_mono, event_utc_ns=recv_utc)
            seq = self._next_seq()
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
                receive_time_utc_ns=recv_utc,
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "mark_price":
            mp = env.mark_price
            meta = mp.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            recv_utc = int(meta.receive_time_utc_ns)
            self._record_activity((mkt, meta.symbol, Stream.MARK_PRICE), t_mono, event_utc_ns=recv_utc)
            seq = self._next_seq()
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
                receive_time_utc_ns=recv_utc,
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "liquidation":
            lq = env.liquidation
            meta = lq.metadata
            mkt = _PROTO_TO_MARKET[meta.market]
            recv_utc = int(meta.receive_time_utc_ns)
            self._record_activity((mkt, meta.symbol, Stream.LIQUIDATION), t_mono, event_utc_ns=recv_utc)
            seq = self._next_seq()
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
                receive_time_utc_ns=recv_utc,
                receive_monotonic_ns=int(meta.receive_monotonic_ns),
                source="gateway_grpc",
                delivery_seq=seq,
            )

        if payload_name == "reference_observation":
            ro = env.reference_observation
            mkt = _PROTO_TO_MARKET[ro.market]
            recv_ns = int(ro.receive_time_utc_ns) if ro.receive_time_utc_ns > 0 else None
            self._record_activity((mkt, ro.symbol, Stream.REFERENCE_DATA), t_mono, event_utc_ns=recv_ns)
            seq = self._next_seq()
            kind_str = _REF_SOURCE_KIND_MAP.get(ro.source_kind, str(ro.source_kind))
            status_str = _REF_STATUS_MAP.get(ro.status, str(ro.status))
            oneof_name = ro.WhichOneof("payload")
            payload_dict: dict[str, Any] = {}
            tick_size = step_size = min_notional = None
            sym_present = trading_status = base_asset = quote_asset = None
            if oneof_name is not None:
                sub = getattr(ro, oneof_name)
                payload_dict[oneof_name] = MessageToDict(sub, preserving_proto_field_name=True)
                if oneof_name == "instrument":
                    sym_present = bool(sub.symbol_present)
                    trading_status = sub.trading_status or None
                    base_asset = sub.base_asset or None
                    quote_asset = sub.quote_asset or None
                    market_lot_step = None
                    for flt in sub.filters:
                        ft = flt.filter_type
                        if ft == "PRICE_FILTER" and flt.HasField("tick_size"):
                            tick_size = _opt_decimal(flt.tick_size)
                        elif ft == "LOT_SIZE" and flt.HasField("step_size"):
                            step_size = _opt_decimal(flt.step_size)
                        elif ft == "MARKET_LOT_SIZE" and flt.HasField("step_size"):
                            market_lot_step = _opt_decimal(flt.step_size)
                        elif ft in ("MIN_NOTIONAL", "NOTIONAL") and flt.HasField("notional"):
                            min_notional = _opt_decimal(flt.notional)
                    if step_size is None:
                        step_size = market_lot_step
            return ReferenceObservationEvent(
                venue="BINANCE",
                market=mkt,
                symbol=ro.symbol,
                source_kind=kind_str,
                status=status_str,
                payload_json=json.dumps(payload_dict, separators=(",", ":"), sort_keys=True),
                source_time_ms=int(ro.source_time_ms) if ro.HasField("source_time_ms") else None,
                observe_time_utc_ns=recv_ns,
                receive_time_utc_ns=recv_ns,
                tick_size=tick_size,
                step_size=step_size,
                min_notional=min_notional,
                symbol_present=sym_present,
                trading_status=trading_status,
                base_asset=base_asset,
                quote_asset=quote_asset,
                error_code=ro.error_code if ro.HasField("error_code") else None,
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
        use_timed_queue = self._async_watchdog
        try:
            if use_timed_queue:
                q: queue.Queue[tuple[str, Any]] = queue.Queue()
                stop_ev = threading.Event()

                def _pump() -> None:
                    try:
                        for env in rpc_stream:
                            if stop_ev.is_set():
                                break
                            q.put(("item", env))
                        q.put(("eof", None))
                    except Exception as exc:  # pragma: no cover - transport failure
                        q.put(("err", exc))

                t = threading.Thread(target=_pump, daemon=True)
                t.start()
                poll_s = min(0.5, max(0.05, self._stale_threshold_ns / 2_000_000_000.0))
                while True:
                    try:
                        kind, payload = q.get(timeout=poll_s)
                    except queue.Empty:
                        for w_ev in self.poll_watchdog():
                            yield w_ev
                        continue
                    if kind == "eof":
                        for w_ev in self.poll_watchdog():
                            yield w_ev
                        break
                    if kind == "err":
                        raise payload
                    ev = self.translate_event_envelope(payload, watched_key=key)
                    for w_ev in self.drain_pending_watchdog():
                        yield w_ev
                    if ev is not None:
                        yield ev
                        count += 1
                        if max_items is not None and count >= max_items:
                            stop_ev.set()
                            break
            else:
                for env in rpc_stream:
                    ev = self.translate_event_envelope(env, watched_key=key)
                    for w_ev in self.drain_pending_watchdog():
                        yield w_ev
                    if ev is not None:
                        yield ev
                        count += 1
                        if max_items is not None and count >= max_items:
                            break
        finally:
            cancel = getattr(rpc_stream, "cancel", None)
            if callable(cancel):
                cancel()
