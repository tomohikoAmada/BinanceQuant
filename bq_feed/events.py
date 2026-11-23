"""Canonical typed market events and serialization for offline and live feeds."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Union


class Market(str, Enum):
    SPOT = "SPOT"
    USDM = "USD_M_PERPETUAL"
    COINM = "COIN_M_PERPETUAL"
    OPTIONS = "OPTIONS"


class Stream(str, Enum):
    BOOK_TICKER = "book_ticker"
    AGG_TRADE = "agg_trade"
    DIFF_DEPTH = "diff_depth"
    ORDERBOOK = "orderbook"
    MARK_PRICE = "mark_price"
    LIQUIDATION = "liquidation"
    REFERENCE_DATA = "reference_data"


def to_decimal(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _opt_decimal(value: Decimal | str | int | float | None) -> Decimal | None:
    if value is None or value == "":
        return None
    return to_decimal(value)


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


@dataclass(frozen=True, slots=True)
class BookTickerEvent:
    venue: str
    market: Market
    symbol: str
    best_bid_price: Decimal
    best_bid_quantity: Decimal
    best_ask_price: Decimal
    best_ask_quantity: Decimal
    update_id: int | None = None
    exchange_event_time_ms: int | None = None
    exchange_transaction_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.BOOK_TICKER


@dataclass(frozen=True, slots=True)
class AggTradeEvent:
    venue: str
    market: Market
    symbol: str
    aggregate_trade_id: int
    price: Decimal
    quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    buyer_is_maker: bool
    normal_quantity: Decimal | None = None
    exchange_event_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.AGG_TRADE


@dataclass(frozen=True, slots=True)
class DepthUpdateEvent:
    venue: str
    market: Market
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    previous_final_update_id: int | None = None
    exchange_event_time_ms: int | None = None
    exchange_transaction_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.DIFF_DEPTH


@dataclass(frozen=True, slots=True)
class OrderBookSnapshotEvent:
    venue: str
    market: Market
    symbol: str
    last_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    is_baseline: bool = True
    connection_generation: int | None = None
    exchange_event_time_ms: int | None = None
    exchange_transaction_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.ORDERBOOK


@dataclass(frozen=True, slots=True)
class MarkPriceEvent:
    venue: str
    market: Market
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    estimated_settle_price: Decimal
    funding_rate: Decimal
    next_funding_time_ms: int
    mark_price_moving_average: Decimal | None = None
    exchange_event_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.MARK_PRICE


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    venue: str
    market: Market
    symbol: str
    side: str
    order_type: str
    time_in_force: str
    original_quantity: Decimal
    price: Decimal
    order_status: str
    last_filled_quantity: Decimal
    accumulated_filled_quantity: Decimal
    order_trade_time_ms: int
    average_price: Decimal | None = None
    exchange_event_time_ms: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.LIQUIDATION


@dataclass(frozen=True, slots=True)
class ReferenceObservationEvent:
    venue: str
    market: Market
    symbol: str
    source_kind: str
    status: str = "VALID"
    payload_json: str = "{}"
    source_time_ms: int | None = None
    observe_time_utc_ns: int | None = None
    receive_time_utc_ns: int | None = None
    receive_monotonic_ns: int | None = None
    tick_size: Decimal | None = None
    step_size: Decimal | None = None
    min_notional: Decimal | None = None
    funding_rate_settled: Decimal | None = None
    funding_time_ms: int | None = None
    source: str = "unknown"
    delivery_seq: int = 0
    quality_flags: tuple[str, ...] = ()
    stream: Stream = Stream.REFERENCE_DATA


@dataclass(frozen=True, slots=True)
class StreamHealthEvent:
    venue: str
    market: Market
    symbol: str
    stream: Stream
    status: str  # "LIVE", "DEGRADED", "STALE", "GAP", "CLOSED", "STOPPED"
    reason: str
    observed_time_utc_ns: int
    delivery_seq: int = 0
    gap_start_seq: int | None = None
    gap_end_seq: int | None = None
    connection_generation: int | None = None
    source: str = "unknown"


MarketEvent = Union[
    BookTickerEvent,
    AggTradeEvent,
    DepthUpdateEvent,
    OrderBookSnapshotEvent,
    MarkPriceEvent,
    LiquidationEvent,
    ReferenceObservationEvent,
]
FeedEvent = Union[MarketEvent, StreamHealthEvent]


def event_identity(event: FeedEvent) -> tuple[str, str, str, str | None]:
    """Return cross-collector identity key: (market, symbol, stream, primary_id)."""
    market = event.market.value
    stream = event.stream.value
    if isinstance(event, BookTickerEvent):
        pid = str(event.update_id) if event.update_id is not None else None
        return (market, event.symbol, stream, pid)
    if isinstance(event, AggTradeEvent):
        return (market, event.symbol, stream, str(event.aggregate_trade_id))
    if isinstance(event, DepthUpdateEvent):
        return (market, event.symbol, stream, f"{event.first_update_id}-{event.final_update_id}")
    if isinstance(event, OrderBookSnapshotEvent):
        return (market, event.symbol, stream, f"snap-{event.last_update_id}")
    if isinstance(event, MarkPriceEvent):
        return (market, event.symbol, stream, f"{event.next_funding_time_ms}:{event.exchange_event_time_ms}")
    if isinstance(event, LiquidationEvent):
        return (
            market,
            event.symbol,
            stream,
            f"{event.order_trade_time_ms}:{event.side}:{event.price}:{event.original_quantity}",
        )
    if isinstance(event, ReferenceObservationEvent):
        key = event.funding_time_ms or event.source_time_ms or event.observe_time_utc_ns
        return (market, event.symbol, stream, f"{event.source_kind}:{key}")
    return (market, event.symbol, stream, f"health:{event.status}:{event.observed_time_utc_ns}")


def _encode_value(val: Any) -> Any:
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, Enum):
        return val.value
    if isinstance(val, tuple):
        return [_encode_value(x) for x in val]
    if isinstance(val, list):
        return [_encode_value(x) for x in val]
    if isinstance(val, dict):
        return {k: _encode_value(v) for k, v in val.items()}
    return val


_TYPE_MAP = {
    "BookTickerEvent": BookTickerEvent,
    "AggTradeEvent": AggTradeEvent,
    "DepthUpdateEvent": DepthUpdateEvent,
    "OrderBookSnapshotEvent": OrderBookSnapshotEvent,
    "MarkPriceEvent": MarkPriceEvent,
    "LiquidationEvent": LiquidationEvent,
    "ReferenceObservationEvent": ReferenceObservationEvent,
    "StreamHealthEvent": StreamHealthEvent,
}


def event_to_dict(event: FeedEvent) -> dict[str, Any]:
    data = asdict(event)
    data = {k: _encode_value(v) for k, v in data.items()}
    data["event_type"] = type(event).__name__
    return data


def _decode_levels(raw: list[list[str]]) -> tuple[tuple[Decimal, Decimal], ...]:
    return tuple((to_decimal(p), to_decimal(q)) for p, q in raw)


def event_from_dict(payload: dict[str, Any]) -> FeedEvent:
    kind = payload["event_type"]
    d = {k: v for k, v in payload.items() if k != "event_type"}
    d["market"] = Market(d["market"])
    d["stream"] = Stream(d["stream"])
    if "quality_flags" in d:
        d["quality_flags"] = tuple(d["quality_flags"])

    if kind == "BookTickerEvent":
        for f in ("best_bid_price", "best_bid_quantity", "best_ask_price", "best_ask_quantity"):
            d[f] = to_decimal(d[f])
        return BookTickerEvent(**d)
    if kind == "AggTradeEvent":
        d["price"] = to_decimal(d["price"])
        d["quantity"] = to_decimal(d["quantity"])
        d["normal_quantity"] = _opt_decimal(d.get("normal_quantity"))
        return AggTradeEvent(**d)
    if kind in ("DepthUpdateEvent", "OrderBookSnapshotEvent"):
        d["bids"] = _decode_levels(d["bids"])
        d["asks"] = _decode_levels(d["asks"])
        return _TYPE_MAP[kind](**d)
    if kind == "MarkPriceEvent":
        for f in ("mark_price", "index_price", "estimated_settle_price", "funding_rate"):
            d[f] = to_decimal(d[f])
        d["mark_price_moving_average"] = _opt_decimal(d.get("mark_price_moving_average"))
        return MarkPriceEvent(**d)
    if kind == "LiquidationEvent":
        for f in (
            "original_quantity",
            "price",
            "last_filled_quantity",
            "accumulated_filled_quantity",
        ):
            d[f] = to_decimal(d[f])
        d["average_price"] = _opt_decimal(d.get("average_price"))
        return LiquidationEvent(**d)
    if kind == "ReferenceObservationEvent":
        for f in ("tick_size", "step_size", "min_notional", "funding_rate_settled"):
            d[f] = _opt_decimal(d.get(f))
        return ReferenceObservationEvent(**d)
    if kind == "StreamHealthEvent":
        return StreamHealthEvent(**d)
    raise ValueError(f"Unknown FeedEvent type: {kind}")
