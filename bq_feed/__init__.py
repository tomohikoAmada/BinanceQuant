"""Unified historical replay and live gRPC market data feed adapters."""

from .events import (
    AggTradeEvent,
    BookTickerEvent,
    DepthUpdateEvent,
    LiquidationEvent,
    Market,
    MarketEvent,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    Stream,
    StreamHealthEvent,
    event_from_dict,
    event_identity,
    event_to_dict,
)
from .live import CapabilityMismatchError, GatewayLiveFeed
from .parity import (
    LIVE_CAPABILITY_MATRIX,
    ParityReport,
    CapabilitySpec,
    check_strategy_capabilities,
    compare_overlapping_windows,
)
from .replay import RecorderReplayFeed, parse_vision_agg_trade, parse_vision_book_ticker

__all__ = [
    "AggTradeEvent",
    "BookTickerEvent",
    "CapabilityMismatchError",
    "CapabilitySpec",
    "DepthUpdateEvent",
    "GatewayLiveFeed",
    "LIVE_CAPABILITY_MATRIX",
    "LiquidationEvent",
    "Market",
    "MarketEvent",
    "MarkPriceEvent",
    "OrderBookSnapshotEvent",
    "ParityReport",
    "RecorderReplayFeed",
    "ReferenceObservationEvent",
    "Stream",
    "StreamHealthEvent",
    "check_strategy_capabilities",
    "compare_overlapping_windows",
    "event_from_dict",
    "event_identity",
    "event_to_dict",
    "parse_vision_agg_trade",
    "parse_vision_book_ticker",
]
