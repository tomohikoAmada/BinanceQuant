"""Nautilus CustomData payloads for sparse/non-book BMD channels (liquidation, reference, factor)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nautilus_trader.model import CustomData, DataType

BMD_LIQUIDATION_TYPE_NAME = "BmdLiquidation"
BMD_REFERENCE_TYPE_NAME = "BmdReference"
BMD_FACTOR_QUOTE_TYPE_NAME = "BmdFactorQuote"

BMD_LIQUIDATION_DATATYPE = DataType(BMD_LIQUIDATION_TYPE_NAME)
BMD_REFERENCE_DATATYPE = DataType(BMD_REFERENCE_TYPE_NAME)
BMD_FACTOR_QUOTE_DATATYPE = DataType(BMD_FACTOR_QUOTE_TYPE_NAME)


@dataclass(frozen=True, slots=True)
class BmdLiquidationData:
    """Throttled (<=1/s per symbol) forceOrder sample wrapped for Nautilus CustomData."""

    market: str
    symbol: str
    side: str
    order_type: str
    time_in_force: str
    original_quantity: str
    price: str
    average_price: str | None
    order_status: str
    last_filled_quantity: str
    accumulated_filled_quantity: str
    order_trade_time_ms: int
    ts_event: int
    ts_init: int
    sampling_note: str = "throttled_1000ms_forceOrder_sample"
    delivery_seq: int = 0


@dataclass(frozen=True, slots=True)
class BmdReferenceData:
    """5m periodic / REST ReferenceObservation payload wrapped for Nautilus CustomData."""

    market: str
    symbol: str
    source_kind: str
    status: str
    payload_json: str
    source_time_ms: int | None
    funding_rate_settled: str | None
    funding_time_ms: int | None
    tick_size: str | None
    step_size: str | None
    min_notional: str | None
    ts_event: int
    ts_init: int
    delivery_seq: int = 0


@dataclass(frozen=True, slots=True)
class BmdFactorQuoteData:
    """Read-only cross-asset factor quote observation (never enters trading portfolio)."""

    market: str
    symbol: str
    bid_price: str
    ask_price: str
    bid_size: str
    ask_size: str
    update_id: int | None
    ts_event: int
    ts_init: int
    delivery_seq: int = 0


def wrap_custom_data(data_type: DataType, payload: Any) -> CustomData:
    return CustomData(data_type, payload)


__all__ = [
    "BMD_FACTOR_QUOTE_DATATYPE",
    "BMD_FACTOR_QUOTE_TYPE_NAME",
    "BMD_LIQUIDATION_DATATYPE",
    "BMD_LIQUIDATION_TYPE_NAME",
    "BMD_REFERENCE_DATATYPE",
    "BMD_REFERENCE_TYPE_NAME",
    "BmdFactorQuoteData",
    "BmdLiquidationData",
    "BmdReferenceData",
    "wrap_custom_data",
]
