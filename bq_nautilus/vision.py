"""Binance Vision historical archive decoding, unit normalization, and coverage registry."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import io
from typing import Any, Iterable

from bq_feed.events import AggTradeEvent, BookTickerEvent, Market, Stream
from bq_feed.replay import (
    normalize_vision_epoch,
    parse_strict_bool,
    parse_vision_agg_trade,
    parse_vision_book_ticker,
)

SPOT_AGG_TRADE_HEADER = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "is_best_match",
)
USDM_AGG_TRADE_HEADER = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
)
USDM_METRICS_HEADER = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)


@dataclass(frozen=True, slots=True)
class VisionCoverageEntry:
    market: Market
    stream: Stream
    available: bool
    timestamp_unit_rule: str
    has_header_since: str | None
    columns: tuple[str, ...]
    notes: str


VISION_COVERAGE_REGISTRY: dict[tuple[Market, Stream], VisionCoverageEntry] = {
    (Market.SPOT, Stream.AGG_TRADE): VisionCoverageEntry(
        market=Market.SPOT,
        stream=Stream.AGG_TRADE,
        available=True,
        timestamp_unit_rule="ms before 2025-01-01; us (16-digit) from 2025-01-01 onward",
        has_header_since=None,
        columns=SPOT_AGG_TRADE_HEADER,
        notes="Spot aggTrades ZIPs omit header row; bool strings must use strict parsing.",
    ),
    (Market.SPOT, Stream.BOOK_TICKER): VisionCoverageEntry(
        market=Market.SPOT,
        stream=Stream.BOOK_TICKER,
        available=False,
        timestamp_unit_rule="N/A (HTTP 404 on data.binance.vision for Spot bookTicker)",
        has_header_since=None,
        columns=(),
        notes="Use Recorder raw/mcp-export for Spot BBO.",
    ),
    (Market.USDM, Stream.AGG_TRADE): VisionCoverageEntry(
        market=Market.USDM,
        stream=Stream.AGG_TRADE,
        available=True,
        timestamp_unit_rule="ms (13-digit)",
        has_header_since="2020-01-01",
        columns=USDM_AGG_TRADE_HEADER,
        notes="Includes CSV header row; is_buyer_maker must use strict parsing.",
    ),
    (Market.USDM, Stream.BOOK_TICKER): VisionCoverageEntry(
        market=Market.USDM,
        stream=Stream.BOOK_TICKER,
        available=False,
        timestamp_unit_rule="N/A (HTTP 404 on data.binance.vision daily bookTicker)",
        has_header_since=None,
        columns=(),
        notes="Use Recorder raw/mcp-export for USD-M BBO.",
    ),
    (Market.USDM, Stream.REFERENCE_DATA): VisionCoverageEntry(
        market=Market.USDM,
        stream=Stream.REFERENCE_DATA,
        available=True,
        timestamp_unit_rule="ISO UTC string create_time (5m interval, 288 rows/day)",
        has_header_since="2021-12-01",
        columns=USDM_METRICS_HEADER,
        notes="Daily metrics ZIPs provide OI & long/short/taker ratios.",
    ),
}


def expected_vision_timestamp_unit(market: Market, obs_date: date) -> str:
    if market == Market.SPOT and obs_date >= date(2025, 1, 1):
        return "us"
    return "ms"


def parse_vision_agg_trade_csv(
    text: str,
    *,
    symbol: str,
    market: Market = Market.SPOT,
) -> list[AggTradeEvent]:
    """Parse Binance Vision aggTrade CSV (with or without header) safely."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    reader = csv.reader(io.StringIO("\n".join(lines)))
    rows = list(reader)
    first = [c.strip() for c in rows[0]]
    has_header = first[0] == "agg_trade_id"
    headers = first if has_header else (
        list(SPOT_AGG_TRADE_HEADER) if len(first) >= 8 else list(USDM_AGG_TRADE_HEADER)
    )
    data_rows = rows[1:] if has_header else rows
    events: list[AggTradeEvent] = []
    for raw_row in data_rows:
        row_dict = {headers[i]: raw_row[i].strip() for i in range(min(len(headers), len(raw_row)))}
        events.append(parse_vision_agg_trade(row_dict, symbol=symbol, market=market))
    return events


def parse_vision_agg_trade_dict(
    row: dict[str, Any], *, symbol: str, market: Market = Market.SPOT
) -> AggTradeEvent:
    return parse_vision_agg_trade(row, symbol=symbol, market=market)


def parse_vision_book_ticker_dict(
    row: dict[str, Any], *, symbol: str, market: Market = Market.SPOT
) -> BookTickerEvent:
    return parse_vision_book_ticker(row, symbol=symbol, market=market)


def vision_agg_trade_to_nautilus(
    ev: AggTradeEvent,
    *,
    instrument_id: Any,
    price_precision: int,
    size_precision: int,
    assumed_delivery_delay_ns: int | None = None,
) -> Any:
    """Convert Vision AggTradeEvent -> Nautilus TradeTick without fabricating capture time.

    `assumed_delivery_delay_ns` must be explicitly provided for research-scenario replay because
    Binance Vision files lack local `receive_time_utc_ns`.
    """
    from nautilus_trader.model import AggressorSide, Price, Quantity, TradeId, TradeTick

    if assumed_delivery_delay_ns is None or assumed_delivery_delay_ns < 0:
        raise ValueError(
            "Vision TradeTick has no receive_time_utc_ns; pass an explicit non-negative "
            "assumed_delivery_delay_ns research scenario rather than fabricating capture time"
        )
    ts_event = ev.trade_time_ns if ev.trade_time_ns is not None else ev.trade_time_ms * 1_000_000
    ts_init = ev.receive_time_utc_ns if ev.receive_time_utc_ns is not None else (ts_event + assumed_delivery_delay_ns)
    # buyer_is_maker=True means seller initiated (aggressor = SELL)
    aggressor = AggressorSide.SELL if ev.buyer_is_maker else AggressorSide.BUY
    return TradeTick(
        instrument_id=instrument_id,
        price=Price.from_decimal_dp(Decimal(ev.price), price_precision),
        size=Quantity.from_decimal_dp(Decimal(ev.quantity), size_precision),
        aggressor_side=aggressor,
        trade_id=TradeId(str(ev.aggregate_trade_id)),
        ts_event=ts_event,
        ts_init=ts_init,
    )


__all__ = [
    "SPOT_AGG_TRADE_HEADER",
    "USDM_AGG_TRADE_HEADER",
    "USDM_METRICS_HEADER",
    "VISION_COVERAGE_REGISTRY",
    "VisionCoverageEntry",
    "expected_vision_timestamp_unit",
    "normalize_vision_epoch",
    "parse_strict_bool",
    "parse_vision_agg_trade_csv",
    "parse_vision_agg_trade_dict",
    "parse_vision_book_ticker_dict",
    "vision_agg_trade_to_nautilus",
]
