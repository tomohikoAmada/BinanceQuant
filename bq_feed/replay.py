"""Recorder Raw / normalized deterministic replay adapter."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

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
    _opt_int,
    to_decimal,
)

_MARKET_ALIASES = {
    "spot": Market.SPOT,
    "SPOT": Market.SPOT,
    "MARKET_SPOT": Market.SPOT,
    "usdm": Market.USDM,
    "um_perpetual": Market.USDM,
    "USD_M_PERPETUAL": Market.USDM,
    "MARKET_USD_M_PERPETUAL": Market.USDM,
    "coinm": Market.COINM,
    "cm_perpetual": Market.COINM,
    "COIN_M_PERPETUAL": Market.COINM,
    "options": Market.OPTIONS,
    "OPTIONS": Market.OPTIONS,
}


def _parse_market(raw_market: Any) -> Market:
    key = str(raw_market)
    if key in _MARKET_ALIASES:
        return _MARKET_ALIASES[key]
    return Market(key)


def _extract_ws_data(payload: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    if "stream" in payload and isinstance(payload.get("data"), dict):
        return str(payload["stream"]), payload["data"]
    return None, payload


def _parse_levels(levels: Iterable[Sequence[Any]]) -> tuple[tuple[Decimal, Decimal], ...]:
    return tuple((to_decimal(p), to_decimal(q)) for p, q in levels)


def _extract_exchange_info_filters(
    symbol_obj: dict[str, Any],
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    tick_size: Decimal | None = None
    step_size: Decimal | None = None
    min_notional: Decimal | None = None
    for flt in symbol_obj.get("filters", []):
        ftype = flt.get("filterType")
        if ftype == "PRICE_FILTER" and flt.get("tickSize") is not None:
            tick_size = to_decimal(flt["tickSize"])
        elif ftype == "LOT_SIZE" and flt.get("stepSize") is not None:
            step_size = to_decimal(flt["stepSize"])
        elif ftype in ("MIN_NOTIONAL", "NOTIONAL") and (
            flt.get("minNotional") is not None or flt.get("notional") is not None
        ):
            val = flt.get("minNotional") if flt.get("minNotional") is not None else flt.get("notional")
            min_notional = to_decimal(val)
    return tick_size, step_size, min_notional


def _classify_ws_stream(stream_hint: str, stream_name: str | None, data: dict[str, Any]) -> str:
    ev_name = str(data.get("e", ""))
    s = stream_name or stream_hint
    if ev_name == "bookTicker" or "bookTicker" in s or s == "book_ticker":
        return Stream.BOOK_TICKER.value
    if ev_name == "aggTrade" or "aggTrade" in s or s == "agg_trade":
        return Stream.AGG_TRADE.value
    if ev_name == "depthUpdate" or "@depth" in s or s == "diff_depth":
        return Stream.DIFF_DEPTH.value
    if ev_name == "markPriceUpdate" or "markPrice" in s or s == "mark_price":
        return Stream.MARK_PRICE.value
    if ev_name == "forceOrder" or "forceOrder" in s or s == "liquidation":
        return Stream.LIQUIDATION.value
    return s


def parse_raw_record(
    *,
    market: Market | str,
    symbol: str,
    stream: str,
    raw_payload: bytes | str | dict[str, Any],
    receive_time_utc_ns: int | None = None,
    receive_monotonic_ns: int | None = None,
    quality_flags: tuple[str, ...] = (),
    source: str = "recorder_raw",
    venue: str = "BINANCE",
    rest_kind: str | None = None,
) -> FeedEvent | list[FeedEvent] | None:
    """Decode one Recorder Raw envelope payload into typed MarketEvent(s)."""
    mkt = _parse_market(market) if not isinstance(market, Market) else market
    if isinstance(raw_payload, bytes):
        obj = json.loads(raw_payload.decode("utf-8"))
    elif isinstance(raw_payload, str):
        obj = json.loads(raw_payload)
    else:
        obj = raw_payload

    if stream in ("rest_observations", "reference_data") or rest_kind is not None:
        return _parse_rest_payload(
            mkt=mkt,
            symbol=symbol,
            obj=obj,
            rest_kind=rest_kind,
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            quality_flags=quality_flags,
            source=source,
            venue=venue,
        )

    stream_name, data = _extract_ws_data(obj)
    kind = _classify_ws_stream(stream, stream_name, data)
    resolved_sym = str(data.get("s") or symbol)

    if kind == Stream.BOOK_TICKER.value:
        # Spot bookTicker omits E and T; preserve None rather than fabricating.
        exch_e = _opt_int(data.get("E"))
        exch_t = _opt_int(data.get("T"))
        return BookTickerEvent(
            venue=venue,
            market=mkt,
            symbol=resolved_sym,
            update_id=_opt_int(data.get("u")),
            best_bid_price=to_decimal(data["b"]),
            best_bid_quantity=to_decimal(data["B"]),
            best_ask_price=to_decimal(data["a"]),
            best_ask_quantity=to_decimal(data["A"]),
            exchange_event_time_ms=exch_e,
            exchange_transaction_time_ms=exch_t,
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    if kind == Stream.AGG_TRADE.value:
        return AggTradeEvent(
            venue=venue,
            market=mkt,
            symbol=resolved_sym,
            aggregate_trade_id=int(data["a"]),
            price=to_decimal(data["p"]),
            quantity=to_decimal(data["q"]),
            first_trade_id=int(data["f"]),
            last_trade_id=int(data["l"]),
            trade_time_ms=int(data["T"]),
            buyer_is_maker=bool(data["m"]),
            normal_quantity=_opt_decimal(data.get("nq")),
            exchange_event_time_ms=_opt_int(data.get("E")),
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    if kind == Stream.DIFF_DEPTH.value:
        return DepthUpdateEvent(
            venue=venue,
            market=mkt,
            symbol=resolved_sym,
            first_update_id=int(data["U"]),
            final_update_id=int(data["u"]),
            previous_final_update_id=_opt_int(data.get("pu")),
            bids=_parse_levels(data.get("b", ())),
            asks=_parse_levels(data.get("a", ())),
            exchange_event_time_ms=_opt_int(data.get("E")),
            exchange_transaction_time_ms=_opt_int(data.get("T")),
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    if kind == Stream.MARK_PRICE.value:
        return MarkPriceEvent(
            venue=venue,
            market=mkt,
            symbol=resolved_sym,
            mark_price=to_decimal(data["p"]),
            index_price=to_decimal(data["i"]),
            estimated_settle_price=to_decimal(data["P"]),
            funding_rate=to_decimal(data["r"]),
            next_funding_time_ms=int(data["T"]),
            mark_price_moving_average=_opt_decimal(data.get("ap")),
            exchange_event_time_ms=_opt_int(data.get("E")),
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    if kind == Stream.LIQUIDATION.value:
        o = data.get("o", data)
        return LiquidationEvent(
            venue=venue,
            market=mkt,
            symbol=str(o.get("s") or resolved_sym),
            side=str(o["S"]),
            order_type=str(o["o"]),
            time_in_force=str(o["f"]),
            original_quantity=to_decimal(o["q"]),
            price=to_decimal(o["p"]),
            average_price=_opt_decimal(o.get("ap")),
            order_status=str(o["X"]),
            last_filled_quantity=to_decimal(o["l"]),
            accumulated_filled_quantity=to_decimal(o["z"]),
            order_trade_time_ms=int(o["T"]),
            exchange_event_time_ms=_opt_int(data.get("E")),
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    return None


def _parse_rest_payload(
    *,
    mkt: Market,
    symbol: str,
    obj: Any,
    rest_kind: str | None,
    receive_time_utc_ns: int | None,
    receive_monotonic_ns: int | None,
    quality_flags: tuple[str, ...],
    source: str,
    venue: str,
) -> FeedEvent | list[FeedEvent] | None:
    if isinstance(obj, dict) and "lastUpdateId" in obj and ("bids" in obj or "asks" in obj):
        return OrderBookSnapshotEvent(
            venue=venue,
            market=mkt,
            symbol=symbol,
            last_update_id=int(obj["lastUpdateId"]),
            bids=_parse_levels(obj.get("bids", ())),
            asks=_parse_levels(obj.get("asks", ())),
            is_baseline=True,
            exchange_event_time_ms=_opt_int(obj.get("E")),
            exchange_transaction_time_ms=_opt_int(obj.get("T")),
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source=source,
            quality_flags=quality_flags,
        )

    if isinstance(obj, dict) and "symbols" in obj:
        server_time = _opt_int(obj.get("serverTime"))
        out: list[FeedEvent] = []
        for sym_entry in obj["symbols"]:
            sym_name = str(sym_entry.get("symbol", ""))
            if symbol not in ("__GLOBAL__", "") and sym_name != symbol:
                continue
            tick, step, min_notional = _extract_exchange_info_filters(sym_entry)
            out.append(
                ReferenceObservationEvent(
                    venue=venue,
                    market=mkt,
                    symbol=sym_name,
                    source_kind="exchange_info",
                    status="VALID",
                    payload_json=json.dumps(sym_entry, separators=(",", ":"), sort_keys=True),
                    source_time_ms=server_time,
                    observe_time_utc_ns=receive_time_utc_ns,
                    receive_time_utc_ns=receive_time_utc_ns,
                    receive_monotonic_ns=receive_monotonic_ns,
                    tick_size=tick,
                    step_size=step,
                    min_notional=min_notional,
                    source=source,
                    quality_flags=quality_flags,
                )
            )
        return out

    if isinstance(obj, list) and obj and isinstance(obj[0], dict) and "fundingRate" in obj[0]:
        out = []
        for entry in obj:
            sym_name = str(entry.get("symbol") or symbol)
            if symbol not in ("__GLOBAL__", "") and sym_name != symbol:
                continue
            out.append(
                ReferenceObservationEvent(
                    venue=venue,
                    market=mkt,
                    symbol=sym_name,
                    source_kind="funding_rate",
                    status="VALID",
                    payload_json=json.dumps(entry, separators=(",", ":"), sort_keys=True),
                    source_time_ms=_opt_int(entry.get("fundingTime")),
                    observe_time_utc_ns=receive_time_utc_ns,
                    receive_time_utc_ns=receive_time_utc_ns,
                    receive_monotonic_ns=receive_monotonic_ns,
                    funding_rate_settled=_opt_decimal(entry.get("fundingRate")),
                    funding_time_ms=_opt_int(entry.get("fundingTime")),
                    source=source,
                    quality_flags=quality_flags,
                )
            )
        return out

    if isinstance(obj, dict):
        kind_name = rest_kind or str(obj.get("source_kind", "reference_data"))
        return ReferenceObservationEvent(
            venue=venue,
            market=mkt,
            symbol=symbol,
            source_kind=kind_name,
            status=str(obj.get("status", "VALID")),
            payload_json=json.dumps(obj, separators=(",", ":"), sort_keys=True),
            source_time_ms=_opt_int(obj.get("timestamp") or obj.get("fundingTime") or obj.get("serverTime")),
            observe_time_utc_ns=receive_time_utc_ns,
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            funding_rate_settled=_opt_decimal(obj.get("fundingRate")),
            funding_time_ms=_opt_int(obj.get("fundingTime")),
            source=source,
            quality_flags=quality_flags,
        )
    return None


def parse_vision_book_ticker(row: dict[str, Any], *, symbol: str, market: Market = Market.SPOT) -> BookTickerEvent:
    """Decode Binance Vision bookTicker CSV/dict row WITHOUT fabricating receive_time_utc_ns."""
    # Vision downloads only contain exchange event/transaction timestamps (for futures) or
    # update_id/best bid-ask; local receive_time_utc_ns NEVER existed and stays None.
    exch_time = _opt_int(row.get("event_time") or row.get("transaction_time") or row.get("time"))
    return BookTickerEvent(
        venue="BINANCE",
        market=market,
        symbol=symbol,
        update_id=_opt_int(row.get("update_id") or row.get("u")),
        best_bid_price=to_decimal(row.get("best_bid_price") or row["b"]),
        best_bid_quantity=to_decimal(row.get("best_bid_qty") or row["B"]),
        best_ask_price=to_decimal(row.get("best_ask_price") or row["a"]),
        best_ask_quantity=to_decimal(row.get("best_ask_qty") or row["A"]),
        exchange_event_time_ms=exch_time,
        exchange_transaction_time_ms=_opt_int(row.get("transaction_time") or row.get("T")),
        receive_time_utc_ns=None,
        receive_monotonic_ns=None,
        source="binance_vision",
    )


def parse_vision_agg_trade(row: dict[str, Any], *, symbol: str, market: Market = Market.SPOT) -> AggTradeEvent:
    """Decode Binance Vision aggTrade row WITHOUT fabricating receive_time_utc_ns."""
    return AggTradeEvent(
        venue="BINANCE",
        market=market,
        symbol=symbol,
        aggregate_trade_id=int(row.get("agg_trade_id") or row["a"]),
        price=to_decimal(row.get("price") or row["p"]),
        quantity=to_decimal(row.get("quantity") or row["q"]),
        first_trade_id=int(row.get("first_trade_id") or row["f"]),
        last_trade_id=int(row.get("last_trade_id") or row["l"]),
        trade_time_ms=int(row.get("transact_time") or row["T"]),
        buyer_is_maker=bool(row.get("is_buyer_maker") if "is_buyer_maker" in row else row["m"]),
        receive_time_utc_ns=None,
        receive_monotonic_ns=None,
        source="binance_vision",
    )


def _with_delivery_seq(event: FeedEvent, seq: int) -> FeedEvent:
    from dataclasses import replace

    return replace(event, delivery_seq=seq)


@dataclass(slots=True)
class _SortableEnvelope:
    sort_time_ns: int
    source_order: int
    event: FeedEvent


class RecorderReplayFeed:
    """Reads sealed Recorder chunks or raw records in causal receive-clock order."""

    def __init__(self, events: Iterable[FeedEvent] | None = None) -> None:
        self._raw_items: list[FeedEvent] = list(events) if events is not None else []

    @classmethod
    def from_raw_records(cls, records: Iterable[dict[str, Any]]) -> "RecorderReplayFeed":
        parsed: list[FeedEvent] = []
        for rec in records:
            res = parse_raw_record(
                market=rec["market"],
                symbol=rec["symbol"],
                stream=rec["stream"],
                raw_payload=rec["raw_payload"],
                receive_time_utc_ns=rec.get("receive_time_utc_ns"),
                receive_monotonic_ns=rec.get("receive_monotonic_ns"),
                quality_flags=tuple(rec.get("quality_flags", ())),
                source=str(rec.get("source", "recorder_raw")),
                rest_kind=rec.get("rest_kind"),
            )
            if res is None:
                continue
            if isinstance(res, list):
                parsed.extend(res)
            else:
                parsed.append(res)
        return cls(parsed)

    @classmethod
    def from_recorder_root(
        cls,
        partition_root: Path,
        *,
        market_filter: set[Market] | None = None,
        symbol_filter: set[str] | None = None,
        stream_filter: set[str] | None = None,
    ) -> "RecorderReplayFeed":
        """Read directly from a verified Recorder partition directory (catalog + sealed/Raw)."""
        from binance_market_data_recorder.normalize.raw import (
            iter_source_records,
            load_source_chunks,
        )
        from binance_market_data_recorder.storage.catalog import Catalog
        from binance_market_data_recorder.storage.layout import StorageLayout

        layout = StorageLayout.from_root(partition_root)
        parsed: list[FeedEvent] = []
        with Catalog(layout.catalog, read_only=True) as catalog:
            chunks = load_source_chunks(layout=layout, catalog=catalog)
        for chunk in sorted(chunks, key=lambda c: c.manifest["created_at_utc_ns"]):
            for rec in iter_source_records(chunk):
                env = rec.envelope
                mkt = _parse_market(env.market)
                if market_filter and mkt not in market_filter:
                    continue
                if symbol_filter and env.symbol != "__GLOBAL__" and env.symbol not in symbol_filter:
                    continue
                if stream_filter and env.stream not in stream_filter:
                    continue
                flags = tuple(getattr(env, "capture_flags", ()))
                res = parse_raw_record(
                    market=mkt,
                    symbol=env.symbol,
                    stream=env.stream,
                    raw_payload=env.raw_payload,
                    receive_time_utc_ns=env.receive_time_utc_ns,
                    receive_monotonic_ns=env.receive_monotonic_ns,
                    quality_flags=flags,
                    source="recorder_raw",
                )
                if res is None:
                    continue
                if isinstance(res, list):
                    parsed.extend(res)
                else:
                    parsed.append(res)
        return cls(parsed)

    def iter_events(self) -> Iterator[FeedEvent]:
        """Yield sorted MarketEvents and injected StreamHealthEvent gaps with monotonic delivery_seq."""
        bucket: list[_SortableEnvelope] = []
        for idx, ev in enumerate(self._raw_items):
            if isinstance(ev, StreamHealthEvent):
                t = ev.observed_time_utc_ns
            else:
                t = ev.receive_time_utc_ns
                if t is None:
                    exch_ms = getattr(ev, "exchange_event_time_ms", None) or getattr(ev, "trade_time_ms", None) or 0
                    t = exch_ms * 1_000_000
            bucket.append(_SortableEnvelope(sort_time_ns=t, source_order=idx, event=ev))

        bucket.sort(key=lambda item: (item.sort_time_ns, item.source_order))

        last_depth_uid: dict[tuple[Market, str], int] = {}
        seq = 0
        for item in bucket:
            ev = item.event
            if isinstance(ev, DepthUpdateEvent):
                key = (ev.market, ev.symbol)
                prev_u = last_depth_uid.get(key)
                has_gap = False
                if "sequence_gap" in ev.quality_flags:
                    has_gap = True
                elif prev_u is not None:
                    if ev.market == Market.USDM and ev.previous_final_update_id is not None:
                        has_gap = ev.previous_final_update_id != prev_u
                    elif ev.first_update_id > prev_u + 1:
                        has_gap = True
                if has_gap:
                    seq += 1
                    yield StreamHealthEvent(
                        venue=ev.venue,
                        market=ev.market,
                        symbol=ev.symbol,
                        stream=Stream.DIFF_DEPTH,
                        status="GAP",
                        reason="depth_sequence_gap",
                        observed_time_utc_ns=item.sort_time_ns,
                        delivery_seq=seq,
                        gap_start_seq=(prev_u + 1) if prev_u is not None else None,
                        gap_end_seq=ev.first_update_id - 1 if ev.first_update_id > 0 else None,
                        source=ev.source,
                    )
                last_depth_uid[key] = ev.final_update_id
            elif isinstance(ev, BookTickerEvent) and "sequence_gap" in ev.quality_flags:
                seq += 1
                yield StreamHealthEvent(
                    venue=ev.venue,
                    market=ev.market,
                    symbol=ev.symbol,
                    stream=Stream.BOOK_TICKER,
                    status="GAP",
                    reason="recorder_sequence_gap",
                    observed_time_utc_ns=item.sort_time_ns,
                    delivery_seq=seq,
                    source=ev.source,
                )

            seq += 1
            yield _with_delivery_seq(ev, seq)
