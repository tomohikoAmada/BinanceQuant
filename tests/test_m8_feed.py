"""M8 tests: unified events, Recorder Raw replay, Gateway live Fail-Fast/STALE, parity."""

from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
from uuid import uuid4

import pytest

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import gateway_messages_pb2
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.seal import _seal_clean_writer
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

from bq_feed import (
    AggTradeEvent,
    BookTickerEvent,
    CapabilityMismatchError,
    DepthUpdateEvent,
    GatewayLiveFeed,
    Market,
    RecorderReplayFeed,
    ReferenceObservationEvent,
    Stream,
    StreamHealthEvent,
    check_strategy_capabilities,
    compare_overlapping_windows,
    event_from_dict,
    event_to_dict,
    parse_vision_agg_trade,
    parse_vision_book_ticker,
)


def _make_sealed_recorder_partition(root: Path) -> None:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            chunk_id=uuid4(),
            market="spot",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="rec-test-1",
            collector_version="1.0.0",
        )
        # Spot bookTicker omits exchange E and T!
        frames = [
            (
                1_700_000_000_010_000_000,
                json.dumps({"u": 5001, "s": "BTCUSDT", "b": "30000.10", "B": "1.500", "a": "30000.20", "A": "2.000"}).encode(),
            ),
            (
                1_700_000_000_020_000_000,
                json.dumps({"u": 5002, "s": "BTCUSDT", "b": "30000.15", "B": "0.800", "a": "30000.25", "A": "1.200"}).encode(),
            ),
        ]
        for idx, (recv_ns, payload) in enumerate(frames, start=1):
            env = EventEnvelope(
                market="spot",
                symbol="BTCUSDT",
                stream="book_ticker",
                module="book_ticker",
                connection_id="spot-ws-1",
                collector_instance_id="rec-test-1",
                collector_version="1.0.0",
                receive_time_utc_ns=recv_ns,
                receive_monotonic_ns=1_000_000 + idx * 1_000,
                source_sequence={"u": 5000 + idx},
                raw_payload=payload,
            )
            writer.append(env)
        writer.sync()
        _seal_clean_writer(writer, layout=layout, catalog=catalog)


def test_recorder_real_raw_chunk_replay_and_optional_timestamps(tmp_path: Path) -> None:
    part = tmp_path / "2026-11-23"
    _make_sealed_recorder_partition(part)

    feed = RecorderReplayFeed.from_recorder_root(part)
    events = list(feed.iter_events())
    assert len(events) == 2
    assert all(isinstance(ev, BookTickerEvent) for ev in events)
    e1, e2 = events
    assert e1.delivery_seq == 1 and e2.delivery_seq == 2
    assert e1.update_id == 5001 and e2.update_id == 5002
    assert e1.best_bid_price == Decimal("30000.10")
    assert e1.exchange_event_time_ms is None
    assert e1.exchange_transaction_time_ms is None
    assert e1.receive_time_utc_ns == 1_700_000_000_010_000_000

    # Serialization round-trip preserves exact Decimals and None timestamps
    rt = event_from_dict(event_to_dict(e1))
    assert rt == e1


def test_vision_never_fabricates_receive_time() -> None:
    bt = parse_vision_book_ticker(
        {"update_id": 10, "best_bid_price": "100.5", "best_bid_qty": "2.0", "best_ask_price": "100.6", "best_ask_qty": "3.0"},
        symbol="BTCUSDT",
    )
    assert bt.receive_time_utc_ns is None
    assert bt.exchange_event_time_ms is None
    tr = parse_vision_agg_trade(
        {"agg_trade_id": 99, "price": "100.5", "quantity": "0.1", "first_trade_id": 1, "last_trade_id": 1, "transact_time": 1700000000000, "is_buyer_maker": True},
        symbol="BTCUSDT",
    )
    assert tr.receive_time_utc_ns is None
    assert tr.trade_time_ms == 1700000000000


def test_multistream_deterministic_merge_and_gap_detection() -> None:
    records = [
        {
            "market": "um_perpetual",
            "symbol": "BTCUSDT",
            "stream": "agg_trade",
            "receive_time_utc_ns": 200,
            "raw_payload": {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": 1, "s": "BTCUSDT", "a": 10, "p": "50000", "q": "0.5", "f": 1, "l": 1, "T": 1, "m": False, "nq": "0.5"}},
        },
        {
            "market": "um_perpetual",
            "symbol": "BTCUSDT",
            "stream": "diff_depth",
            "receive_time_utc_ns": 100,
            "raw_payload": {"e": "depthUpdate", "E": 1, "T": 1, "s": "BTCUSDT", "U": 100, "u": 101, "pu": 99, "b": [["50000", "1"]], "a": []},
        },
        {
            "market": "um_perpetual",
            "symbol": "BTCUSDT",
            "stream": "diff_depth",
            "receive_time_utc_ns": 300,
            # Broken pu=105 != 101 -> must emit StreamHealthEvent(GAP) right before!
            "raw_payload": {"e": "depthUpdate", "E": 2, "T": 2, "s": "BTCUSDT", "U": 106, "u": 107, "pu": 105, "b": [["50000", "2"]], "a": []},
        },
        {
            "market": "spot",
            "symbol": "__GLOBAL__",
            "stream": "rest_observations",
            "receive_time_utc_ns": 150,
            "raw_payload": {
                "serverTime": 1700000000000,
                "symbols": [{"symbol": "BTCUSDT", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.00001"}, {"filterType": "MIN_NOTIONAL", "minNotional": "10.0"}]}],
            },
        },
    ]
    out1 = list(RecorderReplayFeed.from_raw_records(records).iter_events())
    out2 = list(RecorderReplayFeed.from_raw_records(records).iter_events())
    assert out1 == out2
    assert [ev.delivery_seq for ev in out1] == [1, 2, 3, 4, 5]
    assert isinstance(out1[0], DepthUpdateEvent)
    assert isinstance(out1[1], ReferenceObservationEvent) and out1[1].min_notional == Decimal("10.0")
    assert isinstance(out1[2], AggTradeEvent) and out1[2].normal_quantity == Decimal("0.5")
    assert isinstance(out1[3], StreamHealthEvent) and out1[3].status == "GAP"
    assert isinstance(out1[4], DepthUpdateEvent) and out1[4].final_update_id == 107


class _MockGatewayStub:
    def __init__(self, status_snap: gateway_messages_pb2.GatewayStatusSnapshot, envelopes: list[Any]) -> None:
        self._status = status_snap
        self._envelopes = envelopes

    def GetGatewayStatus(self, req: Any, timeout: float = 5.0) -> Any:
        return self._status

    def SubscribeEvents(self, req: Any) -> Any:
        return iter(self._envelopes)


def _make_status(*, book_ticker_enabled: bool, include_caps: bool = True) -> gateway_messages_pb2.GatewayStatusSnapshot:
    snap = gateway_messages_pb2.GatewayStatusSnapshot()
    row = snap.markets.add()
    row.venue = enums_pb2.VENUE_BINANCE
    row.market = enums_pb2.MARKET_SPOT
    row.symbol = "BTCUSDT"
    row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
    if include_caps:
        caps = row.enabled_streams
        caps.orderbook_rpc_enabled = False
        if book_ticker_enabled:
            caps.streams.append(enums_pb2.STREAM_BOOK_TICKER)
        caps.streams.append(enums_pb2.STREAM_AGG_TRADE)
    return snap


def test_gateway_live_fail_fast_capabilities_and_stale_watchdog() -> None:
    # Disabled stream -> Fail-Fast CapabilityMismatchError
    disabled_stub = _MockGatewayStub(_make_status(book_ticker_enabled=False), [])
    feed_bad = GatewayLiveFeed(stub=disabled_stub)
    with pytest.raises(CapabilityMismatchError):
        feed_bad.preflight_capabilities([(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER)])

    # Missing enabled_streams under strict_capabilities=True -> Fail-Fast
    old_stub = _MockGatewayStub(_make_status(book_ticker_enabled=True, include_caps=False), [])
    with pytest.raises(CapabilityMismatchError):
        GatewayLiveFeed(stub=old_stub, strict_capabilities=True).preflight_capabilities(
            [(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER)]
        )

    # D2 frozen market check
    with pytest.raises(CapabilityMismatchError):
        check_strategy_capabilities([(Market.COINM, "BTCUSD_PERP", Stream.BOOK_TICKER)], mode="paper")

    # Normal subscription + ConsumerGapNotice + STALE watchdog recovery
    now = [1_000_000_000]
    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-1"

    ev_bt = gateway_messages_pb2.GatewayEventEnvelope()
    ev_bt.book_ticker.metadata.venue = enums_pb2.VENUE_BINANCE
    ev_bt.book_ticker.metadata.market = enums_pb2.MARKET_SPOT
    ev_bt.book_ticker.metadata.symbol = "BTCUSDT"
    ev_bt.book_ticker.metadata.receive_time_utc_ns = 1_700_000_000_012_000_000
    ev_bt.book_ticker.metadata.receive_monotonic_ns = now[0]
    ev_bt.book_ticker.update_id = 5001
    ev_bt.book_ticker.best_bid_price = "30000.10"
    ev_bt.book_ticker.best_bid_quantity = "1.500"
    ev_bt.book_ticker.best_ask_price = "30000.20"
    ev_bt.book_ticker.best_ask_quantity = "2.000"

    gap = gateway_messages_pb2.GatewayEventEnvelope()
    gap.consumer_gap.market = enums_pb2.MARKET_SPOT
    gap.consumer_gap.symbol = "BTCUSDT"
    gap.consumer_gap.stream = enums_pb2.STREAM_BOOK_TICKER
    gap.consumer_gap.detected_time_utc_ns = 1_700_000_000_050_000_000
    gap.consumer_gap.last_delivered_session_sequence = 1
    gap.consumer_gap.next_available_session_sequence = 3

    ok_stub = _MockGatewayStub(_make_status(book_ticker_enabled=True), [acc, ev_bt, gap])
    live = GatewayLiveFeed(stub=ok_stub, stale_threshold_ns=2_000_000_000, clock_ns=lambda: now[0])
    out = list(live.subscribe_event_stream(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER))
    assert isinstance(out[0], BookTickerEvent)
    assert out[0].exchange_event_time_ms is None
    assert isinstance(out[1], StreamHealthEvent) and out[1].status == "GAP"

    # Advance virtual clock past stale_threshold_ns -> emits STALE
    now[0] += 3_000_000_000
    stale_events = live.poll_watchdog()
    assert len(stale_events) == 1 and stale_events[0].status == "STALE"
    # Second poll does not duplicate STALE until stream recovers
    assert live.poll_watchdog() == []


def test_recorder_vs_gateway_parity_report() -> None:
    rec = [
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("100"), Decimal("1"), Decimal("101"), Decimal("1"), update_id=1, receive_time_utc_ns=1_000_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("101"), Decimal("2"), Decimal("102"), Decimal("1"), update_id=2, receive_time_utc_ns=2_000_000_000),
        # Interior drop on Gateway (u=3 missed by Gateway)
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("102"), Decimal("3"), Decimal("103"), Decimal("1"), update_id=3, receive_time_utc_ns=3_000_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("103"), Decimal("4"), Decimal("104"), Decimal("1"), update_id=4, receive_time_utc_ns=4_000_000_000),
    ]
    gw = [
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("100"), Decimal("1"), Decimal("101"), Decimal("1"), update_id=1, receive_time_utc_ns=1_002_000_000),
        # Mismatched price on u=2, missing interior u=3
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("999"), Decimal("2"), Decimal("102"), Decimal("1"), update_id=2, receive_time_utc_ns=2_004_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("103"), Decimal("4"), Decimal("104"), Decimal("1"), update_id=4, receive_time_utc_ns=4_006_000_000),
    ]
    report = compare_overlapping_windows(rec, gw)
    assert report.matched_count == 3
    assert report.recorder_coverage == pytest.approx(3.0 / 4.0)
    assert report.gateway_coverage == pytest.approx(1.0)
    assert len(report.field_mismatches) == 1
    assert report.field_mismatches[0].field == "best_bid_price"
    assert report.latency_p50_ms == pytest.approx(4.0)
