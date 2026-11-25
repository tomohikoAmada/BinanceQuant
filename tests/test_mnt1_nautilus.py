"""M-NT1 tests: locked Nautilus 2.0.0rc9, BMD/Vision adapters, Spot F1 backtest, LiveNode Sandbox, and reconciliation."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import struct
from typing import Any
from uuid import uuid4

import cbor2
import google_crc32c
import pytest
import zstandard

nt = pytest.importorskip("nautilus_trader")

from nautilus_trader.model import BookType, Price, Quantity, QuoteTick  # noqa: E402

from binance_market_data.common.v1 import enums_pb2  # noqa: E402
from binance_market_data.gateway.v1 import gateway_messages_pb2  # noqa: E402

from bq_feed.events import BookTickerEvent, Market, ReferenceObservationEvent, Stream  # noqa: E402
from bq_feed.live import CapabilityMismatchError  # noqa: E402
from bq_nautilus import (  # noqa: E402
    SUPPORTED_BMD_SYMBOLS,
    VISION_COVERAGE_REGISTRY,
    BmdGatewayConfig,
    BmdDataClientFactory,
    BtcSpotBboStrategyConfig,
    RecorderArtifactError,
    UnknownInstrumentSpecError,
    bmd_book_ticker_to_quote_tick,
    events_to_nautilus_data,
    expected_vision_timestamp_unit,
    hand_verify_spot_ledger,
    latest_instrument_spec_as_of,
    load_partition_chunks,
    parse_vision_agg_trade_csv,
    read_recorder_events,
    reconcile_spot_window,
    run_spot_backtest,
    run_spot_sandbox_live,
    spec_from_exchange_info_entry,
    spec_from_reference_event,
    verify_spot_causality_and_delay,
    vision_agg_trade_to_nautilus,
)
from bq_nautilus.recorder import (  # noqa: E402
    BYTE_ORDER_MARKER,
    FIXED_HEADER,
    FORMAT_MAJOR,
    FORMAT_MINOR,
    FRAME_PREFIX_WITHOUT_CRC,
    RAW_MAGIC,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MATCHED_BBO_PATH = (
    REPO_ROOT.parent / "Milestone Plan" / "evidence" / "team-c-nautilus-20261125" / "matched-bbo.jsonl"
)


def _real_btcusdt_spot_spec(observe_ns: int = 1_795_420_800_000_000_000) -> Any:
    return spec_from_exchange_info_entry(
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.00", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "9000.00000", "stepSize": "0.00001"},
                {"filterType": "NOTIONAL", "minNotional": "5.00"},
            ],
        },
        market=Market.SPOT,
        observe_time_utc_ns=observe_ns,
        source_time_ms=observe_ns // 1_000_000,
        source="binance_exchange_info",
    ).to_nautilus_instrument()


def _build_verified_chunk_partition(
    root: Path,
    *,
    market: str,
    symbol: str,
    stream: str,
    records: list[tuple[int, bytes, tuple[str, ...]]],
) -> Path:
    chunk_uuid = uuid4()
    chunk_id = str(chunk_uuid)
    header_map = {
        "chunk_id": chunk_uuid.bytes,
        "chunk_schema_version": "raw-chunk.v1",
        "collector_instance_id": "rec-test-1",
        "collector_version": "1.0.0",
        "created_at_utc_ns": records[0][0],
        "envelope_schema_version": "event-envelope.v1",
        "format": "bmdr-raw-chunk",
        "market": market,
        "max_frame_bytes": 16 * 1024 * 1024,
        "stream": stream,
        "symbol": symbol,
    }
    hdr_body = cbor2.dumps(header_map, canonical=True)
    fixed_wo_crc = struct.pack(">8sBBHII", RAW_MAGIC, FORMAT_MAJOR, FORMAT_MINOR, BYTE_ORDER_MARKER, 0, len(hdr_body))
    hdr_crc = google_crc32c.value(fixed_wo_crc + hdr_body)
    raw_buf = bytearray(fixed_wo_crc + struct.pack(">I", hdr_crc) + hdr_body)
    for idx, (recv_ns, payload, flags) in enumerate(records, start=1):
        env_map = {
            "capture_flags": list(flags),
            "collector_instance_id": "rec-test-1",
            "connection_id": "ws-1",
            "exchange_event_time": None,
            "exchange_trade_time": None,
            "exchange_transaction_time": None,
            "market": market,
            "module": stream,
            "raw_payload": payload,
            "receive_monotonic_ns": 1_000_000 + idx * 1000,
            "receive_time_utc_ns": recv_ns,
            "schema_version": "event-envelope.v1",
            "source_sequence": {},
            "stream": stream,
            "symbol": symbol,
        }
        body = cbor2.dumps(env_map, canonical=True)
        pref = FRAME_PREFIX_WITHOUT_CRC.pack(len(body), 0, 0)
        crc = google_crc32c.value(pref + body)
        raw_buf.extend(pref + struct.pack(">I", crc) + body)

    raw_bytes = bytes(raw_buf)
    uncompressed_sha = hashlib.sha256(raw_bytes).hexdigest()
    compressed = zstandard.ZstdCompressor(level=3).compress(raw_bytes)
    stored_sha = hashlib.sha256(compressed).hexdigest()

    (root / "sealed").mkdir(parents=True, exist_ok=True)
    (root / "manifests").mkdir(parents=True, exist_ok=True)
    rel_path = f"sealed/{chunk_id}.bmdr.zst"
    (root / rel_path).write_bytes(compressed)
    manifest = {
        "manifest_schema_version": "raw-chunk-manifest.v1",
        "chunk_id": chunk_id,
        "market": market,
        "symbol": symbol,
        "stream": stream,
        "created_at_utc_ns": records[0][0],
        "receive_time_utc_range_ns": {
            "min": min(r[0] for r in records),
            "max": max(r[0] for r in records),
        },
        "relative_path": rel_path,
        "stored_bytes": len(compressed),
        "stored_sha256": stored_sha,
        "uncompressed_sha256": uncompressed_sha,
        "complete": True,
        "gap": False,
    }
    (root / "manifests" / f"{chunk_id}.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return root


def test_locked_nautilus_version_and_wheel_manifest() -> None:
    assert version("nautilus_trader") == "2.0.0rc9"
    wheels_json = json.loads((REPO_ROOT / "locks" / "nautilus-2.0.0rc9-wheels.json").read_text(encoding="utf-8"))
    assert wheels_json["version"] == "2.0.0rc9"
    assert wheels_json["commit"] == "da6b57aefdd62b44f675e434ffcfefca7f4025dd"
    assert "manylinux_2_34_x86_64" in wheels_json["wheels_cp313"]
    assert "macosx_11_0_arm64" in wheels_json["wheels_cp313"]
    assert (REPO_ROOT / "locks" / "nautilus-py313.lock").is_file()


def test_vision_csv_microsecond_strict_bool_and_coverage() -> None:
    csv_text = "\n".join(
        [
            "1001,95000.10,0.00200,1,1,1735689600010866,False,True",
            "1002,95000.20,0.00300,2,2,1735689600020500,True,True",
        ]
    )
    evs = parse_vision_agg_trade_csv(csv_text, symbol="BTCUSDT", market=Market.SPOT)
    assert len(evs) == 2
    assert evs[0].buyer_is_maker is False
    assert evs[1].buyer_is_maker is True
    assert evs[0].trade_time_ms == 1735689600010
    assert evs[0].trade_time_ns == 1735689600010866000
    assert evs[0].receive_time_utc_ns is None

    assert expected_vision_timestamp_unit(Market.SPOT, date(2024, 12, 31)) == "ms"
    assert expected_vision_timestamp_unit(Market.SPOT, date(2025, 1, 1)) == "us"
    assert expected_vision_timestamp_unit(Market.USDM, date(2025, 1, 1)) == "ms"
    assert VISION_COVERAGE_REGISTRY[(Market.SPOT, Stream.BOOK_TICKER)].available is False

    inst = _real_btcusdt_spot_spec()
    with pytest.raises(ValueError, match="receive_time_utc_ns"):
        vision_agg_trade_to_nautilus(
            evs[0],
            instrument_id=inst.id,
            price_precision=inst.price_precision,
            size_precision=inst.size_precision,
        )
    tick = vision_agg_trade_to_nautilus(
        evs[0],
        instrument_id=inst.id,
        price_precision=inst.price_precision,
        size_precision=inst.size_precision,
        assumed_delivery_delay_ns=5_000_000,
    )
    assert int(tick.ts_event) == 1735689600010866000
    assert int(tick.ts_init) == 1735689600015866000


def test_d4_contemporaneous_instruments_five_symbols_spot_and_usdm() -> None:
    obs_early = 1_700_000_000_000_000_000
    obs_late = 1_800_000_000_000_000_000
    for sym in SUPPORTED_BMD_SYMBOLS:
        for mkt in (Market.SPOT, Market.USDM):
            ev = ReferenceObservationEvent(
                venue="BINANCE",
                market=mkt,
                symbol=sym,
                source_kind="instrument",
                status="VALID",
                observe_time_utc_ns=obs_early,
                receive_time_utc_ns=obs_early,
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                min_notional=Decimal("5"),
                trading_status="TRADING",
                source="test_obs",
            )
            inst = spec_from_reference_event(ev).to_nautilus_instrument()
            expected_id = f"{sym}.BINANCE" if mkt == Market.SPOT else f"{sym}-PERP.BINANCE"
            assert str(inst.id) == expected_id
            assert inst.info["observe_time_utc_ns"] == obs_early

    # D4 rejects missing filters or missing timestamp
    with pytest.raises(UnknownInstrumentSpecError):
        spec_from_reference_event(
            ReferenceObservationEvent(
                venue="BINANCE",
                market=Market.SPOT,
                symbol="BTCUSDT",
                source_kind="instrument",
                status="VALID",
                observe_time_utc_ns=obs_early,
                tick_size=Decimal("0.01"),
            )
        )
    with pytest.raises(UnknownInstrumentSpecError):
        spec_from_exchange_info_entry(
            {"symbol": "BTCUSDT", "filters": []},
            market=Market.SPOT,
            observe_time_utc_ns=obs_early,
        )

    # D4 as_of timestamp prevents future spec leakage
    early_ev = ReferenceObservationEvent(
        venue="BINANCE",
        market=Market.SPOT,
        symbol="BTCUSDT",
        source_kind="instrument",
        status="VALID",
        observe_time_utc_ns=obs_early,
        receive_time_utc_ns=obs_early,
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.00001"),
        min_notional=Decimal("10"),
    )
    late_ev = ReferenceObservationEvent(
        venue="BINANCE",
        market=Market.SPOT,
        symbol="BTCUSDT",
        source_kind="instrument",
        status="VALID",
        observe_time_utc_ns=obs_late,
        receive_time_utc_ns=obs_late,
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.00001"),
        min_notional=Decimal("10"),
    )
    as_of_early = latest_instrument_spec_as_of(
        [early_ev, late_ev], market=Market.SPOT, symbol="BTCUSDT", as_of_utc_ns=obs_early + 1000
    )
    assert as_of_early.tick_size == Decimal("0.01")


def test_verified_recorder_chunk_reader_and_tamper_detection(tmp_path: Path) -> None:
    part = _build_verified_chunk_partition(
        tmp_path / "part",
        market="spot",
        symbol="BTCUSDT",
        stream="book_ticker",
        records=[
            (
                1_795_420_863_367_314_158,
                json.dumps({"u": 100, "s": "BTCUSDT", "b": "61483.23", "B": "1.00000", "a": "61483.24", "A": "0.50000"}).encode(),
                (),
            ),
            (
                1_795_420_863_417_467_811,
                json.dumps({"u": 101, "s": "BTCUSDT", "b": "61483.25", "B": "2.00000", "a": "61483.26", "A": "0.50000"}).encode(),
                ("sequence_gap",),
            ),
        ],
    )
    chunks = load_partition_chunks(
        part,
        market_filter={Market.SPOT},
        symbol_filter={"BTCUSDT"},
        stream_filter={Stream.BOOK_TICKER},
    )
    assert len(chunks) == 1
    events = read_recorder_events(chunks, market_filter={Market.SPOT}, symbol_filter={"BTCUSDT"})
    assert len(events) == 3  # BookTicker(100), StreamHealthEvent(GAP), BookTicker(101)
    assert events[1].status == "GAP"
    inst = _real_btcusdt_spot_spec()
    conv = events_to_nautilus_data(events, {(Market.SPOT, "BTCUSDT"): inst})
    assert conv.proxy_ts_event_count == 2
    assert conv.health_events_count == 1
    assert conv.quote_provenance[0]["update_id"] == 100

    # Tampering artifact fails closed
    chunks[0].artifact_path.write_bytes(b"corrupted")
    with pytest.raises(RecorderArtifactError):
        read_recorder_events(chunks)


def test_spot_f1_hand_ledger_liquidity_and_causality() -> None:
    inst = _real_btcusdt_spot_spec()
    if MATCHED_BBO_PATH.is_file():
        raw_rows = [json.loads(line) for line in MATCHED_BBO_PATH.read_text(encoding="utf-8").splitlines()]
    else:
        raw_rows = [
            {
                "venue": "BINANCE",
                "market": "SPOT",
                "symbol": "BTCUSDT",
                "stream": "book_ticker",
                "update_id": i + 1,
                "exchange_event_time_ms": None,
                "receive_time_utc_ns": 1_795_420_863_000_000_000 + i * 50_000_000,
                "delivery_seq": i + 1,
                "best_bid_price": "61483.23",
                "best_ask_price": "61483.24",
                "best_bid_quantity": "1.00000",
                "best_ask_quantity": "0.50000",
            }
            for i in range(12)
        ]

    events = [
        BookTickerEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            best_bid_price=Decimal(r["best_bid_price"]),
            best_bid_quantity=Decimal(r["best_bid_quantity"]),
            best_ask_price=Decimal(r["best_ask_price"]),
            best_ask_quantity=Decimal(r["best_ask_quantity"]),
            update_id=r["update_id"],
            exchange_event_time_ms=r["exchange_event_time_ms"],
            receive_time_utc_ns=r["receive_time_utc_ns"],
            delivery_seq=idx,
            source="recorder_raw",
        )
        for idx, r in enumerate(raw_rows, start=1)
    ]

    ticks = [bmd_book_ticker_to_quote_tick(ev, inst)[0] for ev in events]
    res = run_spot_backtest(
        instrument=inst,
        data=ticks,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="buy_then_sell",
            order_qty="0.00020",
            sell_on_callback=10,
        ),
        starting_usdt=Decimal("50"),
        fee_rate=Decimal("0.001"),
        latency_ns=2_000_000,
    )
    hand = hand_verify_spot_ledger(
        starting_usdt=Decimal("50"),
        fee_rate=Decimal("0.001"),
        fills=res["fills"],
        reported_cash_usdt=res["cash_usdt"],
        reported_base_qty=res["base_qty"],
    )
    assert hand["passed"] is True
    assert len(res["fills"]) == 2
    assert Decimal(res["base_qty"]) == Decimal(0)

    # Thin book: liquidity_consumption=True prevents filling 2x displayed ask_size
    thin_tick = QuoteTick(
        inst.id,
        Price.from_str("61483.23"),
        Price.from_str("61483.24"),
        Quantity.from_str("1.00000"),
        Quantity.from_str("0.00020"),
        events[0].receive_time_utc_ns,
        events[0].receive_time_utc_ns,
    )
    thin_ticks = [
        thin_tick,
        QuoteTick(
            inst.id,
            Price.from_str("61483.23"),
            Price.from_str("61483.24"),
            Quantity.from_str("1.00000"),
            Quantity.from_str("0.00020"),
            events[0].receive_time_utc_ns + 1_000_000,
            events[0].receive_time_utc_ns + 1_000_000,
        ),
    ]
    res_con = run_spot_backtest(
        instrument=inst,
        data=thin_ticks,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="imbalance",
            order_qty="0.00020",
            max_pos_qty="0.00040",
            buy_imbalance_threshold=0.2,
        ),
        liquidity_consumption=True,
    )
    assert Decimal(res_con["base_qty"]) == Decimal("0.00020")

    causality = verify_spot_causality_and_delay(
        instrument=inst,
        events=events,
        split_index=4,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="buy_then_sell",
            order_qty="0.00020",
            sell_on_callback=10,
        ),
    )
    assert causality["passed"] is True

    # Cash reservation guard: 15 USDT cannot afford two 0.00020 BTC buys (~12.30 USDT each)
    low_cash_ticks = [
        QuoteTick(
            inst.id,
            Price.from_str("61483.23"),
            Price.from_str("61483.24"),
            Quantity.from_str("9.00000"),
            Quantity.from_str("1.00000"),
            events[0].receive_time_utc_ns + i * 1_000_000,
            events[0].receive_time_utc_ns + i * 1_000_000,
        )
        for i in range(3)
    ]
    low_cash_res = run_spot_backtest(
        instrument=inst,
        data=low_cash_ticks,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=inst.id,
            rule_mode="imbalance",
            order_qty="0.00020",
            max_pos_qty="0.00060",
            buy_imbalance_threshold=0.2,
        ),
        starting_usdt=Decimal("15"),
        fee_rate=Decimal("0.001"),
    )
    assert len(low_cash_res["fills"]) == 1
    assert Decimal(low_cash_res["cash_usdt"]) > Decimal(0)


class _AsyncStream:
    def __init__(self, items: list[Any], *, delay_between_secs: float = 0.0) -> None:
        self._items = items
        self._delay = delay_between_secs

    def __aiter__(self) -> "_AsyncStream":
        self._idx = 0
        return self

    async def __anext__(self) -> Any:
        if self._idx >= len(self._items):
            raise StopAsyncIteration
        if self._idx > 0 and self._delay > 0:
            await asyncio.sleep(self._delay)
        val = self._items[self._idx]
        self._idx += 1
        return val

    def cancel(self) -> None:
        return None


class _FakeAsyncGatewayStub:
    def __init__(self, *, book_ticker_enabled: bool, envelopes: list[Any], delay_secs: float = 0.0) -> None:
        self._enabled = book_ticker_enabled
        self._envelopes = envelopes
        self._delay = delay_secs

    async def GetGatewayStatus(self, req: Any, timeout: float = 5.0) -> Any:
        snap = gateway_messages_pb2.GatewayStatusSnapshot()
        row = snap.markets.add()
        row.venue = enums_pb2.VENUE_BINANCE
        row.market = enums_pb2.MARKET_SPOT
        row.symbol = "BTCUSDT"
        row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
        if self._enabled:
            row.enabled_streams.streams.append(enums_pb2.STREAM_BOOK_TICKER)
        return snap

    def SubscribeEvents(self, req: Any, timeout: float | None = None) -> _AsyncStream:
        return _AsyncStream(self._envelopes, delay_between_secs=self._delay)


def _mk_bbo_env(uid: int, recv_ns: int, bid: str, bq: str, ask: str, aq: str) -> gateway_messages_pb2.GatewayEventEnvelope:
    env = gateway_messages_pb2.GatewayEventEnvelope()
    env.delivery_metadata.connection_generation = 1
    env.book_ticker.metadata.venue = enums_pb2.VENUE_BINANCE
    env.book_ticker.metadata.market = enums_pb2.MARKET_SPOT
    env.book_ticker.metadata.symbol = "BTCUSDT"
    env.book_ticker.metadata.receive_time_utc_ns = recv_ns
    env.book_ticker.metadata.receive_monotonic_ns = recv_ns
    env.book_ticker.update_id = uid
    env.book_ticker.best_bid_price = bid
    env.book_ticker.best_bid_quantity = bq
    env.book_ticker.best_ask_price = ask
    env.book_ticker.best_ask_quantity = aq
    return env


def _run_in_fresh_thread(coro_fn: Any) -> Any:
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro_fn())).result()


def test_live_node_sandbox_gap_gating_and_two_stage_reconcile() -> None:
    inst = _real_btcusdt_spot_spec()

    # 1. Fail-Fast capability preflight rejects disabled stream
    bad_stub = _FakeAsyncGatewayStub(book_ticker_enabled=False, envelopes=[])
    with pytest.raises(CapabilityMismatchError):
        _run_in_fresh_thread(
            lambda: run_spot_sandbox_live(
                target="stub:50051",
                instrument=inst,
                market=Market.SPOT,
                symbol="BTCUSDT",
                stub=bad_stub,
                max_items=1,
            )
        )

    # 2. GAP blocks opening orders until LIVE, then Stage 1 + Stage 2 pass 100%
    t0 = 1_795_420_863_000_000_000
    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-1"

    gap = gateway_messages_pb2.GatewayEventEnvelope()
    gap.consumer_gap.market = enums_pb2.MARKET_SPOT
    gap.consumer_gap.symbol = "BTCUSDT"
    gap.consumer_gap.stream = enums_pb2.STREAM_BOOK_TICKER
    gap.consumer_gap.detected_time_utc_ns = t0 + 100_000_000
    gap.consumer_gap.reason = 1

    live_ok = gateway_messages_pb2.GatewayEventEnvelope()
    live_ok.stream_status.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
    live_ok.stream_status.observed_time_utc_ns = t0 + 200_000_000

    envelopes = [
        acc,
        gap,
        # Strong buy imbalance during GAP -> MUST be blocked!
        _mk_bbo_env(201, t0 + 150_000_000, "61000.00", "9.00000", "61000.10", "1.00000"),
        live_ok,
        # Recovered LIVE -> imbalance BUY fires!
        _mk_bbo_env(202, t0 + 250_000_000, "61000.00", "9.00000", "61000.10", "1.00000"),
        # Sell imbalance -> SELL fires!
        _mk_bbo_env(203, t0 + 350_000_000, "61001.00", "1.00000", "61001.10", "9.00000"),
    ]
    stub = _FakeAsyncGatewayStub(book_ticker_enabled=True, envelopes=envelopes, delay_secs=0.02)
    live_res = _run_in_fresh_thread(
        lambda: run_spot_sandbox_live(
            target="stub:50051",
            instrument=inst,
            market=Market.SPOT,
            symbol="BTCUSDT",
            max_items=3,
            rule_mode="imbalance",
            order_qty="0.00020",
            max_pos_qty="0.00040",
            stub=stub,
        )
    )
    assert live_res["accepted"] == 1
    assert live_res["blocked_by_health"] == 1
    assert len(live_res["sandbox_fills"]) == 2

    rec_events = [
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("61000.00"), Decimal("9.00000"), Decimal("61000.10"), Decimal("1.00000"), update_id=201, receive_time_utc_ns=t0 + 149_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("61000.00"), Decimal("9.00000"), Decimal("61000.10"), Decimal("1.00000"), update_id=202, receive_time_utc_ns=t0 + 249_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("61001.00"), Decimal("1.00000"), Decimal("61001.10"), Decimal("9.00000"), update_id=203, receive_time_utc_ns=t0 + 351_000_000),
    ]
    # Window ends at t0 + 350_000_000 so uid 203 tests cross-boundary pairing!
    reconcile_ok = reconcile_spot_window(
        instrument=inst,
        gateway_rows=live_res["rows"],
        recorder_events=rec_events,
        window_start_ns=t0,
        window_end_ns=t0 + 350_000_000,
        rule_mode="imbalance",
        order_qty="0.00020",
        max_pos_qty="0.00040",
    )
    assert reconcile_ok.accepted is True
    assert reconcile_ok.cross_boundary_paired_count == 1
    assert reconcile_ok.stage2_parity.matched_count == 3

    # Fake 1% coverage (1/100) MUST fail Stage 2!
    hundred_rec = [
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("61000.00"), Decimal("9.00000"), Decimal("61000.10"), Decimal("1.00000"), update_id=1000 + i, receive_time_utc_ns=t0 + i * 1_000_000)
        for i in range(100)
    ]
    hundred_rec[0] = BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("61000.00"), Decimal("9.00000"), Decimal("61000.10"), Decimal("1.00000"), update_id=201, receive_time_utc_ns=t0 + 150_000_000)
    reconcile_1pct = reconcile_spot_window(
        instrument=inst,
        gateway_rows=live_res["rows"][:1],
        recorder_events=hundred_rec,
        window_start_ns=t0,
        window_end_ns=t0 + 200_000_000,
        rule_mode="buy_once",
        order_qty="0.00020",
    )
    assert reconcile_1pct.accepted is False
    assert reconcile_1pct.stage2_parity.recorder_coverage == pytest.approx(0.01)

    # 3. STALE watchdog fires on idle stream and blocks order on next stale-window quote
    stale_envelopes = [
        acc,
        _mk_bbo_env(301, t0 + 100_000_000, "61000.00", "9.00000", "61000.10", "1.00000"),
    ]
    stale_stub = _FakeAsyncGatewayStub(book_ticker_enabled=True, envelopes=stale_envelopes, delay_secs=0.25)
    stale_res = _run_in_fresh_thread(
        lambda: run_spot_sandbox_live(
            target="stub:50051",
            instrument=inst,
            market=Market.SPOT,
            symbol="BTCUSDT",
            max_items=1,
            rule_mode="buy_once",
            order_qty="0.00020",
            stale_threshold_ns=80_000_000,
            stub=stale_stub,
        )
    )
    assert any("STALE:" in h["reason"] for h in stale_res["health_events"])
