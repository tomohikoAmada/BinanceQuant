"""M-NT2.6 regression tests: MCP market alias, L2 bridge pu state, USD-M T clock, Funding interval, & stream quality isolation."""

from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from bq_feed.events import Market, Stream
from bq_feed.replay import parse_raw_record

nt = pytest.importorskip("nautilus_trader")

from nautilus_trader.adapters.sandbox import (  # noqa: E402
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.backtest import BacktestEngine  # noqa: E402
from nautilus_trader.common import Environment, LogLevel  # noqa: E402
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig  # noqa: E402
from nautilus_trader.execution import MakerTakerFeeModel, StaticLatencyModel  # noqa: E402
from nautilus_trader.live import LiveNode  # noqa: E402
from nautilus_trader.model import (  # noqa: E402
    AccountId,
    AccountType,
    BookAction,
    BookType,
    InstrumentStatus,
    MarketStatusAction,
    Money,
    OmsType,
    Price,
    Quantity,
    TraderId,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2  # noqa: E402
from binance_market_data.gateway.v1 import gateway_messages_pb2  # noqa: E402
from bq_nautilus import (  # noqa: E402
    BmdDataClientFactory,
    BmdGatewayConfig,
    BmdStreamQualityData,
    L2MicrostructureStrategy,
    L2MicrostructureStrategyConfig,
    L2SequenceBridge,
    MultiTimescaleHybridStrategy,
    MultiTimescaleHybridStrategyConfig,
    bmd_book_ticker_to_quote_tick,
    bmd_mark_price_to_nautilus,
    events_to_nautilus_data,
    load_mcp_export_chunks,
    read_recorder_events,
    spec_from_exchange_info_entry,
)
from tests.test_mnt2_nautilus import _MultiStreamStub, _spot_inst, _usdm_inst  # noqa: E402

try:
    import zstandard
except ImportError:  # pragma: no cover
    zstandard = None

VENUE = Venue("BINANCE")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _build_raw_v1_chunk_bytes(
    *,
    chunk_id: str,
    market: str,
    symbol: str,
    stream: str,
    records: list[tuple[int, bytes, tuple[str, ...]]],
) -> tuple[bytes, bytes]:
    import struct
    from uuid import UUID
    import cbor2
    import google_crc32c

    from bq_nautilus.recorder import (
        BYTE_ORDER_MARKER,
        FORMAT_MAJOR,
        FORMAT_MINOR,
        FRAME_PREFIX_WITHOUT_CRC,
        RAW_MAGIC,
    )

    chunk_uuid = UUID(chunk_id)
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
    fixed_wo_crc = struct.pack(
        ">8sBBHII", RAW_MAGIC, FORMAT_MAJOR, FORMAT_MINOR, BYTE_ORDER_MARKER, 0, len(hdr_body)
    )
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
    zst_bytes = zstandard.ZstdCompressor(level=3).compress(raw_bytes)
    return raw_bytes, zst_bytes


def test_mcp_export_usdm_partition_alias_selects_five_chunks_vs_spot(tmp_path: Path) -> None:
    if zstandard is None:
        pytest.skip("zstandard unavailable")

    export_root = tmp_path / "mcp-export"
    archive_root = tmp_path / "archive-usdm"
    rev_id = "20261125T072748Z-test"
    part_id = "2026-11-25T1795564800-usdm-test"
    rev_dir = export_root / "revisions" / rev_id
    part_dir = rev_dir / "partitions" / part_id
    part_dir.mkdir(parents=True)
    raw_dir = archive_root / "raw"
    raw_dir.mkdir(parents=True)

    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
    base_ns = 1_795_581_650_000_000_000
    chunk_lines = []
    for idx, sym in enumerate(symbols):
        cid = f"00000000-0000-0000-0000-{idx:012d}"
        payload = json.dumps(
            {
                "e": "aggTrade",
                "s": sym,
                "a": 1000 + idx,
                "p": "100.0",
                "q": "1.0",
                "f": 1,
                "l": 1,
                "T": 1_795_581_650_000 + idx,
                "m": False,
                "E": 1_795_581_650_001 + idx,
            }
        ).encode("utf-8")
        raw_bytes, zst_bytes = _build_raw_v1_chunk_bytes(
            chunk_id=cid,
            market="um_perpetual",
            symbol=sym,
            stream="agg_trade",
            records=[(base_ns + idx * 1_000_000, payload, ())],
        )
        rel_art = f"raw/{cid.replace('-', '')}.bmdr.zst"
        (archive_root / rel_art).write_bytes(zst_bytes)
        chunk_lines.append(
            json.dumps(
                {
                    "chunk_id": cid,
                    "ready": True,
                    "market": "um_perpetual",
                    "symbol": sym,
                    "stream": "agg_trade",
                    "storage_id": "arch-1",
                    "target_relative_path": rel_art,
                    "stored_bytes": len(zst_bytes),
                    "stored_sha256": _sha256_bytes(zst_bytes),
                    "uncompressed_sha256": _sha256_bytes(raw_bytes),
                    "created_at_utc_ns": base_ns - 1_000_000,
                    "receive_time_utc_range_ns": {"min": base_ns, "max": base_ns + 10_000_000},
                    "gap": False,
                }
            )
        )

    chunks_bytes = ("\n".join(chunk_lines) + "\n").encode("utf-8")
    (part_dir / "chunks.jsonl").write_bytes(chunks_bytes)
    manifest = {
        "revision_id": rev_id,
        "archive_targets": {"arch-1": {"local_path": str(archive_root)}},
        "markets": {"usdm": {"active_roots": []}},
        "partitions": [
            {
                "partition_id": part_id,
                "market": "usdm",
                "files": {
                    "chunks_jsonl": {
                        "url_path": f"export/revisions/{rev_id}/partitions/{part_id}/chunks.jsonl",
                        "sha256": _sha256_bytes(chunks_bytes),
                    }
                },
            }
        ],
    }
    mpath = rev_dir / "manifest.json"
    mpath.write_text(json.dumps(manifest), encoding="utf-8")
    (export_root / "latest.json").write_text(
        json.dumps({"manifest_rel_path": f"revisions/{rev_id}/manifest.json"}),
        encoding="utf-8",
    )

    filtered = load_mcp_export_chunks(
        export_root,
        market_filter={Market.USDM},
        symbol_filter=set(symbols),
        stream_filter={"agg_trade"},
        min_utc_ns=base_ns - 1,
        max_utc_ns=base_ns + 20_000_000,
    )
    unfiltered = [
        c
        for c in load_mcp_export_chunks(
            export_root,
            symbol_filter=set(symbols),
            stream_filter={"agg_trade"},
            min_utc_ns=base_ns - 1,
            max_utc_ns=base_ns + 20_000_000,
        )
        if c.market == "um_perpetual"
    ]
    assert len(filtered) == 5
    assert [c.chunk_id for c in filtered] == [c.chunk_id for c in unfiltered]
    assert len(read_recorder_events(filtered)) == 5
    # Opposite market yields 0, ensuring Spot filter excludes USD-M cleanly
    assert (
        load_mcp_export_chunks(
            export_root,
            market_filter={Market.SPOT},
            stream_filter={"agg_trade"},
        )
        == []
    )


def test_l2_sequence_bridge_awaiting_first_diff_and_bad_pu_77() -> None:
    inst = _usdm_inst("BTCUSDT")
    bridge = L2SequenceBridge()
    side = [(Decimal("61000.0"), Decimal("0.010"))]

    bridge.apply_snapshot(
        instrument=inst, bids=side, asks=[], last_update_id=100, ts_event=1, ts_init=1
    )
    assert bridge.synced is False
    assert bridge.bridged is False

    # First diff (U=99, u=101, pu=98) bridges snapshot 100 cleanly
    patch1, gap1 = bridge.apply_depth_update(
        market=Market.USDM,
        instrument=inst,
        first_update_id=99,
        final_update_id=101,
        previous_final_update_id=98,
        bids=side,
        asks=[],
        ts_event=101,
        ts_init=101,
    )
    assert gap1 is False
    assert bridge.synced is True
    assert bridge.bridged is True
    assert bridge.last_update_id == 101
    assert patch1 is not None and patch1.deltas[0].action == BookAction.UPDATE

    # Second diff (U=102, u=103, pu=77): pu != 101 MUST declare GAP & CLEAR & invalidate bridge
    patch2, gap2 = bridge.apply_depth_update(
        market=Market.USDM,
        instrument=inst,
        first_update_id=102,
        final_update_id=103,
        previous_final_update_id=77,
        bids=side,
        asks=[],
        ts_event=103,
        ts_init=103,
    )
    assert gap2 is True
    assert bridge.synced is False
    assert bridge.bridged is False
    assert bridge.last_update_id is None
    assert patch2 is not None and patch2.deltas[0].action == BookAction.CLEAR

    # Subsequent diff before new snapshot MUST remain ignored
    patch3, gap3 = bridge.apply_depth_update(
        market=Market.USDM,
        instrument=inst,
        first_update_id=104,
        final_update_id=105,
        previous_final_update_id=103,
        bids=side,
        asks=[],
        ts_event=105,
        ts_init=105,
    )
    assert patch3 is None and gap3 is False


def test_usdm_clock_prefers_T_and_unknown_funding_interval_is_none() -> None:
    usdm_btc = _usdm_inst("BTCUSDT")
    spot_btc = _spot_inst("BTCUSDT")

    bbo = parse_raw_record(
        market=Market.USDM,
        symbol="BTCUSDT",
        stream="book_ticker",
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "61000.0",
            "a": "61000.1",
            "B": "0.010",
            "A": "0.020",
            "u": 100,
            "E": 1_700_000_000_009,
            "T": 1_700_000_000_003,
        },
        receive_time_utc_ns=1_700_000_000_012_000_000,
    )
    quote, prov = bmd_book_ticker_to_quote_tick(bbo, usdm_btc)
    assert int(quote.ts_event) == 1_700_000_000_003_000_000
    assert int(quote.ts_init) == 1_700_000_000_012_000_000
    assert prov["ts_event_source"] == "exchange_transaction_time_ms"
    assert prov["exchange_event_time_ms"] == 1_700_000_000_009
    assert prov["exchange_transaction_time_ms"] == 1_700_000_000_003

    # Spot bookTicker omits both E and T -> explicit proxy
    spot_bbo = parse_raw_record(
        market=Market.SPOT,
        symbol="BTCUSDT",
        stream="book_ticker",
        raw_payload={"u": 100, "s": "BTCUSDT", "b": "61000.00", "B": "1.0", "a": "61000.10", "A": "1.0"},
        receive_time_utc_ns=1_700_000_000_012_000_000,
    )
    s_quote, s_prov = bmd_book_ticker_to_quote_tick(spot_bbo, spot_btc)
    assert int(s_quote.ts_event) == 1_700_000_000_012_000_000
    assert s_prov["ts_event_source"] == "capture_receive_proxy"

    # USD-M diff_depth also aligns to T
    summary = events_to_nautilus_data(
        [
            parse_raw_record(
                market=Market.USDM,
                symbol="BTCUSDT",
                stream="rest_observations",
                raw_payload={"lastUpdateId": 100, "bids": [["61000.0", "1.0"]], "asks": [["61000.5", "1.0"]]},
                receive_time_utc_ns=1_700_000_000_000_000_000,
            ),
            parse_raw_record(
                market=Market.USDM,
                symbol="BTCUSDT",
                stream="diff_depth",
                raw_payload={
                    "e": "depthUpdate",
                    "E": 1_700_000_000_009,
                    "T": 1_700_000_000_003,
                    "s": "BTCUSDT",
                    "U": 99,
                    "u": 101,
                    "pu": 98,
                    "b": [["61000.0", "2.0"]],
                    "a": [],
                },
                receive_time_utc_ns=1_700_000_000_012_000_000,
            ),
        ],
        {(Market.USDM, "BTCUSDT"): usdm_btc},
    )
    diff_deltas = [x for x in summary.data if hasattr(x, "deltas") and x.deltas[0].action == BookAction.UPDATE]
    assert len(diff_deltas) == 1
    assert int(diff_deltas[0].ts_event) == 1_700_000_000_003_000_000

    # Funding interval unknown in raw markPriceUpdate -> interval MUST stay None
    mark = parse_raw_record(
        market=Market.USDM,
        symbol="BTCUSDT",
        stream="mark_price",
        raw_payload={
            "e": "markPriceUpdate",
            "s": "BTCUSDT",
            "p": "61000.0",
            "i": "61000.0",
            "P": "61000.0",
            "r": "0.0001",
            "E": 1_700_000_000_009,
            "T": 1_700_000_100_000,
        },
        receive_time_utc_ns=1_700_000_000_012_000_000,
    )
    _, fund = bmd_mark_price_to_nautilus(mark, usdm_btc)
    assert fund.interval is None


def test_stream_quality_isolated_from_trading_status_and_slow_stale() -> None:
    btc = _usdm_inst("BTCUSDT")
    t0 = 1_795_421_200_000_000_000

    def _mk_depth_snap_env(uid: int, recv_ns: int) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        snap = env.reference_observation.depth_baseline
        snap.venue = enums_pb2.VENUE_BINANCE
        snap.market = enums_pb2.MARKET_USD_M_PERPETUAL
        snap.symbol = "BTCUSDT"
        snap.last_update_id = uid
        snap.receive_time_utc_ns = recv_ns
        b = snap.bids.add()
        b.price, b.quantity = "60000.0", "5.000"
        a = snap.asks.add()
        a.price, a.quantity = "60001.0", "1.000"
        env.reference_observation.venue = enums_pb2.VENUE_BINANCE
        env.reference_observation.market = enums_pb2.MARKET_USD_M_PERPETUAL
        env.reference_observation.symbol = "BTCUSDT"
        env.reference_observation.receive_time_utc_ns = recv_ns
        return env

    def _mk_depth_diff_env(u1: int, u2: int, pu: int, recv_ns: int, bid: str, bq: str) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        du = env.depth_update
        du.metadata.venue = enums_pb2.VENUE_BINANCE
        du.metadata.market = enums_pb2.MARKET_USD_M_PERPETUAL
        du.metadata.symbol = "BTCUSDT"
        du.metadata.receive_time_utc_ns = recv_ns
        du.first_update_id = u1
        du.final_update_id = u2
        du.previous_final_update_id = pu
        b = du.bids.add()
        b.price, b.quantity = bid, bq
        return env

    def _mk_trade_env(agg_id: int, recv_ns: int) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        at = env.agg_trade
        at.metadata.venue = enums_pb2.VENUE_BINANCE
        at.metadata.market = enums_pb2.MARKET_USD_M_PERPETUAL
        at.metadata.symbol = "BTCUSDT"
        at.metadata.receive_time_utc_ns = recv_ns
        at.aggregate_trade_id = agg_id
        at.price = "60000.5"
        at.quantity = "0.010"
        at.trade_time_ms = recv_ns // 1_000_000
        at.buyer_is_maker = False
        return env

    def _mk_status(state: int, msg: str, recv_ns: int) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        env.stream_status.state = state
        env.stream_status.message = msg
        env.stream_status.observed_time_utc_ns = recv_ns
        return env

    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-m26"

    # Case A: diff_depth hits GAP -> aggTrade LIVE arrives during GAP -> bad depth stays locked until snapshot resync!
    stub = _MultiStreamStub(
        market_streams={
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT"): [
                enums_pb2.STREAM_DIFF_DEPTH,
                enums_pb2.STREAM_AGG_TRADE,
            ]
        },
        envelopes_by_key={
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", enums_pb2.STREAM_DIFF_DEPTH): [
                (0.0, acc),
                (0.01, _mk_depth_snap_env(100, t0 + 10_000_000)),
                (0.01, _mk_depth_diff_env(99, 102, 98, t0 + 20_000_000, "60000.2", "4.000")),
                # Break pu=77 != 102 -> GAP!
                (0.01, _mk_depth_diff_env(103, 105, 77, t0 + 30_000_000, "60000.9", "1.000")),
                # Delay resync snapshot so aggTrade LIVE + trade arrive DURING bad-book interval
                (0.15, _mk_depth_snap_env(200, t0 + 180_000_000)),
            ],
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", enums_pb2.STREAM_AGG_TRADE): [
                (0.0, acc),
                (0.02, _mk_trade_env(1, t0 + 25_000_000)),
                # Other stream sends explicit LIVE status during diff_depth GAP -> MUST NOT clear bad depth lock!
                (0.03, _mk_status(enums_pb2.STREAM_LIFECYCLE_STATE_LIVE, "trade_still_live", t0 + 50_000_000)),
                (0.01, _mk_trade_env(2, t0 + 60_000_000)),
            ],
        },
    )

    async def _run() -> tuple[dict[str, Any], dict[str, Any]]:
        stats: dict[str, Any] = {}
        cfg = BmdGatewayConfig(
            target="stub:50051",
            instruments={btc.id: (Market.USDM, "BTCUSDT", btc)},
            required_streams=[
                (Market.USDM, "BTCUSDT", Stream.DIFF_DEPTH),
                (Market.USDM, "BTCUSDT", Stream.AGG_TRADE),
            ],
            record_rows=False,
            stats=stats,
        )
        node = (
            LiveNode.builder("MNT26-ISOLATION", TraderId("MNT26-ISO"), Environment.SANDBOX)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_timeout_connection(5)
            .with_timeout_disconnection_secs(1)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .with_reconciliation(reconciliation=False)
            .add_data_client("BMD", BmdDataClientFactory(stub=stub), cfg)
            .add_simulated_exec_client(
                "BINANCE",
                SandboxExecutionClientFactory(),
                SandboxExecutionClientConfig(
                    venue=VENUE,
                    account_id=AccountId("BINANCE-MNT26"),
                    account_type=AccountType.MARGIN,
                    oms_type=OmsType.NETTING,
                    book_type=BookType.L2_MBP,
                    starting_balances=[Money.from_str("2000.00 USDT")],
                    fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
                ),
            )
            .build()
        )
        tel: dict[str, Any] = {}
        strat = L2MicrostructureStrategy(
            L2MicrostructureStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.010",
                data_client_id="BMD",
                exec_client_id="BINANCE",
                telemetry=tel,
            )
        )
        node.add_strategy(strat)

        async def _stop() -> None:
            for _ in range(150):
                if tel.get("recovery_events", 0) >= 1 and tel.get("trade_events", 0) >= 2:
                    break
                await asyncio.sleep(0.02)
            strat.shutdown_system("mnt26 stream isolation verified")

        st = asyncio.create_task(_stop())
        try:
            await asyncio.wait_for(node.run_async(), timeout=5.0)
        finally:
            st.cancel()
            node.dispose()
        return stats, tel

    with ThreadPoolExecutor(max_workers=1) as pool:
        stats, tel = pool.submit(lambda: asyncio.run(_run())).result()

    assert tel["halt_events"] == 1
    assert tel["recovery_events"] == 1
    assert tel["trades_while_depth_bad"] >= 1
    # Verify exact sequence of health events: diff_depth GAP -> agg_trade LIVE (did NOT clear recovery) -> diff_depth resync
    streams_seen = [(h["stream"], h["status"]) for h in stats["health_events"]]
    assert ("diff_depth", "GAP") in streams_seen
    assert ("diff_depth", "LIVE") in streams_seen
    idx_gap = streams_seen.index(("diff_depth", "GAP"))
    idx_rec = streams_seen.index(("diff_depth", "LIVE"))
    assert idx_gap < idx_rec
    # Ensure agg_trade stayed healthy while diff_depth was halted
    assert ("agg_trade", "LIVE") in streams_seen[idx_gap:idx_rec]

    # Case B: slow reference stream STALE timeout must NOT halt BBO / Hybrid strategy stop-loss
    engine = BacktestEngine(
        BacktestEngineConfig(trader_id=TraderId("SLOW-STALE"), logging=LoggerConfig(stdout_level=LogLevel.ERROR))
    )
    try:
        from tests.test_mnt2_nautilus import _mk_quote
        from bq_nautilus import BMD_STREAM_QUALITY_DATATYPE, wrap_custom_data

        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money.from_str("2000.00 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(btc)
        engine.add_data(
            [
                _mk_quote(btc, t0, "100.0", "100.2"),
                wrap_custom_data(
                    BMD_STREAM_QUALITY_DATATYPE,
                    BmdStreamQualityData(
                        market="USD_M_PERPETUAL",
                        symbol="BTCUSDT",
                        stream="reference_data",
                        status="STALE",
                        reason="STALE:slow_oi_timeout",
                        observed_time_utc_ns=t0 + 500_000_000,
                        ts_event=t0 + 500_000_000,
                        ts_init=t0 + 500_000_000,
                    ),
                ),
                _mk_quote(btc, t0 + 1_250_000_000, "97.9", "98.0"),
                _mk_quote(btc, t0 + 2_200_000_000, "98.0", "98.1"),
            ]
        )
        hyb_tel: dict[str, Any] = {}
        engine.add_strategy(
            MultiTimescaleHybridStrategy(
                MultiTimescaleHybridStrategyConfig(
                    instrument_id=btc.id,
                    order_qty="0.100",
                    timer_interval_ms=1000,
                    stop_loss_bps=50.0,
                    telemetry=hyb_tel,
                )
            )
        )
        engine.run()
        assert hyb_tel["slow_stream_stale"] == 1
        assert hyb_tel["stale_halts"] == 0
        assert hyb_tel["entry_ts_ns"] == t0 + 1_000_000_000
        assert hyb_tel["stop_loss_ts_ns"] == t0 + 1_250_000_000
        assert engine.portfolio.is_net_flat(btc.id)
    finally:
        engine.dispose()


def test_bbo_t3_e9_recv12_causality_boundary() -> None:
    """Per Team C doc 11: BBO with T=3ms, E=9ms, recv=12ms MUST remain invisible at t=10ms."""
    usdm_btc = _usdm_inst("BTCUSDT")
    t_base = 1_700_000_000_000
    prior_ev = parse_raw_record(
        market=Market.USDM,
        symbol="BTCUSDT",
        stream="book_ticker",
        raw_payload={"e": "bookTicker", "s": "BTCUSDT", "b": "60000.0", "a": "60000.1", "B": "1.0", "A": "1.0", "u": 1, "E": t_base, "T": t_base},
        receive_time_utc_ns=t_base * 1_000_000,
    )
    future_ev = parse_raw_record(
        market=Market.USDM,
        symbol="BTCUSDT",
        stream="book_ticker",
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "62000.0",
            "a": "62000.1",
            "B": "5.0",
            "A": "5.0",
            "u": 2,
            "E": t_base + 9,
            "T": t_base + 3,
        },
        receive_time_utc_ns=(t_base + 12) * 1_000_000,
    )
    q0, _ = bmd_book_ticker_to_quote_tick(prior_ev, usdm_btc)
    q1, _ = bmd_book_ticker_to_quote_tick(future_ev, usdm_btc)

    assert int(q1.ts_event) == (t_base + 3) * 1_000_000
    assert int(q1.ts_init) == (t_base + 12) * 1_000_000
    # At decision horizon t = 10ms, only quotes with ts_init <= t_base + 10ms are visible
    visible_at_10ms = [q for q in (q0, q1) if int(q.ts_init) <= (t_base + 10) * 1_000_000]
    assert visible_at_10ms == [q0]


def test_bmd2_candidate_fork_config_flags_and_validation() -> None:
    """Assert 2.0.0rc9+bmd2 candidate version & fork flags by default (skip only when BQ_ALLOW_UPSTREAM_RC9=1)."""
    from importlib.metadata import version

    ver = version("nautilus_trader")
    if os.environ.get("BQ_ALLOW_UPSTREAM_RC9") == "1" and "bmd" not in ver:
        pytest.skip("BQ_ALLOW_UPSTREAM_RC9=1 set for upstream rc9 rollback compatibility job")
    assert ver == "2.0.0rc9+bmd2"

    from nautilus_trader.adapters.binance import (
        BinanceDataClientConfig,
        BinanceEnvironment,
        BinanceProductType,
        BinanceSpotMarketDataMode,
    )

    spot_cfg = BinanceDataClientConfig(
        product_type=BinanceProductType.SPOT,
        environment=BinanceEnvironment.LIVE,
        spot_market_data_mode=BinanceSpotMarketDataMode.Json,
        us=False,
        use_spot_agg_trade=True,
    )
    assert spot_cfg.us is False
    assert spot_cfg.use_spot_agg_trade is True

    usdm_cfg = BinanceDataClientConfig(
        product_type=BinanceProductType.USD_M,
        environment=BinanceEnvironment.LIVE,
        futures_diff_depth_interval_ms=100,
    )
    assert usdm_cfg.futures_diff_depth_interval_ms == 100
    with pytest.raises(ValueError, match="Unsupported Binance Futures diff depth interval"):
        BinanceDataClientConfig(
            product_type=BinanceProductType.USD_M,
            futures_diff_depth_interval_ms=333,
        )


def test_provenance_depth_snapshot_unwrapping_and_usdm_strict_bridge(tmp_path: Path) -> None:
    """Verify Recorder binance-public-rest-provenance.v1 snapshot decoding and strict USD-M L2 bridge."""
    from bq_feed.events import OrderBookSnapshotEvent, ReferenceObservationEvent

    snap_body = {"lastUpdateId": 100, "bids": [["61000.0", "2.0"]], "asks": [["61000.5", "3.0"]]}
    env_b64 = {
        "schema_version": "binance-public-rest-provenance.v1",
        "representation": "http-body-base64.v1",
        "source_stream": "depth_snapshot",
        "request": {"parameters": {"symbol": "SOLUSDT"}},
        "response": {
            "status": 200,
            "valid": True,
            "raw_body_base64": base64.b64encode(json.dumps(snap_body).encode("utf-8")).decode("ascii"),
        },
    }
    ev_b64 = parse_raw_record(
        market=Market.USDM,
        symbol="__GLOBAL__",
        stream="rest_observations",
        raw_payload=env_b64,
        receive_time_utc_ns=1_700_000_000_000_000_000,
        quality_flags=("rest_poll", "exact_rest_http_body_preserved", "depth_snapshot_pending"),
    )
    assert isinstance(ev_b64, OrderBookSnapshotEvent)
    assert ev_b64.symbol == "SOLUSDT"
    assert ev_b64.last_update_id == 100
    assert len(ev_b64.bids) == 1 and len(ev_b64.asks) == 1

    env_sdk = {
        "schema_version": "binance-public-rest-provenance.v1",
        "representation": "sdk-parsed-json.v1",
        "source_stream": "depth_snapshot",
        "request": {"parameters": {"symbol": "BTCUSDT"}},
        "response": {"status": 200, "valid": True, "model": snap_body},
    }
    ev_sdk = parse_raw_record(
        market=Market.SPOT,
        symbol="__GLOBAL__",
        stream="rest_observations",
        raw_payload=env_sdk,
        receive_time_utc_ns=1_700_000_000_000_000_000,
    )
    assert isinstance(ev_sdk, OrderBookSnapshotEvent) and ev_sdk.symbol == "BTCUSDT"

    # Superseded / malformed / HTTP error snapshots MUST NOT become OrderBookSnapshotEvent
    ev_bad = parse_raw_record(
        market=Market.USDM,
        symbol="SOLUSDT",
        stream="rest_observations",
        raw_payload=env_b64,
        receive_time_utc_ns=1_700_000_000_000_000_000,
        quality_flags=("depth_snapshot_superseded",),
    )
    assert isinstance(ev_bad, ReferenceObservationEvent) and ev_bad.status.startswith("INVALID:")

    # Strict USD-M L2SequenceBridge counterexamples
    inst = _usdm_inst("SOLUSDT")
    side = [(Decimal("100.0"), Decimal("1.0"))]

    # 1) First diff above snapshot (U=101, u=102, pu=100) for snapshot=100 MUST fail (GAP), not continue_seam!
    b1 = L2SequenceBridge()
    b1.apply_snapshot(instrument=inst, bids=side, asks=[], last_update_id=100, ts_event=1, ts_init=1)
    d1, g1 = b1.apply_depth_update(
        market=Market.USDM, instrument=inst, first_update_id=101, final_update_id=102, previous_final_update_id=100,
        bids=side, asks=[], ts_event=2, ts_init=2,
    )
    assert g1 is True and b1.synced is False and d1.deltas[0].action == BookAction.CLEAR

    # 2) Valid bridge (U=99, u=101, pu=98) followed by missing pu=None (U=102, u=103, pu=None) MUST GAP!
    b2 = L2SequenceBridge()
    b2.apply_snapshot(instrument=inst, bids=side, asks=[], last_update_id=100, ts_event=1, ts_init=1)
    _, ok_g = b2.apply_depth_update(
        market=Market.USDM, instrument=inst, first_update_id=99, final_update_id=101, previous_final_update_id=98,
        bids=side, asks=[], ts_event=2, ts_init=2,
    )
    assert ok_g is False and b2.synced is True
    d_miss_pu, g_miss_pu = b2.apply_depth_update(
        market=Market.USDM, instrument=inst, first_update_id=102, final_update_id=103, previous_final_update_id=None,
        bids=side, asks=[], ts_event=3, ts_init=3,
    )
    assert g_miss_pu is True and b2.synced is False and d_miss_pu.deltas[0].action == BookAction.CLEAR

    # 3) Verified seam diff ending right on snapshot (U=95, u=100) links cleanly without double-applying, then pu=100 continues!
    b3 = L2SequenceBridge()
    b3.apply_snapshot(instrument=inst, bids=side, asks=[], last_update_id=100, ts_event=1, ts_init=1)
    d_seam, g_seam = b3.apply_depth_update(
        market=Market.USDM, instrument=inst, first_update_id=95, final_update_id=100, previous_final_update_id=94,
        bids=side, asks=[], ts_event=2, ts_init=2,
    )
    assert g_seam is False and d_seam is None and b3.synced is True and b3.last_update_id == 100
    d_next, g_next = b3.apply_depth_update(
        market=Market.USDM, instrument=inst, first_update_id=101, final_update_id=102, previous_final_update_id=100,
        bids=side, asks=[], ts_event=3, ts_init=3,
    )
    assert g_next is False and d_next is not None and b3.last_update_id == 102

    # 4) Verify stream_filter={"orderbook"} resolves rest_observations Raw chunks containing depth_snapshot
    if zstandard is not None:
        export_root = tmp_path / "exp"
        arch_root = tmp_path / "arch"
        (arch_root / "raw").mkdir(parents=True)
        rev_id = "20261126T000000Z-t"
        part_id = "2026-11-26T1-usdm"
        pdir = export_root / "revisions" / rev_id / "partitions" / part_id
        pdir.mkdir(parents=True)
        cid = "11111111-1111-1111-1111-111111111111"
        raw_b, zst_b = _build_raw_v1_chunk_bytes(
            chunk_id=cid, market="um_perpetual", symbol="__GLOBAL__", stream="rest_observations",
            records=[(1_700_000_000_000_000_000, json.dumps(env_b64).encode("utf-8"), ())],
        )
        rel = "raw/snap.zst"
        (arch_root / rel).write_bytes(zst_b)
        row_line = json.dumps({
            "chunk_id": cid, "ready": True, "market": "um_perpetual", "symbol": "__GLOBAL__", "stream": "rest_observations",
            "storage_id": "a1", "target_relative_path": rel, "stored_bytes": len(zst_b),
            "stored_sha256": _sha256_bytes(zst_b), "uncompressed_sha256": _sha256_bytes(raw_b),
            "created_at_utc_ns": 1_700_000_000_000_000_000,
            "receive_time_utc_range_ns": {"min": 1_700_000_000_000_000_000, "max": 1_700_000_000_000_000_000}, "gap": False,
        })
        cb = (row_line + "\n").encode("utf-8")
        (pdir / "chunks.jsonl").write_bytes(cb)
        m = {
            "revision_id": rev_id,
            "archive_targets": {"a1": {"local_path": str(arch_root)}},
            "markets": {"usdm": {"active_roots": []}},
            "partitions": [{"partition_id": part_id, "market": "usdm", "files": {"chunks_jsonl": {"url_path": f"export/revisions/{rev_id}/partitions/{part_id}/chunks.jsonl", "sha256": _sha256_bytes(cb)}}}],
        }
        (export_root / "revisions" / rev_id / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
        (export_root / "latest.json").write_text(json.dumps({"manifest_rel_path": f"revisions/{rev_id}/manifest.json"}), encoding="utf-8")
        ob_chunks = load_mcp_export_chunks(export_root, market_filter={Market.USDM}, stream_filter={"orderbook"})
        assert len(ob_chunks) == 1
        ob_evs = read_recorder_events(ob_chunks, market_filter={Market.USDM}, symbol_filter={"SOLUSDT"}, stream_filter={"orderbook"})
        assert len(ob_evs) == 1 and isinstance(ob_evs[0], OrderBookSnapshotEvent) and ob_evs[0].last_update_id == 100
