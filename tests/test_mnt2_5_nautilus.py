"""M-NT2.5 verification tests: Route A Rust Tonic GAP/dedup/fail-fast and Route B GAP recovery audit."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any

import grpc
import pytest

nt = pytest.importorskip("nautilus_trader")

from nautilus_trader.adapters.sandbox import (  # noqa: E402
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.common import Environment, LogLevel  # noqa: E402
from nautilus_trader.config import LoggerConfig  # noqa: E402
from nautilus_trader.execution import MakerTakerFeeModel  # noqa: E402
from nautilus_trader.live import LiveNode  # noqa: E402
from nautilus_trader.model import (  # noqa: E402
    AccountId,
    AccountType,
    BookType,
    Money,
    OmsType,
    TraderId,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2  # noqa: E402
from binance_market_data.gateway.v1 import (  # noqa: E402
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)
from bq_feed.events import Market, Stream  # noqa: E402
from bq_feed.live import CapabilityMismatchError  # noqa: E402
from bq_nautilus import L2MicrostructureStrategy, L2MicrostructureStrategyConfig  # noqa: E402
from bq_nautilus.tonic_live import BmdTonicDataClientFactory, BmdTonicGatewayConfig  # noqa: E402
from scripts.mnt2_5_route_b_bench import _run_stage as _run_route_b_stage  # noqa: E402
from tests.test_mnt2_nautilus import _spot_inst  # noqa: E402


class _RouteAGapServicer(gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceServicer):
    def __init__(self, *, enable_l2: bool = True) -> None:
        self._enable_l2 = enable_l2

    async def GetGatewayStatus(self, request: Any, context: Any) -> Any:
        snap = gateway_messages_pb2.GatewayStatusSnapshot()
        row = snap.markets.add()
        row.venue = enums_pb2.VENUE_BINANCE
        row.market = enums_pb2.MARKET_SPOT
        row.symbol = "BTCUSDT"
        row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
        if self._enable_l2:
            row.enabled_streams.streams.extend([enums_pb2.STREAM_DIFF_DEPTH, enums_pb2.STREAM_AGG_TRADE])
        else:
            row.enabled_streams.streams.append(enums_pb2.STREAM_BOOK_TICKER)
        return snap

    async def SubscribeEvents(self, request: Any, context: Any) -> Any:
        sel = request.selectors[0]
        acc = gateway_messages_pb2.GatewayEventEnvelope()
        acc.subscription_accepted.subscription_id = "route-a-test"
        yield acc

        t0 = 1_795_580_000_000_000_000
        if sel.stream == enums_pb2.STREAM_DIFF_DEPTH:
            snap1 = gateway_messages_pb2.GatewayEventEnvelope()
            s1 = snap1.reference_observation.depth_baseline
            s1.venue, s1.market, s1.symbol = enums_pb2.VENUE_BINANCE, enums_pb2.MARKET_SPOT, "BTCUSDT"
            s1.last_update_id, s1.receive_time_utc_ns = 100, t0 + 10_000_000
            b, a = s1.bids.add(), s1.asks.add()
            b.price, b.quantity, a.price, a.quantity = "60000.00", "5.00000", "60001.00", "1.00000"
            yield snap1
            await asyncio.sleep(0.02)

            du_ok = gateway_messages_pb2.GatewayEventEnvelope()
            du_ok.depth_update.metadata.receive_time_utc_ns = t0 + 30_000_000
            du_ok.depth_update.first_update_id = 101
            du_ok.depth_update.final_update_id = 102
            b2 = du_ok.depth_update.bids.add()
            b2.price, b2.quantity = "60000.50", "4.00000"
            yield du_ok
            await asyncio.sleep(0.02)

            # Inject GAP -> triggers Rust L2SeqState CLEAR + HALT
            du_gap = gateway_messages_pb2.GatewayEventEnvelope()
            du_gap.depth_update.metadata.receive_time_utc_ns = t0 + 50_000_000
            du_gap.depth_update.first_update_id = 150
            du_gap.depth_update.final_update_id = 155
            b3 = du_gap.depth_update.bids.add()
            b3.price, b3.quantity = "60002.00", "1.00000"
            yield du_gap
            await asyncio.sleep(0.02)

            snap2 = gateway_messages_pb2.GatewayEventEnvelope()
            snap2.CopyFrom(snap1)
            snap2.reference_observation.depth_baseline.last_update_id = 200
            snap2.reference_observation.depth_baseline.receive_time_utc_ns = t0 + 70_000_000
            live = gateway_messages_pb2.GatewayEventEnvelope()
            live.stream_status.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
            live.stream_status.observed_time_utc_ns = t0 + 71_000_000
            live.stream_status.message = "l2_baseline_resync"
            yield snap2
            yield live
        elif sel.stream == enums_pb2.STREAM_AGG_TRADE:
            for idx, (aid, px) in enumerate([(10, "60001.00"), (10, "60001.00"), (11, "60000.00")]):
                at = gateway_messages_pb2.GatewayEventEnvelope()
                at.agg_trade.metadata.receive_time_utc_ns = t0 + (20 + idx * 10) * 1_000_000
                at.agg_trade.aggregate_trade_id = aid
                at.agg_trade.price = px
                at.agg_trade.quantity = "0.01000"
                at.agg_trade.trade_time_ms = (t0 + idx * 10_000_000) // 1_000_000
                at.agg_trade.buyer_is_maker = bool(idx % 2)
                yield at
                await asyncio.sleep(0.01)

        await asyncio.sleep(2.0)


def _run_fresh_thread(coro_fn: Any) -> Any:
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro_fn())).result()


def test_route_a_rust_tonic_gap_clear_halt_recovery_and_fail_fast() -> None:
    try:
        from bq_nautilus import bmd_tonic_ext  # noqa: F401
    except ImportError:
        pytest.skip("bmd_tonic_ext not compiled for current host")

    btc = _spot_inst("BTCUSDT")

    async def _run_fail_fast() -> None:
        srv = grpc.aio.server()
        gateway_service_pb2_grpc.add_BinanceMarketDataGatewayServiceServicer_to_server(
            _RouteAGapServicer(enable_l2=False),
            srv,
        )
        port = srv.add_insecure_port("127.0.0.1:0")
        await srv.start()
        cfg = BmdTonicGatewayConfig(
            target=f"127.0.0.1:{port}",
            instruments={btc.id: (Market.SPOT, "BTCUSDT", btc)},
            required_streams=[(Market.SPOT, "BTCUSDT", Stream.DIFF_DEPTH)],
        )
        node = (
            LiveNode.builder("MNT25-FAILFAST", TraderId("MNT25-FF"), Environment.SANDBOX)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_timeout_connection(5)
            .with_timeout_disconnection_secs(1)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .with_reconciliation(reconciliation=False)
            .add_data_client("BMD", BmdTonicDataClientFactory(), cfg)
            .build()
        )
        try:
            with pytest.raises((CapabilityMismatchError, RuntimeError)):
                await asyncio.wait_for(node.run_async(), timeout=5.0)
        finally:
            node.dispose()
            await srv.stop(grace=0.2)

    _run_fresh_thread(_run_fail_fast)

    async def _run_gap_recovery() -> tuple[dict[str, Any], dict[str, Any]]:
        srv = grpc.aio.server()
        gateway_service_pb2_grpc.add_BinanceMarketDataGatewayServiceServicer_to_server(
            _RouteAGapServicer(enable_l2=True),
            srv,
        )
        port = srv.add_insecure_port("127.0.0.1:0")
        await srv.start()
        stats: dict[str, Any] = {}
        cfg = BmdTonicGatewayConfig(
            target=f"127.0.0.1:{port}",
            instruments={btc.id: (Market.SPOT, "BTCUSDT", btc)},
            required_streams=[
                (Market.SPOT, "BTCUSDT", Stream.DIFF_DEPTH),
                (Market.SPOT, "BTCUSDT", Stream.AGG_TRADE),
            ],
            stats=stats,
        )
        node = (
            LiveNode.builder("MNT25-TONIC-GAP", TraderId("MNT25-TG"), Environment.SANDBOX)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_timeout_connection(5)
            .with_timeout_disconnection_secs(1)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .with_reconciliation(reconciliation=False)
            .add_data_client("BMD", BmdTonicDataClientFactory(), cfg)
            .add_simulated_exec_client(
                "BINANCE",
                SandboxExecutionClientFactory(),
                SandboxExecutionClientConfig(
                    venue=Venue("BINANCE"),
                    account_id=AccountId("BINANCE-TONIC"),
                    account_type=AccountType.CASH,
                    oms_type=OmsType.NETTING,
                    book_type=BookType.L2_MBP,
                    starting_balances=[Money.from_str("2000.00 USDT")],
                    fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
                ),
            )
            .build()
        )
        tel: dict[str, Any] = {}
        strat = L2MicrostructureStrategy(
            L2MicrostructureStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.01000",
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
            strat.shutdown_system("route_a gap verified")

        st = asyncio.create_task(_stop())
        try:
            await asyncio.wait_for(node.run_async(), timeout=5.0)
        finally:
            st.cancel()
            node.dispose()
            await srv.stop(grace=0.2)
        return stats, tel

    stats, tel = _run_fresh_thread(_run_gap_recovery)
    assert tel["halt_events"] >= 1
    assert tel["recovery_events"] >= 1
    assert tel["trade_events"] == 2  # duplicate agg_trade_id=10 dropped in Rust
    assert any("GAP:l2_sequence_gap" in h["reason"] for h in stats["health_events"])


def test_route_b_native_rust_binance_gap_recovery_semantics() -> None:
    res = _run_fresh_thread(
        lambda: _run_route_b_stage(
            name="gap-ci",
            rate_per_symbol_stream=20.0,
            duration_secs=1.5,
            gap_test=True,
            snapshot_delay_ms=120,
        )
    )
    audit = res["gap_recovery_audit"]
    assert res["deltas_events"] >= 4
    # Empirically verifies rc9 BookSyncTracker does NOT emit InstrumentStatus(HALT) on GAP
    assert audit["status_halts_observed"] == 0
    assert audit["book_clears_observed"] == 2
    tl = audit["timeline"]
    idx_recover = next(i for i, row in enumerate(tl) if i > 0 and row["has_clear"])
    stale_window_ms = (tl[idx_recover]["cb_ns"] - tl[idx_recover - 1]["cb_ns"]) / 1_000_000.0
    assert stale_window_ms >= 100.0
