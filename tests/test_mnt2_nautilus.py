"""M-NT2 tests: multi-stream L2/MarkPrice/Liquidation, 5 strategy patterns, causal ML embargo/cutoff, and LiveNode Sandbox."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
import time
from typing import Any

import pytest

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
    AggressorSide,
    BookAction,
    BookType,
    Currency,
    FundingRateUpdate,
    InstrumentStatus,
    MarketStatusAction,
    MarkPriceUpdate,
    Money,
    OmsType,
    OrderBookDeltas,
    Price,
    Quantity,
    QuoteTick,
    TradeId,
    TradeTick,
    TraderId,
    Venue,
)

from binance_market_data.common.v1 import enums_pb2  # noqa: E402
from binance_market_data.gateway.v1 import gateway_messages_pb2  # noqa: E402

from bq_feed.events import (  # noqa: E402
    AggTradeEvent,
    DepthUpdateEvent,
    LiquidationEvent,
    Market,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    Stream,
)
from bq_model.split import purged_embargoed_splits  # noqa: E402
from bq_nautilus import (  # noqa: E402
    BMD_FACTOR_QUOTE_DATATYPE,
    BMD_LIQUIDATION_DATATYPE,
    MAKER_QUEUE_BOUNDARY_NOTICE,
    AllowlistViolationError,
    BmdDataClientFactory,
    BmdFactorQuoteData,
    BmdGatewayConfig,
    BmdLiquidationData,
    BmdReferenceData,
    CausalCutoffError,
    CausalMlStrategy,
    CausalMlStrategyConfig,
    CrossAssetFactorStrategy,
    CrossAssetFactorStrategyConfig,
    L2MicrostructureStrategy,
    L2MicrostructureStrategyConfig,
    MakerLimitPostOnlyStrategy,
    MakerLimitPostOnlyStrategyConfig,
    MultiTimescaleHybridStrategy,
    MultiTimescaleHybridStrategyConfig,
    TradingAllowlistGuard,
    events_to_nautilus_data,
    spec_from_exchange_info_entry,
    train_causal_nautilus_bundle,
    wrap_custom_data,
)

VENUE = Venue("BINANCE")
USDT = Currency.from_str("USDT")


def _spot_inst(symbol: str, *, tick_size: str = "0.01", step_size: str = "0.00001") -> Any:
    base = symbol[:-4]
    return spec_from_exchange_info_entry(
        {
            "symbol": symbol,
            "status": "TRADING",
            "baseAsset": base,
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": tick_size, "maxPrice": "1000000.00", "tickSize": tick_size},
                {"filterType": "LOT_SIZE", "minQty": step_size, "maxQty": "100000.00000", "stepSize": step_size},
                {"filterType": "NOTIONAL", "minNotional": "5.00"},
            ],
        },
        market=Market.SPOT,
        observe_time_utc_ns=1_795_420_800_000_000_000,
        source_time_ms=1_795_420_800_000,
        source="binance_exchange_info",
    ).to_nautilus_instrument()


def _usdm_inst(symbol: str = "BTCUSDT") -> Any:
    base = symbol[:-4]
    return spec_from_exchange_info_entry(
        {
            "symbol": symbol,
            "status": "TRADING",
            "baseAsset": base,
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.10", "maxPrice": "1000000.00", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000.000", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "5.00"},
            ],
        },
        market=Market.USDM,
        observe_time_utc_ns=1_795_420_800_000_000_000,
        source_time_ms=1_795_420_800_000,
        source="binance_exchange_info",
    ).to_nautilus_instrument()


def _mk_quote(inst: Any, ts_ns: int, bid: str, ask: str, bq: str = "1.0", aq: str = "1.0") -> QuoteTick:
    return QuoteTick(
        instrument_id=inst.id,
        bid_price=Price.from_decimal_dp(Decimal(bid), inst.price_precision),
        ask_price=Price.from_decimal_dp(Decimal(ask), inst.price_precision),
        bid_size=Quantity.from_decimal_dp(Decimal(bq), inst.size_precision),
        ask_size=Quantity.from_decimal_dp(Decimal(aq), inst.size_precision),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _run_in_fresh_thread(coro_fn: Any) -> Any:
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro_fn())).result()


class _MultiStreamAsyncIterator:
    def __init__(self, items: list[tuple[float, Any]]) -> None:
        self._items = items
        self._idx = 0
        self._cancelled = False

    def __aiter__(self) -> _MultiStreamAsyncIterator:
        return self

    async def __anext__(self) -> Any:
        if self._cancelled or self._idx >= len(self._items):
            raise StopAsyncIteration
        delay_s, val = self._items[self._idx]
        self._idx += 1
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        if self._cancelled:
            raise StopAsyncIteration
        return val

    def cancel(self) -> None:
        self._cancelled = True


class _MultiStreamStub:
    def __init__(
        self,
        *,
        market_streams: dict[tuple[int, str], list[int]],
        envelopes_by_key: dict[tuple[int, str, int], list[tuple[float, Any]]],
    ) -> None:
        self._market_streams = market_streams
        self._envelopes = envelopes_by_key

    async def GetGatewayStatus(self, req: Any, timeout: float = 5.0) -> Any:
        snap = gateway_messages_pb2.GatewayStatusSnapshot()
        for (mkt, sym), streams in self._market_streams.items():
            row = snap.markets.add()
            row.venue = enums_pb2.VENUE_BINANCE
            row.market = mkt
            row.symbol = sym
            row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
            row.enabled_streams.streams.extend(streams)
        return snap

    def SubscribeEvents(self, req: Any, timeout: float | None = None) -> _MultiStreamAsyncIterator:
        sel = req.selectors[0]
        key = (sel.market, sel.symbol, sel.stream)
        return _MultiStreamAsyncIterator(list(self._envelopes.get(key, [])))


def test_multi_stream_conversion_and_l2_sequence_gap_recovery() -> None:
    spot_btc = _spot_inst("BTCUSDT")
    usdm_btc = _usdm_inst("BTCUSDT")
    instruments = {
        (Market.SPOT, "BTCUSDT"): spot_btc,
        (Market.USDM, "BTCUSDT"): usdm_btc,
    }
    base_ns = 1_795_420_900_000_000_000
    events = [
        # AggTrade with buyer_is_maker=True (SELLER) + duplicate aggregate_trade_id=101 + buyer_is_maker=False (BUYER)
        AggTradeEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=1,
            exchange_event_time_ms=base_ns // 1_000_000,
            receive_time_utc_ns=base_ns,
            aggregate_trade_id=101,
            price=Decimal("60000.00"),
            quantity=Decimal("0.01000"),
            first_trade_id=1,
            last_trade_id=1,
            trade_time_ms=base_ns // 1_000_000,
            buyer_is_maker=True,
        ),
        AggTradeEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=2,
            exchange_event_time_ms=base_ns // 1_000_000 + 1,
            receive_time_utc_ns=base_ns + 1_000_000,
            aggregate_trade_id=101,  # duplicate -> MUST drop
            price=Decimal("60000.00"),
            quantity=Decimal("0.01000"),
            first_trade_id=1,
            last_trade_id=1,
            trade_time_ms=base_ns // 1_000_000 + 1,
            buyer_is_maker=True,
        ),
        AggTradeEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=3,
            exchange_event_time_ms=base_ns // 1_000_000 + 2,
            receive_time_utc_ns=base_ns + 2_000_000,
            aggregate_trade_id=102,
            price=Decimal("60001.00"),
            quantity=Decimal("0.02000"),
            first_trade_id=2,
            last_trade_id=2,
            trade_time_ms=base_ns // 1_000_000 + 2,
            buyer_is_maker=False,
        ),
        # USD-M L2 snapshot (u=500), continuous diff (U=501,u=505,pu=500), GAP diff (pu=499), and resync snapshot (u=600)
        OrderBookSnapshotEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=4,
            exchange_event_time_ms=base_ns // 1_000_000 + 3,
            receive_time_utc_ns=base_ns + 3_000_000,
            last_update_id=500,
            bids=[(Decimal("60000.0"), Decimal("2.000"))],
            asks=[(Decimal("60001.0"), Decimal("2.000"))],
        ),
        DepthUpdateEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=5,
            exchange_event_time_ms=base_ns // 1_000_000 + 4,
            receive_time_utc_ns=base_ns + 4_000_000,
            first_update_id=501,
            final_update_id=505,
            previous_final_update_id=500,
            bids=[(Decimal("60000.5"), Decimal("3.000"))],
            asks=[],
        ),
        DepthUpdateEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=6,
            exchange_event_time_ms=base_ns // 1_000_000 + 5,
            receive_time_utc_ns=base_ns + 5_000_000,
            first_update_id=510,
            final_update_id=515,
            previous_final_update_id=509,  # pu != 505 -> GAP!
            bids=[(Decimal("60002.0"), Decimal("1.000"))],
            asks=[],
        ),
        # Post-GAP diff without snapshot must stay ignored until baseline snapshot arrives
        DepthUpdateEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=7,
            exchange_event_time_ms=base_ns // 1_000_000 + 6,
            receive_time_utc_ns=base_ns + 6_000_000,
            first_update_id=516,
            final_update_id=520,
            previous_final_update_id=515,
            bids=[(Decimal("60002.0"), Decimal("1.000"))],
            asks=[],
        ),
        OrderBookSnapshotEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=8,
            exchange_event_time_ms=base_ns // 1_000_000 + 7,
            receive_time_utc_ns=base_ns + 7_000_000,
            last_update_id=600,
            bids=[(Decimal("60005.0"), Decimal("1.500"))],
            asks=[(Decimal("60006.0"), Decimal("1.500"))],
        ),
        # MarkPrice -> MarkPriceUpdate + FundingRateUpdate
        MarkPriceEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=9,
            exchange_event_time_ms=base_ns // 1_000_000 + 8,
            receive_time_utc_ns=base_ns + 8_000_000,
            mark_price=Decimal("60005.5"),
            index_price=Decimal("60005.0"),
            estimated_settle_price=Decimal("60005.0"),
            funding_rate=Decimal("0.000125"),
            next_funding_time_ms=(base_ns // 1_000_000) + 28_800_000,
        ),
        # Sparse liquidation + ReferenceObservation CustomData
        LiquidationEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=10,
            exchange_event_time_ms=base_ns // 1_000_000 + 9,
            receive_time_utc_ns=base_ns + 9_000_000,
            side="SELL",
            order_type="LIMIT",
            time_in_force="IOC",
            original_quantity=Decimal("0.500"),
            price=Decimal("59990.0"),
            average_price=Decimal("59992.0"),
            order_status="FILLED",
            last_filled_quantity=Decimal("0.500"),
            accumulated_filled_quantity=Decimal("0.500"),
            order_trade_time_ms=base_ns // 1_000_000 + 9,
        ),
        ReferenceObservationEvent(
            venue="BINANCE",
            market=Market.USDM,
            symbol="BTCUSDT",
            delivery_seq=11,
            observe_time_utc_ns=base_ns + 10_000_000,
            source_time_ms=base_ns // 1_000_000 + 10,
            receive_time_utc_ns=base_ns + 10_000_000,
            source_kind="funding_rate_history",
            status="OK",
            payload_json='{"fundingRate": "0.0001"}',
            funding_rate_settled=Decimal("0.0001"),
            funding_time_ms=base_ns // 1_000_000,
        ),
    ]

    summary = events_to_nautilus_data(
        events,
        instruments,
        include_funding_from_mark_price=True,
    )
    trades = [x for x in summary.data if isinstance(x, TradeTick)]
    assert len(trades) == 2
    assert trades[0].aggressor_side == AggressorSide.SELL
    assert trades[0].trade_id == TradeId("101")
    assert trades[1].aggressor_side == AggressorSide.BUY
    assert trades[1].trade_id == TradeId("102")

    deltas = [x for x in summary.data if isinstance(x, OrderBookDeltas)]
    # 1 snapshot(u=500), 1 diff(505), 1 CLEAR on GAP(515), 1 recovery snapshot(600)
    assert len(deltas) == 4
    assert deltas[2].deltas[0].action == BookAction.CLEAR

    statuses = [x for x in summary.data if isinstance(x, InstrumentStatus)]
    assert len(statuses) == 1
    assert not statuses[0].is_trading

    mks = [x for x in summary.data if isinstance(x, MarkPriceUpdate)]
    frs = [x for x in summary.data if isinstance(x, FundingRateUpdate)]
    assert len(mks) == 1 and str(mks[0].value) == "60005.5"
    assert len(frs) == 1 and frs[0].rate == Decimal("0.000125")

    liq_custom = [x for x in summary.data if getattr(x, "data", None).__class__ is BmdLiquidationData]
    ref_custom = [x for x in summary.data if getattr(x, "data", None).__class__ is BmdReferenceData]
    assert len(liq_custom) == 1 and "throttled" in liq_custom[0].data.sampling_note
    assert len(ref_custom) == 1 and ref_custom[0].data.funding_rate_settled == "0.0001"


def test_maker_limit_post_only_interface_and_boundary_disclaimer() -> None:
    assert "HftBacktest" in MAKER_QUEUE_BOUNDARY_NOTICE
    assert "queue" in MAKER_QUEUE_BOUNDARY_NOTICE.lower()

    btc = _spot_inst("BTCUSDT")
    t0 = 1_795_421_000_000_000_000
    quotes = [
        _mk_quote(btc, t0, "60000.00", "60010.00", "1.0", "1.0"),
        _mk_quote(btc, t0 + 100_000_000, "59990.00", "59995.00", "1.0", "1.0"),
    ]
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("MAKER-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("2000.00 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.001")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(btc)
        engine.add_data(quotes)
        telemetry: dict[str, Any] = {}
        strat = MakerLimitPostOnlyStrategy(
            MakerLimitPostOnlyStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.01000",
                bid_offset_ticks=0,
                telemetry=telemetry,
            ),
        )
        engine.add_strategy(strat)
        engine.run()
        assert telemetry["orders_submitted"] == 1
        assert telemetry["orders_rejected"] == 0
        assert telemetry["orders_filled"] == 1
        assert telemetry["queue_boundary_notice"] == MAKER_QUEUE_BOUNDARY_NOTICE
    finally:
        engine.dispose()


def test_cross_asset_factor_isolation_and_allowlist_guard() -> None:
    btc = _spot_inst("BTCUSDT", tick_size="0.01", step_size="0.00001")
    sol = _spot_inst("SOLUSDT", tick_size="0.01", step_size="0.001")
    t0 = 1_795_421_100_000_000_000

    # Guard hard unit check
    guard = TradingAllowlistGuard(frozenset({sol.id}))
    assert guard.assert_allowed(sol.id) is True
    with pytest.raises(AllowlistViolationError):
        guard.assert_allowed(btc.id)

    # Extreme 10x BTC price swings must NEVER affect SOL equity when SOL stays flat!
    data_a = [
        _mk_quote(sol, t0, "150.00", "150.10", "5.000", "5.000"),
        _mk_quote(btc, t0 + 1_000_000, "60000.00", "60005.00", "1.0", "1.0"),
        _mk_quote(btc, t0 + 2_000_000, "600000.00", "600010.00", "1.0", "1.0"),
        _mk_quote(btc, t0 + 3_000_000, "6000.00", "6005.00", "1.0", "1.0"),
        wrap_custom_data(
            BMD_FACTOR_QUOTE_DATATYPE,
            BmdFactorQuoteData("SPOT", "BTCUSDT", "900000.00", "900010.00", "1.0", "1.0", 9, t0 + 4_000_000, t0 + 4_000_000),
        ),
        _mk_quote(sol, t0 + 5_000_000, "151.00", "151.10", "5.000", "5.000"),
    ]
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("FACTOR-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("1000.00 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(btc)
        engine.add_instrument(sol)
        engine.add_data(data_a)
        tel: dict[str, Any] = {}
        strat = CrossAssetFactorStrategy(
            CrossAssetFactorStrategyConfig(
                factor_instrument_id=btc.id,
                target_instrument_id=sol.id,
                trade_target_on_signal=False,
                probe_factor_order_blocked=True,
                telemetry=tel,
            ),
        )
        engine.add_strategy(strat)
        engine.run()

        acc = engine.portfolio.account(VENUE)
        assert acc.balance_total(USDT).as_decimal() == Decimal("1000.00000000")
        assert engine.portfolio.is_net_flat(btc.id)
        assert engine.portfolio.is_net_flat(sol.id)
        assert tel["factor_updates"] == 3
        assert tel["target_updates"] == 2
        assert tel["blocked_factor_orders"] == 1
        assert tel["target_orders"] == 0
    finally:
        engine.dispose()

    # Now enable target execution driven by BTC factor jump (via CustomData) -> SOL trades, BTC stays completely zero!
    data_b = [
        _mk_quote(sol, t0, "150.00", "150.10", "5.000", "5.000"),
        wrap_custom_data(
            BMD_FACTOR_QUOTE_DATATYPE,
            BmdFactorQuoteData("SPOT", "BTCUSDT", "60000.00", "60005.00", "1.0", "1.0", 1, t0 + 1_000_000, t0 + 1_000_000),
        ),
        wrap_custom_data(
            BMD_FACTOR_QUOTE_DATATYPE,
            BmdFactorQuoteData("SPOT", "BTCUSDT", "63000.00", "63005.00", "1.0", "1.0", 2, t0 + 2_000_000, t0 + 2_000_000),
        ),
    ]
    engine2 = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("FACTOR-002"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine2.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("1000.00 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine2.add_instrument(btc)
        engine2.add_instrument(sol)
        engine2.add_data(data_b)
        tel2: dict[str, Any] = {}
        strat2 = CrossAssetFactorStrategy(
            CrossAssetFactorStrategyConfig(
                factor_instrument_id=btc.id,
                target_instrument_id=sol.id,
                order_qty="1.000",
                factor_jump_bps=250.0,
                trade_target_on_signal=True,
                use_custom_factor_stream=True,
                telemetry=tel2,
            ),
        )
        engine2.add_strategy(strat2)
        engine2.run()
        assert tel2["target_orders"] == 1
        assert engine2.portfolio.is_net_long(sol.id)
        assert engine2.portfolio.is_net_flat(btc.id)
        assert engine2.portfolio.account(VENUE).balance_total(btc.base_currency) is None
    finally:
        engine2.dispose()


def test_l2_microstructure_backtest_and_live_node_gap_resync() -> None:
    btc = _spot_inst("BTCUSDT", tick_size="0.01", step_size="0.00001")
    t0 = 1_795_421_200_000_000_000

    # 1. BacktestEngine L2_MBP multi-level + TradeTick + GAP halt + recovery
    bt_events = [
        OrderBookSnapshotEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=1,
            exchange_event_time_ms=t0 // 1_000_000,
            receive_time_utc_ns=t0,
            last_update_id=100,
            bids=[(Decimal("60000.00"), Decimal("4.00000")), (Decimal("59999.00"), Decimal("2.00000"))],
            asks=[(Decimal("60001.00"), Decimal("1.00000")), (Decimal("60002.00"), Decimal("3.00000"))],
        ),
        AggTradeEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=2,
            exchange_event_time_ms=t0 // 1_000_000 + 10,
            receive_time_utc_ns=t0 + 10_000_000,
            aggregate_trade_id=501,
            price=Decimal("60001.00"),
            quantity=Decimal("0.05000"),
            first_trade_id=1,
            last_trade_id=1,
            trade_time_ms=t0 // 1_000_000 + 10,
            buyer_is_maker=False,
        ),
        DepthUpdateEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=3,
            exchange_event_time_ms=t0 // 1_000_000 + 20,
            receive_time_utc_ns=t0 + 20_000_000,
            first_update_id=101,
            final_update_id=102,
            previous_final_update_id=100,
            bids=[(Decimal("60000.50"), Decimal("5.00000"))],
            asks=[],
        ),
        # GAP jump: first_update_id=150 > 102 + 1 -> CLEAR + HALT
        DepthUpdateEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=4,
            exchange_event_time_ms=t0 // 1_000_000 + 30,
            receive_time_utc_ns=t0 + 30_000_000,
            first_update_id=150,
            final_update_id=155,
            previous_final_update_id=149,
            bids=[(Decimal("60005.00"), Decimal("1.00000"))],
            asks=[],
        ),
        OrderBookSnapshotEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            delivery_seq=5,
            exchange_event_time_ms=t0 // 1_000_000 + 40,
            receive_time_utc_ns=t0 + 40_000_000,
            last_update_id=200,
            bids=[(Decimal("60002.00"), Decimal("3.00000"))],
            asks=[(Decimal("60003.00"), Decimal("3.00000"))],
        ),
    ]
    summary = events_to_nautilus_data(bt_events, {(Market.SPOT, "BTCUSDT"): btc})
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("L2-BT-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("2000.00 USDT")],
            book_type=BookType.L2_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(btc)
        engine.add_data(summary.data)
        tel: dict[str, Any] = {}
        strat = L2MicrostructureStrategy(
            L2MicrostructureStrategyConfig(instrument_id=btc.id, order_qty="0.01000", telemetry=tel),
        )
        engine.add_strategy(strat)
        engine.run()
        assert tel["deltas_events"] >= 3
        assert tel["trade_events"] == 1
        assert tel["halt_events"] == 1
        assert tel["orders_submitted"] == 1
        assert any(lvl[2] >= 2 and lvl[3] >= 2 for lvl in tel["top_levels_seen"])
    finally:
        engine.dispose()

    # 2. LiveNode (Sandbox) L2 + AggTrade + GAP halt + baseline resync over gRPC stub
    def _mk_depth_snap_env(uid: int, recv_ns: int, bid: str, ask: str) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        snap = env.reference_observation.depth_baseline
        snap.venue = enums_pb2.VENUE_BINANCE
        snap.market = enums_pb2.MARKET_SPOT
        snap.symbol = "BTCUSDT"
        snap.last_update_id = uid
        snap.receive_time_utc_ns = recv_ns
        b = snap.bids.add()
        b.price = bid
        b.quantity = "5.00000"
        a = snap.asks.add()
        a.price = ask
        a.quantity = "1.00000"
        env.reference_observation.venue = enums_pb2.VENUE_BINANCE
        env.reference_observation.market = enums_pb2.MARKET_SPOT
        env.reference_observation.symbol = "BTCUSDT"
        env.reference_observation.receive_time_utc_ns = recv_ns
        return env

    def _mk_depth_diff_env(u1: int, u2: int, recv_ns: int, bid: str, bq: str) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        du = env.depth_update
        du.metadata.venue = enums_pb2.VENUE_BINANCE
        du.metadata.market = enums_pb2.MARKET_SPOT
        du.metadata.symbol = "BTCUSDT"
        du.metadata.receive_time_utc_ns = recv_ns
        du.first_update_id = u1
        du.final_update_id = u2
        b = du.bids.add()
        b.price = bid
        b.quantity = bq
        return env

    def _mk_trade_env(agg_id: int, recv_ns: int, px: str, qty: str, buyer_maker: bool) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        at = env.agg_trade
        at.metadata.venue = enums_pb2.VENUE_BINANCE
        at.metadata.market = enums_pb2.MARKET_SPOT
        at.metadata.symbol = "BTCUSDT"
        at.metadata.receive_time_utc_ns = recv_ns
        at.aggregate_trade_id = agg_id
        at.price = px
        at.quantity = qty
        at.trade_time_ms = recv_ns // 1_000_000
        at.buyer_is_maker = buyer_maker
        return env

    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-l2"
    stub = _MultiStreamStub(
        market_streams={
            (enums_pb2.MARKET_SPOT, "BTCUSDT"): [
                enums_pb2.STREAM_DIFF_DEPTH,
                enums_pb2.STREAM_AGG_TRADE,
            ]
        },
        envelopes_by_key={
            (enums_pb2.MARKET_SPOT, "BTCUSDT", enums_pb2.STREAM_DIFF_DEPTH): [
                (0.0, acc),
                (0.01, _mk_depth_snap_env(100, t0 + 10_000_000, "60000.00", "60001.00")),
                (0.02, _mk_depth_diff_env(101, 102, t0 + 30_000_000, "60000.50", "4.00000")),
                # GAP: first_update_id=150 > 103
                (0.02, _mk_depth_diff_env(150, 155, t0 + 50_000_000, "60002.00", "1.00000")),
                # Recovery baseline snapshot u=200
                (0.02, _mk_depth_snap_env(200, t0 + 70_000_000, "60003.00", "60004.00")),
            ],
            (enums_pb2.MARKET_SPOT, "BTCUSDT", enums_pb2.STREAM_AGG_TRADE): [
                (0.0, acc),
                (0.01, _mk_trade_env(10, t0 + 20_000_000, "60001.00", "0.02000", False)),
                (0.01, _mk_trade_env(10, t0 + 25_000_000, "60001.00", "0.02000", False)),  # duplicate!
                (0.01, _mk_trade_env(11, t0 + 40_000_000, "60000.00", "0.01000", True)),
            ],
        },
    )

    async def _run_live() -> tuple[dict[str, Any], dict[str, Any]]:
        stats: dict[str, Any] = {}
        cfg = BmdGatewayConfig(
            target="stub:50051",
            instruments={btc.id: (Market.SPOT, "BTCUSDT", btc)},
            required_streams=[
                (Market.SPOT, "BTCUSDT", Stream.DIFF_DEPTH),
                (Market.SPOT, "BTCUSDT", Stream.AGG_TRADE),
            ],
            max_items=6,
            record_rows=False,
            stats=stats,
        )
        node = (
            LiveNode.builder("MNT2-L2-SANDBOX", TraderId("MNT2-001"), Environment.SANDBOX)
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
                    account_id=AccountId("BINANCE-MNT2-L2"),
                    account_type=AccountType.CASH,
                    oms_type=OmsType.NETTING,
                    book_type=BookType.L2_MBP,
                    starting_balances=[Money.from_str("2000.00 USDT")],
                    fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
                    liquidity_consumption=True,
                    bar_execution=False,
                ),
            )
            .build()
        )
        live_tel: dict[str, Any] = {}
        strat = L2MicrostructureStrategy(
            L2MicrostructureStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.01000",
                data_client_id="BMD",
                exec_client_id="BINANCE",
                telemetry=live_tel,
            ),
        )
        node.add_strategy(strat)

        async def _stop_when_ready() -> None:
            for _ in range(150):
                if live_tel.get("recovery_events", 0) >= 1 and live_tel.get("trade_events", 0) >= 2:
                    break
                await asyncio.sleep(0.02)
            strat.shutdown_system("test_l2_microstructure done")

        stop_task = asyncio.create_task(_stop_when_ready())
        try:
            await asyncio.wait_for(node.run_async(), timeout=5.0)
        finally:
            stop_task.cancel()
            node.dispose()
        return stats, live_tel

    live_stats, live_tel = _run_in_fresh_thread(_run_live)
    assert live_tel["deltas_events"] >= 3
    assert live_tel["trade_events"] == 2  # dedup dropped duplicate aggTrade id=10!
    assert live_tel["halt_events"] >= 1
    assert live_tel["recovery_events"] >= 1
    assert any(h["reason"] == "GAP:l2_sequence_gap" for h in live_stats["health_events"])
    assert any(h["reason"] == "LIVE:l2_baseline_resync" for h in live_stats["health_events"])


def test_multi_timescale_hybrid_and_sparse_liquidation_no_stale() -> None:
    btc = _usdm_inst("BTCUSDT")
    t0 = 1_795_421_300_000_000_000
    liq_ev = LiquidationEvent(
        venue="BINANCE",
        market=Market.USDM,
        symbol="BTCUSDT",
        delivery_seq=1,
        exchange_event_time_ms=(t0 + 500_000_000) // 1_000_000,
        receive_time_utc_ns=t0 + 500_000_000,
        side="SELL",
        order_type="LIMIT",
        time_in_force="IOC",
        original_quantity=Decimal("1.000"),
        price=Decimal("60000.0"),
        average_price=Decimal("60000.0"),
        order_status="FILLED",
        last_filled_quantity=Decimal("1.000"),
        accumulated_filled_quantity=Decimal("1.000"),
        order_trade_time_ms=(t0 + 500_000_000) // 1_000_000,
    )
    summary = events_to_nautilus_data([liq_ev], {(Market.USDM, "BTCUSDT"): btc})
    data = [
        _mk_quote(btc, t0, "100.0", "100.2"),
        summary.data[0],
        # Intra-second crash at t0 + 1.25s (-200 bps >= 50 bps) BEFORE 2.0s timer!
        _mk_quote(btc, t0 + 1_250_000_000, "97.9", "98.0"),
        _mk_quote(btc, t0 + 2_200_000_000, "98.0", "98.1"),
    ]
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("HYBRID-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
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
        engine.add_data(data)
        tel: dict[str, Any] = {}
        strat = MultiTimescaleHybridStrategy(
            MultiTimescaleHybridStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.100",
                timer_interval_ms=1000,
                stop_loss_bps=50.0,
                telemetry=tel,
            ),
        )
        engine.add_strategy(strat)
        engine.run()
        assert len(tel["timer_ticks"]) >= 2
        assert tel["entry_ts_ns"] == t0 + 1_000_000_000
        assert tel["stop_loss_ts_ns"] == t0 + 1_250_000_000
        assert tel["stop_loss_ts_ns"] < tel["timer_ticks"][1]
        assert engine.portfolio.is_net_flat(btc.id)
        assert len(tel["liquidations"]) == 1
    finally:
        engine.dispose()

    # LiveNode sparse liquidation silence check:
    # stale_threshold_ns=120ms, sparse_stale_threshold_ns=None -> 450ms liquidation silence emits ZERO STALE!
    def _mk_bbo_env(recv_ns: int) -> Any:
        env = gateway_messages_pb2.GatewayEventEnvelope()
        bt = env.book_ticker
        bt.metadata.venue = enums_pb2.VENUE_BINANCE
        bt.metadata.market = enums_pb2.MARKET_USD_M_PERPETUAL
        bt.metadata.symbol = "BTCUSDT"
        bt.metadata.receive_time_utc_ns = recv_ns
        bt.best_bid_price = "60000.0"
        bt.best_bid_quantity = "1.000"
        bt.best_ask_price = "60001.0"
        bt.best_ask_quantity = "1.000"
        return env

    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-sparse"
    bbo_stream = [(0.0, acc)] + [(0.04, _mk_bbo_env(t0 + i * 40_000_000)) for i in range(1, 40)]
    liq_env = gateway_messages_pb2.GatewayEventEnvelope()
    liq_env.liquidation.metadata.venue = enums_pb2.VENUE_BINANCE
    liq_env.liquidation.metadata.market = enums_pb2.MARKET_USD_M_PERPETUAL
    liq_env.liquidation.metadata.symbol = "BTCUSDT"
    liq_env.liquidation.metadata.receive_time_utc_ns = t0 + 600_000_000
    liq_env.liquidation.side = "BUY"
    liq_env.liquidation.order_type = "LIMIT"
    liq_env.liquidation.time_in_force = "IOC"
    liq_env.liquidation.original_quantity = "0.500"
    liq_env.liquidation.price = "60000.0"
    liq_env.liquidation.order_status = "FILLED"
    liq_env.liquidation.last_filled_quantity = "0.500"
    liq_env.liquidation.accumulated_filled_quantity = "0.500"
    liq_env.liquidation.order_trade_time_ms = (t0 + 600_000_000) // 1_000_000

    stub = _MultiStreamStub(
        market_streams={
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT"): [
                enums_pb2.STREAM_BOOK_TICKER,
                enums_pb2.STREAM_LIQUIDATION,
            ]
        },
        envelopes_by_key={
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", enums_pb2.STREAM_BOOK_TICKER): bbo_stream,
            (enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", enums_pb2.STREAM_LIQUIDATION): [
                (0.0, acc),
                (0.60, liq_env),
            ],
        },
    )

    async def _run_sparse_live() -> tuple[dict[str, Any], dict[str, Any]]:
        stats: dict[str, Any] = {}
        cfg = BmdGatewayConfig(
            target="stub:50051",
            instruments={btc.id: (Market.USDM, "BTCUSDT", btc)},
            required_streams=[
                (Market.USDM, "BTCUSDT", Stream.BOOK_TICKER),
                (Market.USDM, "BTCUSDT", Stream.LIQUIDATION),
            ],
            stale_threshold_ns=250_000_000,
            sparse_stale_threshold_ns=None,
            record_rows=False,
            stats=stats,
        )
        node = (
            LiveNode.builder("MNT2-SPARSE", TraderId("MNT2-002"), Environment.SANDBOX)
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
                    account_id=AccountId("BINANCE-SPARSE"),
                    account_type=AccountType.MARGIN,
                    oms_type=OmsType.NETTING,
                    book_type=BookType.L1_MBP,
                    starting_balances=[Money.from_str("2000.00 USDT")],
                    fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
                ),
            )
            .build()
        )
        live_tel: dict[str, Any] = {}
        strat = MultiTimescaleHybridStrategy(
            MultiTimescaleHybridStrategyConfig(
                instrument_id=btc.id,
                order_qty="0.010",
                timer_interval_ms=250,
                data_client_id="BMD",
                exec_client_id="BINANCE",
                telemetry=live_tel,
            ),
        )
        node.add_strategy(strat)

        async def _stop_when_done() -> None:
            for _ in range(150):
                if len(live_tel.get("liquidations", [])) >= 1:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.05)
            strat.shutdown_system("test_sparse_liquidation done")

        stop_task = asyncio.create_task(_stop_when_done())
        try:
            await asyncio.wait_for(node.run_async(), timeout=5.0)
        finally:
            stop_task.cancel()
            node.dispose()
        return stats, live_tel

    sparse_stats, sparse_tel = _run_in_fresh_thread(_run_sparse_live)
    assert len(sparse_tel["liquidations"]) == 1
    assert sparse_tel["stale_halts"] == 0
    assert not any("STALE" in h["reason"] for h in sparse_stats["health_events"])


def test_causal_ml_pipeline_embargo_cutoff_and_zero_prefix_mutation(tmp_path: Path) -> None:
    # 1. Verify walk_forward_only enforces embargo_ns strictly
    ts = [i * 1_000_000_000 for i in range(1, 21)]
    le = [t + 2_000_000_000 for t in ts]
    folds = purged_embargoed_splits(
        ts,
        le,
        n_splits=3,
        embargo_ns=2_000_000_000,
        walk_forward_only=True,
    )
    assert folds
    for f in folds:
        val_start = ts[f.val_indices[0]]
        for idx in f.train_indices:
            assert le[idx] + 2_000_000_000 <= val_start

    btc = _spot_inst("BTCUSDT")
    t0 = 1_795_421_400_000_000_000
    train_quotes = []
    for i in range(1, 31):
        base_px = Decimal("60000.00") + Decimal(i) * Decimal("5.00")
        # Create alternating BBO imbalance correlated with next step so Ridge learns non-zero weights
        bq = "5.00000" if i % 2 == 0 else "1.00000"
        aq = "1.00000" if i % 2 == 0 else "5.00000"
        train_quotes.append(_mk_quote(btc, t0 + i * 1_000_000_000, str(base_px), str(base_px + Decimal("1.00")), bq, aq))

    # Inject mid-train HALT/TRADING status to ensure epoch isolation drops cross-gap labels cleanly
    halt_st = InstrumentStatus(
        instrument_id=btc.id,
        action=MarketStatusAction.HALT,
        ts_event=t0 + 15_500_000_000,
        ts_init=t0 + 15_500_000_000,
        reason="GAP:test_epoch_break",
        is_trading=False,
        is_quoting=False,
    )
    live_st = InstrumentStatus(
        instrument_id=btc.id,
        action=MarketStatusAction.TRADING,
        ts_event=t0 + 16_500_000_000,
        ts_init=t0 + 16_500_000_000,
        reason="LIVE:resync",
        is_trading=True,
        is_quoting=True,
    )

    bundle_a, rep_a = train_causal_nautilus_bundle(
        quotes=train_quotes,
        statuses=[halt_st, live_st],
        market=Market.SPOT,
        symbol="BTCUSDT",
        models_root=tmp_path / "models_a",
        model_id="causal-btc-v1",
        embargo_ns=1_000_000_000,
        n_splits=3,
    )
    cutoff_ns = bundle_a.trained_until_ns
    assert cutoff_ns == t0 + 30_000_000_000

    # Verify strict fail-closed cutoff both directly (CausalCutoffError) and inside BacktestEngine
    guard_tel: dict[str, Any] = {}
    guard_strat = CausalMlStrategy(
        CausalMlStrategyConfig(
            instrument_id=btc.id,
            market="SPOT",
            symbol="BTCUSDT",
            model_bundle_dir=rep_a.bundle_dir,
            raise_on_pre_cutoff=True,
            telemetry=guard_tel,
        )
    )
    with pytest.raises(CausalCutoffError):
        guard_strat.on_quote(train_quotes[0])
    assert guard_tel["rejected_pre_cutoff"] == 1
    assert guard_tel["orders_submitted"] == []

    eng_err = BacktestEngine(
        BacktestEngineConfig(trader_id=TraderId("ML-CUTOFF"), logging=LoggerConfig(stdout_level=LogLevel.ERROR))
    )
    eng_tel: dict[str, Any] = {}
    try:
        eng_err.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("2000.00 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
            latency_model=StaticLatencyModel(base_latency_nanos=0),
            liquidity_consumption=True,
            bar_execution=False,
        )
        eng_err.add_instrument(btc)
        eng_err.add_data(train_quotes[:5])
        eng_err.add_strategy(
            CausalMlStrategy(
                CausalMlStrategyConfig(
                    instrument_id=btc.id,
                    market="SPOT",
                    symbol="BTCUSDT",
                    model_bundle_dir=rep_a.bundle_dir,
                    raise_on_pre_cutoff=True,
                    telemetry=eng_tel,
                )
            )
        )
        eng_err.run()
        assert eng_tel["rejected_pre_cutoff"] == 5
        assert eng_tel["orders_submitted"] == []
    finally:
        eng_err.dispose()

    # Prefix (t=1..45s: includes <=30s pre-cutoff AND 31..45s OOS) + Suffix A vs Suffix B (wildly mutated future)
    oos_prefix = []
    for i in range(31, 46):
        px = Decimal("60200.00") + Decimal(i - 30) * Decimal("2.00")
        bq = "8.00000" if i % 3 == 0 else "1.00000"
        aq = "1.00000" if i % 3 == 0 else "8.00000"
        oos_prefix.append(_mk_quote(btc, t0 + i * 1_000_000_000, str(px), str(px + Decimal("1.00")), bq, aq))

    suffix_a = [
        _mk_quote(btc, t0 + i * 1_000_000_000, "60300.00", "60301.00", "2.00000", "2.00000")
        for i in range(46, 56)
    ]
    suffix_b = [
        _mk_quote(btc, t0 + i * 1_000_000_000, "120000.00", "120005.00", "20.00000", "0.10000")
        for i in range(46, 56)
    ]

    # Retraining on same prefix cutoff while future suffix_b mutated -> bit-identical bundle weights & digest
    bundle_b, rep_b = train_causal_nautilus_bundle(
        quotes=train_quotes,
        statuses=[halt_st, live_st],
        market=Market.SPOT,
        symbol="BTCUSDT",
        models_root=tmp_path / "models_b",
        model_id="causal-btc-v1",
        embargo_ns=1_000_000_000,
        n_splits=3,
    )
    assert bundle_b.metadata.model_digest == bundle_a.metadata.model_digest
    assert bundle_b.metadata.weights == bundle_a.metadata.weights

    def _run_ml_bt(quotes_seq: list[QuoteTick], bundle_dir: str) -> dict[str, Any]:
        eng = BacktestEngine(
            BacktestEngineConfig(
                trader_id=TraderId("ML-BT-001"),
                logging=LoggerConfig(stdout_level=LogLevel.ERROR),
            )
        )
        try:
            eng.add_venue(
                venue=VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                starting_balances=[Money.from_str("2000.00 USDT")],
                book_type=BookType.L1_MBP,
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
                latency_model=StaticLatencyModel(base_latency_nanos=0),
                liquidity_consumption=True,
                bar_execution=False,
            )
            eng.add_instrument(btc)
            eng.add_data(quotes_seq)
            tel: dict[str, Any] = {}
            eng.add_strategy(
                CausalMlStrategy(
                    CausalMlStrategyConfig(
                        instrument_id=btc.id,
                        market="SPOT",
                        symbol="BTCUSDT",
                        model_bundle_dir=bundle_dir,
                        order_qty="0.00500",
                        entry_threshold=1e-7,
                        raise_on_pre_cutoff=False,
                        telemetry=tel,
                    ),
                )
            )
            eng.run()
            return tel
        finally:
            eng.dispose()

    tel_a = _run_ml_bt(train_quotes + oos_prefix + suffix_a, rep_a.bundle_dir)
    tel_b = _run_ml_bt(train_quotes + oos_prefix + suffix_b, rep_b.bundle_dir)

    assert tel_a["rejected_pre_cutoff"] == len(train_quotes)
    assert all(ts > cutoff_ns for ts, _ in tel_a["predictions"])
    assert max(tel_a["inference_latencies_ns"]) < 1_000_000  # < 1ms per sample

    split_ts = t0 + 45_000_000_000
    prefix_preds_a = [x for x in tel_a["predictions"] if x[0] <= split_ts]
    prefix_preds_b = [x for x in tel_b["predictions"] if x[0] <= split_ts]
    assert prefix_preds_a and prefix_preds_a == prefix_preds_b

    prefix_orders_a = [x for x in tel_a["orders_submitted"] if x[0] <= split_ts]
    prefix_orders_b = [x for x in tel_b["orders_submitted"] if x[0] <= split_ts]
    assert prefix_orders_a == prefix_orders_b
