"""M-NT3 tests: USD-M one-way isolated accounting, funding causality, filters, brackets, liquidation & Nautilus parity."""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any

import pytest

nt = pytest.importorskip("nautilus_trader")

from bq_feed.events import Market  # noqa: E402
from bq_nautilus.instruments import (  # noqa: E402
    DEFAULT_USDM_TIER1_BRACKETS,
    LeverageBracketTier,
    SUPPORTED_BMD_SYMBOLS,
    compute_tiered_maintenance_margin,
    spec_from_exchange_info_entry,
)
from bq_nautilus.usdm_accounting import (  # noqa: E402
    IsolatedUsdmLedger,
    compute_isolated_liquidation_price,
    plan_usdm_isolated_order,
)


def _sample_usdm_spec(symbol: str = "SOLUSDT", *, status: str = "TRADING") -> Any:
    specs = {
        "BTCUSDT": ("0.10", "0.0001", "0.0001", "120", "50"),
        "ETHUSDT": ("0.01", "0.001", "0.001", "10000", "20"),
        "SOLUSDT": ("0.01", "0.01", "0.01", "6000", "5"),
        "BNBUSDT": ("0.01", "0.01", "0.01", "100000", "5"),
        "XRPUSDT": ("0.0001", "0.1", "0.1", "1000000", "5"),
    }
    tick, lot_step, mkt_step, mkt_max, min_not = specs[symbol]
    return spec_from_exchange_info_entry(
        {
            "symbol": symbol,
            "contractType": "PERPETUAL",
            "status": status,
            "baseAsset": symbol.removesuffix("USDT"),
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "maintMarginPercent": "2.5000",
            "requiredMarginPercent": "5.0000",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": tick, "maxPrice": "1000000", "tickSize": tick},
                {"filterType": "LOT_SIZE", "minQty": lot_step, "maxQty": "1000000", "stepSize": lot_step},
                {"filterType": "MARKET_LOT_SIZE", "minQty": mkt_step, "maxQty": mkt_max, "stepSize": mkt_step},
                {"filterType": "MIN_NOTIONAL", "notional": min_not},
            ],
        },
        market=Market.USDM,
        observe_time_utc_ns=1_700_000_000_000_000_000,
        source_time_ms=1_700_000_000_000,
        source="fixture_demo_exchange_info",
    )


def test_five_symbol_usdm_specs_and_50_100_usdt_sizing_thresholds() -> None:
    # Verify all 5 symbols parse with distinct LOT_SIZE/MARKET_LOT_SIZE and MIN_NOTIONAL
    prices = {
        "BTCUSDT": Decimal("90000"),
        "ETHUSDT": Decimal("3200"),
        "SOLUSDT": Decimal("150"),
        "BNBUSDT": Decimal("620"),
        "XRPUSDT": Decimal("1.40"),
    }
    for sym in SUPPORTED_BMD_SYMBOLS:
        spec = _sample_usdm_spec(sym)
        brackets = DEFAULT_USDM_TIER1_BRACKETS[sym]
        plan100 = plan_usdm_isolated_order(
            spec=spec,
            brackets=brackets,
            leverage=5,
            side="BUY",
            order_type="MARKET",
            ref_price=prices[sym],
            free_collateral_usdt=Decimal("100"),
            loss_budget_usdt=Decimal("2.00"),
            stop_distance_pct=Decimal("0.01"),
            round_trip_fee_rate=Decimal("0.001"),
            adverse_slippage_rate=Decimal("0.0005"),
            taker_fee_rate=Decimal("0.0005"),
        )
        assert plan100.accepted is True, f"{sym} should be affordable at 100 USDT: {plan100}"
        assert plan100.notional_usdt >= spec.min_notional

    # Explicit 50 USDT vs below-MIN_NOTIONAL boundary on SOLUSDT (MIN_NOTIONAL = 5)
    sol_spec = _sample_usdm_spec("SOLUSDT")
    sol_brackets = DEFAULT_USDM_TIER1_BRACKETS["SOLUSDT"]
    # Risk budget 0.40 USDT / (0.01 + 0.001 + 0.0005 = 0.0115) = 34.7826 -> floor(34.7826 / 150, 0.01) = 0.23 SOL = 34.50 USDT
    p_ok = plan_usdm_isolated_order(
        spec=sol_spec,
        brackets=sol_brackets,
        leverage=5,
        side="SELL",
        order_type="MARKET",
        ref_price=Decimal("150"),
        free_collateral_usdt=Decimal("50"),
        loss_budget_usdt=Decimal("0.4025"),
        stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"),
        adverse_slippage_rate=Decimal("0.0005"),
        taker_fee_rate=Decimal("0.0005"),
    )
    assert p_ok.accepted is True
    assert p_ok.qty == Decimal("0.23")
    assert p_ok.notional_usdt == Decimal("34.50")

    # Small risk budget giving 4.90 USDT (< 5 USDT MIN_NOTIONAL) MUST reject, NEVER clamp upward to 5 USDT
    p_small = plan_usdm_isolated_order(
        spec=sol_spec,
        brackets=sol_brackets,
        leverage=5,
        side="BUY",
        order_type="MARKET",
        ref_price=Decimal("150"),
        free_collateral_usdt=Decimal("50"),
        loss_budget_usdt=Decimal("0.05635"),  # / 0.0115 = 4.90 USDT -> 0.03 SOL * 150 = 4.50 USDT < 5
        stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"),
        adverse_slippage_rate=Decimal("0.0005"),
        taker_fee_rate=Decimal("0.0005"),
    )
    assert p_small.accepted is False
    assert p_small.reject_reason == "BELOW_MIN_NOTIONAL"

    # BTCUSDT with 50 USDT at 1x leverage: taker fee + buffer prevents buying 50 USDT notional -> REJECT
    btc_spec = _sample_usdm_spec("BTCUSDT")
    p_btc_1x = plan_usdm_isolated_order(
        spec=btc_spec,
        brackets=DEFAULT_USDM_TIER1_BRACKETS["BTCUSDT"],
        leverage=1,
        side="BUY",
        order_type="MARKET",
        ref_price=Decimal("100000"),
        free_collateral_usdt=Decimal("50"),
        loss_budget_usdt=Decimal("2.00"),
        stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"),
        adverse_slippage_rate=Decimal("0.0005"),
        taker_fee_rate=Decimal("0.0005"),
    )
    assert p_btc_1x.accepted is False
    assert p_btc_1x.reject_reason in ("BELOW_MIN_NOTIONAL", "INSUFFICIENT_MARGIN_AFTER_FLOOR")


def test_causal_funding_crossing_boundaries_late_duplicates_and_non_anticipation() -> None:
    t0 = 1_700_000_000_000_000_000
    t_b1 = t0 + 10_000_000_000
    t_b2 = t0 + 20_000_000_000

    ledger = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("100"), leverage=5)
    # Prediction arrived at t0 + 1s for t_b1
    ledger.record_mark_and_predicted_funding(
        ts_event_ns=t0 + 1_000_000_000,
        visible_utc_ns=t0 + 1_000_000_000,
        mark_price=Decimal("150"),
        predicted_rate=Decimal("0.0005"),
        next_funding_ns=t_b1,
    )
    assert ledger.visible_predicted_funding(t0) is None
    assert ledger.visible_predicted_funding(t0 + 1_000_000_000).rate == Decimal("0.0005")

    # Open 0.50 SOL long BEFORE t_b1 (notional=75, IM=15, fee=0.0375 USDT)
    ledger.apply_fill(
        ts_ns=t0 + 2_000_000_000,
        order_id="e1",
        side="BUY",
        qty=Decimal("0.50"),
        price=Decimal("150"),
        fee_amount=Decimal("0.0375"),
        fee_asset="USDT",
    )
    # Mark at boundary t_b1 = 160
    ledger.record_mark_and_predicted_funding(
        ts_event_ns=t_b1,
        visible_utc_ns=t_b1,
        mark_price=Decimal("160"),
        predicted_rate=Decimal("-0.001"),  # negative rate -> longs RECEIVE!
        next_funding_ns=t_b1,
    )
    # Close BEFORE settled billing record arrives!
    ledger.apply_fill(
        ts_ns=t_b1 + 1_000_000,
        order_id="e2",
        side="SELL",
        qty=Decimal("0.50"),
        price=Decimal("160"),
        fee_amount=Decimal("0.04"),
        fee_asset="USDT",
        funding_boundaries_before_fill=(t_b1,),
    )
    assert ledger.position_qty == Decimal(0)
    avail_before_visible = ledger.available_for_new_orders_usdt()

    # Late funding settlement arrives at t_b1 + 500ms, processed at current clock t_b1 + 10ms -> MUST be pending!
    entry = ledger.settle_funding(
        funding_time_ns=t_b1,
        settled_rate=Decimal("-0.001"),
        settlement_mark_price=Decimal("160"),
        visible_utc_ns=t_b1 + 500_000_000,
        source="demo_income",
    )
    assert entry.boundary_pos_qty == Decimal("0.50")  # boundary pos before close!
    assert entry.posted_amount_usdt == Decimal("0.08000")
    assert entry.posted_to_wallet is False
    # Cash CANNOT be spent prematurely before visible_utc_ns
    assert ledger.available_for_new_orders_usdt() == avail_before_visible

    # Advance to visible_utc_ns -> funds post
    released = ledger.advance_time(t_b1 + 500_000_000)
    assert len(released) == 1 and released[0].posted_to_wallet is True
    assert ledger.available_for_new_orders_usdt() == avail_before_visible + Decimal("0.08000")

    # Duplicate replay MUST be ignored
    dup = ledger.settle_funding(
        funding_time_ns=t_b1,
        settled_rate=Decimal("-0.001"),
        settlement_mark_price=Decimal("160"),
        visible_utc_ns=t_b1 + 600_000_000,
    )
    assert ledger.duplicate_funding_ignored == 1
    assert ledger.available_for_new_orders_usdt() == avail_before_visible + Decimal("0.08000")
    assert dup.posted_amount_usdt == Decimal("0.08000")

    # Zero-position boundary (opened AFTER t_b2 -> 0 funding at t_b2)
    zero_entry = ledger.settle_funding(
        funding_time_ns=t_b2,
        settled_rate=Decimal("0.002"),
        settlement_mark_price=Decimal("162"),
        visible_utc_ns=t_b2,
    )
    assert zero_entry.boundary_pos_qty == Decimal(0)
    assert zero_entry.posted_amount_usdt == Decimal(0)

    # Non-anticipation: mutating future funding_rate at t_b2 + 10s CANNOT alter anything up to t_b1 + 500ms
    snap_before = (ledger.cross_wallet_usdt, ledger.funding_pnl_usdt)
    ledger2 = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("100"), leverage=5)
    ledger2.record_mark_and_predicted_funding(
        ts_event_ns=t0 + 1_000_000_000,
        visible_utc_ns=t0 + 1_000_000_000,
        mark_price=Decimal("150"),
        predicted_rate=Decimal("0.0005"),
        next_funding_ns=t_b1,
    )
    ledger2.apply_fill(ts_ns=t0 + 2_000_000_000, order_id="e1", side="BUY", qty=Decimal("0.50"), price=Decimal("150"), fee_amount=Decimal("0.0375"))
    ledger2.record_mark_and_predicted_funding(ts_event_ns=t_b1, visible_utc_ns=t_b1, mark_price=Decimal("160"), predicted_rate=Decimal("-0.001"), next_funding_ns=t_b1)
    ledger2.apply_fill(ts_ns=t_b1 + 1_000_000, order_id="e2", side="SELL", qty=Decimal("0.50"), price=Decimal("160"), fee_amount=Decimal("0.04"), funding_boundaries_before_fill=(t_b1,))
    ledger2.settle_funding(funding_time_ns=t_b1, settled_rate=Decimal("-0.001"), settlement_mark_price=Decimal("160"), visible_utc_ns=t_b1 + 500_000_000)
    ledger2.advance_time(t_b1 + 500_000_000)
    assert (ledger2.cross_wallet_usdt, ledger2.funding_pnl_usdt) == snap_before


def test_fee_asset_separation_usdt_base_bnb_and_rounding_delta() -> None:
    ledger = IsolatedUsdmLedger(
        symbol="SOLUSDT",
        cross_wallet_usdt=Decimal("100"),
        leverage=5,
        asset_wallets={"USDT": Decimal("100"), "BNB": Decimal("0.1"), "SOL": Decimal("1")},
    )
    ledger.apply_fill(
        ts_ns=1_000,
        order_id="o1",
        side="BUY",
        qty=Decimal("0.20"),
        price=Decimal("150"),
        fee_amount=Decimal("0.015"),
        fee_asset="USDT",
    )
    ledger.apply_fill(
        ts_ns=2_000,
        order_id="o2",
        side="BUY",
        qty=Decimal("0.20"),
        price=Decimal("150"),
        fee_amount=Decimal("0.000025"),
        fee_asset="BNB",
        fee_conversion_rate=Decimal("600"),
        fee_conversion_ts_ns=2_000,
    )
    ledger.apply_fill(
        ts_ns=3_000,
        order_id="o3",
        side="SELL",
        qty=Decimal("0.40"),
        price=Decimal("150"),
        fee_amount=Decimal("0.0002"),
        fee_asset="SOL",
        fee_conversion_rate=Decimal("150"),
        fee_conversion_ts_ns=3_000,
    )
    assert ledger.fee_totals_by_asset == {
        "USDT": Decimal("0.015"),
        "BNB": Decimal("0.000025"),
        "SOL": Decimal("0.0002"),
    }
    assert ledger.asset_wallets["BNB"] == Decimal("0.099975")
    assert ledger.asset_wallets["SOL"] == Decimal("0.9998")
    # Rounding delta audit when exact exchange income differs by 1e-8
    ent = ledger.settle_funding(
        funding_time_ns=4_000,
        settled_rate=Decimal("0.0001"),
        settlement_mark_price=Decimal("150"),
        visible_utc_ns=4_000,
        exact_income_usdt=Decimal("-0.00000001"),
    )
    assert ent.rounding_delta_usdt == Decimal("-0.00000001")


def test_isolated_liquidation_long_short_threshold_and_gap_sensitivity() -> None:
    brackets = (
        LeverageBracketTier(1, 20, Decimal("0"), Decimal("10000"), Decimal("0.005"), Decimal("0")),
        LeverageBracketTier(2, 10, Decimal("10000"), Decimal("100000"), Decimal("0.01"), Decimal("50")),
    )
    # Tier-2 continuity check at notional 20,000: 20,000 * 0.01 - 50 = 150
    mm_t2, t2 = compute_tiered_maintenance_margin(Decimal("20000"), brackets)
    assert t2.tier == 2 and mm_t2 == Decimal("150")

    # Isolated Long: 50 USDT total, allocate 20 USDT isolated (`auto_allocate_isolated=False`) for 1.00 SOL @ 100 (5x)
    # Cross wallet stays 30 USDT untouched!
    ledger_l = IsolatedUsdmLedger(
        symbol="SOLUSDT",
        cross_wallet_usdt=Decimal("30"),
        isolated_wallet_usdt=Decimal("20"),
        leverage=5,
        brackets=brackets,
        auto_allocate_isolated=False,
    )
    ledger_l.apply_fill(ts_ns=100, order_id="l1", side="BUY", qty=Decimal("1.00"), price=Decimal("100"), fee_amount=Decimal("0.05"))
    # WB_iso = 19.95 -> P_liq = (100 - 19.95) / (1 - 0.005) = 80.05 / 0.995 = 80.45226...
    liq_px, bkr_px, _ = ledger_l.liquidation_prices()
    assert liq_px is not None and bkr_px is not None
    assert bkr_px == Decimal("80.05")
    assert Decimal("80.45") < liq_px < Decimal("80.46")

    # Above liquidation threshold -> NOT liquidated
    safe = ledger_l.evaluate_mark_and_liquidation(ts_ns=200, mark_price=Decimal("81.00"), protection_trigger_price=Decimal("90.00"))
    assert safe.triggered is False
    # Price gaps BELOW stop (90) AND BELOW bankruptcy (80.05) -> 75.00!
    gap = ledger_l.evaluate_mark_and_liquidation(ts_ns=300, mark_price=Decimal("75.00"), protection_trigger_price=Decimal("90.00"))
    assert gap.triggered is True
    assert gap.jumped_through_protection is True
    assert gap.gap_deficit_usdt > 0
    assert gap.sensitivity_band_usdt is not None
    # Isolated invariant: unallocated cross_wallet (30) is UNTOUCHED!
    assert ledger_l.cross_wallet_usdt == Decimal("30")
    assert ledger_l.isolated_wallet_usdt == Decimal("0")

    # Short liquidation boundary: Short 1.00 SOL @ 100 with WB_iso = 20 - 0.05 = 19.95 -> P_liq = (119.95) / 1.005 = 119.3532...
    ledger_s = IsolatedUsdmLedger(
        symbol="SOLUSDT",
        cross_wallet_usdt=Decimal("30"),
        isolated_wallet_usdt=Decimal("20"),
        leverage=5,
        brackets=brackets,
        auto_allocate_isolated=False,
    )
    ledger_s.apply_fill(ts_ns=100, order_id="s1", side="SELL", qty=Decimal("1.00"), price=Decimal("100"), fee_amount=Decimal("0.05"))
    liq_s, bkr_s, _ = ledger_s.liquidation_prices()
    assert liq_s is not None and bkr_s == Decimal("119.95")
    assert ledger_s.evaluate_mark_and_liquidation(ts_ns=200, mark_price=Decimal("119.00")).triggered is False
    gap_s = ledger_s.evaluate_mark_and_liquidation(ts_ns=300, mark_price=Decimal("121.00"), protection_trigger_price=Decimal("105.00"))
    assert gap_s.triggered is True and gap_s.jumped_through_protection is True
    assert ledger_s.cross_wallet_usdt == Decimal("30")

    # Exact parity against real observed Demo SOLUSDT positionRisk:
    # entry=71.38, qty=0.20, iso_wallet=2.85045171, Tier-1 MMR=0.01, cum=0 -> exchange reported 57.70478934
    liq_obs, _, _ = compute_isolated_liquidation_price(
        position_qty=Decimal("0.20"),
        entry_price=Decimal("71.38"),
        isolated_wallet_usdt=Decimal("2.85045171"),
        brackets=DEFAULT_USDM_TIER1_BRACKETS["SOLUSDT"],
    )
    assert liq_obs is not None
    assert abs(liq_obs - Decimal("57.70478934")) < Decimal("1e-8")


def test_nautilus_usdm_backtest_parity_and_five_symbol_protection() -> None:
    pytest.importorskip("nautilus_trader")
    from nautilus_trader.model import AggressorSide, FundingRateUpdate, InstrumentId, MarkPriceUpdate, Price, Quantity, QuoteTick, TradeId, TradeTick
    from bq_nautilus.usdm_runner import UsdmPerpAuditStrategyConfig, run_usdm_backtest, SignedUsdmRestClient, redact_sensitive

    # Verify all 5 symbols produce valid CryptoPerpetual & allowlist rejects unapproved symbols
    for sym in SUPPORTED_BMD_SYMBOLS:
        inst = _sample_usdm_spec(sym).to_nautilus_instrument()
        assert str(inst.id) == f"{sym}-PERP.BINANCE"

    sol_spec = _sample_usdm_spec("SOLUSDT")
    sol_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    t0 = 1_700_000_000_000_000_000
    t_fund = t0 + 10_000_000_000
    data = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        FundingRateUpdate(sol_id, Decimal("0.001"), t0 + 2_000_000, t0 + 2_000_000, interval=None, next_funding_ns=t_fund),
        MarkPriceUpdate(sol_id, Price.from_str("160.00"), t_fund - 1_000_000, t_fund - 1_000_000),
        # Book drops to 146.80/146.90 first, then TradeTick 146.90 triggers CONTRACT_PRICE STOP_MARKET (fills at bid 146.80)
        QuoteTick(sol_id, Price.from_str("146.80"), Price.from_str("146.90"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t_fund + 1_000_000_000, t_fund + 1_000_000_000),
        TradeTick(sol_id, Price.from_str("146.90"), Quantity.from_str("1.00"), AggressorSide.SELL, TradeId("TRD-1"), t_fund + 1_000_000_100, t_fund + 1_000_000_100),
    ]
    res = run_usdm_backtest(
        spec=sol_spec,
        data=data,
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            allowed_symbol="SOLUSDT",
            leverage=5,
            order_qty="0.30",
            side="BUY",
            protection_mode="quantity_reduce_only",
            working_type="CONTRACT_PRICE",
            stop_distance_pct="0.02",
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
    )
    # Check entry + stop exit fills and exact Decimal ledger parity
    assert len(res["fills"]) == 2
    assert Decimal(res["net_pos"]) == Decimal(0)
    assert Decimal(res["fills"][1]["px"]) <= Decimal("147.00")

    hand = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    hand.apply_fill(ts_ns=t0, order_id="f1", side="BUY", qty=Decimal("0.30"), price=Decimal("150.00"), fee_amount=Decimal("0.30") * Decimal("150.00") * Decimal("0.0005"))
    hand.settle_funding(funding_time_ns=t_fund, settled_rate=Decimal("0.001"), settlement_mark_price=Decimal("160.00"), visible_utc_ns=t_fund)
    expected_contract_stop_px = Decimal("146.90")
    assert Decimal(res["fills"][1]["px"]) == expected_contract_stop_px
    hand.apply_fill(ts_ns=t_fund + 1_000_000_100, order_id="f2", side="SELL", qty=Decimal("0.30"), price=expected_contract_stop_px, fee_amount=Decimal("0.30") * expected_contract_stop_px * Decimal("0.0005"))
    assert Decimal(res["final_total_usdt"]) == hand.cross_wallet_usdt == Decimal("48.97746500")

    # Separate MARK_PRICE LONG stop vs visible L1 bid (146.80 != mark 146.90) -> independent 48.94748000!
    res_mark_long = run_usdm_backtest(
        spec=sol_spec,
        data=[
            QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
            MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
            FundingRateUpdate(sol_id, Decimal("0.001"), t0 + 2_000_000, t0 + 2_000_000, interval=None, next_funding_ns=t_fund),
            MarkPriceUpdate(sol_id, Price.from_str("160.00"), t_fund - 1_000_000, t_fund - 1_000_000),
            QuoteTick(sol_id, Price.from_str("146.80"), Price.from_str("146.90"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t_fund + 1_000_000_000, t_fund + 1_000_000_000),
            MarkPriceUpdate(sol_id, Price.from_str("146.90"), t_fund + 1_000_000_100, t_fund + 1_000_000_100),
        ],
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            allowed_symbol="SOLUSDT",
            leverage=5,
            order_qty="0.30",
            side="BUY",
            protection_mode="quantity_reduce_only",
            working_type="MARK_PRICE",
            stop_distance_pct="0.02",
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
    )
    assert Decimal(res_mark_long["fills"][1]["px"]) == Decimal("146.80")
    assert Decimal(res_mark_long["final_total_usdt"]) == Decimal("48.94748000")

    # Verify algoOrder validation rules & secret redaction
    client = SignedUsdmRestClient(api_key="SECRET_KEY", api_secret="SECRET_VAL")
    cp_payload = client.validate_algo_order_params(
        symbol="SOLUSDT",
        side="SELL",
        order_type="STOP_MARKET",
        trigger_price="147.00",
        working_type="MARK_PRICE",
        close_position=True,
    )
    assert "quantity" not in cp_payload and "reduceOnly" not in cp_payload and cp_payload["closePosition"] is True
    with pytest.raises(ValueError, match="forbids quantity and reduceOnly"):
        client.validate_algo_order_params(
            symbol="SOLUSDT",
            side="SELL",
            order_type="STOP_MARKET",
            trigger_price="147.00",
            close_position=True,
            quantity="0.30",
        )
    redacted = redact_sensitive({"X-MBX-APIKEY": "ABC", "url": "https://x?a=1&signature=deadbeef"})
    assert redacted["X-MBX-APIKEY"] == "***REDACTED***"
    assert redacted["url"].endswith("signature=***REDACTED***")


def test_nautilus_usdm_liquidation_and_close_position_parity() -> None:
    from nautilus_trader.model import InstrumentId, MarkPriceUpdate, Price, Quantity, QuoteTick
    from bq_nautilus.usdm_runner import UsdmPerpAuditStrategyConfig, run_usdm_backtest

    sol_spec = _sample_usdm_spec("SOLUSDT")
    sol_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    t0 = 1_700_000_000_000_000_000

    # 50 USDT all-in at 5x: open 1.00 SOL @ 150 -> gap to 100.00 -> both Nautilus BacktestEngine & IsolatedUsdmLedger liquidate!
    data = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        # Skip past stop trigger (147) down to liquidation gap (100)
        MarkPriceUpdate(sol_id, Price.from_str("100.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
        QuoteTick(sol_id, Price.from_str("100.00"), Price.from_str("100.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
    ]
    res = run_usdm_backtest(
        spec=sol_spec,
        data=data,
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            allowed_symbol="SOLUSDT",
            leverage=5,
            order_qty="1.00",
            side="BUY",
            protection_mode="close_position",
            working_type="MARK_PRICE",
            stop_distance_pct="0.02",
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
        liquidation_enabled=True,
    )
    assert Decimal(res["net_pos"]) == Decimal(0)
    assert len(res["fills"]) == 2  # entry + stop/liquidation exit
    assert Decimal(res["final_locked_usdt"]) == Decimal(0)
    # Mark trigger vs book fill separation test: MARK_PRICE stop triggers on MarkPriceUpdate(146.90)
    # AFTER QuoteTick(146.50, 146.60) and therefore fills at taker bid 146.50 (NOT mark 146.90)!
    res_mark = run_usdm_backtest(
        spec=sol_spec,
        data=[
            QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
            MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
            QuoteTick(sol_id, Price.from_str("146.50"), Price.from_str("146.60"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
            MarkPriceUpdate(sol_id, Price.from_str("146.90"), t0 + 2_000_000_100, t0 + 2_000_000_100),
        ],
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            allowed_symbol="SOLUSDT",
            leverage=5,
            order_qty="0.30",
            side="BUY",
            protection_mode="close_position",
            working_type="MARK_PRICE",
            stop_distance_pct="0.02",
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
        liquidation_enabled=True,
    )
    assert len(res_mark["fills"]) == 2
    assert res_mark["fills"][1]["px"] == "146.50"
    assert Decimal(res_mark["net_pos"]) == Decimal(0)


def test_team_c_mnt3_p1_fixes(monkeypatch: pytest.MonkeyPatch) -> None:
    from nautilus_trader.model import InstrumentId, MarkPriceUpdate, Price, Quantity, QuoteTick
    from bq_nautilus.usdm_runner import UsdmPerpAuditStrategyConfig, run_usdm_backtest
    from scripts.mnt3_usdm_verify import main as mnt3_main

    sol_spec = _sample_usdm_spec("SOLUSDT")
    sol_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    t0 = 1_700_000_000_000_000_000

    # 1. Equal-qty reversal protection in both quantity_reduce_only & close_position
    for pmode in ("quantity_reduce_only", "close_position"):
        res = run_usdm_backtest(
            spec=sol_spec,
            data=[
                QuoteTick(sol_id, Price.from_str("100.00"), Price.from_str("100.01"), Quantity.from_str("10.00"), Quantity.from_str("10.00"), t0, t0),
                MarkPriceUpdate(sol_id, Price.from_str("100.00"), t0 + 1, t0 + 1),
                QuoteTick(sol_id, Price.from_str("105.00"), Price.from_str("105.01"), Quantity.from_str("10.00"), Quantity.from_str("10.00"), t0 + 1_000_000_000, t0 + 1_000_000_000),
                MarkPriceUpdate(sol_id, Price.from_str("105.00"), t0 + 1_000_000_001, t0 + 1_000_000_001),
                QuoteTick(sol_id, Price.from_str("120.00"), Price.from_str("120.01"), Quantity.from_str("10.00"), Quantity.from_str("10.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
                MarkPriceUpdate(sol_id, Price.from_str("120.00"), t0 + 2_000_000_001, t0 + 2_000_000_001),
            ],
            strategy_config=UsdmPerpAuditStrategyConfig(
                instrument_id=sol_id,
                protection_mode=pmode,
                working_type="MARK_PRICE",
                entry_mode="plan_scripted",
                stop_distance_pct="0.02",
                scripted_actions=(
                    {"callback": 1, "side": "BUY", "qty": "0.20"},
                    {"callback": 2, "side": "SELL", "qty": "0.40"},
                ),
            ),
            starting_usdt=Decimal("50"),
            leverage=5,
            liquidation_enabled=False,
        )
        assert Decimal(res["net_pos"]) == Decimal(0)
        assert len(res["fills"]) == 3
        assert res["fills"][-1]["side"] == "BUY"
        assert Decimal(res["fills"][-1]["px"]) == Decimal("120.01")
        prot_sides = [i["side"] for i in res["intents"] if i["kind"].startswith("protection_")]
        assert prot_sides == ["SELL", "BUY"]

    # Optional close-then-open enforcement rejects single cross-zero order
    res_block = run_usdm_backtest(
        spec=sol_spec,
        data=[
            QuoteTick(sol_id, Price.from_str("100.00"), Price.from_str("100.01"), Quantity.from_str("10.00"), Quantity.from_str("10.00"), t0, t0),
            QuoteTick(sol_id, Price.from_str("105.00"), Price.from_str("105.01"), Quantity.from_str("10.00"), Quantity.from_str("10.00"), t0 + 1_000_000_000, t0 + 1_000_000_000),
        ],
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            protection_mode="quantity_reduce_only",
            entry_mode="plan_scripted",
            forbid_cross_zero_reversal=True,
            scripted_actions=(
                {"callback": 1, "side": "BUY", "qty": "0.20"},
                {"callback": 2, "side": "SELL", "qty": "0.40"},
            ),
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
        liquidation_enabled=False,
    )
    assert any(b["reason"] == "FORBID_CROSS_ZERO_REVERSAL" for b in res_block["blocked_risk_actions"])
    assert Decimal(res_block["net_pos"]) == Decimal("0.20")

    # 2. Funding boundary after-open vs pre-close + cross-before-isolated deduction
    late = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    late.apply_fill(ts_ns=11, order_id="o-late", side="BUY", qty=Decimal("1"), price=Decimal("100"), fee_amount=Decimal("0"))
    ent_late = late.settle_funding(funding_time_ns=10, settled_rate=Decimal("0.01"), settlement_mark_price=Decimal("100"), visible_utc_ns=15)
    late.advance_time(15)
    assert ent_late.boundary_pos_qty == Decimal(0)
    assert late.funding_pnl_usdt == Decimal(0)

    payer = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    payer.apply_fill(ts_ns=1, order_id="o-pay", side="BUY", qty=Decimal("1"), price=Decimal("100"), fee_amount=Decimal("0"))
    payer.advance_time(10)
    payer.settle_funding(funding_time_ns=10, settled_rate=Decimal("0.01"), settlement_mark_price=Decimal("100"), visible_utc_ns=10)
    assert payer.cross_wallet_usdt == Decimal("29.00")
    assert payer.isolated_wallet_usdt == Decimal("20")

    # 3. Reduce-only planner ignores free_collateral=0, blocks flat/wrong-side/over-allocation, enforces closePosition mutex
    brackets = DEFAULT_USDM_TIER1_BRACKETS["SOLUSDT"]
    p_exit_zero = plan_usdm_isolated_order(
        spec=sol_spec, brackets=brackets, leverage=5, side="SELL", order_type="MARKET", ref_price=Decimal("100"),
        free_collateral_usdt=Decimal("0"), loss_budget_usdt=Decimal("0"), stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"), adverse_slippage_rate=Decimal("0.0005"), taker_fee_rate=Decimal("0.0005"),
        is_reduce_only=True, current_position_qty=Decimal("0.20"),
    )
    assert p_exit_zero.accepted is True and p_exit_zero.qty == Decimal("0.20")

    p_flat = plan_usdm_isolated_order(
        spec=sol_spec, brackets=brackets, leverage=5, side="SELL", order_type="MARKET", ref_price=Decimal("100"),
        free_collateral_usdt=Decimal("50"), loss_budget_usdt=Decimal("2"), stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"), adverse_slippage_rate=Decimal("0.0005"), taker_fee_rate=Decimal("0.0005"),
        is_reduce_only=True, current_position_qty=Decimal("0"),
    )
    assert p_flat.accepted is False and p_flat.reject_reason == "NO_OPEN_POSITION_FOR_REDUCE_ONLY"

    p_wrong = plan_usdm_isolated_order(
        spec=sol_spec, brackets=brackets, leverage=5, side="BUY", order_type="MARKET", ref_price=Decimal("100"),
        free_collateral_usdt=Decimal("50"), loss_budget_usdt=Decimal("2"), stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"), adverse_slippage_rate=Decimal("0.0005"), taker_fee_rate=Decimal("0.0005"),
        is_reduce_only=True, current_position_qty=Decimal("0.20"),
    )
    assert p_wrong.accepted is False and p_wrong.reject_reason == "WRONG_SIDE_FOR_REDUCE_ONLY"

    p_dup_ro = plan_usdm_isolated_order(
        spec=sol_spec, brackets=brackets, leverage=5, side="SELL", order_type="MARKET", ref_price=Decimal("100"),
        free_collateral_usdt=Decimal("50"), loss_budget_usdt=Decimal("2"), stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"), adverse_slippage_rate=Decimal("0.0005"), taker_fee_rate=Decimal("0.0005"),
        is_reduce_only=True, current_position_qty=Decimal("0.20"), open_reduce_only_qty=Decimal("0.20"),
    )
    assert p_dup_ro.accepted is False and p_dup_ro.reject_reason == "REDUCE_ONLY_EXCEEDS_REMAINING_POSITION"

    p_cp = plan_usdm_isolated_order(
        spec=sol_spec, brackets=brackets, leverage=5, side="SELL", order_type="MARKET", ref_price=Decimal("100"),
        free_collateral_usdt=Decimal("0"), loss_budget_usdt=Decimal("0"), stop_distance_pct=Decimal("0.01"),
        round_trip_fee_rate=Decimal("0.001"), adverse_slippage_rate=Decimal("0.0005"), taker_fee_rate=Decimal("0.0005"),
        is_reduce_only=True, current_position_qty=Decimal("0.20"), close_position=True,
    )
    assert p_cp.accepted is True and p_cp.wire_omit_quantity is True and p_cp.wire_omit_reduce_only is True

    # 4. apply_fill atomicity + duplicate fill_id dedup + two partial fills on same order_id
    atomic = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    atomic.apply_fill(ts_ns=1, order_id="ord-1", side="BUY", qty=Decimal("0.5"), price=Decimal("100"), fee_amount=Decimal("0"), fill_id="t1")
    atomic.apply_fill(ts_ns=2, order_id="ord-1", side="BUY", qty=Decimal("0.5"), price=Decimal("100"), fee_amount=Decimal("0"), fill_id="t2")
    atomic.apply_fill(ts_ns=2, order_id="ord-1", side="BUY", qty=Decimal("0.5"), price=Decimal("100"), fee_amount=Decimal("0"), fill_id="t2")  # dup
    assert atomic.position_qty == Decimal("1")
    snap_before = (atomic.position_qty, atomic.entry_price, atomic.cross_wallet_usdt, atomic.isolated_wallet_usdt, atomic.clock_ns)
    with pytest.raises(ValueError, match="Insufficient cross wallet"):
        atomic.apply_fill(ts_ns=3, order_id="ord-bad", side="BUY", qty=Decimal("2"), price=Decimal("200"), fee_amount=Decimal("0"), fill_id="t3")
    assert (atomic.position_qty, atomic.entry_price, atomic.cross_wallet_usdt, atomic.isolated_wallet_usdt, atomic.clock_ns) == snap_before

    # 5. evaluate_mark_and_liquidation preserves both pre-liq trigger snapshot and post-liq zero balances
    liq_l = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("0"), isolated_wallet_usdt=Decimal("50"), leverage=5, auto_allocate_isolated=False)
    liq_l.apply_fill(ts_ns=10, order_id="liq-1", side="BUY", qty=Decimal("1.00"), price=Decimal("150.00"), fee_amount=Decimal("0.075"))
    liq_ev = liq_l.evaluate_mark_and_liquidation(ts_ns=20, mark_price=Decimal("100.00"), protection_trigger_price=Decimal("147.00"))
    assert liq_ev.triggered is True
    assert liq_ev.position_qty == Decimal("1.00") and liq_ev.isolated_wallet_usdt == Decimal("49.925")
    assert liq_ev.post_position_qty == Decimal(0) and liq_ev.post_isolated_wallet_usdt == Decimal(0)

    # 6. live-demo without credentials MUST exit non-zero (1)
    monkeypatch.delenv("BINANCE_DEMO_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_DEMO_API_SECRET", raising=False)
    monkeypatch.setattr("sys.argv", ["mnt3_usdm_verify.py", "--suite", "live-demo"])
    assert mnt3_main() == 1


