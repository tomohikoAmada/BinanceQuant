"""M-NT3 Verification Harness: Public Specs, Decimal vs Nautilus Parity, Mock USD-M Demo E2E & Live Demo."""

from __future__ import annotations

import argparse
import asyncio
import base64
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
import urllib.request

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bq_feed.events import Market  # noqa: E402
from bq_nautilus.instruments import (  # noqa: E402
    DEFAULT_USDM_TIER1_BRACKETS,
    LeverageBracketTier,
    SUPPORTED_BMD_SYMBOLS,
    compute_tiered_maintenance_margin,
    fetch_binance_exchange_info_specs,
    spec_from_exchange_info_entry,
)
from bq_nautilus.usdm_accounting import (  # noqa: E402
    IsolatedUsdmLedger,
    plan_usdm_isolated_order,
)
from bq_nautilus.usdm_runner import (  # noqa: E402
    SignedUsdmRestClient,
    UsdmPerpAuditStrategy,
    UsdmPerpAuditStrategyConfig,
    build_binance_usdm_demo_configs,
    redact_sensitive,
    run_usdm_backtest,
)

WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_encode_text(payload: str) -> bytes:
    data = payload.encode("utf-8")
    n = len(data)
    if n < 126:
        header = struct.pack("!BB", 0x81, n)
    elif n < 65536:
        header = struct.pack("!BBH", 0x81, 126, n)
    else:
        header = struct.pack("!BBQ", 0x81, 127, n)
    return header + data


async def _ws_read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes] | None:
    try:
        b1, b2 = await reader.readexactly(2)
    except asyncio.IncompleteReadError:
        return None
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = bytearray(await reader.readexactly(length))
    if masked:
        for i in range(length):
            payload[i] ^= mask[i % 4]
    return opcode, bytes(payload)


def _fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "BinanceQuant/M-NT3"})
    with urllib.request.urlopen(req, timeout=10.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_public_specs_suite() -> dict[str, Any]:
    """Verify contemporaneous 5-symbol Demo vs Live USD-M specs, fundingInfo, & 50/100 USDT bounds."""
    demo_specs = fetch_binance_exchange_info_specs(market=Market.USDM, demo=True)
    live_specs = fetch_binance_exchange_info_specs(market=Market.USDM, demo=False)
    demo_prem = {x["symbol"]: x for x in _fetch_json("https://demo-fapi.binance.com/fapi/v1/premiumIndex") if x.get("symbol") in SUPPORTED_BMD_SYMBOLS}
    live_prem = {x["symbol"]: x for x in _fetch_json("https://fapi.binance.com/fapi/v1/premiumIndex") if x.get("symbol") in SUPPORTED_BMD_SYMBOLS}
    demo_fi = {x["symbol"]: x for x in _fetch_json("https://demo-fapi.binance.com/fapi/v1/fundingInfo") if x.get("symbol") in SUPPORTED_BMD_SYMBOLS}

    per_symbol: dict[str, Any] = {}
    for sym in SUPPORTED_BMD_SYMBOLS:
        ds = demo_specs[sym]
        ls = live_specs[sym]
        mark_px = Decimal(str(live_prem[sym]["markPrice"]))
        brackets = DEFAULT_USDM_TIER1_BRACKETS[sym]

        plans: dict[str, Any] = {}
        for capital in (Decimal("50"), Decimal("100")):
            for lev in (1, 5):
                for side in ("BUY", "SELL"):
                    pl = plan_usdm_isolated_order(
                        spec=ls,
                        brackets=brackets,
                        leverage=lev,
                        side=side,
                        order_type="MARKET",
                        ref_price=mark_px,
                        free_collateral_usdt=capital,
                        loss_budget_usdt=Decimal("2.00"),
                        stop_distance_pct=Decimal("0.01"),
                        round_trip_fee_rate=Decimal("0.001"),
                        adverse_slippage_rate=Decimal("0.0005"),
                        taker_fee_rate=Decimal("0.0005"),
                    )
                    plans[f"{capital}usdt_{lev}x_{side}"] = {
                        "accepted": pl.accepted,
                        "reject_reason": pl.reject_reason,
                        "qty": str(pl.qty),
                        "notional_usdt": str(pl.notional_usdt),
                        "initial_margin_usdt": str(pl.initial_margin_usdt),
                        "maint_margin_usdt": str(pl.maint_margin_usdt),
                    }
        per_symbol[sym] = {
            "demo": {
                "tick_size": str(ds.tick_size),
                "step_size": str(ds.step_size),
                "market_step_size": str(ds.market_step_size),
                "min_notional": str(ds.min_notional),
                "margin_init": str(ds.margin_init),
                "margin_maint": str(ds.margin_maint),
                "mark_price": str(demo_prem[sym]["markPrice"]),
                "funding_rate": str(demo_prem[sym]["lastFundingRate"]),
                "next_funding_time_ms": int(demo_prem[sym]["nextFundingTime"]),
                "funding_interval_hours": demo_fi.get(sym, {}).get("fundingIntervalHours"),
            },
            "live": {
                "tick_size": str(ls.tick_size),
                "step_size": str(ls.step_size),
                "market_step_size": str(ls.market_step_size),
                "min_notional": str(ls.min_notional),
                "margin_init": str(ls.margin_init),
                "margin_maint": str(ls.margin_maint),
                "mark_price": str(mark_px),
                "funding_rate": str(live_prem[sym]["lastFundingRate"]),
            },
            "demo_vs_live_diff": {
                "step_size_diff": str(ds.step_size) != str(ls.step_size),
                "tick_size_diff": str(ds.tick_size) != str(ls.tick_size),
                "min_notional_diff": str(ds.min_notional) != str(ls.min_notional),
            },
            "live_sizing_50_100": plans,
        }

    return {
        "suite": "public-specs",
        "ts_utc_ns": time.time_ns(),
        "per_symbol": per_symbol,
        "status": "PASSED",
    }


_PINNED_SOLUSDT_EXCHANGE_INFO = {
    "symbol": "SOLUSDT",
    "contractType": "PERPETUAL",
    "status": "TRADING",
    "baseAsset": "SOL",
    "quoteAsset": "USDT",
    "marginAsset": "USDT",
    "maintMarginPercent": "2.5000",
    "requiredMarginPercent": "5.0000",
    "filters": [
        {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "1000000", "stepSize": "0.01"},
        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.01", "maxQty": "6000", "stepSize": "0.01"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
    ],
}


def run_accounting_parity_suite() -> dict[str, Any]:
    """Run strictly offline Decimal isolated ledger vs Nautilus BacktestEngine parity checks."""
    from nautilus_trader.model import AggressorSide, FundingRateUpdate, InstrumentId, MarkPriceUpdate, Price, Quantity, QuoteTick, TradeId, TradeTick

    raw_spec_bytes = json.dumps(_PINNED_SOLUSDT_EXCHANGE_INFO, sort_keys=True, separators=(",", ":")).encode("utf-8")
    spec_sha256 = hashlib.sha256(raw_spec_bytes).hexdigest()
    sol_spec = spec_from_exchange_info_entry(
        _PINNED_SOLUSDT_EXCHANGE_INFO,
        market=Market.USDM,
        observe_time_utc_ns=1_700_000_000_000_000_000,
        source_time_ms=1_700_000_000_000,
        source="pinned_offline_solusdt_fixture",
    )
    sol_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    t0 = 1_700_000_000_000_000_000
    t_b1 = t0 + 10_000_000_000

    # 1a. MARK_PRICE trigger vs visible L1 book fill separation (LONG):
    # Book moves to 146.80/146.90, then MarkPrice=146.90 crosses stop trigger (147.00).
    # Independent expected exit fill MUST be L1 best bid 146.80 (NOT mark 146.90) -> 48.94748000 USDT!
    expected_mark_long_exit_px = Decimal("146.80")
    expected_mark_long_final = Decimal("48.94748000")
    data_mark_long = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        FundingRateUpdate(sol_id, Decimal("0.001"), t0 + 2_000_000, t0 + 2_000_000, interval=None, next_funding_ns=t_b1),
        MarkPriceUpdate(sol_id, Price.from_str("160.00"), t_b1 - 1_000_000, t_b1 - 1_000_000),
        QuoteTick(sol_id, Price.from_str("146.80"), Price.from_str("146.90"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t_b1 + 1_000_000_000, t_b1 + 1_000_000_000),
        MarkPriceUpdate(sol_id, Price.from_str("146.90"), t_b1 + 1_000_000_100, t_b1 + 1_000_000_100),
    ]
    bt_mark_long = run_usdm_backtest(
        spec=sol_spec,
        data=data_mark_long,
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
    hand_mark_long = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    hand_mark_long.apply_fill(ts_ns=t0, order_id="e1", side="BUY", qty=Decimal("0.30"), price=Decimal("150.00"), fee_amount=Decimal("0.0225"))
    hand_mark_long.settle_funding(funding_time_ns=t_b1, settled_rate=Decimal("0.001"), settlement_mark_price=Decimal("160.00"), visible_utc_ns=t_b1)
    hand_mark_long.apply_fill(
        ts_ns=t_b1 + 1_000_000_100,
        order_id="e2",
        side="SELL",
        qty=Decimal("0.30"),
        price=expected_mark_long_exit_px,
        fee_amount=Decimal("0.30") * expected_mark_long_exit_px * Decimal("0.0005"),
    )
    mark_long_diff = abs(Decimal(bt_mark_long["final_total_usdt"]) - hand_mark_long.cross_wallet_usdt)

    # 1b. Short-side MARK_PRICE protection separation:
    # Sell 0.30 @ 150.00 -> Book 153.10/153.20 -> Mark 153.10 triggers BUY stop (153.00) -> fills at L1 ask 153.20!
    expected_mark_short_exit_px = Decimal("153.20")
    data_mark_short = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        QuoteTick(sol_id, Price.from_str("153.10"), Price.from_str("153.20"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
        MarkPriceUpdate(sol_id, Price.from_str("153.10"), t0 + 2_000_000_100, t0 + 2_000_000_100),
    ]
    bt_mark_short = run_usdm_backtest(
        spec=sol_spec,
        data=data_mark_short,
        strategy_config=UsdmPerpAuditStrategyConfig(
            instrument_id=sol_id,
            allowed_symbol="SOLUSDT",
            leverage=5,
            order_qty="0.30",
            side="SELL",
            protection_mode="quantity_reduce_only",
            working_type="MARK_PRICE",
            stop_distance_pct="0.02",
        ),
        starting_usdt=Decimal("50"),
        leverage=5,
    )
    hand_mark_short = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    hand_mark_short.apply_fill(ts_ns=t0, order_id="s1", side="SELL", qty=Decimal("0.30"), price=Decimal("150.00"), fee_amount=Decimal("0.0225"))
    hand_mark_short.apply_fill(
        ts_ns=t0 + 2_000_000_100,
        order_id="s2",
        side="BUY",
        qty=Decimal("0.30"),
        price=expected_mark_short_exit_px,
        fee_amount=Decimal("0.30") * expected_mark_short_exit_px * Decimal("0.0005"),
    )
    mark_short_diff = abs(Decimal(bt_mark_short["final_total_usdt"]) - hand_mark_short.cross_wallet_usdt)

    # 1c. CONTRACT_PRICE TradeTick trigger parity (TradeTick updates simulated L1 quote in L1_MBP -> expected 146.90)
    expected_contract_exit_px = Decimal("146.90")
    data_prot = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        FundingRateUpdate(sol_id, Decimal("0.001"), t0 + 2_000_000, t0 + 2_000_000, interval=None, next_funding_ns=t_b1),
        MarkPriceUpdate(sol_id, Price.from_str("160.00"), t_b1 - 1_000_000, t_b1 - 1_000_000),
        QuoteTick(sol_id, Price.from_str("146.80"), Price.from_str("146.90"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t_b1 + 1_000_000_000, t_b1 + 1_000_000_000),
        TradeTick(sol_id, Price.from_str("146.90"), Quantity.from_str("1.00"), AggressorSide.SELL, TradeId("T-1"), t_b1 + 1_000_000_100, t_b1 + 1_000_000_100),
    ]
    bt_prot = run_usdm_backtest(
        spec=sol_spec,
        data=data_prot,
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
    hand = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("50"), leverage=5)
    hand.apply_fill(ts_ns=t0, order_id="e1", side="BUY", qty=Decimal("0.30"), price=Decimal("150.00"), fee_amount=Decimal("0.0225"))
    hand.settle_funding(funding_time_ns=t_b1, settled_rate=Decimal("0.001"), settlement_mark_price=Decimal("160.00"), visible_utc_ns=t_b1)
    hand.apply_fill(
        ts_ns=t_b1 + 1_000_000_100,
        order_id="e2",
        side="SELL",
        qty=Decimal("0.30"),
        price=expected_contract_exit_px,
        fee_amount=Decimal("0.30") * expected_contract_exit_px * Decimal("0.0005"),
    )
    prot_diff = abs(Decimal(bt_prot["final_total_usdt"]) - hand.cross_wallet_usdt)

    # 2. All-in 50 USDT 5x Isolated Liquidation Jump-Through-Protection Parity (strictly pre & post snapshot!)
    data_liq = [
        QuoteTick(sol_id, Price.from_str("150.00"), Price.from_str("150.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0, t0),
        MarkPriceUpdate(sol_id, Price.from_str("150.00"), t0 + 1_000_000, t0 + 1_000_000),
        QuoteTick(sol_id, Price.from_str("100.00"), Price.from_str("100.00"), Quantity.from_str("100.00"), Quantity.from_str("100.00"), t0 + 2_000_000_000, t0 + 2_000_000_000),
        MarkPriceUpdate(sol_id, Price.from_str("100.00"), t0 + 2_000_000_100, t0 + 2_000_000_100),
    ]
    bt_liq = run_usdm_backtest(
        spec=sol_spec,
        data=data_liq,
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
    hand_liq = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("0"), isolated_wallet_usdt=Decimal("50"), leverage=5, auto_allocate_isolated=False)
    hand_liq.apply_fill(ts_ns=t0, order_id="l1", side="BUY", qty=Decimal("1.00"), price=Decimal("150.00"), fee_amount=Decimal("0.075"))
    liq_res = hand_liq.evaluate_mark_and_liquidation(ts_ns=t0 + 2_000_000_000, mark_price=Decimal("100.00"), protection_trigger_price=Decimal("147.00"))

    # 3. Partial-capital isolated comparison (20 USDT isolated inside 50 USDT total account)
    hand_partial_iso = IsolatedUsdmLedger(symbol="SOLUSDT", cross_wallet_usdt=Decimal("30"), isolated_wallet_usdt=Decimal("20"), leverage=5, auto_allocate_isolated=False)
    hand_partial_iso.apply_fill(ts_ns=t0, order_id="p1", side="BUY", qty=Decimal("1.00"), price=Decimal("100.00"), fee_amount=Decimal("0.05"))
    liq_partial = hand_partial_iso.evaluate_mark_and_liquidation(ts_ns=t0 + 1_000, mark_price=Decimal("78.00"), protection_trigger_price=Decimal("90.00"))

    # 4. Equal-qty reversal protection parity (quantity_reduce_only & close_position)
    rev_results: dict[str, Any] = {}
    for pmode in ("quantity_reduce_only", "close_position"):
        bt_rev = run_usdm_backtest(
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
        rev_results[pmode] = {
            "net_pos": bt_rev["net_pos"],
            "fills_count": len(bt_rev["fills"]),
            "last_fill_side": bt_rev["fills"][-1]["side"] if bt_rev["fills"] else None,
            "last_fill_px": bt_rev["fills"][-1]["px"] if bt_rev["fills"] else None,
            "ok": Decimal(bt_rev["net_pos"]) == 0 and len(bt_rev["fills"]) == 3 and bt_rev["fills"][-1]["side"] == "BUY",
        }

    checks = {
        "mark_long_fill_at_bid": (
            len(bt_mark_long["fills"]) == 2
            and Decimal(bt_mark_long["fills"][1]["px"]) == expected_mark_long_exit_px
            and Decimal(bt_mark_long["final_total_usdt"]) == expected_mark_long_final
            and mark_long_diff == 0
        ),
        "mark_short_fill_at_ask": (
            len(bt_mark_short["fills"]) == 2
            and Decimal(bt_mark_short["fills"][1]["px"]) == expected_mark_short_exit_px
            and mark_short_diff == 0
        ),
        "contract_trade_trigger": (
            len(bt_prot["fills"]) == 2
            and Decimal(bt_prot["fills"][1]["px"]) == expected_contract_exit_px
            and prot_diff == 0
        ),
        "liquidation_snapshot_consistent": (
            liq_res.triggered
            and liq_res.jumped_through_protection
            and liq_res.position_qty == Decimal("1.00")
            and liq_res.isolated_wallet_usdt == Decimal("49.925")
            and liq_res.post_position_qty == Decimal(0)
            and liq_res.post_isolated_wallet_usdt == Decimal(0)
            and hand_partial_iso.cross_wallet_usdt == Decimal("30")
            and liq_partial.triggered
        ),
        "reversal_protection": all(v["ok"] for v in rev_results.values()),
    }

    return {
        "suite": "accounting-parity",
        "offline_spec_fixture_sha256": spec_sha256,
        "mark_trigger_vs_book_fill_long": {
            "expected_exit_px": str(expected_mark_long_exit_px),
            "observed_exit_px": bt_mark_long["fills"][1]["px"] if len(bt_mark_long["fills"]) >= 2 else None,
            "nautilus_final_usdt": bt_mark_long["final_total_usdt"],
            "hand_ledger_final_usdt": str(hand_mark_long.cross_wallet_usdt),
            "abs_diff_usdt": str(mark_long_diff),
        },
        "mark_trigger_vs_book_fill_short": {
            "expected_exit_px": str(expected_mark_short_exit_px),
            "observed_exit_px": bt_mark_short["fills"][1]["px"] if len(bt_mark_short["fills"]) >= 2 else None,
            "nautilus_final_usdt": bt_mark_short["final_total_usdt"],
            "hand_ledger_final_usdt": str(hand_mark_short.cross_wallet_usdt),
            "abs_diff_usdt": str(mark_short_diff),
        },
        "funding_and_stop_protection": {
            "expected_exit_px": str(expected_contract_exit_px),
            "nautilus_final_usdt": bt_prot["final_total_usdt"],
            "hand_ledger_final_usdt": str(hand.cross_wallet_usdt),
            "abs_diff_usdt": str(prot_diff),
            "exact_match": prot_diff == 0,
            "fills": bt_prot["fills"],
        },
        "all_in_gap_liquidation": {
            "nautilus_final_usdt": bt_liq["final_total_usdt"],
            "nautilus_fills": bt_liq["fills"],
            "isolated_ledger_triggered": liq_res.triggered,
            "jumped_through_protection": liq_res.jumped_through_protection,
            "pre_liq_position_qty": str(liq_res.position_qty),
            "pre_liq_isolated_wallet_usdt": str(liq_res.isolated_wallet_usdt),
            "post_liq_position_qty": str(liq_res.post_position_qty),
            "post_liq_isolated_wallet_usdt": str(liq_res.post_isolated_wallet_usdt),
            "liquidation_price": str(liq_res.liquidation_price),
            "bankruptcy_price": str(liq_res.bankruptcy_price),
            "sensitivity_band_usdt": liq_res.sensitivity_band_usdt,
            "gap_deficit_usdt": str(liq_res.gap_deficit_usdt),
            "isolated_note": "Nautilus MarginAccount lets wallet go negative (-0.125) on gap; strict Binance Isolated floors WB_iso at 0 (insurance fund absorbs gap).",
        },
        "partial_capital_isolated_envelope": {
            "cross_wallet_preserved_usdt": str(hand_partial_iso.cross_wallet_usdt),
            "isolated_wallet_after_liq_usdt": str(hand_partial_iso.isolated_wallet_usdt),
            "triggered": liq_partial.triggered,
            "sensitivity_band_usdt": liq_partial.sensitivity_band_usdt,
            "boundary_note": "Out-of-scope multi-symbol cross/partial isolated sharing remains disabled until per-position isolated collateral is modeled natively.",
        },
        "reversal_protection": rev_results,
        "checks": checks,
        "status": "PASSED" if all(checks.values()) else "FAILED",
    }


class _MockUsdmDemoServer:
    """Local mock Binance USD-M HTTP + WebSocket Demo server exercising real Nautilus Rust execution."""

    def __init__(self) -> None:
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0
        self.user_ws_writer: asyncio.StreamWriter | None = None
        self.public_ws_writer: asyncio.StreamWriter | None = None
        self.user_ws_ready = asyncio.Event()
        self.quote_ws_ready = asyncio.Event()
        self.wire_audit: list[dict[str, Any]] = []
        self.next_order_id = 10_000
        self.next_algo_id = 90_000
        self.next_trade_id = 50_000
        self.regular_orders: dict[int, dict[str, Any]] = {}
        self.algo_orders: dict[int, dict[str, Any]] = {}
        self.trades: list[dict[str, Any]] = []
        self.position_qty = Decimal(0)
        self.entry_price = Decimal(0)
        self.wallet_usdt = Decimal("100.00000000")
        self.leverage = 5
        self.margin_type = "CROSSED"
        self.inject_timeout_next_regular = False
        self.partial_first_entry = True
        self._entry_count = 0
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle_conn, "127.0.0.1", 0)
        self.port = int(self.server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        if self.user_ws_writer is not None:
            self.user_ws_writer.close()
        if self.public_ws_writer is not None:
            self.public_ws_writer.close()
        if self.server is not None:
            self.server.close()

    async def _send_ws(self, writer: asyncio.StreamWriter | None, obj: dict[str, Any]) -> None:
        if writer is None:
            return
        async with self._write_lock:
            writer.write(_ws_encode_text(json.dumps(obj)))
            await writer.drain()

    async def send_quote_and_mark(self, bid: str, ask: str, mark: str) -> None:
        now_ms = int(time.time() * 1000)
        await self._send_ws(
            self.public_ws_writer,
            {
                "e": "bookTicker",
                "u": int(time.time() * 1000),
                "E": now_ms,
                "T": now_ms - 1,
                "s": "SOLUSDT",
                "b": bid,
                "B": "50.00",
                "a": ask,
                "A": "50.00",
            },
        )
        await self._send_ws(
            self.public_ws_writer,
            {
                "e": "markPriceUpdate",
                "E": now_ms,
                "s": "SOLUSDT",
                "p": mark,
                "i": mark,
                "P": mark,
                "r": "0.00010000",
                "T": (now_ms // 28800000 + 1) * 28800000,
            },
        )

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            try:
                raw_hdr = await reader.readuntil(b"\r\n\r\n")
            except Exception:
                writer.close()
                return
            lines = raw_hdr.decode("latin1").split("\r\n")
            req_parts = lines[0].split(" ")
            method = req_parts[0]
            target = req_parts[1] if len(req_parts) >= 2 else "/"
            headers: dict[str, str] = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if headers.get("upgrade", "").lower() == "websocket":
                key = headers["sec-websocket-key"]
                accept = base64.b64encode(hashlib.sha1((key + WS_MAGIC).encode("ascii")).digest()).decode("ascii")
                writer.write(
                    (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\n"
                        "Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                    ).encode("ascii")
                )
                await writer.drain()
                await self._ws_loop(target, reader, writer)
                return

            content_len = int(headers.get("content-length", "0") or "0")
            raw_body = (await reader.readexactly(content_len)).decode("utf-8") if content_len > 0 else ""
            parsed = urlparse(target)
            path = parsed.path
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            if raw_body:
                for k, v in parse_qs(raw_body).items():
                    params[k] = v[0]

            status, resp_obj = await self._dispatch_http(method, path, params)
            resp_str = json.dumps(resp_obj)
            reason = "OK" if status == 200 else "Gateway Timeout" if status == 504 else "Bad Request"
            writer.write(
                (
                    f"HTTP/1.1 {status} {reason}\r\n"
                    "Content-Type: application/json\r\n"
                    f"Content-Length: {len(resp_str)}\r\n"
                    "Connection: keep-alive\r\n\r\n"
                    f"{resp_str}"
                ).encode("utf-8")
            )
            await writer.drain()

    async def _ws_loop(self, target: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        is_user = "listenKey=" in target
        if is_user:
            self.user_ws_writer = writer
            self.user_ws_ready.set()
        else:
            self.public_ws_writer = writer
        try:
            while True:
                frame = await _ws_read_frame(reader)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    writer.write(b"\x8a\x00")
                    await writer.drain()
                    continue
                if opcode == 0x1:
                    msg = json.loads(payload.decode("utf-8"))
                    if msg.get("method") == "SUBSCRIBE":
                        writer.write(_ws_encode_text(json.dumps({"result": None, "id": msg.get("id")})))
                        await writer.drain()
                        self.quote_ws_ready.set()
        finally:
            if is_user and self.user_ws_writer is writer:
                self.user_ws_writer = None
            elif self.public_ws_writer is writer:
                self.public_ws_writer = None
            writer.close()

    async def _emit_order_trade_ws(self, order: dict[str, Any], *, exec_type: str, last_qty: str, last_px: str, fee: str, trade_id: int = 0) -> None:
        now_ms = int(time.time() * 1000)
        await self._send_ws(
            self.user_ws_writer,
            {
                "e": "ORDER_TRADE_UPDATE",
                "T": now_ms,
                "E": now_ms,
                "o": {
                    "s": order["symbol"],
                    "c": order["clientOrderId"],
                    "S": order["side"],
                    "o": order["type"],
                    "f": order["timeInForce"],
                    "q": order["origQty"],
                    "p": order["price"],
                    "ap": order["avgPrice"],
                    "sp": order["stopPrice"],
                    "x": exec_type,
                    "X": order["status"],
                    "i": order["orderId"],
                    "l": last_qty,
                    "z": order["executedQty"],
                    "L": last_px,
                    "N": "USDT",
                    "n": fee,
                    "T": now_ms,
                    "t": trade_id,
                    "b": "0",
                    "a": "0",
                    "m": False,
                    "R": bool(order["reduceOnly"]),
                    "wt": order["workingType"],
                    "ot": order["origType"],
                    "ps": "BOTH",
                    "cp": False,
                    "pP": False,
                    "rp": "0",
                },
            },
        )

    async def _emit_algo_ws(self, algo: dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        await self._send_ws(
            self.user_ws_writer,
            {
                "e": "ALGO_UPDATE",
                "T": now_ms,
                "E": now_ms,
                "o": {
                    "caid": algo["clientAlgoId"],
                    "aid": algo["algoId"],
                    "at": "CONDITIONAL",
                    "o": algo["orderType"],
                    "s": algo["symbol"],
                    "S": algo["side"],
                    "ps": "BOTH",
                    "f": algo.get("timeInForce", "GTC"),
                    "q": algo.get("quantity", "0"),
                    "X": algo["algoStatus"],
                    "ai": algo.get("actualOrderId", ""),
                    "ap": algo.get("actualPrice", "0.00"),
                    "aq": algo.get("actualQty", "0.00"),
                    "act": algo.get("actualType", "0"),
                    "tp": algo["triggerPrice"],
                    "p": algo.get("price", "0.00"),
                    "wt": algo["workingType"],
                    "pm": "NONE",
                    "cp": bool(algo.get("closePosition", False)),
                    "pP": False,
                    "R": bool(algo.get("reduceOnly", False)),
                    "tt": algo.get("triggerTime", 0),
                    "gtd": 0,
                },
            },
        )

    async def trigger_algo_order(self, algo_id: int, fill_px: str = "147.00") -> None:
        algo = self.algo_orders[algo_id]
        now_ms = int(time.time() * 1000)
        self.next_order_id += 1
        child_oid = self.next_order_id
        self.next_trade_id += 1
        tid = self.next_trade_id
        qty_dec = abs(self.position_qty) if algo.get("closePosition") else Decimal(str(algo["quantity"]))
        qty_str = f"{qty_dec:.2f}"
        px_dec = Decimal(fill_px)
        fee_dec = qty_dec * px_dec * Decimal("0.0005")
        self.wallet_usdt -= fee_dec
        delta = qty_dec if algo["side"] == "BUY" else -qty_dec
        self.position_qty += delta
        if self.position_qty == 0:
            self.entry_price = Decimal(0)

        algo["algoStatus"] = "FINISHED"
        algo["actualOrderId"] = str(child_oid)
        algo["actualPrice"] = fill_px
        algo["actualType"] = "MARKET"
        algo["actualQty"] = qty_str
        algo["triggerTime"] = now_ms
        algo["updateTime"] = now_ms
        await self._emit_algo_ws(algo)

        child_order = {
            "orderId": child_oid,
            "symbol": algo["symbol"],
            "status": "FILLED",
            "clientOrderId": f"child-{child_oid}",
            "price": "0.00",
            "avgPrice": fill_px,
            "origQty": qty_str,
            "executedQty": qty_str,
            "cumQuote": str(qty_dec * px_dec),
            "timeInForce": "GTC",
            "type": "MARKET",
            "reduceOnly": True,
            "closePosition": False,
            "side": algo["side"],
            "positionSide": "BOTH",
            "stopPrice": "0.00",
            "workingType": algo["workingType"],
            "priceProtect": False,
            "origType": algo["orderType"],
            "time": now_ms,
            "updateTime": now_ms,
        }
        self.regular_orders[child_oid] = child_order
        self.trades.append(
            {
                "symbol": algo["symbol"],
                "id": tid,
                "orderId": child_oid,
                "price": fill_px,
                "qty": qty_str,
                "quoteQty": str(qty_dec * px_dec),
                "realizedPnl": "0",
                "side": algo["side"],
                "positionSide": "BOTH",
                "time": now_ms,
                "buyer": algo["side"] == "BUY",
                "maker": False,
                "commission": f"{fee_dec:.8f}",
                "commissionAsset": "USDT",
                "marginAsset": "USDT",
            }
        )
        await self._emit_order_trade_ws(child_order, exec_type="TRADE", last_qty=qty_str, last_px=fill_px, fee=f"{fee_dec:.8f}", trade_id=tid)

    async def _dispatch_http(self, method: str, path: str, params: dict[str, Any]) -> tuple[int, Any]:
        clean_params = redact_sensitive({k: v for k, v in params.items() if k != "timestamp"})
        self.wire_audit.append({"method": method, "path": path, "params": clean_params, "ts_ns": time.time_ns()})
        now_ms = int(time.time() * 1000)

        if path == "/fapi/v1/exchangeInfo":
            return 200, {
                "timezone": "UTC",
                "serverTime": now_ms,
                "futuresType": "U_MARGINED",
                "rateLimits": [],
                "exchangeFilters": [],
                "assets": [{"asset": "USDT", "marginAvailable": True, "autoAssetExchange": "-100"}],
                "symbols": [
                    {
                        "symbol": "SOLUSDT",
                        "pair": "SOLUSDT",
                        "contractType": "PERPETUAL",
                        "deliveryDate": 4133404800000,
                        "onboardDate": 1569398400000,
                        "status": "TRADING",
                        "maintMarginPercent": "2.5000",
                        "requiredMarginPercent": "5.0000",
                        "baseAsset": "SOL",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "pricePrecision": 2,
                        "quantityPrecision": 2,
                        "baseAssetPrecision": 8,
                        "quotePrecision": 8,
                        "filters": [
                            {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "10000", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "1000000", "stepSize": "0.01"},
                            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.01", "maxQty": "6000", "stepSize": "0.01"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                        "orderTypes": ["LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"],
                        "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
                    }
                ],
            }
        if path == "/fapi/v1/time":
            return 200, {"serverTime": now_ms}
        if path == "/fapi/v1/positionSide/dual":
            return 200, {"dualSidePosition": False}
        if path == "/fapi/v1/leverage":
            self.leverage = int(params.get("leverage", 5))
            return 200, {"leverage": self.leverage, "maxNotionalValue": "100000", "symbol": params.get("symbol", "SOLUSDT")}
        if path == "/fapi/v1/marginType":
            self.margin_type = str(params.get("marginType", "ISOLATED"))
            return 200, {"code": 200, "msg": "success"}
        if path == "/fapi/v1/listenKey":
            return 200, {"listenKey": "mock-demo-listen-key-1"} if method == "POST" else {}
        if path == "/fapi/v2/account":
            return 200, {
                "feeTier": 0,
                "canTrade": True,
                "canDeposit": True,
                "canWithdraw": True,
                "updateTime": now_ms,
                "multiAssetsMargin": False,
                "totalInitialMargin": "0.00000000",
                "totalMaintMargin": "0.00000000",
                "totalWalletBalance": f"{self.wallet_usdt:.8f}",
                "totalUnrealizedProfit": "0.00000000",
                "totalMarginBalance": f"{self.wallet_usdt:.8f}",
                "totalCrossWalletBalance": f"{self.wallet_usdt:.8f}",
                "totalCrossUnPnl": "0.00000000",
                "availableBalance": f"{self.wallet_usdt:.8f}",
                "maxWithdrawAmount": f"{self.wallet_usdt:.8f}",
                "assets": [
                    {
                        "asset": "USDT",
                        "walletBalance": f"{self.wallet_usdt:.8f}",
                        "unrealizedProfit": "0.00000000",
                        "marginBalance": f"{self.wallet_usdt:.8f}",
                        "maintMargin": "0.00000000",
                        "initialMargin": "0.00000000",
                        "crossWalletBalance": f"{self.wallet_usdt:.8f}",
                        "crossUnPnl": "0.00000000",
                        "availableBalance": f"{self.wallet_usdt:.8f}",
                        "maxWithdrawAmount": f"{self.wallet_usdt:.8f}",
                        "marginAvailable": True,
                        "updateTime": now_ms,
                    }
                ],
                "positions": [],
            }
        if path == "/fapi/v2/positionRisk":
            pos_str = f"{self.position_qty:.2f}" if self.position_qty != 0 else "0"
            return 200, [
                {
                    "symbol": "SOLUSDT",
                    "positionAmt": pos_str,
                    "entryPrice": f"{self.entry_price:.2f}",
                    "markPrice": "150.00",
                    "unRealizedProfit": "0.00000000",
                    "liquidationPrice": "120.00",
                    "leverage": str(self.leverage),
                    "maxNotionalValue": "100000",
                    "marginType": self.margin_type.lower(),
                    "isolatedMargin": "6.00000000" if self.position_qty != 0 else "0.00000000",
                    "isAutoAddMargin": "false",
                    "positionSide": "BOTH",
                    "notional": str(abs(self.position_qty) * Decimal("150.00")),
                    "isolatedWallet": "6.00000000" if self.position_qty != 0 else "0",
                    "updateTime": now_ms,
                }
            ]
        if path == "/fapi/v1/openOrders":
            return 200, [o for o in self.regular_orders.values() if o["status"] in ("NEW", "PARTIALLY_FILLED")]
        if path == "/fapi/v1/allOrders":
            return 200, list(self.regular_orders.values())
        if path == "/fapi/v1/openAlgoOrders":
            return 200, [a for a in self.algo_orders.values() if a["algoStatus"] == "NEW"]
        if path == "/fapi/v1/allAlgoOrders":
            return 200, list(self.algo_orders.values())
        if path == "/fapi/v1/userTrades":
            return 200, self.trades
        if path == "/fapi/v1/order":
            if method == "POST":
                self.next_order_id += 1
                oid = self.next_order_id
                cid = str(params["newClientOrderId"])
                otype = str(params["type"])
                qty_s = str(params.get("quantity", "0.20"))
                if otype == "MARKET":
                    self._entry_count += 1
                    full_q = Decimal(qty_s)
                    fill_q = Decimal("0.10") if (self.partial_first_entry and self._entry_count == 1 and full_q >= Decimal("0.20")) else full_q
                    px_s = "150.00"
                    px_d = Decimal(px_s)
                    fee_d = fill_q * px_d * Decimal("0.0005")
                    self.wallet_usdt -= fee_d
                    delta = fill_q if params["side"] == "BUY" else -fill_q
                    self.position_qty += delta
                    self.entry_price = px_d if self.position_qty != 0 else Decimal(0)
                    status = "PARTIALLY_FILLED" if fill_q < full_q else "FILLED"
                    order = {
                        "orderId": oid,
                        "symbol": params["symbol"],
                        "status": status,
                        "clientOrderId": cid,
                        "price": "0.00",
                        "avgPrice": px_s,
                        "origQty": qty_s,
                        "executedQty": f"{fill_q:.2f}",
                        "cumQuote": str(fill_q * px_d),
                        "timeInForce": "GTC",
                        "type": "MARKET",
                        "reduceOnly": params.get("reduceOnly") == "true",
                        "closePosition": False,
                        "side": params["side"],
                        "positionSide": "BOTH",
                        "stopPrice": "0.00",
                        "workingType": "CONTRACT_PRICE",
                        "priceProtect": False,
                        "origType": "MARKET",
                        "time": now_ms,
                        "updateTime": now_ms,
                    }
                    self.regular_orders[oid] = order
                    self.next_trade_id += 1
                    tid = self.next_trade_id
                    self.trades.append(
                        {
                            "symbol": params["symbol"],
                            "id": tid,
                            "orderId": oid,
                            "price": px_s,
                            "qty": f"{fill_q:.2f}",
                            "quoteQty": str(fill_q * px_d),
                            "realizedPnl": "0",
                            "side": params["side"],
                            "positionSide": "BOTH",
                            "time": now_ms,
                            "buyer": params["side"] == "BUY",
                            "maker": False,
                            "commission": f"{fee_d:.8f}",
                            "commissionAsset": "USDT",
                            "marginAsset": "USDT",
                        }
                    )
                    if self.inject_timeout_next_regular:
                        self.inject_timeout_next_regular = False
                        # Simulate dropped ACK (504) while order actually executed!
                        return 504, {"code": -1007, "msg": "Timeout waiting for response from backend server"}
                    asyncio.create_task(self._emit_order_trade_ws(order, exec_type="NEW", last_qty="0", last_px="0", fee="0"))
                    asyncio.create_task(self._emit_order_trade_ws(order, exec_type="TRADE", last_qty=f"{fill_q:.2f}", last_px=px_s, fee=f"{fee_dec:.8f}" if False else f"{fee_d:.8f}", trade_id=tid))
                    return 200, order
                # LIMIT order resting on book
                order = {
                    "orderId": oid,
                    "symbol": params["symbol"],
                    "status": "NEW",
                    "clientOrderId": cid,
                    "price": str(params.get("price", "140.00")),
                    "avgPrice": "0.00",
                    "origQty": qty_s,
                    "executedQty": "0.00",
                    "cumQuote": "0.00",
                    "timeInForce": str(params.get("timeInForce", "GTC")),
                    "type": otype,
                    "reduceOnly": params.get("reduceOnly") == "true",
                    "closePosition": False,
                    "side": params["side"],
                    "positionSide": "BOTH",
                    "stopPrice": "0.00",
                    "workingType": "CONTRACT_PRICE",
                    "priceProtect": False,
                    "origType": otype,
                    "time": now_ms,
                    "updateTime": now_ms,
                }
                self.regular_orders[oid] = order
                asyncio.create_task(self._emit_order_trade_ws(order, exec_type="NEW", last_qty="0", last_px="0", fee="0"))
                return 200, order
            if method == "GET":
                cid = params.get("origClientOrderId")
                oid = int(params["orderId"]) if params.get("orderId") else None
                for o in self.regular_orders.values():
                    if (oid and o["orderId"] == oid) or (cid and o["clientOrderId"] == cid):
                        return 200, o
                return 400, {"code": -2013, "msg": "Order does not exist."}
            if method == "DELETE":
                cid = params.get("origClientOrderId")
                oid = int(params["orderId"]) if params.get("orderId") else None
                for o in self.regular_orders.values():
                    if (oid and o["orderId"] == oid) or (cid and o["clientOrderId"] == cid):
                        o["status"] = "CANCELED"
                        o["updateTime"] = now_ms
                        asyncio.create_task(self._emit_order_trade_ws(o, exec_type="CANCELED", last_qty="0", last_px="0", fee="0"))
                        return 200, o
                return 400, {"code": -2011, "msg": "Unknown order sent."}
        if path == "/fapi/v1/algoOrder":
            if method == "POST":
                # Strict Binance rule: closePosition=true MUST NOT send quantity or reduceOnly!
                is_cp = params.get("closePosition") == "true"
                if is_cp and ("quantity" in params or "reduceOnly" in params):
                    return 400, {"code": -1106, "msg": "Parameter quantity/reduceOnly not required when closePosition=true"}
                self.next_algo_id += 1
                aid = self.next_algo_id
                algo = {
                    "algoId": aid,
                    "clientAlgoId": str(params.get("clientAlgoId", f"algo-{aid}")),
                    "algoType": "CONDITIONAL",
                    "orderType": str(params["type"]),
                    "symbol": str(params["symbol"]),
                    "side": str(params["side"]),
                    "positionSide": "BOTH",
                    "timeInForce": str(params.get("timeInForce", "GTC")),
                    "quantity": "0" if is_cp else str(params["quantity"]),
                    "algoStatus": "NEW",
                    "actualOrderId": "",
                    "actualPrice": "0.00",
                    "actualType": "0",
                    "actualQty": "0.00",
                    "triggerPrice": str(params["triggerPrice"]),
                    "price": str(params.get("price", "0.00")),
                    "workingType": str(params.get("workingType", "CONTRACT_PRICE")),
                    "priceMatch": "NONE",
                    "closePosition": is_cp,
                    "priceProtect": False,
                    "reduceOnly": False if is_cp else (params.get("reduceOnly") == "true"),
                    "createTime": now_ms,
                    "updateTime": now_ms,
                    "triggerTime": 0,
                    "goodTillDate": 0,
                }
                self.algo_orders[aid] = algo
                asyncio.create_task(self._emit_algo_ws(algo))
                return 200, algo
            if method == "GET":
                caid = params.get("clientAlgoId")
                aid = int(params["algoId"]) if params.get("algoId") else None
                for a in self.algo_orders.values():
                    if (aid and a["algoId"] == aid) or (caid and a["clientAlgoId"] == caid):
                        return 200, a
                return 400, {"code": -2013, "msg": "Algo order does not exist."}
            if method == "DELETE":
                caid = params.get("clientAlgoId")
                aid = int(params["algoId"]) if params.get("algoId") else None
                for a in self.algo_orders.values():
                    if (aid and a["algoId"] == aid) or (caid and a["clientAlgoId"] == caid):
                        a["algoStatus"] = "CANCELED"
                        a["updateTime"] = now_ms
                        asyncio.create_task(self._emit_algo_ws(a))
                        return 200, a
                return 400, {"code": -2011, "msg": "Unknown algo order sent."}
        return 200, {}


async def _run_mock_demo_e2e_async() -> dict[str, Any]:
    from nautilus_trader.adapters.binance import BinanceDataClientFactory, BinanceExecutionClientFactory
    from nautilus_trader.common import Environment, LogLevel
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import InstrumentId, OrderSide, Price, Quantity, TraderId, TriggerType
    from nautilus_trader.trading import Strategy
    from nautilus_trader.config import LoggerConfig, StrategyConfig

    srv = _MockUsdmDemoServer()
    await srv.start()
    http_url = f"http://127.0.0.1:{srv.port}"
    ws_url = f"ws://127.0.0.1:{srv.port}/ws"
    sol_id = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")

    class _DemoDriverStrategy(Strategy):
        def __init__(self, cfg: StrategyConfig) -> None:
            super().__init__(cfg)
            self.quotes: list[Any] = []
            self.fills: list[dict[str, Any]] = []
            self.events: list[str] = []

        def on_start(self) -> None:
            self.subscribe_quotes(sol_id)

        def on_quote(self, q: Any) -> None:
            self.quotes.append(q)

        def on_order_denied(self, ev: Any) -> None:
            self.events.append(f"DENIED:{ev.client_order_id}:{ev.reason}")

        def on_order_accepted(self, ev: Any) -> None:
            self.events.append(f"ACCEPTED:{ev.client_order_id}:{ev.venue_order_id}")

        def on_order_canceled(self, ev: Any) -> None:
            self.events.append(f"CANCELED:{ev.client_order_id}")

        def on_order_filled(self, ev: Any) -> None:
            self.fills.append(
                {
                    "client_order_id": str(ev.client_order_id),
                    "venue_order_id": str(ev.venue_order_id),
                    "trade_id": str(ev.trade_id),
                    "qty": str(ev.last_qty),
                    "px": str(ev.last_px),
                }
            )
            self.events.append(f"FILLED:{ev.client_order_id}:{ev.last_qty}")

    async def _drive_node1() -> tuple[_DemoDriverStrategy, dict[str, Any]]:
        data_cfg, exec_cfg = build_binance_usdm_demo_configs(
            account_id="BINANCE-MOCK-DEMO",
            symbols=("SOLUSDT",),
            leverage=5,
            api_key="MOCK_DEMO_KEY",
            api_secret="MOCK_DEMO_SECRET",
            base_url_http=http_url,
            base_url_ws=ws_url,
            demo=True,
        )
        node = (
            LiveNode.builder("MNT3-MOCK-1", TraderId("MNT3-001"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_reconciliation(True)
            .with_reconciliation_lookback_mins(60)
            .with_timeout_connection(5)
            .with_timeout_reconciliation(5)
            .with_delay_post_stop_secs(1)
            .add_data_client("BINANCE", BinanceDataClientFactory(), data_cfg)
            .add_exec_client("BINANCE", BinanceExecutionClientFactory(), exec_cfg)
            .build()
        )
        strat = _DemoDriverStrategy(StrategyConfig())
        node.add_strategy(strat)
        handle1 = node.handle()
        cache1 = node.cache

        async def _script() -> dict[str, Any]:
            try:
                await asyncio.wait_for(srv.user_ws_ready.wait(), timeout=8.0)
                await asyncio.wait_for(srv.quote_ws_ready.wait(), timeout=8.0)
                await asyncio.sleep(0.2)
                # Push initial bookTicker + markPriceUpdate so RiskEngine has active market price
                await srv.send_quote_and_mark("150.00", "150.10", "150.05")
                for _ in range(40):
                    if len(strat.quotes) >= 1:
                        break
                    await asyncio.sleep(0.05)

                # Step 1: MARKET BUY 0.20 (mock fills 0.10 PARTIALLY_FILLED)
                entry_o = strat.order_factory.market(sol_id, OrderSide.BUY, Quantity.from_str("0.20"))
                strat.submit_order(entry_o)
                for _ in range(40):
                    if len(strat.fills) >= 1:
                        break
                    await asyncio.sleep(0.05)

                # Step 2: Arm quantity reduceOnly=True STOP_MARKET MARK_PRICE matching partial exposure 0.10
                stop_qty_o = strat.order_factory.stop_market(
                    sol_id,
                    OrderSide.SELL,
                    Quantity.from_str("0.10"),
                    Price.from_str("147.00"),
                    trigger_type=TriggerType.MARK_PRICE,
                    reduce_only=True,
                )
                strat.submit_order(stop_qty_o)
                for _ in range(40):
                    if any(a["algoStatus"] == "NEW" for a in srv.algo_orders.values()):
                        break
                    await asyncio.sleep(0.05)
                algo1_id = next(iter(srv.algo_orders.keys()))

                # Step 3: Cancel algo1 and submit closePosition=True STOP_MARKET CONTRACT_PRICE (verifying wire omits qty/reduceOnly)
                strat.cancel_order(stop_qty_o.client_order_id)
                for _ in range(40):
                    if srv.algo_orders[algo1_id]["algoStatus"] == "CANCELED":
                        break
                    await asyncio.sleep(0.05)

                stop_cp_o = strat.order_factory.stop_market(
                    sol_id,
                    OrderSide.SELL,
                    Quantity.from_str("0.10"),
                    Price.from_str("146.50"),
                    trigger_type=TriggerType.LAST_PRICE,
                    reduce_only=True,
                )
                strat.submit_order(stop_cp_o, params={"close_position": True})
                for _ in range(40):
                    if sum(1 for a in srv.algo_orders.values() if a["algoStatus"] == "NEW") == 1:
                        break
                    await asyncio.sleep(0.05)
                algo2_id = [aid for aid, a in srv.algo_orders.items() if a["algoStatus"] == "NEW"][0]

                # Step 4: Trigger algo2 -> child actualOrderId fills & closes 0.10 position
                await srv.trigger_algo_order(algo2_id, fill_px="146.50")
                for _ in range(40):
                    if srv.position_qty == 0 and any(a["algoStatus"] == "FINISHED" for a in srv.algo_orders.values()):
                        break
                    await asyncio.sleep(0.05)

                # Step 5: Simulate Dropped ACK (HTTP 504) on new BUY 0.20 entry, leaving position=0.20 + resting LIMIT + closePosition algo on exchange!
                srv.partial_first_entry = False
                srv.inject_timeout_next_regular = True
                dropped_o = strat.order_factory.market(sol_id, OrderSide.BUY, Quantity.from_str("0.20"))
                strat.submit_order(dropped_o)
                await asyncio.sleep(0.5)

                # Also arm a resting closePosition algo & resting LIMIT order for Node 2 startup reconciliation
                cp3 = strat.order_factory.stop_market(
                    sol_id,
                    OrderSide.SELL,
                    Quantity.from_str("0.20"),
                    Price.from_str("145.00"),
                    trigger_type=TriggerType.MARK_PRICE,
                    reduce_only=True,
                )
                strat.submit_order(cp3, params={"close_position": True})
                lim_o = strat.order_factory.limit(sol_id, OrderSide.BUY, Quantity.from_str("0.10"), Price.from_str("140.00"))
                strat.submit_order(lim_o)
                await asyncio.sleep(0.5)

                return {
                    "node1_fills": list(strat.fills),
                    "node1_events": list(strat.events),
                    "exchange_pos_before_restart": str(srv.position_qty),
                    "exchange_open_regular_before_restart": len([o for o in srv.regular_orders.values() if o["status"] == "NEW"]),
                    "exchange_open_algo_before_restart": len([a for a in srv.algo_orders.values() if a["algoStatus"] == "NEW"]),
                }
            finally:
                handle1.stop()

        runner = asyncio.create_task(_script())
        await asyncio.wait_for(node.run_async(), timeout=20.0)
        res1 = await runner
        node.dispose()
        return strat, res1

    # Start second LiveNode representing process restart with open regular order, closePosition algo, and 0.20 position!
    async def _drive_node2() -> dict[str, Any]:
        data_cfg, exec_cfg = build_binance_usdm_demo_configs(
            account_id="BINANCE-MOCK-DEMO",
            symbols=("SOLUSDT",),
            leverage=5,
            api_key="MOCK_DEMO_KEY",
            api_secret="MOCK_DEMO_SECRET",
            base_url_http=http_url,
            base_url_ws=ws_url,
            demo=True,
        )
        node2 = (
            LiveNode.builder("MNT3-MOCK-2", TraderId("MNT3-001"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_reconciliation(True)
            .with_reconciliation_lookback_mins(60)
            .with_timeout_connection(5)
            .with_timeout_reconciliation(5)
            .with_delay_post_stop_secs(1)
            .add_data_client("BINANCE", BinanceDataClientFactory(), data_cfg)
            .add_exec_client("BINANCE", BinanceExecutionClientFactory(), exec_cfg)
            .build()
        )
        strat2 = _DemoDriverStrategy(StrategyConfig())
        node2.add_strategy(strat2)
        handle2 = node2.handle()
        cache2 = node2.cache

        async def _check() -> dict[str, Any]:
            try:
                await asyncio.wait_for(srv.user_ws_ready.wait(), timeout=8.0)
                await asyncio.sleep(0.8)
                open_orders_reconciled = [
                    {
                        "client_order_id": str(o.client_order_id),
                        "order_type": str(o.order_type),
                        "qty": str(o.quantity),
                        "is_reduce_only": bool(o.is_reduce_only),
                    }
                    for o in cache2.orders_open()
                ]
                positions_reconciled = [
                    {
                        "instrument_id": str(p.instrument_id),
                        "side": str(p.side),
                        "signed_qty": str(p.signed_qty),
                    }
                    for p in cache2.positions_open()
                ]
                # Cancel both resting regular and algo order cleanly before stopping
                for o in cache2.orders_open():
                    strat2.cancel_order(o.client_order_id)
                await asyncio.sleep(0.5)
                return {
                    "reconciled_open_orders": open_orders_reconciled,
                    "reconciled_open_positions": positions_reconciled,
                    "post_cleanup_open_regular": len([o for o in srv.regular_orders.values() if o["status"] == "NEW"]),
                    "post_cleanup_open_algo": len([a for a in srv.algo_orders.values() if a["algoStatus"] == "NEW"]),
                }
            finally:
                handle2.stop()

        chk = asyncio.create_task(_check())
        await asyncio.wait_for(node2.run_async(), timeout=20.0)
        res2 = await chk
        node2.dispose()
        return res2

    try:
        _, res1 = await _drive_node1()
        res2 = await _drive_node2()

        algo_posts = [w for w in srv.wire_audit if w["method"] == "POST" and w["path"] == "/fapi/v1/algoOrder"]
        wire_checks = {
            "set_isolated_called": any(w["path"] == "/fapi/v1/marginType" and w["params"].get("marginType") == "ISOLATED" for w in srv.wire_audit),
            "set_leverage_called": any(w["path"] == "/fapi/v1/leverage" and w["params"].get("leverage") == "5" for w in srv.wire_audit),
            "query_hedge_mode_called": any(w["path"] == "/fapi/v1/positionSide/dual" for w in srv.wire_audit),
            "algo_qty_reduce_only_sent": any(
                w["params"].get("reduceOnly") == "true" and w["params"].get("quantity") == "0.10" and w["params"].get("workingType") == "MARK_PRICE"
                for w in algo_posts
            ),
            "algo_close_position_omits_qty_and_reduce_only": any(
                w["params"].get("closePosition") == "true"
                and "quantity" not in w["params"]
                and "reduceOnly" not in w["params"]
                and w["params"].get("workingType") == "CONTRACT_PRICE"
                for w in algo_posts
            ),
            "startup_queried_open_orders_and_algo_orders": (
                any(w["path"] == "/fapi/v1/openOrders" for w in srv.wire_audit)
                and any(w["path"] == "/fapi/v1/openAlgoOrders" for w in srv.wire_audit)
                and any(w["path"] == "/fapi/v2/positionRisk" for w in srv.wire_audit)
            ),
            "reconciled_close_position_restored_qty": any(
                o["qty"] == "0.20" and o["is_reduce_only"] for o in res2["reconciled_open_orders"]
            ),
            "no_duplicate_submit_on_504": len([w for w in srv.wire_audit if w["method"] == "POST" and w["path"] == "/fapi/v1/order" and w["params"].get("type") == "MARKET"]) == 2,
        }
        return {
            "suite": "mock-demo-e2e",
            "node1": res1,
            "node2_reconciliation": res2,
            "wire_checks": wire_checks,
            "algo_orders_observed": redact_sensitive(list(srv.algo_orders.values())),
            "wire_audit_count": len(srv.wire_audit),
            "wire_audit_sample": srv.wire_audit[:15],
            "status": "PASSED" if all(wire_checks.values()) else "FAILED",
        }
    finally:
        await srv.stop()


def run_live_demo_suite(*, symbol: str = "SOLUSDT") -> dict[str, Any]:
    """Run live USD-M Demo verification against https://demo-fapi.binance.com when keys exist."""
    client = SignedUsdmRestClient(base_url="https://demo-fapi.binance.com")
    if not client.has_credentials():
        return {
            "suite": "live-demo",
            "status": "BLOCKED_NO_DEMO_CREDENTIALS",
            "execution_enabled": False,
            "reason": "BINANCE_DEMO_API_KEY and BINANCE_DEMO_API_SECRET are not set in environment; execution remains disabled per M-NT3 gate.",
        }

    # Read-only preflight: never mutate account dual-side mode or touch foreign orders/positions
    dual = client.request("GET", "/fapi/v1/positionSide/dual")
    if dual.get("dualSidePosition"):
        return {
            "suite": "live-demo",
            "symbol": symbol,
            "status": "BLOCKED_ACCOUNT_NOT_ONE_WAY",
            "execution_enabled": False,
            "reason": "Demo account has dualSidePosition=true; dedicated one-way account required without mutating shared mode.",
        }
    pre_oo = client.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol})
    pre_oa = client.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})
    pre_pos = client.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol})
    pre_nonzero_pos = [p for p in pre_pos if Decimal(str(p.get("positionAmt", "0"))) != 0]
    if pre_oo or pre_oa or pre_nonzero_pos:
        return {
            "suite": "live-demo",
            "symbol": symbol,
            "status": "BLOCKED_FOREIGN_STATE_PRESENT",
            "execution_enabled": False,
            "pre_open_orders": len(pre_oo),
            "pre_open_algos": len(pre_oa),
            "pre_positions": pre_nonzero_pos,
            "reason": "Refusing to wipe foreign orders/positions on symbol before dedicated Demo verification.",
        }
    try:
        client.request("POST", "/fapi/v1/marginType", params={"symbol": symbol, "marginType": "ISOLATED"})
    except RuntimeError as exc:
        if "-4046" not in str(exc):
            raise
    lev_resp = client.request("POST", "/fapi/v1/leverage", params={"symbol": symbol, "leverage": 5})
    brackets = client.request("GET", "/fapi/v1/leverageBracket", params={"symbol": symbol})
    acc_before = client.request("GET", "/fapi/v2/account")
    start_wallet = Decimal(str(acc_before.get("totalWalletBalance", "0")))
    if start_wallet < Decimal("20"):
        return {
            "suite": "live-demo",
            "status": "BLOCKED_ZERO_DEMO_FUTURES_BALANCE",
            "execution_enabled": False,
            "totalWalletBalance": str(start_wallet),
            "reason": "USD-M Demo wallet balance < 20 USDT; fund USD-M Demo wallet before executing live orders.",
        }

    t_start_ms = int(time.time() * 1000) - 2000
    qty = "0.20" if symbol == "SOLUSDT" else "0.01"
    half_qty = "0.10" if symbol == "SOLUSDT" else "0.01"
    created_order_ids: set[int] = set()
    created_algo_ids: set[int] = set()
    created_client_ids: set[str] = set()

    # Negative mutual-exclusion test against real demo-fapi: closePosition=true + quantity MUST fail!
    close_pos_mutex_rejected = False
    close_pos_mutex_err = ""
    try:
        client.request(
            "POST",
            "/fapi/v1/algoOrder",
            params={
                "algoType": "CONDITIONAL",
                "symbol": symbol,
                "side": "SELL",
                "type": "STOP_MARKET",
                "triggerPrice": "40.00",
                "workingType": "MARK_PRICE",
                "closePosition": True,
                "quantity": half_qty,
            },
        )
    except RuntimeError as exc:
        close_pos_mutex_rejected = True
        close_pos_mutex_err = str(exc)

    entry: dict[str, Any] = {}
    algo_qty: dict[str, Any] = {}
    algo_cp: dict[str, Any] = {}
    algo_tp_trig: dict[str, Any] | None = None
    algo_sl_trig: dict[str, Any] | None = None
    triggered_algo: dict[str, Any] | None = None
    triggered_child_order: dict[str, Any] | None = None
    triggered_child_trades: list[dict[str, Any]] = []
    pos_mid: list[dict[str, Any]] = []
    open_algos_seen: list[dict[str, Any]] = []
    algo_qty_queried: dict[str, Any] = {}

    try:
        prem = _fetch_json(f"https://demo-fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
        ticker = _fetch_json(f"https://demo-fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}")
        mark_px = Decimal(str(prem["markPrice"]))
        last_px = Decimal(str(ticker["price"]))

        # Open 0.20 SOL MARKET BUY
        entry_cid = f"mnt3e{int(time.time() * 1000) % 1000000}"
        created_client_ids.add(entry_cid)
        entry = client.request(
            "POST",
            "/fapi/v1/order",
            params={"symbol": symbol, "side": "BUY", "type": "MARKET", "quantity": qty, "newClientOrderId": entry_cid, "newOrderRespType": "RESULT"},
        )
        if entry.get("orderId"):
            created_order_ids.add(int(entry["orderId"]))
        pos_mid = client.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol})

        stop_px = f"{(mark_px * Decimal('0.90')):.2f}"
        cp_px = f"{(mark_px * Decimal('0.89')):.2f}"

        # 1. Quantity reduceOnly algoOrder with MARK_PRICE
        q_cid = f"mnt3q{int(time.time())}"
        created_client_ids.add(q_cid)
        algo_qty = client.request(
            "POST",
            "/fapi/v1/algoOrder",
            params=client.validate_algo_order_params(
                symbol=symbol,
                side="SELL",
                order_type="STOP_MARKET",
                trigger_price=stop_px,
                working_type="MARK_PRICE",
                close_position=False,
                quantity=half_qty,
                reduce_only=True,
                client_algo_id=q_cid,
            ),
        )
        if algo_qty.get("algoId"):
            created_algo_ids.add(int(algo_qty["algoId"]))
        algo_qty_queried = client.request("GET", "/fapi/v1/algoOrder", params={"algoId": algo_qty["algoId"]})

        # 2. closePosition=true algoOrder with CONTRACT_PRICE (omitting quantity & reduceOnly)
        cp_cid = f"mnt3c{int(time.time())}"
        created_client_ids.add(cp_cid)
        algo_cp = client.request(
            "POST",
            "/fapi/v1/algoOrder",
            params=client.validate_algo_order_params(
                symbol=symbol,
                side="SELL",
                order_type="STOP_MARKET",
                trigger_price=cp_px,
                working_type="CONTRACT_PRICE",
                close_position=True,
                client_algo_id=cp_cid,
            ),
        )
        if algo_cp.get("algoId"):
            created_algo_ids.add(int(algo_cp["algoId"]))
        open_algos_seen = client.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})
        client.request("DELETE", "/fapi/v1/algoOrder", params={"algoId": algo_qty["algoId"]})

        # 3. Attempt live conditional trigger -> child actualOrderId & userTrades correlation
        for offset in (Decimal("0.02"), Decimal("0.05"), Decimal("0.10")):
            try:
                t_now = Decimal(str(_fetch_json(f"https://demo-fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}")["price"]))
                tp_cid = f"mnt3tp{int(time.time() * 1000) % 1000000}"
                created_client_ids.add(tp_cid)
                algo_tp_trig = client.request(
                    "POST",
                    "/fapi/v1/algoOrder",
                    params=client.validate_algo_order_params(
                        symbol=symbol,
                        side="SELL",
                        order_type="TAKE_PROFIT_MARKET",
                        trigger_price=f"{(t_now + offset):.2f}",
                        working_type="CONTRACT_PRICE",
                        close_position=False,
                        quantity=half_qty,
                        reduce_only=True,
                        client_algo_id=tp_cid,
                    ),
                )
                if algo_tp_trig.get("algoId"):
                    created_algo_ids.add(int(algo_tp_trig["algoId"]))
                break
            except RuntimeError:
                continue
        for offset in (Decimal("0.02"), Decimal("0.05"), Decimal("0.10")):
            try:
                t_now = Decimal(str(_fetch_json(f"https://demo-fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}")["price"]))
                sl_cid = f"mnt3sl{int(time.time() * 1000) % 1000000}"
                created_client_ids.add(sl_cid)
                algo_sl_trig = client.request(
                    "POST",
                    "/fapi/v1/algoOrder",
                    params=client.validate_algo_order_params(
                        symbol=symbol,
                        side="SELL",
                        order_type="STOP_MARKET",
                        trigger_price=f"{(t_now - offset):.2f}",
                        working_type="CONTRACT_PRICE",
                        close_position=False,
                        quantity=half_qty,
                        reduce_only=True,
                        client_algo_id=sl_cid,
                    ),
                )
                if algo_sl_trig.get("algoId"):
                    created_algo_ids.add(int(algo_sl_trig["algoId"]))
                break
            except RuntimeError:
                continue

        watch_ids = [x["algoId"] for x in (algo_tp_trig, algo_sl_trig) if x and x.get("algoId")]
        for _ in range(25):
            for aid in watch_ids:
                q_res = client.request("GET", "/fapi/v1/algoOrder", params={"algoId": aid})
                if q_res.get("algoStatus") in ("FINISHED", "TRIGGERED") and q_res.get("actualOrderId"):
                    triggered_algo = q_res
                    break
            if triggered_algo is not None:
                break
            time.sleep(1.0)

        if triggered_algo is not None and triggered_algo.get("actualOrderId"):
            child_oid = int(triggered_algo["actualOrderId"])
            created_order_ids.add(child_oid)
            triggered_child_order = client.request("GET", "/fapi/v1/order", params={"symbol": symbol, "orderId": child_oid})
            all_tr = client.request("GET", "/fapi/v1/userTrades", params={"symbol": symbol, "startTime": t_start_ms, "limit": 50})
            triggered_child_trades = [t for t in all_tr if int(t.get("orderId", -1)) == child_oid]
    finally:
        # Scoped cleanup: cancel ONLY orders/algos belonging to THIS run and close ONLY opened run qty!
        for o in client.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol}):
            if int(o.get("algoId", -1)) in created_algo_ids or str(o.get("clientAlgoId", "")) in created_client_ids:
                try:
                    client.request("DELETE", "/fapi/v1/algoOrder", params={"algoId": o["algoId"]})
                except Exception:
                    pass
        for o in client.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol}):
            if int(o.get("orderId", -1)) in created_order_ids or str(o.get("clientOrderId", "")) in created_client_ids:
                try:
                    client.request("DELETE", "/fapi/v1/order", params={"symbol": symbol, "orderId": o["orderId"]})
                except Exception:
                    pass
        if entry:
            for p in client.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol}):
                amt = Decimal(str(p.get("positionAmt", "0")))
                if amt != 0:
                    close_qty = min(abs(amt), Decimal(qty))
                    close_side = "SELL" if amt > 0 else "BUY"
                    client.request(
                        "POST",
                        "/fapi/v1/order",
                        params={"symbol": symbol, "side": close_side, "type": "MARKET", "quantity": f"{close_qty}", "reduceOnly": True},
                    )

    final_open_orders = client.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol})
    final_open_algos = client.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})
    final_pos = client.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol})
    trades = client.request("GET", "/fapi/v1/userTrades", params={"symbol": symbol, "startTime": t_start_ms, "limit": 50})
    income = client.request("GET", "/fapi/v1/income", params={"symbol": symbol, "startTime": t_start_ms, "limit": 50})
    acc_after = client.request("GET", "/fapi/v2/account")
    end_wallet = Decimal(str(acc_after.get("totalWalletBalance", "0")))

    # Reconcile exchange trades & income with Independent Decimal IsolatedUsdmLedger!
    ledger = IsolatedUsdmLedger(symbol=symbol, cross_wallet_usdt=start_wallet, leverage=5)
    for tr in sorted(trades, key=lambda t: (int(t["time"]), int(t["id"]))):
        ledger.apply_fill(
            ts_ns=int(tr["time"]) * 1_000_000,
            order_id=str(tr["orderId"]),
            side=str(tr["side"]),
            qty=Decimal(str(tr["qty"])),
            price=Decimal(str(tr["price"])),
            fee_amount=Decimal(str(tr["commission"])),
            fee_asset=str(tr["commissionAsset"]),
            fill_id=str(tr["id"]),
            confirmed_exchange_fill=True,
        )
    ledger_diff = abs(end_wallet - ledger.cross_wallet_usdt)

    sym_pos_rows = [p for p in final_pos if p.get("symbol") == symbol]
    checks = {
        "final_open_orders_zero": len(final_open_orders) == 0,
        "final_open_algos_zero": len(final_open_algos) == 0,
        "final_pos_present_and_flat": bool(sym_pos_rows) and all(Decimal(str(p.get("positionAmt", "0"))) == 0 for p in sym_pos_rows),
        "close_position_mutex_rejected": close_pos_mutex_rejected,
        "triggered_algo_present": bool(triggered_algo and triggered_algo.get("algoId") in created_algo_ids and triggered_algo.get("actualOrderId")),
        "triggered_child_order_filled": bool(
            triggered_child_order
            and str(triggered_child_order.get("orderId")) == str((triggered_algo or {}).get("actualOrderId"))
            and triggered_child_order.get("status") == "FILLED"
            and Decimal(str(triggered_child_order.get("executedQty", "0"))) > 0
        ),
        "triggered_child_trades_present": bool(triggered_child_trades),
        "ledger_reconciled": ledger_diff < Decimal("0.000001"),
    }

    return {
        "suite": "live-demo",
        "symbol": symbol,
        "dual_side_position": dual,
        "leverage_set": lev_resp,
        "brackets": brackets,
        "close_position_mutex_rejected": close_pos_mutex_rejected,
        "close_position_mutex_err": close_pos_mutex_err,
        "position_mid_isolated": pos_mid,
        "entry_order": redact_sensitive(entry),
        "algo_qty_order": redact_sensitive(algo_qty),
        "algo_qty_queried": redact_sensitive(algo_qty_queried),
        "algo_close_pos_order": redact_sensitive(algo_cp),
        "open_algos_seen_count": len(open_algos_seen),
        "triggered_algo": redact_sensitive(triggered_algo),
        "triggered_child_order": redact_sensitive(triggered_child_order),
        "triggered_child_trades": redact_sensitive(triggered_child_trades),
        "final_open_orders_count": len(final_open_orders),
        "final_open_algos_count": len(final_open_algos),
        "final_position": final_pos,
        "recent_trades_count": len(trades),
        "recent_trades": redact_sensitive(trades),
        "income": redact_sensitive(income),
        "start_wallet_usdt": str(start_wallet),
        "end_wallet_usdt": str(end_wallet),
        "hand_ledger_end_usdt": str(ledger.cross_wallet_usdt),
        "wallet_vs_hand_ledger_diff": str(ledger_diff),
        "checks": checks,
        "audit_log": redact_sensitive(client.audit_log),
        "status": "PASSED" if all(checks.values()) else "FAILED",
    }


async def _run_live_demo_nautilus_async(*, symbol: str = "SOLUSDT") -> dict[str, Any]:
    """Run real 2-Node Nautilus LiveNode (BinanceExecutionClientFactory DEMO) against https://demo-fapi.binance.com."""
    rest = SignedUsdmRestClient(base_url="https://demo-fapi.binance.com")
    if not rest.has_credentials():
        return {"suite": "live-demo-nautilus", "status": "BLOCKED_NO_DEMO_CREDENTIALS", "execution_enabled": False}

    from nautilus_trader.adapters.binance import BinanceDataClientFactory, BinanceExecutionClientFactory
    from nautilus_trader.common import Environment, LogLevel
    from nautilus_trader.config import LoggerConfig, StrategyConfig
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import InstrumentId, OrderSide, Price, Quantity, TraderId, TriggerType
    from nautilus_trader.trading import Strategy

    # Read-only preflight check: never wipe foreign orders/positions or mutate account mode
    dual = rest.request("GET", "/fapi/v1/positionSide/dual")
    if dual.get("dualSidePosition"):
        return {"suite": "live-demo-nautilus", "symbol": symbol, "status": "BLOCKED_ACCOUNT_NOT_ONE_WAY", "execution_enabled": False}
    pre_oo = rest.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol})
    pre_oa = rest.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})
    pre_pos = [p for p in rest.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol}) if Decimal(str(p.get("positionAmt", "0"))) != 0]
    if pre_oo or pre_oa or pre_pos:
        return {
            "suite": "live-demo-nautilus",
            "symbol": symbol,
            "status": "BLOCKED_FOREIGN_STATE_PRESENT",
            "execution_enabled": False,
            "pre_open_orders": len(pre_oo),
            "pre_open_algos": len(pre_oa),
            "pre_positions": pre_pos,
        }
    inst_id = InstrumentId.from_str(f"{symbol}-PERP.BINANCE")
    order_qty_str = "0.20" if symbol == "SOLUSDT" else "0.01"

    class _LiveDriver(Strategy):
        def __init__(self, cfg: StrategyConfig) -> None:
            super().__init__(cfg)
            self.quotes: list[Any] = []
            self.fills: list[dict[str, Any]] = []
            self.events: list[str] = []

        def on_start(self) -> None:
            self.subscribe_quotes(inst_id)

        def on_quote(self, q: Any) -> None:
            self.quotes.append((str(q.bid_price), str(q.ask_price)))

        def on_order_accepted(self, ev: Any) -> None:
            self.events.append(f"ACCEPTED:{ev.client_order_id}:{ev.venue_order_id}")

        def on_order_canceled(self, ev: Any) -> None:
            self.events.append(f"CANCELED:{ev.client_order_id}")

        def on_order_rejected(self, ev: Any) -> None:
            self.events.append(f"REJECTED:{ev.client_order_id}:{ev.reason}")

        def on_order_filled(self, ev: Any) -> None:
            self.fills.append(
                {
                    "client_order_id": str(ev.client_order_id),
                    "venue_order_id": str(ev.venue_order_id),
                    "trade_id": str(ev.trade_id),
                    "qty": str(ev.last_qty),
                    "px": str(ev.last_px),
                }
            )
            self.events.append(f"FILLED:{ev.client_order_id}:{ev.last_qty}")

    async def _node1() -> dict[str, Any]:
        data_cfg, exec_cfg = build_binance_usdm_demo_configs(
            account_id="BINANCE-DEMOTEST-USDM",
            symbols=(symbol,),
            leverage=5,
            demo=True,
        )
        node = (
            LiveNode.builder("MNT3-LIVE-1", TraderId("MNT3-LIVE"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.INFO))
            .with_reconciliation(True)
            .with_reconciliation_lookback_mins(30)
            .with_timeout_connection(15)
            .with_timeout_reconciliation(15)
            .with_delay_post_stop_secs(1)
            .add_data_client("BINANCE", BinanceDataClientFactory(), data_cfg)
            .add_exec_client("BINANCE", BinanceExecutionClientFactory(), exec_cfg)
            .build()
        )
        strat = _LiveDriver(StrategyConfig())
        node.add_strategy(strat)
        h = node.handle()
        cache = node.cache

        async def _act() -> dict[str, Any]:
            try:
                for _ in range(100):
                    if len(strat.quotes) >= 1:
                        break
                    await asyncio.sleep(0.1)
                mid = (Decimal(strat.quotes[-1][0]) + Decimal(strat.quotes[-1][1])) / Decimal(2)
                qty_s = order_qty_str

                # 1. Live MARKET BUY
                o_mkt = strat.order_factory.market(inst_id, OrderSide.BUY, Quantity.from_str(qty_s))
                strat.submit_order(o_mkt)
                for _ in range(80):
                    if len(strat.fills) >= 1:
                        break
                    await asyncio.sleep(0.1)

                # 2. Live closePosition=True STOP_MARKET MARK_PRICE + resting far LIMIT BUY (notional >= 6 USDT)
                stop_px = f"{(mid * Decimal('0.85')):.2f}"
                lim_px = f"{(mid * Decimal('0.55')):.2f}"
                o_cp = strat.order_factory.stop_market(
                    inst_id,
                    OrderSide.SELL,
                    Quantity.from_str(qty_s),
                    Price.from_str(stop_px),
                    trigger_type=TriggerType.MARK_PRICE,
                    reduce_only=True,
                )
                strat.submit_order(o_cp, params={"close_position": True})

                o_lim = strat.order_factory.limit(
                    inst_id,
                    OrderSide.BUY,
                    Quantity.from_str(qty_s),
                    Price.from_str(lim_px),
                )
                strat.submit_order(o_lim)

                for _ in range(80):
                    acc_cnt = sum(1 for e in strat.events if e.startswith("ACCEPTED:"))
                    if acc_cnt >= 3 or len(cache.orders_open()) >= 2:
                        break
                    await asyncio.sleep(0.1)
                await asyncio.sleep(1.0)
                return {
                    "quotes_seen": len(strat.quotes),
                    "node1_fills": list(strat.fills),
                    "node1_events": list(strat.events),
                    "node1_stop_client_id": str(o_cp.client_order_id),
                    "node1_limit_client_id": str(o_lim.client_order_id),
                    "exchange_open_orders": len(rest.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol})),
                    "exchange_open_algos": len(rest.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})),
                    "exchange_pos": rest.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol}),
                }
            finally:
                h.stop()

        t = asyncio.create_task(_act())
        await asyncio.wait_for(node.run_async(), timeout=55.0)
        r = await t
        node.dispose()
        return r

    async def _node2_reconcile_and_cleanup() -> dict[str, Any]:
        data_cfg, exec_cfg = build_binance_usdm_demo_configs(
            account_id="BINANCE-DEMOTEST-USDM",
            symbols=(symbol,),
            leverage=5,
            demo=True,
        )
        node2 = (
            LiveNode.builder("MNT3-LIVE-2", TraderId("MNT3-LIVE"), Environment.LIVE)
            .with_logging(LoggerConfig(stdout_level=LogLevel.INFO))
            .with_reconciliation(True)
            .with_reconciliation_lookback_mins(30)
            .with_timeout_connection(15)
            .with_timeout_reconciliation(15)
            .with_delay_post_stop_secs(1)
            .add_data_client("BINANCE", BinanceDataClientFactory(), data_cfg)
            .add_exec_client("BINANCE", BinanceExecutionClientFactory(), exec_cfg)
            .build()
        )
        strat2 = _LiveDriver(StrategyConfig())
        node2.add_strategy(strat2)
        h2 = node2.handle()
        cache2 = node2.cache

        async def _check() -> dict[str, Any]:
            try:
                for _ in range(80):
                    if len(strat2.quotes) >= 1 and len(cache2.orders_open()) >= 2 and len(cache2.positions_open()) >= 1:
                        break
                    await asyncio.sleep(0.1)
                await asyncio.sleep(0.5)
                reconciled_orders = [
                    {
                        "client_order_id": str(o.client_order_id),
                        "venue_order_id": str(o.venue_order_id) if o.venue_order_id is not None else None,
                        "order_type": str(o.order_type),
                        "qty": str(o.quantity),
                        "is_reduce_only": bool(o.is_reduce_only),
                    }
                    for o in cache2.orders_open()
                ]
                reconciled_positions = [
                    {
                        "instrument_id": str(p.instrument_id),
                        "side": str(p.side),
                        "signed_qty": str(p.signed_qty),
                    }
                    for p in cache2.positions_open()
                ]
                # Cancel both reconciled orders via Nautilus cancel_order, then flatten position via MARKET reduce_only=True
                for o in cache2.orders_open():
                    strat2.cancel_order(o.client_order_id)
                for _ in range(60):
                    if len(cache2.orders_open()) == 0:
                        break
                    await asyncio.sleep(0.1)
                for p in cache2.positions_open():
                    q_abs = abs(Decimal(str(p.signed_qty)))
                    if q_abs > 0:
                        c_side = OrderSide.SELL if Decimal(str(p.signed_qty)) > 0 else OrderSide.BUY
                        o_flat = strat2.order_factory.market(inst_id, c_side, Quantity.from_str(str(q_abs)), reduce_only=True)
                        strat2.submit_order(o_flat)
                for _ in range(60):
                    if len(cache2.positions_open()) == 0 and len(strat2.fills) >= 1:
                        break
                    await asyncio.sleep(0.1)
                await asyncio.sleep(0.5)
                return {
                    "reconciled_open_orders": reconciled_orders,
                    "reconciled_open_positions": reconciled_positions,
                    "node2_events": list(strat2.events),
                    "node2_fills": list(strat2.fills),
                }
            finally:
                h2.stop()

        t2 = asyncio.create_task(_check())
        await asyncio.wait_for(node2.run_async(), timeout=55.0)
        r2 = await t2
        node2.dispose()
        return r2

    r1: dict[str, Any] = {}
    r2: dict[str, Any] = {}
    fallback_cleanup_actions: list[str] = []
    pre_fallback_open_orders = 0
    pre_fallback_open_algos = 0
    pre_fallback_nonzero_pos = 0
    try:
        r1 = await _node1()
        r2 = await _node2_reconcile_and_cleanup()
        # Capture post-Node-2 account state BEFORE safety fallback cleanup!
        pre_fallback_open_orders = len(rest.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol}))
        pre_fallback_open_algos = len(rest.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol}))
        pre_fallback_nonzero_pos = sum(
            1
            for p in rest.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol})
            if Decimal(str(p.get("positionAmt", "0"))) != 0
        )
    finally:
        # Scoped emergency cleanup ONLY for orders/exposure created by THIS run (if native Node 2 failed)
        run_cids = {str(x) for x in (r1.get("node1_stop_client_id"), r1.get("node1_limit_client_id")) if x}
        for o in rest.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol}):
            if not run_cids or str(o.get("clientAlgoId", "")) in run_cids:
                try:
                    rest.request("DELETE", "/fapi/v1/algoOrder", params={"algoId": o["algoId"]})
                    fallback_cleanup_actions.append(f"cancel_algo:{o['algoId']}")
                except Exception:
                    pass
        for o in rest.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol}):
            if not run_cids or str(o.get("clientOrderId", "")) in run_cids:
                try:
                    rest.request("DELETE", "/fapi/v1/order", params={"symbol": symbol, "orderId": o["orderId"]})
                    fallback_cleanup_actions.append(f"cancel_order:{o['orderId']}")
                except Exception:
                    pass
        if r1.get("node1_fills") and pre_fallback_nonzero_pos > 0:
            for p in rest.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol}):
                amt = Decimal(str(p.get("positionAmt", "0")))
                if amt != 0:
                    close_qty = min(abs(amt), Decimal(order_qty_str))
                    rest.request(
                        "POST",
                        "/fapi/v1/order",
                        params={"symbol": symbol, "side": ("SELL" if amt > 0 else "BUY"), "type": "MARKET", "quantity": f"{close_qty}", "reduceOnly": True},
                    )
                    fallback_cleanup_actions.append(f"flatten_pos:{close_qty}")

    final_oo = rest.request("GET", "/fapi/v1/openOrders", params={"symbol": symbol})
    final_oa = rest.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": symbol})
    final_pos = rest.request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol})
    sym_pos_rows = [p for p in final_pos if p.get("symbol") == symbol]

    rec_orders = r2.get("reconciled_open_orders", [])
    rec_positions = r2.get("reconciled_open_positions", [])
    rec_events = r2.get("node2_events", [])
    stop_cid = str(r1.get("node1_stop_client_id", ""))
    lim_cid = str(r1.get("node1_limit_client_id", ""))
    checks = {
        "node1_filled": len(r1.get("node1_fills", [])) >= 1 and Decimal(str(r1["node1_fills"][0]["qty"])) == Decimal(order_qty_str),
        "node1_left_open_orders": r1.get("exchange_open_orders") == 1 and r1.get("exchange_open_algos") == 1,
        "node2_reconciled_exact_stop_id_qty": any(
            o["client_order_id"] == stop_cid
            and o["order_type"] == "STOP_MARKET"
            and Decimal(str(o["qty"])) == Decimal(order_qty_str)
            and o["is_reduce_only"]
            for o in rec_orders
        ),
        "node2_reconciled_exact_limit_id": any(
            o["client_order_id"] == lim_cid and o["order_type"] == "LIMIT" for o in rec_orders
        ),
        "node2_reconciled_signed_position": any(
            p["instrument_id"] == str(inst_id) and p["side"] == "LONG" and Decimal(str(p["signed_qty"])) == Decimal(order_qty_str)
            for p in rec_positions
        ),
        "node2_canceled_both_natively": f"CANCELED:{stop_cid}" in rec_events and f"CANCELED:{lim_cid}" in rec_events,
        "node2_flattened_natively": (
            len(r2.get("node2_fills", [])) >= 1
            and sum((Decimal(str(f["qty"])) for f in r2.get("node2_fills", [])), Decimal(0)) == Decimal(order_qty_str)
            and pre_fallback_open_orders == 0
            and pre_fallback_open_algos == 0
            and pre_fallback_nonzero_pos == 0
            and not fallback_cleanup_actions
        ),
        "final_clean": (
            len(final_oo) == 0
            and len(final_oa) == 0
            and bool(sym_pos_rows)
            and all(Decimal(str(p.get("positionAmt", "0"))) == 0 for p in sym_pos_rows)
        ),
    }
    return {
        "suite": "live-demo-nautilus",
        "symbol": symbol,
        "order_qty": order_qty_str,
        "node1": r1,
        "node2": r2,
        "pre_fallback_counts": {
            "open_orders": pre_fallback_open_orders,
            "open_algos": pre_fallback_open_algos,
            "nonzero_positions": pre_fallback_nonzero_pos,
        },
        "fallback_cleanup_actions": fallback_cleanup_actions,
        "final_open_orders_count": len(final_oo),
        "final_open_algos_count": len(final_oa),
        "final_position": final_pos,
        "checks": checks,
        "status": "PASSED" if all(checks.values()) else "FAILED",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="M-NT3 USD-M Perpetual Verification Harness")
    parser.add_argument(
        "--suite",
        choices=(
            "offline",
            "public-specs",
            "accounting-parity",
            "mock-demo-e2e",
            "live-demo",
            "live-demo-nautilus",
            "all-offline",
            "all-with-live-demo",
        ),
        default="offline",
    )
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    out: dict[str, Any] = {}
    if args.suite in ("public-specs", "all-with-live-demo"):
        out["public_specs"] = run_public_specs_suite()
    if args.suite in ("offline", "all-offline", "accounting-parity", "all-with-live-demo"):
        out["accounting_parity"] = run_accounting_parity_suite()
    if args.suite in ("offline", "all-offline", "mock-demo-e2e", "all-with-live-demo"):
        out["mock_demo_e2e"] = asyncio.run(_run_mock_demo_e2e_async())
    if args.suite in ("live-demo", "all-with-live-demo"):
        out["live_demo"] = run_live_demo_suite(symbol=args.symbol)
    if args.suite == "live-demo-nautilus":
        out["live_demo_nautilus"] = asyncio.run(_run_live_demo_nautilus_async(symbol=args.symbol))

    payload = json.dumps(redact_sensitive(out), indent=2, default=str)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    all_passed = bool(out) and all(isinstance(v, dict) and v.get("status") == "PASSED" for v in out.values())
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
