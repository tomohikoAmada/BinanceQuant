"""M-NT4 verification tests: single-keyed lock, synchronous risk gate, L2 simulation, dual recovery & sandbox replay."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

import pytest

pytest.importorskip("nautilus_trader.adapters.sandbox")

from nautilus_trader.backtest import BacktestEngine  # noqa: E402
from nautilus_trader.common import Environment, LogLevel  # noqa: E402
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig, StrategyConfig  # noqa: E402
from nautilus_trader.execution import MakerTakerFeeModel  # noqa: E402
from nautilus_trader.model import (  # noqa: E402
    AccountType,
    BookType,
    Money,
    OmsType,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    TimeInForce,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bq_feed.events import Market  # noqa: E402
from bq_nautilus.instruments import BmdInstrumentSpec  # noqa: E402
from bq_nautilus.mnt4_recovery import (  # noqa: E402
    audit_credential_and_host_boundary,
    run_spot_fault_recovery_suite,
)
from bq_nautilus.mnt4_risk import (  # noqa: E402
    Mnt4RiskConfig,
    PersistentUtcRiskStateStore,
    RiskGuardedStrategyMixin,
    SynchronousRiskGate,
)
from bq_nautilus.mnt4_sandbox import (  # noqa: E402
    HourlySanitizedWriter,
    SandboxHourlyAuditStrategy,
    SandboxHourlyAuditStrategyConfig,
    replay_sealed_hour,
)
from bq_nautilus.mnt4_simulation import run_mnt4_market_simulation_suite  # noqa: E402
from scripts.mnt4_verify import run_risk_boundary_suite  # noqa: E402


def _spot_btc_inst():
    spec = BmdInstrumentSpec(
        market=Market.SPOT,
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.00001"),
        min_notional=Decimal("5"),
        observe_time_utc_ns=1_795_700_000_000_000_000,
        min_quantity=Decimal("0.00001"),
        max_quantity=Decimal("9000"),
    )
    return spec.to_nautilus_instrument()


def test_mnt4_risk_boundary_single_keyed_and_daily_loss_lock(tmp_path: Path) -> None:
    res = run_risk_boundary_suite(tmp_path)
    assert res["status"] == "PASSED"
    assert res["second_keyed_instance_blocked"]
    assert res["pre_reconcile_blocked"]
    assert res["daily_loss_locked_at_4_usdt"]
    assert res["modify_up_blocked_when_locked"]
    assert res["cross_zero_reversal_blocked_when_locked"]
    assert res["cancel_allowed_when_locked"]
    assert res["reduce_only_allowed_when_locked"]
    assert res["utc_rollover_requires_reconcile"]
    assert res["db_identity_mismatch_blocked"]


def test_mnt4_synchronous_risk_gate_inside_nautilus_strategy_and_cashflow(tmp_path: Path) -> None:
    inst = _spot_btc_inst()
    cfg = Mnt4RiskConfig(
        environment="BACKTEST",
        account_id="MNT4-STRAT-001",
        trader_id="MNT4-STRAT-001",
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({"BTCUSDT"}),
        starting_equity_usdt=Decimal("50"),
        max_daily_loss_usdt=Decimal("2"),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={"BTCUSDT": Decimal("0.01")},
    )
    store = PersistentUtcRiskStateStore(
        tmp_path / "strat_risk.sqlite3",
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
        utc_day="2026-11-27",
    )
    # External deposit/withdrawal adjusts baseline cleanly without tripping false daily loss
    store.record_external_cashflow(Decimal("-10"))
    assert store.daily_loss_usdt() == Decimal(0)
    store.record_external_cashflow(Decimal("10"))
    gate = SynchronousRiskGate(config=cfg, store=store)
    gate.complete_reconciliation(utc_day="2026-11-27")

    class _Cfg(StrategyConfig):
        def __init__(self, *, g: SynchronousRiskGate, **kw) -> None:
            super().__init__(**kw)
            self.g = g

    class _Strat(Strategy, RiskGuardedStrategyMixin):
        def __init__(self, c: _Cfg) -> None:
            super().__init__(c)
            self._init_risk_gate(c.g, market="SPOT", symbol="BTCUSDT")
            self.n = 0
            self.rest_order = None

        def on_start(self) -> None:
            self.subscribe_quotes(inst.id)

        def on_quote(self, q: QuoteTick) -> None:
            self.n += 1
            if self.n == 1:
                o = self.order_factory.limit(
                    inst.id,
                    OrderSide.BUY,
                    Quantity.from_str("0.00020"),
                    Price.from_str("90.00"),
                    time_in_force=TimeInForce.GTC,
                )
                self.rest_order = o
                self.submit_guarded_order(o, reference_price=Decimal("100.00"), now_utc_day="2026-11-27")
                # Attempt to sell unfilled BUY as if it were settled balance -> must be blocked!
                s_unfilled = self.order_factory.market(inst.id, OrderSide.SELL, Quantity.from_str("0.00020"))
                self.submit_guarded_order(s_unfilled, reference_price=Decimal("100.00"), now_utc_day="2026-11-27")
            elif self.n == 2:
                # Simulate realized loss 3.00 USDT -> exceeds max_daily_loss_usdt=2 -> kill further risk increases
                self._risk_gate.store.record_realized_loss_or_pnl(Decimal("-3.00"))
                # Attempt MODIFY qty increase -> blocked synchronously BEFORE RiskEngine/venue
                self.modify_guarded_order(self.rest_order, new_quantity=Decimal("0.00040"), reference_price=Decimal("100.00"), now_utc_day="2026-11-27")
                # Cancel must still pass cleanly
                self.cancel_guarded_order(self.rest_order)

    # Also verify live LiveRiskEngineConfig & LiveExecutionEngineConfig generation works cleanly
    live_risk_cfg = cfg.to_nautilus_risk_engine_config()
    live_exec_cfg = cfg.to_nautilus_exec_engine_config()
    assert live_risk_cfg.bypass is False
    assert live_exec_cfg.reconciliation is True

    engine = BacktestEngine(BacktestEngineConfig(trader_id=TraderId(cfg.trader_id), logging=LoggerConfig(stdout_level=LogLevel.ERROR)))
    try:
        engine.add_venue(
            venue=Venue("BINANCE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("50.00000000 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(Decimal("0.001"), Decimal("0.001")),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(inst)
        strat = _Strat(_Cfg(g=gate))
        engine.add_strategy(strat)
        t0 = 1_795_700_000_000_000_000
        engine.add_data(
            [
                QuoteTick(inst.id, Price.from_str("100.00"), Price.from_str("101.00"), Quantity.from_str("1"), Quantity.from_str("1"), t0, t0),
                QuoteTick(inst.id, Price.from_str("100.00"), Price.from_str("101.00"), Quantity.from_str("1"), Quantity.from_str("1"), t0 + 1000, t0 + 1000),
            ]
        )
        engine.run()
    finally:
        engine.dispose()
        store.close()

    reasons = [r["reason"] for r in strat.risk_rejections]
    assert any("SPOT_SHORT_BLOCKED" in r or "SPOT_SELL_EXCEEDS" in r for r in reasons), reasons
    assert any("DAILY_LOSS_LOCK" in r for r in reasons), reasons


def test_mnt4_market_simulation_l2_latency_contention_and_races() -> None:
    inst = _spot_btc_inst()
    res = run_mnt4_market_simulation_suite(spot_btc_instrument=inst)
    assert res["status"] == "PASSED"
    assert res["case1_same_level_contention"]["first_order_got_100"]
    assert res["case1_same_level_contention"]["second_order_consumed_101"]
    assert res["case2_liquidity_shortage_partial"]["filled_qty_matches_l1_depth"]
    assert res["case3_price_jump_missed_fill"]["missed_because_delayed"]
    assert res["case4_cancel_vs_fill_race"]["fast_cancel_won"]
    assert res["case4_cancel_vs_fill_race"]["slow_cancel_filled_first"]
    assert res["case5_spot_proxy_latency_sensitivity"]["sweep"]["0ms"]["fill_px"] == "100.00"
    assert res["case5_spot_proxy_latency_sensitivity"]["sweep"]["50ms"]["fill_px"] == "103.00"


def test_mnt4_spot_fault_recovery_and_credential_boundary(tmp_path: Path) -> None:
    res = run_spot_fault_recovery_suite(tmp_path / "rec")
    assert res["status"] == "PASSED"
    assert res["drop_ack_quarantined"]
    assert res["duplicate_resend_blocked"]
    assert res["transient_not_found_kept_frozen"]
    assert res["transient_resolved_after_retry"]
    assert res["duplicate_report_ignored"]
    assert res["cross_symbol_same_trade_id_preserved"]
    assert res["restart_kill_switch_terminal"]

    cfg = Mnt4RiskConfig()
    bad_host = audit_credential_and_host_boundary(config=cfg, endpoint_urls=("https://evil.example.com/api",))
    assert bad_host["status"] == "FAILED" and bad_host["bad_hosts"] == ["evil.example.com"]
    bad_withdraw = audit_credential_and_host_boundary(
        config=cfg,
        endpoint_urls=("https://demo-fapi.binance.com/fapi/v2/account",),
        live_account_payloads=({"canTrade": True, "canWithdraw": True},),
    )
    assert bad_withdraw["status"] == "FAILED" and len(bad_withdraw["withdraw_violations"]) == 1


def test_mnt4_sandbox_hourly_seal_and_deterministic_replay_zero_diff(tmp_path: Path) -> None:
    inst = _spot_btc_inst()
    sdir = tmp_path / "sandbox"
    cfg = Mnt4RiskConfig(
        environment="SANDBOX",
        account_id="MNT4-SANDBOX-001",
        trader_id="MNT4-SANDBOX-001",
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({"BTCUSDT"}),
        starting_equity_usdt=Decimal("50"),
        max_daily_loss_usdt=Decimal("2"),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={"BTCUSDT": Decimal("0.01")},
    )
    store = PersistentUtcRiskStateStore(
        sdir / "sb.sqlite3",
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
        utc_day="2026-11-26",
    )
    gate = SynchronousRiskGate(config=cfg, store=store)
    metrics: dict = {}
    meta = {"spot_symbol": "BTCUSDT", "risk_starting_equity_usdt": "50", "risk_max_daily_loss_usdt": "2"}
    writer = HourlySanitizedWriter(state_dir=sdir, risk_gate=gate, metrics=metrics, spot_symbol="BTCUSDT", metadata=meta)

    t0 = 1_795_700_000_000_000_000
    quotes = [
        QuoteTick(
            inst.id,
            Price.from_decimal_dp(Decimal("100.00") + Decimal(i % 3), inst.price_precision),
            Price.from_decimal_dp(Decimal("101.00") + Decimal(i % 3), inst.price_precision),
            Quantity.from_str("1.00000"),
            Quantity.from_str("1.00000"),
            t0 + i * 1_000_000,
            t0 + i * 1_000_000,
        )
        for i in range(6)
    ]

    engine = BacktestEngine(BacktestEngineConfig(trader_id=TraderId(cfg.trader_id), logging=LoggerConfig(stdout_level=LogLevel.ERROR)))
    try:
        engine.add_venue(
            venue=Venue("BINANCE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str("50.00000000 USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(Decimal("0.001"), Decimal("0.001")),
            liquidity_consumption=True,
            bar_execution=False,
        )
        engine.add_instrument(inst)
        strat = SandboxHourlyAuditStrategy(
            SandboxHourlyAuditStrategyConfig(
                spot_symbol="BTCUSDT",
                trade_every_n_quotes=2,
                risk_gate=gate,
                hourly_writer=writer,
                metrics=metrics,
                online=False,
                subscribe_factor_usdm=False,
            )
        )
        engine.add_strategy(strat)
        engine.add_data(quotes)
        engine.run()
    finally:
        engine.dispose()
        audit_path = writer.seal_current_hour(reason="test_seal")
        store.close()

    rep = replay_sealed_hour(audit_path, spot_instrument=inst, work_dir=tmp_path / "replay", trade_every_n_quotes=2)
    assert rep["status"] == "PASSED"
    assert rep["events_sha256_verified"]
    assert rep["intents_exact_match"]
    assert rep["replayed_pos_qty"] == rep["live_reported_pos_qty"]
