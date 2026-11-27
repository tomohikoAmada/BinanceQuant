"""M-NT4 End-to-End Verification CLI: Single-Keyed Risk Gate, Dual-Market Recovery, L2 Simulation, & Live Sandbox."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile

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
    LedgerIdentityMismatchError,
    Mnt4RiskConfig,
    PersistentUtcRiskStateStore,
    SingleInstanceLockError,
    SingleKeyedProcessLock,
    SynchronousRiskGate,
)
from bq_nautilus.mnt4_sandbox import run_sandbox_session  # noqa: E402
from bq_nautilus.mnt4_simulation import run_mnt4_market_simulation_suite  # noqa: E402


def _btc_spot_inst():
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


def run_risk_boundary_suite(work_dir: Path) -> dict:
    """Verify deterministic 50 USDT / daily-limit 2 / loss 4 hard lock, modify-up/reversal block, reduce/cancel open."""
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock_path = work_dir / "keyed.lock"
    db_path = work_dir / "risk.sqlite3"

    cfg = Mnt4RiskConfig(
        environment="DEMO",
        account_id="BINANCE-MNT4-50",
        trader_id="MNT4-RISK-001",
        allowed_markets=frozenset({"USDM", "SPOT"}),
        allowed_symbols=frozenset({"BTCUSDT", "SOLUSDT"}),
        starting_equity_usdt=Decimal("50"),
        max_daily_loss_usdt=Decimal("2"),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={"BTCUSDT": Decimal("0.01"), "SOLUSDT": Decimal("1.0")},
    )
    store = PersistentUtcRiskStateStore(
        db_path,
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
        utc_day="2026-11-27",
    )
    gate = SynchronousRiskGate(config=cfg, store=store)

    # 1. Second instance rejected cleanly
    l1 = SingleKeyedProcessLock(lock_path, environment=cfg.environment, account_id=cfg.account_id, trader_id=cfg.trader_id)
    l1.acquire()
    second_blocked = False
    try:
        l2 = SingleKeyedProcessLock(lock_path, environment=cfg.environment, account_id=cfg.account_id, trader_id="SECOND")
        l2.acquire()
    except SingleInstanceLockError:
        second_blocked = True

    # 2. Boot reconciliation required before risk-increasing orders
    pre_reconcile = gate.evaluate_command(
        command_kind="SUBMIT", market="USDM", symbol="SOLUSDT", side="BUY", qty=Decimal("0.10"), reference_price=Decimal("100"), now_utc_day="2026-11-27"
    )
    gate.complete_reconciliation(utc_day="2026-11-27")

    # 3. 50 USDT equity, buy 1 @ 20 then sell 1 @ 16 -> realized loss 4 >= limit 2 -> LOCKED!
    store.record_fill_idempotent(market="SPOT", symbol="SOLUSDT", trade_id="f1", client_order_id="c1", side="BUY", qty=Decimal("1"), price=Decimal("20"))
    store.record_fill_idempotent(market="SPOT", symbol="SOLUSDT", trade_id="f2", client_order_id="c2", side="SELL", qty=Decimal("1"), price=Decimal("16"))
    # Also seed an active LONG USDM position 0.20 so we test cancel + reduce_only while locked vs reversal & modify-up
    store.set_position("USDM", "SOLUSDT", Decimal("0.20"), Decimal("100"))

    buy_blocked = gate.evaluate_command(
        command_kind="SUBMIT", market="USDM", symbol="SOLUSDT", side="BUY", qty=Decimal("0.10"), reference_price=Decimal("100"), now_utc_day="2026-11-27"
    )
    modify_up_blocked = gate.evaluate_command(
        command_kind="MODIFY", market="USDM", symbol="SOLUSDT", side="BUY", qty=Decimal("0.30"), previous_qty=Decimal("0.10"), reference_price=Decimal("100"), now_utc_day="2026-11-27"
    )
    reversal_blocked = gate.evaluate_command(
        command_kind="SUBMIT", market="USDM", symbol="SOLUSDT", side="SELL", qty=Decimal("0.40"), reference_price=Decimal("100"), now_utc_day="2026-11-27"
    )
    cancel_allowed = gate.evaluate_command(
        command_kind="CANCEL", market="USDM", symbol="SOLUSDT", side="BUY", qty=Decimal("0.10"), reference_price=Decimal("100"), now_utc_day="2026-11-27"
    )
    reduce_allowed = gate.evaluate_command(
        command_kind="SUBMIT", market="USDM", symbol="SOLUSDT", side="SELL", qty=Decimal("0.20"), reference_price=Decimal("100"), reduce_only=True, now_utc_day="2026-11-27"
    )

    # 4. Cross-UTC rollover blocks until reconciled again
    rollover_blocked = gate.evaluate_command(
        command_kind="SUBMIT", market="USDM", symbol="SOLUSDT", side="BUY", qty=Decimal("0.10"), reference_price=Decimal("100"), now_utc_day="2026-11-28"
    )

    # 5. Wrong identity cannot open persisted risk DB
    id_mismatch_blocked = False
    try:
        PersistentUtcRiskStateStore(db_path, environment="LIVE", account_id="OTHER", trader_id="BAD", starting_equity_usdt=Decimal("50"))
    except LedgerIdentityMismatchError:
        id_mismatch_blocked = True

    l1.release()
    store.close()
    ok = (
        second_blocked
        and not pre_reconcile.allowed
        and not buy_blocked.allowed
        and not modify_up_blocked.allowed
        and not reversal_blocked.allowed
        and cancel_allowed.allowed
        and reduce_allowed.allowed
        and not rollover_blocked.allowed
        and id_mismatch_blocked
    )
    return {
        "status": "PASSED" if ok else "FAILED",
        "second_keyed_instance_blocked": second_blocked,
        "pre_reconcile_blocked": not pre_reconcile.allowed,
        "daily_loss_locked_at_4_usdt": (not buy_blocked.allowed) and buy_blocked.daily_loss_usdt == Decimal("4"),
        "modify_up_blocked_when_locked": not modify_up_blocked.allowed,
        "cross_zero_reversal_blocked_when_locked": not reversal_blocked.allowed,
        "cancel_allowed_when_locked": cancel_allowed.allowed,
        "reduce_only_allowed_when_locked": reduce_allowed.allowed,
        "utc_rollover_requires_reconcile": not rollover_blocked.allowed,
        "db_identity_mismatch_blocked": id_mismatch_blocked,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--suite",
        choices=("offline", "risk-boundary", "simulation", "dual-recovery", "live-sandbox-smoke"),
        default="offline",
    )
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--sandbox-secs", type=float, default=20.0)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    tmp_ctx = tempfile.TemporaryDirectory() if args.work_dir is None else None
    wdir = Path(tmp_ctx.name) if tmp_ctx is not None else args.work_dir
    try:
        report: dict = {}
        if args.suite in ("offline", "risk-boundary"):
            report["risk_boundary"] = run_risk_boundary_suite(wdir / "risk")
        if args.suite in ("offline", "simulation"):
            report["market_simulation"] = run_mnt4_market_simulation_suite(spot_btc_instrument=_btc_spot_inst())
        if args.suite in ("offline", "dual-recovery"):
            from scripts.mnt3_usdm_verify import _run_mock_demo_e2e_async

            report["spot_fault_recovery"] = run_spot_fault_recovery_suite(wdir / "spot")
            report["usdm_mock_fault_recovery"] = asyncio.run(_run_mock_demo_e2e_async())
            report["credential_host_boundary"] = audit_credential_and_host_boundary(
                config=Mnt4RiskConfig(),
                endpoint_urls=("https://demo-fapi.binance.com/fapi/v2/account", "https://testnet.binance.vision/api/v3/account"),
                live_account_payloads=({"canTrade": True, "canWithdraw": False},),
                live_dual_side_position=False,
            )
        if args.suite == "live-sandbox-smoke":
            report["live_sandbox_smoke"] = asyncio.run(
                run_sandbox_session(state_dir=wdir / "sandbox_smoke", duration_secs=float(args.sandbox_secs))
            )

        txt = json.dumps(report, indent=2)
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(txt + "\n", encoding="utf-8")
        print(txt)

        failed = [k for k, v in report.items() if not isinstance(v, dict) or v.get("status") != "PASSED"]
        if failed:
            raise SystemExit(1)
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()


if __name__ == "__main__":
    main()
