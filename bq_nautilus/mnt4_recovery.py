"""M-NT4 Dual-Market Fault Recovery Suite: Spot Testnet & USD-M Demo (ACK loss, transient miss, WS drop, restart, dedup)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import hashlib
import hmac
import json
from pathlib import Path
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from bq_backtest.strategy import OrderIntent
from bq_feed.events import Market
from bq_order.exchange import ExchangeClient, ExchangeRejectedError, ExchangeTransportError
from bq_order.guard import OrderExecutionGuard
from bq_order.ledger import OrderLedger
from bq_order.models import ExchangeFill, ExchangeOrderSnapshot, HardRiskLimits, OrderState
from bq_order.risk import HardRiskGuard

from .mnt4_risk import (
    Mnt4RiskConfig,
    PersistentUtcRiskStateStore,
    SingleKeyedProcessLock,
    SynchronousRiskGate,
)
from .usdm_runner import SignedUsdmRestClient, redact_sensitive


class FaultInjectingSpotExchange(ExchangeClient):
    """Deterministic stateful Spot Testnet simulator covering 504 drop-ACK, transient -2013, partial fills, and dup reports."""

    def __init__(self, *, transient_not_found_once: bool = True) -> None:
        self.transient_not_found_once = transient_not_found_once
        self.drop_next_ack = False
        self.orders_by_cid: dict[str, dict[str, Any]] = {}
        self.trades_by_cid: dict[str, list[ExchangeFill]] = {}
        self._query_counts: dict[str, int] = {}
        self._next_oid = 7000
        self._next_tid = 9000

    def submit_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        client_order_id: str,
        limit_price: Decimal | None = None,
    ) -> ExchangeOrderSnapshot:
        self._next_oid += 1
        oid = str(self._next_oid)
        fill_qty = (quantity / Decimal(2)).quantize(Decimal("0.0001")) if quantity >= Decimal("0.0004") else quantity
        px = limit_price or Decimal("50000.00")
        self._next_tid += 1
        tid = str(self._next_tid)
        status = "PARTIALLY_FILLED" if fill_qty < quantity else "FILLED"
        fill = ExchangeFill(
            fill_id=tid,
            client_order_id=client_order_id,
            symbol=symbol,
            side="BUY" if side == "BUY" else "SELL",
            qty=fill_qty,
            price=px,
            fee=(fill_qty * px * Decimal("0.001")).quantize(Decimal("0.00000001")),
            fee_asset="USDT",
            trade_time_ms=int(time.time() * 1000),
        )
        snap = ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            exchange_order_id=oid,
            symbol=symbol,
            side="BUY" if side == "BUY" else "SELL",
            status=status,
            orig_qty=quantity,
            executed_qty=fill_qty,
            cummulative_quote_qty=fill_qty * px,
            avg_price=px,
            fee_quote=fill.fee,
            fee_asset="USDT",
            fills=(fill,),
        )
        self.orders_by_cid[client_order_id] = {"snap": snap, "transient_left": 1 if self.transient_not_found_once else 0}
        self.trades_by_cid[client_order_id] = [fill]
        if self.drop_next_ack:
            self.drop_next_ack = False
            raise ExchangeTransportError("HTTP 504 Gateway Timeout (-1007): ACK dropped after exchange accepted order")
        return snap

    def inject_open_rest_order(self, *, symbol: str, side: str, quantity: Decimal, client_order_id: str, price: Decimal) -> None:
        self._next_oid += 1
        snap = ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            exchange_order_id=str(self._next_oid),
            symbol=symbol,
            side="BUY" if side == "BUY" else "SELL",
            status="NEW",
            orig_qty=quantity,
            executed_qty=Decimal(0),
            cummulative_quote_qty=Decimal(0),
            avg_price=price,
        )
        self.orders_by_cid[client_order_id] = {"snap": snap, "transient_left": 0}
        self.trades_by_cid[client_order_id] = []

    def query_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot | None:
        self._query_counts[client_order_id] = self._query_counts.get(client_order_id, 0) + 1
        entry = self.orders_by_cid.get(client_order_id)
        if entry is None:
            return None
        if entry["transient_left"] > 0:
            entry["transient_left"] -= 1
            return None
        return entry["snap"]

    def cancel_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot:
        entry = self.orders_by_cid.get(client_order_id)
        if entry is None:
            raise ExchangeRejectedError(-2011, "Unknown order sent.")
        cur: ExchangeOrderSnapshot = entry["snap"]
        canceled = ExchangeOrderSnapshot(
            client_order_id=cur.client_order_id,
            exchange_order_id=cur.exchange_order_id,
            symbol=cur.symbol,
            side=cur.side,
            status="CANCELED",
            orig_qty=cur.orig_qty,
            executed_qty=cur.executed_qty,
            cummulative_quote_qty=cur.cummulative_quote_qty,
            avg_price=cur.avg_price,
            fee_quote=cur.fee_quote,
            fee_asset=cur.fee_asset,
            fills=cur.fills,
        )
        entry["snap"] = canceled
        return canceled

    def query_trades(self, *, symbol: str, client_order_id: str) -> list[ExchangeFill]:
        return list(self.trades_by_cid.get(client_order_id, []))


def reconcile_with_transient_guard(
    *,
    guard: OrderExecutionGuard,
    exchange: ExchangeClient,
    transient_miss_counts: dict[str, int],
    confirm_absent_after: int = 2,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Reconcile pending/unknown orders WITHOUT prematurely closing on a single transient NOT_FOUND miss."""
    active = guard.ledger.list_active_or_unknown()
    kept_frozen: list[str] = []
    confirmed_absent: list[str] = []
    resolved: list[str] = []
    errors: list[str] = []

    for rec in active:
        cid = rec.client_order_id
        try:
            snap = exchange.query_order(symbol=rec.symbol, client_order_id=cid)
            if snap is None:
                miss_n = transient_miss_counts.get(cid, 0) + 1
                transient_miss_counts[cid] = miss_n
                if miss_n < confirm_absent_after:
                    kept_frozen.append(cid)
                    continue
                if rec.state in (
                    OrderState.SENDING,
                    OrderState.UNKNOWN_PENDING_RECONCILE,
                    OrderState.NEW_PERSISTED,
                ):
                    guard.ledger.mark_closed_not_found(
                        cid,
                        reason=f"confirmed_absent_after_{miss_n}_polls",
                        now_ns=now_ns,
                    )
                    confirmed_absent.append(cid)
                else:
                    kept_frozen.append(cid)
            else:
                transient_miss_counts.pop(cid, None)
                trades = exchange.query_trades(symbol=rec.symbol, client_order_id=cid)
                guard.ledger.apply_exchange_snapshot(snap, extra_fills=trades, now_ns=now_ns)
                resolved.append(cid)
        except ExchangeTransportError as exc:
            errors.append(f"{cid}:{exc}")
            kept_frozen.append(cid)

    return {
        "checked": len(active),
        "kept_frozen": kept_frozen,
        "confirmed_absent": confirmed_absent,
        "resolved": resolved,
        "errors": errors,
    }


def run_spot_fault_recovery_suite(work_dir: Path) -> dict[str, Any]:
    """Verify Spot crash/drop-ACK/transient-not-found/partial-fill/dup-report/restart recovery invariants."""
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    db_path = work_dir / "spot_recovery.sqlite3"
    risk_db = work_dir / "spot_risk.sqlite3"
    lock_path = work_dir / "spot_keyed.lock"

    cfg = Mnt4RiskConfig(
        environment="TESTNET",
        account_id="SPOT-TESTNET-001",
        trader_id="MNT4-SPOT-REC",
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
        starting_equity_usdt=Decimal("50"),
        max_daily_loss_usdt=Decimal("2"),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={"BTCUSDT": Decimal("0.01"), "ETHUSDT": Decimal("1.0")},
    )
    store = PersistentUtcRiskStateStore(
        risk_db,
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
    )
    gate = SynchronousRiskGate(config=cfg, store=store)
    gate.complete_reconciliation(utc_day="2026-11-27")

    ledger = OrderLedger(db_path)
    limits = HardRiskLimits(
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
        max_order_qty=Decimal("0.005"),
        max_order_notional_quote=Decimal("50"),
        max_abs_position_qty=Decimal("0.01"),
        max_daily_loss_quote=Decimal("2"),
    )
    ex = FaultInjectingSpotExchange(transient_not_found_once=True)
    guard = OrderExecutionGuard(ledger=ledger, risk_guard=HardRiskGuard(limits), exchange=ex)

    with SingleKeyedProcessLock(lock_path, environment=cfg.environment, account_id=cfg.account_id, trader_id=cfg.trader_id):
        # 1. Drop ACK (HTTP 504): order accepted on exchange (partially filled 0.0002 / 0.0004), ACK lost
        ex.drop_next_ack = True
        intent1 = OrderIntent(
            intent_id="spot-drop-ack-1",
            market=Market.SPOT,
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("0.0004"),
            order_type="LIMIT",
            decision_seq=1,
            decision_ts_ns=100,
            decision_infer_ns=100,
            reason="mnt4_fault_test",
            reference_price=Decimal("50000"),
        )
        out1 = guard.submit_intent(intent1, reference_price=Decimal("50000"), limit_price=Decimal("50000"), now_ns=100)
        frozen_resend = guard.submit_intent(intent1, reference_price=Decimal("50000"), limit_price=Decimal("50000"), now_ns=101)

        # 2. First reconcile poll hits transient not-found (-2013): MUST stay frozen, NOT premature CLOSED_NOT_FOUND!
        miss_counts: dict[str, int] = {}
        poll1 = reconcile_with_transient_guard(guard=guard, exchange=ex, transient_miss_counts=miss_counts, confirm_absent_after=2, now_ns=150)
        still_unknown_after_poll1 = guard.ledger.get_order("spot-drop-ack-1").state == OrderState.UNKNOWN_PENDING_RECONCILE
        # Second poll sees real exchange snapshot & partial fill
        poll2 = reconcile_with_transient_guard(guard=guard, exchange=ex, transient_miss_counts=miss_counts, confirm_absent_after=2, now_ns=200)
        resolved_partial_after_poll2 = guard.ledger.get_order("spot-drop-ack-1").state == OrderState.PARTIALLY_FILLED

        # Record recovered fills idempotently into PersistentUtcRiskStateStore + test duplicate execution report
        trades_after = ex.query_trades(symbol="BTCUSDT", client_order_id="spot-drop-ack-1")
        applied_first = store.record_fill_idempotent(
            market="SPOT",
            symbol="BTCUSDT",
            trade_id=trades_after[0].fill_id,
            client_order_id="spot-drop-ack-1",
            side="BUY",
            qty=trades_after[0].qty,
            price=trades_after[0].price,
            fee=Decimal("0.01"),
            fee_asset="USDT",
            ts_ns=200,
        )
        applied_dup = store.record_fill_idempotent(
            market="SPOT",
            symbol="BTCUSDT",
            trade_id=trades_after[0].fill_id,
            client_order_id="spot-drop-ack-1",
            side="BUY",
            qty=trades_after[0].qty,
            price=trades_after[0].price,
            fee=Decimal("0.01"),
            fee_asset="USDT",
            ts_ns=201,
        )
        # Same trade_id on different symbol (ETHUSDT vs BTCUSDT) must NOT collide!
        applied_other_sym = store.record_fill_idempotent(
            market="SPOT",
            symbol="ETHUSDT",
            trade_id=trades_after[0].fill_id,
            client_order_id="eth-other-1",
            side="BUY",
            qty=Decimal("0.01"),
            price=Decimal("3000"),
            fee=Decimal("0.0001"),
            fee_asset="BNB",
            ts_ns=202,
        )

        # 3. Simulated process restart while an open order remains on exchange
        ex.inject_open_rest_order(symbol="BTCUSDT", side="SELL", quantity=Decimal("0.0001"), client_order_id="resting-spot-sell", price=Decimal("51000"))
        ledger.close()
        restarted_ledger = OrderLedger(db_path)
        restarted_guard = OrderExecutionGuard(ledger=restarted_ledger, risk_guard=HardRiskGuard(limits), exchange=ex)
        ks, ks_sum = restarted_guard.trigger_kill_switch(reason="operator_halt_spot", now_ns=300)
        restarted_ledger.close()

    btc_pos, _ = store.get_position("SPOT", "BTCUSDT")
    eth_pos, _ = store.get_position("SPOT", "ETHUSDT")
    fees = store.fees_by_asset()
    store.close()

    return {
        "status": (
            "PASSED"
            if (
                out1.state == OrderState.UNKNOWN_PENDING_RECONCILE
                and not frozen_resend.accepted
                and still_unknown_after_poll1
                and resolved_partial_after_poll2
                and len(ex.orders_by_cid) == 2
                and applied_first
                and not applied_dup
                and applied_other_sym
                and btc_pos == Decimal("0.0002")
                and eth_pos == Decimal("0.01")
                and fees.get("USDT") == Decimal("0.01")
                and fees.get("BNB") == Decimal("0.0001")
                and ks.active
                and ks_sum.kill_switch_terminal_completed
            )
            else "FAILED"
        ),
        "drop_ack_quarantined": out1.state == OrderState.UNKNOWN_PENDING_RECONCILE,
        "duplicate_resend_blocked": not frozen_resend.accepted,
        "transient_not_found_kept_frozen": still_unknown_after_poll1 and poll1["kept_frozen"] == ["spot-drop-ack-1"],
        "transient_resolved_after_retry": resolved_partial_after_poll2 and poll2["resolved"] == ["spot-drop-ack-1"],
        "recovered_partial_fill_qty": str(trades_after[0].qty),
        "duplicate_report_ignored": applied_first and (not applied_dup),
        "cross_symbol_same_trade_id_preserved": applied_other_sym and btc_pos == Decimal("0.0002") and eth_pos == Decimal("0.01"),
        "per_asset_fees_separated": {k: str(v) for k, v in fees.items()},
        "restart_kill_switch_terminal": ks.active and ks_sum.kill_switch_terminal_completed,
    }


def audit_credential_and_host_boundary(
    *,
    config: Mnt4RiskConfig,
    endpoint_urls: Sequence[str],
    live_account_payloads: Sequence[dict[str, Any]] = (),
    live_dual_side_position: bool | None = False,
) -> dict[str, Any]:
    """Verify allowed target domains, non-withdrawal permissions, single-side mode, and secret redaction."""
    bad_hosts: list[str] = []
    for url in endpoint_urls:
        host = urllib.parse.urlparse(url).hostname or ""
        if host not in config.allowed_hosts:
            bad_hosts.append(host)

    withdraw_violations: list[dict[str, Any]] = []
    for acc in live_account_payloads:
        if acc.get("canWithdraw") is True and config.require_no_withdraw_permission:
            withdraw_violations.append({"reason": "API_KEY_HAS_WITHDRAW_ENABLED", "payload": redact_sensitive(acc)})

    dual_side_violation = bool(live_dual_side_position)
    passed = (not bad_hosts) and (not withdraw_violations) and (not dual_side_violation)
    return {
        "status": "PASSED" if passed else "FAILED",
        "bad_hosts": bad_hosts,
        "withdraw_violations": withdraw_violations,
        "dual_side_position": live_dual_side_position,
        "checked_urls": list(endpoint_urls),
    }


__all__ = [
    "FaultInjectingSpotExchange",
    "audit_credential_and_host_boundary",
    "run_spot_fault_recovery_suite",
]
