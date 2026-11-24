"""Idempotent OrderExecutionGuard implementing pre-send WAL persistence, crash recovery, and Kill Switch."""

from __future__ import annotations

from decimal import Decimal
import time
from typing import Literal

from bq_backtest.strategy import OrderIntent

from .exchange import ExchangeClient, ExchangeRejectedError, ExchangeTransportError
from .ledger import OrderLedger
from .models import (
    ACTIVE_OR_UNKNOWN_STATES,
    ExchangeOrderSnapshot,
    KillSwitchState,
    OrderState,
    ReconcileSummary,
    SubmitOutcome,
    TERMINAL_STATES,
)
from .risk import HardRiskGuard


class OrderExecutionGuard:
    """Isolated credentialed execution guard obeying Team C M11 crash/ACK/kill-switch invariants."""

    def __init__(
        self,
        *,
        ledger: OrderLedger,
        risk_guard: HardRiskGuard,
        exchange: ExchangeClient,
    ) -> None:
        self._ledger = ledger
        self._risk = risk_guard
        self._exchange = exchange

    @property
    def ledger(self) -> OrderLedger:
        return self._ledger

    @property
    def risk_guard(self) -> HardRiskGuard:
        return self._risk

    def submit_intent(
        self,
        intent: OrderIntent,
        *,
        reference_price: Decimal,
        limit_price: Decimal | None = None,
        daily_realized_loss_quote: Decimal = Decimal(0),
        now_ns: int | None = None,
    ) -> SubmitOutcome:
        # 1. Check hard risk & Kill Switch BEFORE touching exchange
        reject_reason = self._risk.evaluate(
            intent,
            reference_price=reference_price,
            ledger=self._ledger,
            daily_realized_loss_quote=daily_realized_loss_quote,
        )
        if reject_reason is not None:
            return SubmitOutcome(
                intent_id=intent.intent_id,
                accepted=False,
                state=OrderState.REJECTED,
                reason=reject_reason,
                record=None,
            )

        wire_order_type: Literal["MARKET", "LIMIT"] = "LIMIT" if intent.order_type == "LIMIT" else "MARKET"

        # 2. Persist pre-wire send intent atomically in SQLite WAL; NEVER re-send duplicate cid
        created_new, rec = self._ledger.reserve_before_send(
            intent_id=intent.intent_id,
            client_order_id=intent.intent_id,
            market=intent.market.value,
            symbol=intent.symbol,
            side="BUY" if intent.side == "BUY" else "SELL",
            order_type=wire_order_type,
            quantity=intent.quantity,
            limit_price=limit_price,
            reference_price=reference_price,
            now_ns=now_ns,
        )
        if not created_new:
            if rec.state in (OrderState.SENDING, OrderState.UNKNOWN_PENDING_RECONCILE):
                return SubmitOutcome(
                    intent_id=intent.intent_id,
                    accepted=False,
                    state=rec.state,
                    reason="FROZEN_PENDING_RECONCILE: duplicate resend blocked",
                    record=rec,
                )
            return SubmitOutcome(
                intent_id=intent.intent_id,
                accepted=False,
                state=rec.state,
                reason=f"DUPLICATE_INTENT_IDEMPOTENT:{rec.state.value}",
                record=rec,
            )

        # 3. Single wire transmission
        try:
            snap = self._exchange.submit_order(
                symbol=intent.symbol,
                side=rec.side,
                order_type=rec.order_type,
                quantity=intent.quantity,
                client_order_id=rec.client_order_id,
                limit_price=limit_price,
            )
            updated = self._ledger.apply_exchange_snapshot(snap, now_ns=now_ns)
            return SubmitOutcome(
                intent_id=intent.intent_id,
                accepted=True,
                state=updated.state,
                reason=None,
                record=updated,
            )
        except ExchangeRejectedError as exc:
            rej = ExchangeOrderSnapshot(
                client_order_id=rec.client_order_id,
                exchange_order_id=None,
                symbol=rec.symbol,
                side=rec.side,
                status="REJECTED",
                orig_qty=rec.requested_qty,
                executed_qty=Decimal(0),
                cummulative_quote_qty=Decimal(0),
                error_code=exc.code,
                error_msg=exc.msg,
            )
            updated = self._ledger.apply_exchange_snapshot(rej, now_ns=now_ns)
            return SubmitOutcome(
                intent_id=intent.intent_id,
                accepted=False,
                state=updated.state,
                reason=f"EXCHANGE_REJECTED:{exc.code}:{exc.msg}",
                record=updated,
            )
        except ExchangeTransportError as exc:
            # Timeout/drop ACK -> mark UNKNOWN_PENDING_RECONCILE and FREEZE blind resend
            unk = self._ledger.mark_unknown_after_error(
                rec.intent_id, error=str(exc), now_ns=now_ns
            )
            return SubmitOutcome(
                intent_id=intent.intent_id,
                accepted=False,
                state=unk.state,
                reason=f"TRANSPORT_UNKNOWN_FROZEN:{exc}",
                record=unk,
            )

    def recover_pending(self, *, now_ns: int | None = None) -> ReconcileSummary:
        """Query exchange FIRST for SENDING / UNKNOWN / active orders on startup or reconciliation."""
        active = self._ledger.list_active_or_unknown()
        resolved = 0
        still_pending = 0
        errors: list[str] = []

        for rec in active:
            try:
                snap = self._exchange.query_order(
                    symbol=rec.symbol, client_order_id=rec.client_order_id
                )
                if snap is None:
                    # Only treat as safely absent if it never finished wire acknowledgement
                    if rec.state in (
                        OrderState.SENDING,
                        OrderState.UNKNOWN_PENDING_RECONCILE,
                        OrderState.NEW_PERSISTED,
                    ):
                        self._ledger.mark_closed_not_found(
                            rec.client_order_id,
                            reason="reconciled_not_found_on_exchange",
                            now_ns=now_ns,
                        )
                        resolved += 1
                    else:
                        still_pending += 1
                else:
                    trades = self._exchange.query_trades(
                        symbol=rec.symbol, client_order_id=rec.client_order_id
                    )
                    updated = self._ledger.apply_exchange_snapshot(
                        snap, extra_fills=trades, now_ns=now_ns
                    )
                    if updated.state in TERMINAL_STATES:
                        resolved += 1
                    else:
                        still_pending += 1
            except ExchangeTransportError as exc:
                errors.append(f"{rec.client_order_id}:{exc}")
                still_pending += 1

        ks = self._ledger.get_kill_switch()
        ks_done = False
        if ks.active and still_pending == 0 and not self._ledger.list_active_or_unknown():
            self._ledger.record_kill_switch_terminal(now_ns=now_ns)
            ks_done = True

        return ReconcileSummary(
            checked_count=len(active),
            resolved_count=resolved,
            still_pending_count=still_pending,
            kill_switch_terminal_completed=ks_done,
            errors=tuple(errors),
        )

    def trigger_kill_switch(self, *, reason: str, now_ns: int | None = None) -> tuple[KillSwitchState, ReconcileSummary]:
        """Persist Kill Switch immediately, issue cancels for open/unknown orders, and track 3 timestamps."""
        t0 = time.monotonic_ns() if now_ns is None else now_ns
        ks = self._ledger.activate_kill_switch(reason=reason, now_ns=t0)
        errors: list[str] = []

        # Step 1: Resolve unknown/sending orders first so we know what is actually open
        self.recover_pending(now_ns=t0)

        # Step 2: Issue cancel for every active order and record cancel_sent_ts_ns
        open_orders = self._ledger.list_orders(states=ACTIVE_OR_UNKNOWN_STATES)
        t_cancel = t0 + 1 if now_ns is not None else None
        if open_orders:
            ks = self._ledger.record_kill_switch_cancel_sent(now_ns=t_cancel)
        for rec in open_orders:
            try:
                snap = self._exchange.cancel_order(
                    symbol=rec.symbol, client_order_id=rec.client_order_id
                )
                trades = self._exchange.query_trades(
                    symbol=rec.symbol, client_order_id=rec.client_order_id
                )
                self._ledger.apply_exchange_snapshot(
                    snap, extra_fills=trades, now_ns=t_cancel
                )
            except ExchangeRejectedError:
                # Race condition (e.g., partial/full fill completed before cancel arrived): reconcile state
                try:
                    snap = self._exchange.query_order(
                        symbol=rec.symbol, client_order_id=rec.client_order_id
                    )
                    trades = self._exchange.query_trades(
                        symbol=rec.symbol, client_order_id=rec.client_order_id
                    )
                    if snap is not None:
                        self._ledger.apply_exchange_snapshot(
                            snap, extra_fills=trades, now_ns=t_cancel
                        )
                except ExchangeTransportError as exc2:
                    errors.append(f"{rec.client_order_id}:{exc2}")
            except ExchangeTransportError as exc:
                errors.append(f"{rec.client_order_id}:{exc}")

        if not open_orders:
            self._ledger.record_kill_switch_cancel_sent(now_ns=t_cancel)

        t_term = (t_cancel + 1) if t_cancel is not None else None
        remaining = self._ledger.list_active_or_unknown()
        ks_done = len(remaining) == 0 and len(errors) == 0
        if ks_done:
            ks = self._ledger.record_kill_switch_terminal(now_ns=t_term)
        else:
            ks = self._ledger.get_kill_switch()

        summary = ReconcileSummary(
            checked_count=len(open_orders),
            resolved_count=max(0, len(open_orders) - len(remaining)),
            still_pending_count=len(remaining),
            kill_switch_terminal_completed=ks_done,
            errors=tuple(errors),
        )
        return ks, summary

    def poll_kill_switch_reconcile(self, *, now_ns: int | None = None) -> tuple[KillSwitchState, ReconcileSummary]:
        """Continue cancelling/reconciling remaining open orders after network recovery."""
        ks = self._ledger.get_kill_switch()
        if not ks.active:
            return ks, ReconcileSummary(0, 0, 0, False, ())
        return self.trigger_kill_switch(reason=ks.reason or "kill_switch_retry", now_ns=now_ns)

    def clear_kill_switch(self, *, operator: str, now_ns: int | None = None) -> KillSwitchState:
        """Explicitly clear Kill Switch ONLY after all open/unknown orders reached terminal states."""
        remaining = self._ledger.list_active_or_unknown()
        if remaining:
            raise RuntimeError(
                f"Cannot clear kill switch while {len(remaining)} orders remain non-terminal"
            )
        return self._ledger.clear_kill_switch(cleared_by=operator, now_ns=now_ns)
