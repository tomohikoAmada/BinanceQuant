"""Independent pre-trade hard risk limits inside BQOrder."""

from __future__ import annotations

from decimal import Decimal

from bq_backtest.strategy import OrderIntent

from .ledger import OrderLedger
from .models import ACTIVE_OR_UNKNOWN_STATES, HardRiskLimits


class HardRiskGuard:
    """Evaluates hard risk invariants against durable SQLite state before any network order."""

    def __init__(self, limits: HardRiskLimits) -> None:
        self._limits = limits

    @property
    def limits(self) -> HardRiskLimits:
        return self._limits

    def evaluate(
        self,
        intent: OrderIntent,
        *,
        reference_price: Decimal,
        ledger: OrderLedger,
        daily_realized_loss_quote: Decimal = Decimal(0),
    ) -> str | None:
        ks = ledger.get_kill_switch()
        if ks.active:
            return f"KILL_SWITCH_ACTIVE:{ks.reason or 'locked'}"

        if intent.market.value not in self._limits.allowed_markets:
            return f"DISALLOWED_MARKET:{intent.market.value}"
        if intent.symbol not in self._limits.allowed_symbols:
            return f"DISALLOWED_SYMBOL:{intent.symbol}"

        if daily_realized_loss_quote >= self._limits.max_daily_loss_quote:
            return f"DAILY_LOSS_LIMIT:{daily_realized_loss_quote}>={self._limits.max_daily_loss_quote}"

        qty = intent.quantity
        if qty <= 0 or qty > self._limits.max_order_qty:
            return f"ORDER_QTY_LIMIT:{qty}>{self._limits.max_order_qty}"

        notional = qty * reference_price
        if notional <= 0 or notional > self._limits.max_order_notional_quote:
            return f"ORDER_NOTIONAL_LIMIT:{notional}>{self._limits.max_order_notional_quote}"

        open_orders = ledger.list_orders(states=ACTIVE_OR_UNKNOWN_STATES)
        if len(open_orders) >= self._limits.max_open_orders:
            return f"MAX_OPEN_ORDERS:{len(open_orders)}>={self._limits.max_open_orders}"

        cur_pos = ledger.net_position_qty(intent.symbol)
        next_pos = cur_pos + qty if intent.side == "BUY" else cur_pos - qty
        if intent.market.value == "SPOT" and not self._limits.allow_short_spot and next_pos < 0:
            return f"SPOT_SHORT_BLOCKED:{next_pos}"
        if abs(next_pos) > self._limits.max_abs_position_qty:
            return f"MAX_ABS_POSITION:{abs(next_pos)}>{self._limits.max_abs_position_qty}"

        return None
