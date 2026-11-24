"""Typed models, statuses, limits, and execution reports for M11 BQOrder."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Literal


class OrderState(str, Enum):
    NEW_PERSISTED = "NEW_PERSISTED"
    SENDING = "SENDING"
    ACKED_OPEN = "ACKED_OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN_PENDING_RECONCILE = "UNKNOWN_PENDING_RECONCILE"
    CLOSED_NOT_FOUND = "CLOSED_NOT_FOUND"


TERMINAL_STATES: frozenset[OrderState] = frozenset(
    {
        OrderState.FILLED,
        OrderState.CANCELED,
        OrderState.REJECTED,
        OrderState.CLOSED_NOT_FOUND,
    }
)

ACTIVE_OR_UNKNOWN_STATES: frozenset[OrderState] = frozenset(
    {
        OrderState.NEW_PERSISTED,
        OrderState.SENDING,
        OrderState.ACKED_OPEN,
        OrderState.PARTIALLY_FILLED,
        OrderState.UNKNOWN_PENDING_RECONCILE,
    }
)


@dataclass(frozen=True, slots=True)
class ExchangeFill:
    fill_id: str
    client_order_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    qty: Decimal
    price: Decimal
    fee: Decimal
    fee_asset: str
    trade_time_ms: int


@dataclass(frozen=True, slots=True)
class ExchangeOrderSnapshot:
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    side: Literal["BUY", "SELL"]
    status: Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"]
    orig_qty: Decimal
    executed_qty: Decimal
    cummulative_quote_qty: Decimal
    avg_price: Decimal | None = None
    fee_quote: Decimal = Decimal(0)
    fee_asset: str = "USDT"
    fills: tuple[ExchangeFill, ...] = ()
    error_code: int | None = None
    error_msg: str | None = None


@dataclass(frozen=True, slots=True)
class LedgerOrderRecord:
    intent_id: str
    client_order_id: str
    market: str
    symbol: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT"]
    requested_qty: Decimal
    limit_price: Decimal | None
    reference_price: Decimal
    state: OrderState
    exchange_order_id: str | None
    cumulative_filled_qty: Decimal
    cumulative_quote_qty: Decimal
    avg_fill_price: Decimal
    cumulative_fee: Decimal
    fee_asset: str
    submit_attempts: int
    last_error: str | None
    created_ts_ns: int
    updated_ts_ns: int
    terminal_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class HardRiskLimits:
    allowed_markets: frozenset[str] = frozenset({"SPOT"})
    allowed_symbols: frozenset[str] = frozenset({"BTCUSDT"})
    max_order_qty: Decimal = Decimal("0.10")
    max_order_notional_quote: Decimal = Decimal("10000")
    max_abs_position_qty: Decimal = Decimal("0.20")
    max_open_orders: int = 3
    max_daily_loss_quote: Decimal = Decimal("500")
    allow_short_spot: bool = False


@dataclass(frozen=True, slots=True)
class KillSwitchState:
    active: bool
    reason: str | None
    local_block_new_orders_ts_ns: int | None
    cancel_sent_ts_ns: int | None
    exchange_terminal_ts_ns: int | None
    cleared_by: str | None = None
    cleared_ts_ns: int | None = None


@dataclass(frozen=True, slots=True)
class SubmitOutcome:
    intent_id: str
    accepted: bool
    state: OrderState
    reason: str | None
    record: LedgerOrderRecord | None = None


@dataclass(frozen=True, slots=True)
class ReconcileSummary:
    checked_count: int
    resolved_count: int
    still_pending_count: int
    kill_switch_terminal_completed: bool
    errors: tuple[str, ...] = field(default_factory=tuple)
