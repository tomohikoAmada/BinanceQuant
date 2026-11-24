"""F1/F2 FillSimulators with shared remaining level qty, gap skipping, and ExchangeFilters."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Literal

from .strategy import OrderIntent


@dataclass(frozen=True, slots=True)
class ExchangeFilters:
    tick_size: Decimal | None = None
    step_size: Decimal | None = None
    min_notional: Decimal | None = None
    is_known: bool = True

    def round_step_down(self, qty: Decimal) -> Decimal:
        if self.step_size is None or self.step_size <= 0:
            return qty
        steps = (qty / self.step_size).to_integral_value(rounding=ROUND_DOWN)
        return steps * self.step_size

    def round_tick(self, price: Decimal) -> Decimal:
        if self.tick_size is None or self.tick_size <= 0:
            return price
        ticks = (price / self.tick_size).to_integral_value(rounding=ROUND_DOWN)
        return ticks * self.tick_size


@dataclass(slots=True)
class OrderBookView:
    symbol: str
    book_version: int
    bids: tuple[tuple[Decimal, Decimal], ...]  # descending price
    asks: tuple[tuple[Decimal, Decimal], ...]  # ascending price
    health_valid: bool = True
    crossed: bool = False


@dataclass(frozen=True, slots=True)
class FillResult:
    status: Literal[
        "FILLED",
        "PARTIAL",
        "SKIPPED_GAP",
        "REJECTED_UNKNOWN_FILTERS",
        "REJECTED_MIN_NOTIONAL",
        "REJECTED_ZERO_QTY",
        "REJECTED_INSUFFICIENT_FUNDS",
        "REJECTED_POSITION_LIMIT",
        "UNFILLED_EMPTY",
    ]
    filled_qty: Decimal
    avg_fill_price: Decimal
    notional_quote: Decimal
    fee_quote: Decimal
    fee_asset: str
    levels_consumed: int
    reason: str = ""


class FillSimulator:
    """Deterministic F1 (top-of-book) & F2 (K-level sweep) simulator sharing remaining book qty per side."""

    def __init__(
        self,
        *,
        mode: Literal["F1", "F2"] = "F1",
        taker_fee_bps: Decimal = Decimal("10"),
        fee_asset: str = "USDT",
        max_sweep_levels: int = 10,
    ) -> None:
        self._mode = mode
        self._fee_bps = taker_fee_bps
        self._fee_asset = fee_asset
        self._max_levels = max_sweep_levels
        self._last_side_levels: dict[tuple[str, str], tuple[tuple[Decimal, Decimal], ...]] = {}
        self._remaining: dict[tuple[str, str, Decimal], Decimal] = {}

    def _sync_book(self, book: OrderBookView) -> None:
        # Reset remaining liquidity independently per side ONLY when that side's visible snapshot changed.
        for side_name, levels in (("BID", book.bids), ("ASK", book.asks)):
            skey = (book.symbol, side_name)
            if self._last_side_levels.get(skey) != levels:
                self._last_side_levels[skey] = levels
                stale_keys = [k for k in self._remaining if k[0] == book.symbol and k[1] == side_name]
                for k in stale_keys:
                    del self._remaining[k]
                for p, q in levels:
                    self._remaining[(book.symbol, side_name, p)] = q

    def execute(
        self,
        intent: OrderIntent,
        book: OrderBookView,
        filters: ExchangeFilters,
        *,
        max_buy_notional_plus_fee: Decimal | None = None,
    ) -> FillResult:
        if not filters.is_known:
            return FillResult("REJECTED_UNKNOWN_FILTERS", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "D4_unknown_filters")
        if not book.health_valid or book.crossed:
            return FillResult("SKIPPED_GAP", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "stream_gap_or_crossed")

        self._sync_book(book)

        if intent.side == "BUY":
            opp_side = "ASK"
            levels = book.asks
        else:
            opp_side = "BID"
            levels = book.bids

        if not levels:
            return FillResult("UNFILLED_EMPTY", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "empty_opposite_book")

        max_lvls = 1 if self._mode == "F1" else min(len(levels), self._max_levels)
        req_qty = filters.round_step_down(intent.quantity)
        if req_qty <= 0:
            return FillResult("REJECTED_ZERO_QTY", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "below_step_size")

        fee_mult = Decimal(1) + (self._fee_bps / Decimal("10000"))
        remaining_needed = req_qty
        filled_total = Decimal(0)
        notional_total = Decimal(0)
        consumed_lvls = 0
        tentative: list[tuple[tuple[str, str, Decimal], Decimal]] = []

        for idx in range(max_lvls):
            lvl_price_raw, _ = levels[idx]
            key = (book.symbol, opp_side, lvl_price_raw)
            avail = self._remaining.get(key, Decimal(0))
            if avail <= 0:
                continue
            lvl_price = filters.round_tick(lvl_price_raw)
            cap_qty = remaining_needed
            if intent.side == "BUY" and max_buy_notional_plus_fee is not None and lvl_price > 0:
                rem_budget = max_buy_notional_plus_fee - notional_total * fee_mult
                if rem_budget <= 0:
                    if filled_total == 0:
                        return FillResult("REJECTED_INSUFFICIENT_FUNDS", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "insufficient_quote_cash")
                    break
                max_lvl_qty = filters.round_step_down(rem_budget / (lvl_price * fee_mult))
                cap_qty = min(cap_qty, max_lvl_qty)
            take = filters.round_step_down(min(cap_qty, avail))
            if take <= 0:
                if intent.side == "BUY" and max_buy_notional_plus_fee is not None and filled_total == 0:
                    return FillResult("REJECTED_INSUFFICIENT_FUNDS", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "insufficient_quote_cash")
                continue
            filled_total += take
            notional_total += take * lvl_price
            tentative.append((key, avail - take))
            remaining_needed -= take
            consumed_lvls += 1
            if remaining_needed <= 0:
                break

        if filled_total <= 0:
            return FillResult("UNFILLED_EMPTY", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "visible_liquidity_exhausted")

        if filters.min_notional is not None and notional_total < filters.min_notional:
            # Zero side effects: do NOT commit tentative liquidity reductions!
            return FillResult("REJECTED_MIN_NOTIONAL", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, f"notional {notional_total} < min_notional {filters.min_notional}")

        fee = notional_total * (self._fee_bps / Decimal("10000"))
        if intent.side == "BUY" and max_buy_notional_plus_fee is not None and (notional_total + fee) > max_buy_notional_plus_fee:
            return FillResult("REJECTED_INSUFFICIENT_FUNDS", Decimal(0), Decimal(0), Decimal(0), Decimal(0), self._fee_asset, 0, "insufficient_quote_cash")

        # Atomically commit remaining liquidity only after all checks pass
        for key, new_avail in tentative:
            self._remaining[key] = new_avail

        avg_px = notional_total / filled_total
        status: Literal["FILLED", "PARTIAL"] = "FILLED" if filled_total >= req_qty else "PARTIAL"
        return FillResult(
            status=status,
            filled_qty=filled_total,
            avg_fill_price=avg_px,
            notional_quote=notional_total,
            fee_quote=fee,
            fee_asset=self._fee_asset,
            levels_consumed=consumed_lvls,
            reason=f"{self._mode}_fill",
        )
