"""Independent Decimal Isolated USD-M Perpetual Accounting, Risk, Funding & Liquidation Audit (M-NT3)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Literal, Sequence

from bq_feed.events import Market

from .instruments import (
    BmdInstrumentSpec,
    DEFAULT_USDM_TIER1_BRACKETS,
    LeverageBracketTier,
    UnknownInstrumentSpecError,
    compute_tiered_maintenance_margin,
)


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError(f"Step must be positive, got {step!r}")
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return (units * step).quantize(step.normalize() if step == step.normalize() else step)


@dataclass(frozen=True, slots=True)
class UsdmOrderPlan:
    """Result of risk + margin + Binance filter sizing and post-floor revalidation."""

    symbol: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT"]
    accepted: bool
    reject_reason: str | None
    exec_price: Decimal
    raw_qty: Decimal
    qty: Decimal
    notional_usdt: Decimal
    leverage: int
    bracket_tier: int
    mmr: Decimal
    cum: Decimal
    initial_margin_usdt: Decimal
    maint_margin_usdt: Decimal
    open_fee_usdt: Decimal
    risk_allowed_notional_usdt: Decimal
    margin_allowed_notional_usdt: Decimal
    close_position: bool = False
    wire_omit_quantity: bool = False
    wire_omit_reduce_only: bool = False


def plan_usdm_isolated_order(
    *,
    spec: BmdInstrumentSpec,
    brackets: Sequence[LeverageBracketTier],
    leverage: int,
    side: Literal["BUY", "SELL"],
    order_type: Literal["MARKET", "LIMIT"],
    ref_price: Decimal,
    free_collateral_usdt: Decimal,
    loss_budget_usdt: Decimal,
    stop_distance_pct: Decimal,
    round_trip_fee_rate: Decimal,
    adverse_slippage_rate: Decimal,
    taker_fee_rate: Decimal,
    max_order_notional_usdt: Decimal | None = None,
    max_position_notional_usdt: Decimal | None = None,
    existing_position_notional_usdt: Decimal = Decimal(0),
    fee_buffer_rate: Decimal = Decimal("0.001"),
    is_reduce_only: bool = False,
    current_position_qty: Decimal = Decimal(0),
    open_reduce_only_qty: Decimal = Decimal(0),
    requested_qty: Decimal | None = None,
    close_position: bool = False,
) -> UsdmOrderPlan:
    """Size a one-way isolated USD-M order from risk & margin budgets, floor to step, and revalidate.

    Opening orders size from min(risk_budget, free_collateral, caps). Reduce-only / closePosition
    orders bypass opening margin/loss budgets and size strictly from signed reconciled net
    exposure minus resting reduce-only orders, rejecting flat/wrong-direction/oversized cases.
    Never clamps an opening budget below exchange MIN_NOTIONAL/minQty upward: under-minimum plans reject.
    """
    if spec.market != Market.USDM:
        raise ValueError(f"Expected Market.USDM spec, got {spec.market}")
    if ref_price <= 0:
        raise ValueError(f"Reference price must be positive, got {ref_price}")
    if leverage < 1:
        raise ValueError(f"Leverage must be >= 1, got {leverage}")
    if open_reduce_only_qty < 0:
        raise ValueError(f"open_reduce_only_qty must be non-negative, got {open_reduce_only_qty}")
    if not brackets:
        raise UnknownInstrumentSpecError(f"No leverage brackets supplied for {spec.symbol}")

    tier1 = sorted(brackets, key=lambda b: b.notional_floor)[0]

    def _reject(
        reason: str,
        raw_qty: Decimal = Decimal(0),
        qty: Decimal = Decimal(0),
        risk_n: Decimal = Decimal(0),
        marg_n: Decimal = Decimal(0),
    ) -> UsdmOrderPlan:
        return UsdmOrderPlan(
            symbol=spec.symbol,
            side=side,
            order_type=order_type,
            accepted=False,
            reject_reason=reason,
            exec_price=ref_price,
            raw_qty=raw_qty,
            qty=qty,
            notional_usdt=qty * ref_price,
            leverage=leverage,
            bracket_tier=tier1.tier,
            mmr=tier1.maint_margin_ratio,
            cum=tier1.cum,
            initial_margin_usdt=Decimal(0),
            maint_margin_usdt=Decimal(0),
            open_fee_usdt=Decimal(0),
            risk_allowed_notional_usdt=risk_n,
            margin_allowed_notional_usdt=marg_n,
            close_position=close_position,
            wire_omit_quantity=close_position,
            wire_omit_reduce_only=close_position,
        )

    if spec.trading_status != "TRADING":
        return _reject(f"TRADING_STATUS_{spec.trading_status}")

    exec_price = _floor_to_step(ref_price, spec.tick_size)
    if exec_price <= 0:
        return _reject("PRICE_BELOW_TICK")
    if spec.min_price is not None and exec_price < spec.min_price:
        return _reject("PRICE_BELOW_MIN_PRICE")
    if spec.max_price is not None and exec_price > spec.max_price:
        return _reject("PRICE_ABOVE_MAX_PRICE")

    eff_step = spec.step_size
    eff_min_qty = spec.min_quantity or spec.step_size
    eff_max_qty = spec.max_quantity
    if order_type == "MARKET":
        if spec.market_step_size is not None and spec.market_step_size > eff_step:
            eff_step = spec.market_step_size
        if spec.market_min_quantity is not None and spec.market_min_quantity > eff_min_qty:
            eff_min_qty = spec.market_min_quantity
        if spec.market_max_quantity is not None:
            eff_max_qty = (
                min(eff_max_qty, spec.market_max_quantity)
                if eff_max_qty is not None
                else spec.market_max_quantity
            )

    if is_reduce_only or close_position:
        if current_position_qty == 0:
            return _reject("NO_OPEN_POSITION_FOR_REDUCE_ONLY")
        expected_close_side: Literal["BUY", "SELL"] = "SELL" if current_position_qty > 0 else "BUY"
        if side != expected_close_side:
            return _reject("WRONG_SIDE_FOR_REDUCE_ONLY")
        reducible_qty = abs(current_position_qty) - open_reduce_only_qty
        if reducible_qty <= 0:
            return _reject("REDUCE_ONLY_EXCEEDS_REMAINING_POSITION")

        if close_position:
            if requested_qty is not None:
                return _reject("CLOSE_POSITION_FORBIDS_QUANTITY")
            if order_type == "LIMIT":
                return _reject("CLOSE_POSITION_REQUIRES_CONDITIONAL_TYPE")
            cover_notional = abs(current_position_qty) * exec_price
            mm, chosen_tier = compute_tiered_maintenance_margin(cover_notional, brackets)
            return UsdmOrderPlan(
                symbol=spec.symbol,
                side=side,
                order_type=order_type,
                accepted=True,
                reject_reason=None,
                exec_price=exec_price,
                raw_qty=reducible_qty,
                qty=Decimal(0),
                notional_usdt=cover_notional,
                leverage=leverage,
                bracket_tier=chosen_tier.tier,
                mmr=chosen_tier.maint_margin_ratio,
                cum=chosen_tier.cum,
                initial_margin_usdt=Decimal(0),
                maint_margin_usdt=mm,
                open_fee_usdt=cover_notional * taker_fee_rate,
                risk_allowed_notional_usdt=cover_notional,
                margin_allowed_notional_usdt=cover_notional,
                close_position=True,
                wire_omit_quantity=True,
                wire_omit_reduce_only=True,
            )

        if requested_qty is not None and requested_qty <= 0:
            return _reject("NON_POSITIVE_REQUESTED_QTY")
        target_qty = reducible_qty if requested_qty is None else min(abs(requested_qty), reducible_qty)
        if max_order_notional_usdt is not None:
            if max_order_notional_usdt <= 0:
                return _reject("ZERO_ALLOWED_NOTIONAL")
            target_qty = min(target_qty, max_order_notional_usdt / exec_price)

        qty = _floor_to_step(target_qty, eff_step)
        notional = qty * exec_price
        if qty <= 0 or qty < eff_min_qty:
            return _reject("BELOW_MIN_QTY", raw_qty=target_qty, qty=qty)
        if eff_max_qty is not None and qty > eff_max_qty:
            return _reject("ABOVE_MAX_QTY", raw_qty=target_qty, qty=qty)

        mm, chosen_tier = compute_tiered_maintenance_margin(notional, brackets)
        return UsdmOrderPlan(
            symbol=spec.symbol,
            side=side,
            order_type=order_type,
            accepted=True,
            reject_reason=None,
            exec_price=exec_price,
            raw_qty=target_qty,
            qty=qty,
            notional_usdt=notional,
            leverage=leverage,
            bracket_tier=chosen_tier.tier,
            mmr=chosen_tier.maint_margin_ratio,
            cum=chosen_tier.cum,
            initial_margin_usdt=Decimal(0),
            maint_margin_usdt=mm,
            open_fee_usdt=notional * taker_fee_rate,
            risk_allowed_notional_usdt=notional,
            margin_allowed_notional_usdt=notional,
            close_position=False,
            wire_omit_quantity=False,
            wire_omit_reduce_only=False,
        )

    total_cost_pct = stop_distance_pct + round_trip_fee_rate + adverse_slippage_rate
    if total_cost_pct <= 0 or loss_budget_usdt <= 0:
        return _reject("INVALID_RISK_BUDGET")

    risk_allowed_notional = loss_budget_usdt / total_cost_pct
    margin_denom = (Decimal(1) / Decimal(leverage)) + taker_fee_rate + fee_buffer_rate
    if margin_denom <= 0 or free_collateral_usdt <= 0:
        return _reject("INSUFFICIENT_FREE_COLLATERAL", risk_n=risk_allowed_notional)
    margin_allowed_notional = free_collateral_usdt / margin_denom

    # Enforce bracket notional ceiling for chosen leverage
    eligible_brackets = [b for b in brackets if b.initial_leverage >= leverage]
    if not eligible_brackets:
        return _reject("LEVERAGE_EXCEEDS_BRACKET_MAX", risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)
    bracket_cap = max(b.notional_cap for b in eligible_brackets)

    candidates = [
        risk_allowed_notional,
        margin_allowed_notional,
        max(Decimal(0), bracket_cap - abs(existing_position_notional_usdt)),
    ]
    if max_order_notional_usdt is not None:
        candidates.append(max_order_notional_usdt)
    if max_position_notional_usdt is not None:
        candidates.append(max(Decimal(0), max_position_notional_usdt - abs(existing_position_notional_usdt)))

    allowed_notional = min(candidates)
    if allowed_notional <= 0:
        return _reject("ZERO_ALLOWED_NOTIONAL", risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)

    raw_qty = allowed_notional / exec_price
    qty = _floor_to_step(raw_qty, eff_step)
    notional = qty * exec_price

    if qty <= 0 or qty < eff_min_qty:
        return _reject("BELOW_MIN_QTY", raw_qty=raw_qty, qty=qty, risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)
    if eff_max_qty is not None and qty > eff_max_qty:
        return _reject("ABOVE_MAX_QTY", raw_qty=raw_qty, qty=qty, risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)
    if notional < spec.min_notional:
        return _reject("BELOW_MIN_NOTIONAL", raw_qty=raw_qty, qty=qty, risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)

    mm, chosen_tier = compute_tiered_maintenance_margin(notional, brackets)
    if chosen_tier.initial_leverage < leverage:
        return _reject("NOTIONAL_EXCEEDS_LEVERAGE_TIER_CAP", raw_qty=raw_qty, qty=qty, risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)

    im = notional / Decimal(leverage)
    open_fee = notional * taker_fee_rate
    if (im + open_fee) > free_collateral_usdt:
        return _reject("INSUFFICIENT_MARGIN_AFTER_FLOOR", raw_qty=raw_qty, qty=qty, risk_n=risk_allowed_notional, marg_n=margin_allowed_notional)

    return UsdmOrderPlan(
        symbol=spec.symbol,
        side=side,
        order_type=order_type,
        accepted=True,
        reject_reason=None,
        exec_price=exec_price,
        raw_qty=raw_qty,
        qty=qty,
        notional_usdt=notional,
        leverage=leverage,
        bracket_tier=chosen_tier.tier,
        mmr=chosen_tier.maint_margin_ratio,
        cum=chosen_tier.cum,
        initial_margin_usdt=im,
        maint_margin_usdt=mm,
        open_fee_usdt=open_fee,
        risk_allowed_notional_usdt=risk_allowed_notional,
        margin_allowed_notional_usdt=margin_allowed_notional,
    )


@dataclass(frozen=True, slots=True)
class FeeRecord:
    ts_ns: int
    order_id: str
    asset: str
    amount: Decimal
    quote_equivalent_usdt: Decimal | None
    conversion_rate: Decimal | None
    conversion_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class FundingPrediction:
    rate: Decimal
    next_funding_ns: int
    mark_price: Decimal
    ts_event_ns: int
    visible_utc_ns: int


@dataclass(frozen=True, slots=True)
class FundingLedgerEntry:
    symbol: str
    funding_time_ns: int
    visible_utc_ns: int
    boundary_pos_qty: Decimal
    settlement_mark_price: Decimal
    funding_rate: Decimal
    computed_amount_usdt: Decimal
    posted_amount_usdt: Decimal
    rounding_delta_usdt: Decimal
    source: str
    posted_to_wallet: bool
    posted_to_cross_usdt: Decimal = Decimal(0)
    posted_to_isolated_usdt: Decimal = Decimal(0)
    boundary_status: Literal["BOUNDARY_KNOWN", "PENDING_RECONCILIATION"] = "BOUNDARY_KNOWN"


@dataclass(frozen=True, slots=True)
class LiquidationTriggerResult:
    triggered: bool
    ts_ns: int
    mark_price: Decimal
    position_qty: Decimal
    entry_price: Decimal
    isolated_wallet_usdt: Decimal
    unrealized_pnl_usdt: Decimal
    isolated_equity_usdt: Decimal
    maintenance_margin_usdt: Decimal
    liquidation_price: Decimal | None
    bankruptcy_price: Decimal | None
    jumped_through_protection: bool
    protection_trigger_price: Decimal | None
    gap_deficit_usdt: Decimal
    sensitivity_band_usdt: dict[str, str] | None
    post_position_qty: Decimal = Decimal(0)
    post_entry_price: Decimal = Decimal(0)
    post_isolated_wallet_usdt: Decimal = Decimal(0)
    post_cross_wallet_usdt: Decimal = Decimal(0)


def compute_isolated_liquidation_price(
    *,
    position_qty: Decimal,
    entry_price: Decimal,
    isolated_wallet_usdt: Decimal,
    brackets: Sequence[LeverageBracketTier],
) -> tuple[Decimal | None, Decimal | None, LeverageBracketTier | None]:
    """Compute exact tiered isolated liquidation price and bankruptcy price (zero fees approx)."""
    if position_qty == 0:
        return None, None, None
    abs_qty = abs(position_qty)
    bankrupt_px = (
        (abs_qty * entry_price - isolated_wallet_usdt) / abs_qty
        if position_qty > 0
        else (isolated_wallet_usdt + abs_qty * entry_price) / abs_qty
    )
    ordered = sorted(brackets, key=lambda b: b.notional_floor)
    chosen_liq: Decimal | None = None
    chosen_tier: LeverageBracketTier | None = None
    for tier in ordered:
        mmr = tier.maint_margin_ratio
        cum = tier.cum
        if position_qty > 0:
            denom = abs_qty * (Decimal(1) - mmr)
            if denom <= 0:
                continue
            cand = (abs_qty * entry_price - isolated_wallet_usdt - cum) / denom
        else:
            denom = abs_qty * (Decimal(1) + mmr)
            cand = (isolated_wallet_usdt + abs_qty * entry_price + cum) / denom
        cand_notional = abs_qty * max(Decimal(0), cand)
        if cand_notional >= tier.notional_floor and cand_notional <= tier.notional_cap:
            chosen_liq = cand
            chosen_tier = tier
            break
    if chosen_liq is None:
        # Fall back to lowest or highest bracket boundary deterministically
        fallback = ordered[-1] if abs_qty * entry_price > ordered[-1].notional_cap else ordered[0]
        mmr = fallback.maint_margin_ratio
        cum = fallback.cum
        if position_qty > 0:
            chosen_liq = (abs_qty * entry_price - isolated_wallet_usdt - cum) / (abs_qty * (Decimal(1) - mmr))
        else:
            chosen_liq = (isolated_wallet_usdt + abs_qty * entry_price + cum) / (abs_qty * (Decimal(1) + mmr))
        chosen_tier = fallback
    return chosen_liq, bankrupt_px, chosen_tier


@dataclass
class IsolatedUsdmLedger:
    """Exact Decimal single-symbol, one-way (BOTH), isolated-margin USD-M ledger.

    Models Binance USD-M rules independently of Nautilus so strategies, demo records,
    and BacktestEngine results can be audited bit-for-bit.
    """

    symbol: str
    cross_wallet_usdt: Decimal
    isolated_wallet_usdt: Decimal = Decimal(0)
    leverage: int = 5
    brackets: tuple[LeverageBracketTier, ...] = field(default_factory=tuple)
    auto_allocate_isolated: bool = True
    position_qty: Decimal = Decimal(0)
    entry_price: Decimal = Decimal(0)
    realized_pnl_usdt: Decimal = Decimal(0)
    funding_pnl_usdt: Decimal = Decimal(0)
    clock_ns: int = 0
    last_mark_price: Decimal | None = None
    asset_wallets: dict[str, Decimal] = field(default_factory=dict)
    fee_totals_by_asset: dict[str, Decimal] = field(default_factory=dict)
    fee_records: list[FeeRecord] = field(default_factory=list)
    predictions: list[FundingPrediction] = field(default_factory=list)
    boundary_snapshots: dict[int, tuple[Decimal, Decimal]] = field(default_factory=dict)
    funding_entries: list[FundingLedgerEntry] = field(default_factory=list)
    settled_funding_times: set[int] = field(default_factory=set)
    pending_funding_postings: list[FundingLedgerEntry] = field(default_factory=list)
    duplicate_funding_ignored: int = 0
    liquidations: list[LiquidationTriggerResult] = field(default_factory=list)
    cross_wallet_touched_on_iso_liq: bool = False
    unclamped_liquidation_deficit_usdt: Decimal = Decimal(0)
    fill_history: list[tuple[int, Decimal]] = field(default_factory=list)
    seen_fill_ids: set[str] = field(default_factory=set)
    order_filled_qty: dict[str, Decimal] = field(default_factory=dict)
    reconciliation_shortfall_usdt: Decimal = Decimal(0)
    pending_reconciliation: bool = False

    def __post_init__(self) -> None:
        if not self.brackets:
            self.brackets = DEFAULT_USDM_TIER1_BRACKETS.get(
                self.symbol,
                (LeverageBracketTier(1, 20, Decimal("0"), Decimal("100000"), Decimal("0.005"), Decimal("0")),),
            )
        if "USDT" not in self.asset_wallets:
            self.asset_wallets["USDT"] = self.cross_wallet_usdt + self.isolated_wallet_usdt

    def _sync_usdt_wallet_total(self) -> None:
        self.asset_wallets["USDT"] = self.cross_wallet_usdt + self.isolated_wallet_usdt

    def _snapshot_mutable_state(self) -> dict[str, Any]:
        return {
            "cross_wallet_usdt": self.cross_wallet_usdt,
            "isolated_wallet_usdt": self.isolated_wallet_usdt,
            "position_qty": self.position_qty,
            "entry_price": self.entry_price,
            "realized_pnl_usdt": self.realized_pnl_usdt,
            "funding_pnl_usdt": self.funding_pnl_usdt,
            "clock_ns": self.clock_ns,
            "last_mark_price": self.last_mark_price,
            "asset_wallets": dict(self.asset_wallets),
            "fee_totals_by_asset": dict(self.fee_totals_by_asset),
            "fee_records": list(self.fee_records),
            "boundary_snapshots": dict(self.boundary_snapshots),
            "funding_entries": list(self.funding_entries),
            "settled_funding_times": set(self.settled_funding_times),
            "pending_funding_postings": list(self.pending_funding_postings),
            "duplicate_funding_ignored": self.duplicate_funding_ignored,
            "liquidations": list(self.liquidations),
            "cross_wallet_touched_on_iso_liq": self.cross_wallet_touched_on_iso_liq,
            "unclamped_liquidation_deficit_usdt": self.unclamped_liquidation_deficit_usdt,
            "fill_history": list(self.fill_history),
            "seen_fill_ids": set(self.seen_fill_ids),
            "order_filled_qty": dict(self.order_filled_qty),
            "reconciliation_shortfall_usdt": self.reconciliation_shortfall_usdt,
            "pending_reconciliation": self.pending_reconciliation,
        }

    def _restore_mutable_state(self, snap: dict[str, Any]) -> None:
        for k, v in snap.items():
            setattr(self, k, v)

    def _position_at_time(self, target_ts_ns: int) -> tuple[Decimal, bool]:
        """Return (signed_position_qty, boundary_known) from chronological fill history / snapshots."""
        if target_ts_ns in self.boundary_snapshots:
            return self.boundary_snapshots[target_ts_ns][0], True
        if target_ts_ns <= self.clock_ns or self.fill_history:
            pos = Decimal(0)
            for fill_ts, pos_after in self.fill_history:
                if fill_ts <= target_ts_ns:
                    pos = pos_after
                else:
                    break
            return pos, True
        return self.position_qty, False

    def advance_time(self, ts_ns: int, *, observed_boundaries: Sequence[int] = ()) -> list[FundingLedgerEntry]:
        """Advance local clock, snapshot any crossed funding boundaries, and release due cash postings."""
        if ts_ns < self.clock_ns:
            raise ValueError(f"Non-monotonic clock advance {ts_ns} < {self.clock_ns}")
        for b_ns in sorted(observed_boundaries):
            if self.clock_ns <= b_ns <= ts_ns and b_ns not in self.boundary_snapshots:
                if self.last_mark_price is not None:
                    self.boundary_snapshots[b_ns] = (self.position_qty, self.last_mark_price)
        self.clock_ns = ts_ns
        released: list[FundingLedgerEntry] = []
        still_pending: list[FundingLedgerEntry] = []
        for entry in self.pending_funding_postings:
            if entry.visible_utc_ns <= ts_ns:
                posted = self._post_funding_amount(entry)
                released.append(posted)
            else:
                still_pending.append(entry)
        self.pending_funding_postings = still_pending
        return released

    def _post_funding_amount(self, entry: FundingLedgerEntry) -> FundingLedgerEntry:
        amt = entry.posted_amount_usdt
        self.funding_pnl_usdt += amt
        cross_delta = Decimal(0)
        iso_delta = Decimal(0)
        if amt < 0:
            # Per Binance USD-M rules, funding fees debit available cross wallet balance first,
            # preserving isolated position margin & liquidation price unless free balance is exhausted.
            cost = -amt
            from_cross = min(max(self.cross_wallet_usdt, Decimal(0)), cost)
            rem = cost - from_cross
            if from_cross > 0:
                self.cross_wallet_usdt -= from_cross
                cross_delta -= from_cross
            if rem > 0:
                self.isolated_wallet_usdt -= rem
                iso_delta -= rem
        elif amt > 0:
            self.cross_wallet_usdt += amt
            cross_delta += amt
        self._sync_usdt_wallet_total()
        posted = FundingLedgerEntry(
            symbol=entry.symbol,
            funding_time_ns=entry.funding_time_ns,
            visible_utc_ns=entry.visible_utc_ns,
            boundary_pos_qty=entry.boundary_pos_qty,
            settlement_mark_price=entry.settlement_mark_price,
            funding_rate=entry.funding_rate,
            computed_amount_usdt=entry.computed_amount_usdt,
            posted_amount_usdt=entry.posted_amount_usdt,
            rounding_delta_usdt=entry.rounding_delta_usdt,
            source=entry.source,
            posted_to_wallet=True,
            posted_to_cross_usdt=cross_delta,
            posted_to_isolated_usdt=iso_delta,
            boundary_status=entry.boundary_status,
        )
        # Replace pending record in funding_entries
        for idx, existing in enumerate(self.funding_entries):
            if existing.funding_time_ns == entry.funding_time_ns:
                self.funding_entries[idx] = posted
                break
        return posted

    def record_mark_and_predicted_funding(
        self,
        *,
        ts_event_ns: int,
        visible_utc_ns: int,
        mark_price: Decimal,
        predicted_rate: Decimal | None = None,
        next_funding_ns: int | None = None,
    ) -> None:
        self.advance_time(max(self.clock_ns, visible_utc_ns))
        self.last_mark_price = mark_price
        if next_funding_ns is not None and next_funding_ns <= visible_utc_ns:
            self.boundary_snapshots.setdefault(next_funding_ns, (self.position_qty, mark_price))
        if predicted_rate is not None and next_funding_ns is not None:
            self.predictions.append(
                FundingPrediction(
                    rate=predicted_rate,
                    next_funding_ns=next_funding_ns,
                    mark_price=mark_price,
                    ts_event_ns=ts_event_ns,
                    visible_utc_ns=visible_utc_ns,
                )
            )

    def snapshot_boundary(self, funding_time_ns: int, settlement_mark_price: Decimal) -> None:
        self.boundary_snapshots[funding_time_ns] = (self.position_qty, settlement_mark_price)

    def visible_predicted_funding(self, as_of_utc_ns: int) -> FundingPrediction | None:
        """Return latest predicted funding visible on or before `as_of_utc_ns` (never future data)."""
        candidates = [p for p in self.predictions if p.visible_utc_ns <= as_of_utc_ns]
        return candidates[-1] if candidates else None

    def available_for_new_orders_usdt(self) -> Decimal:
        """Free cross collateral excluding any unposted/future-visible funding credits."""
        return self.cross_wallet_usdt

    def apply_fill(
        self,
        *,
        ts_ns: int,
        order_id: str,
        side: Literal["BUY", "SELL"],
        qty: Decimal,
        price: Decimal,
        fee_amount: Decimal,
        fee_asset: str = "USDT",
        fee_conversion_rate: Decimal | None = None,
        fee_conversion_ts_ns: int | None = None,
        allocated_margin_usdt: Decimal | None = None,
        funding_boundaries_before_fill: Sequence[int] = (),
        fill_id: str | None = None,
        confirmed_exchange_fill: bool = False,
    ) -> Decimal:
        """Apply a one-way isolated perpetual fill strictly chronologically & atomically."""
        if qty <= 0 or price <= 0:
            raise ValueError(f"Fill qty/price must be positive: qty={qty}, px={price}")
        if fee_amount < 0:
            raise ValueError(f"Negative fee_amount not supported here: {fee_amount}")
        if fill_id is not None and fill_id in self.seen_fill_ids:
            return Decimal(0)

        snap = self._snapshot_mutable_state()
        try:
            self.advance_time(ts_ns, observed_boundaries=funding_boundaries_before_fill)
            signed_delta = qty if side == "BUY" else -qty
            old_qty = self.position_qty
            new_qty = old_qty + signed_delta
            realized_trade_pnl = Decimal(0)

            if old_qty == 0 or (old_qty > 0 and signed_delta > 0) or (old_qty < 0 and signed_delta < 0):
                # Opening or adding to existing direction
                new_notional = abs(old_qty) * self.entry_price + qty * price
                new_entry = new_notional / abs(new_qty)
                if self.auto_allocate_isolated:
                    add_margin = (
                        allocated_margin_usdt
                        if allocated_margin_usdt is not None
                        else (qty * price) / Decimal(self.leverage)
                    )
                    if add_margin > self.cross_wallet_usdt:
                        if not confirmed_exchange_fill:
                            raise ValueError(
                                f"Insufficient cross wallet {self.cross_wallet_usdt} for isolated IM {add_margin}"
                            )
                        alloc = max(self.cross_wallet_usdt, Decimal(0))
                        self.reconciliation_shortfall_usdt += add_margin - alloc
                        self.pending_reconciliation = True
                        self.cross_wallet_usdt -= alloc
                        self.isolated_wallet_usdt += alloc
                    else:
                        self.cross_wallet_usdt -= add_margin
                        self.isolated_wallet_usdt += add_margin
                self.entry_price = new_entry
            else:
                # Reducing, closing, or flipping
                closed_qty = min(abs(old_qty), qty)
                direction = Decimal(1) if old_qty > 0 else Decimal(-1)
                realized_trade_pnl = closed_qty * (price - self.entry_price) * direction
                self.realized_pnl_usdt += realized_trade_pnl
                self.isolated_wallet_usdt += realized_trade_pnl

                if new_qty == 0:
                    self.entry_price = Decimal(0)
                    if self.auto_allocate_isolated:
                        self.cross_wallet_usdt += self.isolated_wallet_usdt
                        self.isolated_wallet_usdt = Decimal(0)
                elif (old_qty > 0 and new_qty < 0) or (old_qty < 0 and new_qty > 0):
                    # Reversed through flat
                    if self.auto_allocate_isolated:
                        self.cross_wallet_usdt += self.isolated_wallet_usdt
                        self.isolated_wallet_usdt = Decimal(0)
                    rem_margin = (abs(new_qty) * price) / Decimal(self.leverage)
                    if self.auto_allocate_isolated:
                        if rem_margin > self.cross_wallet_usdt:
                            if not confirmed_exchange_fill:
                                raise ValueError(
                                    f"Insufficient cross wallet {self.cross_wallet_usdt} for flip IM {rem_margin}"
                                )
                            alloc = max(self.cross_wallet_usdt, Decimal(0))
                            self.reconciliation_shortfall_usdt += rem_margin - alloc
                            self.pending_reconciliation = True
                            self.cross_wallet_usdt -= alloc
                            self.isolated_wallet_usdt += alloc
                        else:
                            self.cross_wallet_usdt -= rem_margin
                            self.isolated_wallet_usdt += rem_margin
                    self.entry_price = price
                else:
                    # Partial close: entry_price remains unchanged; proportional IM stays in WB_iso
                    pass

            self.position_qty = new_qty
            if fill_id is not None:
                self.seen_fill_ids.add(fill_id)
            self.order_filled_qty[order_id] = self.order_filled_qty.get(order_id, Decimal(0)) + qty
            self.fill_history.append((ts_ns, new_qty))

            # Track fees strictly per fee_asset (never mix USDT + BNB + BTC into one scalar)
            self.fee_totals_by_asset[fee_asset] = self.fee_totals_by_asset.get(fee_asset, Decimal(0)) + fee_amount
            quote_equiv: Decimal | None = None
            if fee_asset == "USDT":
                quote_equiv = fee_amount
                if self.isolated_wallet_usdt > 0 or self.position_qty != 0:
                    if self.isolated_wallet_usdt >= fee_amount:
                        self.isolated_wallet_usdt -= fee_amount
                    else:
                        rem_fee = fee_amount - self.isolated_wallet_usdt
                        self.isolated_wallet_usdt = Decimal(0)
                        self.cross_wallet_usdt -= rem_fee
                else:
                    self.cross_wallet_usdt -= fee_amount
                self._sync_usdt_wallet_total()
            else:
                prev_asset_bal = self.asset_wallets.get(fee_asset, Decimal(0))
                self.asset_wallets[fee_asset] = prev_asset_bal - fee_amount
                if fee_conversion_rate is not None:
                    quote_equiv = fee_amount * fee_conversion_rate

            self.fee_records.append(
                FeeRecord(
                    ts_ns=ts_ns,
                    order_id=order_id,
                    asset=fee_asset,
                    amount=fee_amount,
                    quote_equivalent_usdt=quote_equiv,
                    conversion_rate=fee_conversion_rate,
                    conversion_ts_ns=fee_conversion_ts_ns,
                )
            )
            return realized_trade_pnl
        except Exception:
            self._restore_mutable_state(snap)
            raise

    def settle_funding(
        self,
        *,
        funding_time_ns: int,
        settled_rate: Decimal,
        settlement_mark_price: Decimal,
        visible_utc_ns: int,
        exact_income_usdt: Decimal | None = None,
        position_qty_at_boundary: Decimal | None = None,
        source: str = "local",
        boundary_status: Literal["BOUNDARY_KNOWN", "PENDING_RECONCILIATION"] = "BOUNDARY_KNOWN",
    ) -> FundingLedgerEntry:
        """Settle funding exactly once per `(symbol, funding_time_ns)` using chronological boundary position."""
        if funding_time_ns in self.settled_funding_times:
            self.duplicate_funding_ignored += 1
            for existing in self.funding_entries:
                if existing.funding_time_ns == funding_time_ns:
                    return existing
            raise AssertionError("settled_funding_times inconsistent with funding_entries")

        resolved_status = boundary_status
        if position_qty_at_boundary is not None:
            pos_qty = position_qty_at_boundary
            mark_px = settlement_mark_price
        elif funding_time_ns in self.boundary_snapshots:
            snap_qty, snap_mark = self.boundary_snapshots[funding_time_ns]
            pos_qty = snap_qty
            mark_px = settlement_mark_price or snap_mark
        else:
            pos_qty, known = self._position_at_time(funding_time_ns)
            mark_px = settlement_mark_price
            if not known:
                resolved_status = "PENDING_RECONCILIATION"
                self.pending_reconciliation = True
            self.boundary_snapshots[funding_time_ns] = (pos_qty, mark_px)

        computed = -(pos_qty * mark_px * settled_rate)
        posted_amt = exact_income_usdt if exact_income_usdt is not None else computed
        rounding_delta = posted_amt - computed

        entry = FundingLedgerEntry(
            symbol=self.symbol,
            funding_time_ns=funding_time_ns,
            visible_utc_ns=visible_utc_ns,
            boundary_pos_qty=pos_qty,
            settlement_mark_price=mark_px,
            funding_rate=settled_rate,
            computed_amount_usdt=computed,
            posted_amount_usdt=posted_amt,
            rounding_delta_usdt=rounding_delta,
            source=source,
            posted_to_wallet=False,
            boundary_status=resolved_status,
        )
        self.settled_funding_times.add(funding_time_ns)
        self.funding_entries.append(entry)

        if visible_utc_ns <= self.clock_ns and resolved_status == "BOUNDARY_KNOWN":
            entry = self._post_funding_amount(entry)
        elif resolved_status == "BOUNDARY_KNOWN":
            self.pending_funding_postings.append(entry)
        return entry

    def unrealized_pnl_usdt(self, mark_price: Decimal | None = None) -> Decimal:
        px = mark_price if mark_price is not None else self.last_mark_price
        if self.position_qty == 0 or px is None:
            return Decimal(0)
        return self.position_qty * (px - self.entry_price)

    def isolated_equity_usdt(self, mark_price: Decimal | None = None) -> Decimal:
        return self.isolated_wallet_usdt + self.unrealized_pnl_usdt(mark_price)

    def liquidation_prices(self) -> tuple[Decimal | None, Decimal | None, LeverageBracketTier | None]:
        return compute_isolated_liquidation_price(
            position_qty=self.position_qty,
            entry_price=self.entry_price,
            isolated_wallet_usdt=self.isolated_wallet_usdt,
            brackets=self.brackets,
        )

    def evaluate_mark_and_liquidation(
        self,
        *,
        ts_ns: int,
        mark_price: Decimal,
        protection_trigger_price: Decimal | None = None,
        liquidation_close_fee_rate: Decimal = Decimal("0.0005"),
    ) -> LiquidationTriggerResult:
        """Check whether mark price breaches isolated maintenance margin; handle gap liquidation sensitivity."""
        self.advance_time(ts_ns)
        self.last_mark_price = mark_price
        if self.position_qty == 0:
            return LiquidationTriggerResult(
                triggered=False,
                ts_ns=ts_ns,
                mark_price=mark_price,
                position_qty=Decimal(0),
                entry_price=Decimal(0),
                isolated_wallet_usdt=self.isolated_wallet_usdt,
                unrealized_pnl_usdt=Decimal(0),
                isolated_equity_usdt=self.isolated_wallet_usdt,
                maintenance_margin_usdt=Decimal(0),
                liquidation_price=None,
                bankruptcy_price=None,
                jumped_through_protection=False,
                protection_trigger_price=protection_trigger_price,
                gap_deficit_usdt=Decimal(0),
                sensitivity_band_usdt=None,
            )

        pre_pos = self.position_qty
        pre_entry = self.entry_price
        pre_iso = self.isolated_wallet_usdt

        upnl = self.unrealized_pnl_usdt(mark_price)
        iso_eq = pre_iso + upnl
        notional = abs(pre_pos) * mark_price
        mm, _ = compute_tiered_maintenance_margin(notional, self.brackets)
        liq_px, bankrupt_px, _ = self.liquidation_prices()

        jumped_through_prot = False
        if protection_trigger_price is not None:
            if pre_pos > 0 and mark_price < protection_trigger_price:
                jumped_through_prot = True
            elif pre_pos < 0 and mark_price > protection_trigger_price:
                jumped_through_prot = True

        triggered = iso_eq <= mm
        gap_deficit = Decimal(0)
        band: dict[str, str] | None = None
        if triggered:
            abs_q = abs(pre_pos)
            assert liq_px is not None and bankrupt_px is not None
            # Range of plausible close-out equity across [liq_px, bankrupt_px, mark_price]
            def _eq_at(px: Decimal) -> Decimal:
                raw_eq = pre_iso + pre_pos * (px - pre_entry)
                fee = abs_q * px * liquidation_close_fee_rate
                return raw_eq - fee

            eq_liq = _eq_at(liq_px)
            eq_bankrupt = _eq_at(bankrupt_px)
            eq_mark = _eq_at(mark_price)
            band = {
                "at_liquidation_price": str(eq_liq),
                "at_bankruptcy_price": str(eq_bankrupt),
                "at_observed_mark_price": str(eq_mark),
                "isolated_loss_capped_to": str(pre_iso),
            }
            if eq_mark < 0:
                gap_deficit = -eq_mark
                self.unclamped_liquidation_deficit_usdt += gap_deficit

            # Under strict Binance Isolated Margin, the trader's isolated loss cannot spill
            # into unallocated `cross_wallet_usdt`: WB_iso floors at 0 (insurance fund absorbs gap).
            self.isolated_wallet_usdt = Decimal(0)
            self.position_qty = Decimal(0)
            self.entry_price = Decimal(0)
            self._sync_usdt_wallet_total()

        res = LiquidationTriggerResult(
            triggered=triggered,
            ts_ns=ts_ns,
            mark_price=mark_price,
            position_qty=pre_pos,
            entry_price=pre_entry,
            isolated_wallet_usdt=pre_iso,
            unrealized_pnl_usdt=upnl,
            isolated_equity_usdt=iso_eq,
            maintenance_margin_usdt=mm,
            liquidation_price=liq_px,
            bankruptcy_price=bankrupt_px,
            jumped_through_protection=jumped_through_prot,
            protection_trigger_price=protection_trigger_price,
            gap_deficit_usdt=gap_deficit,
            sensitivity_band_usdt=band,
            post_position_qty=self.position_qty,
            post_entry_price=self.entry_price,
            post_isolated_wallet_usdt=self.isolated_wallet_usdt,
            post_cross_wallet_usdt=self.cross_wallet_usdt,
        )
        if triggered:
            self.liquidations.append(res)
        return res


class BinanceTieredMarginModel:
    """Custom Nautilus MarginModel implementing Binance USD-M leverage & tiered MMR-cum."""

    def __init__(
        self,
        brackets_by_instrument: dict[str, Sequence[LeverageBracketTier]] | None = None,
        default_mmr: Decimal = Decimal("0.005"),
        default_cum: Decimal = Decimal("0"),
    ) -> None:
        self._brackets = brackets_by_instrument or {}
        self._default_mmr = default_mmr
        self._default_cum = default_cum

    def calculate_initial_margin(
        self,
        instrument: Any,
        quantity: Any,
        price: Any,
        leverage: Any,
        use_quote_for_inverse: bool = False,
    ) -> Any:
        from nautilus_trader.model import Money

        notional = abs(quantity.as_decimal() * price.as_decimal())
        lev = Decimal(str(leverage))
        im = notional / lev if lev > 0 else notional
        return Money.from_str(f"{im:.8f} {instrument.settlement_currency.code}")

    def calculate_maintenance_margin(
        self,
        instrument: Any,
        quantity: Any,
        price: Any,
        leverage: Any,
        use_quote_for_inverse: bool = False,
    ) -> Any:
        from nautilus_trader.model import Money

        del leverage, use_quote_for_inverse
        notional = abs(quantity.as_decimal() * price.as_decimal())
        inst_key = str(instrument.id)
        raw_sym = str(instrument.raw_symbol)
        tiers = self._brackets.get(inst_key) or self._brackets.get(raw_sym)
        if tiers:
            mm, _ = compute_tiered_maintenance_margin(notional, tiers)
        else:
            mm = max(Decimal(0), notional * self._default_mmr - self._default_cum)
        return Money.from_str(f"{mm:.8f} {instrument.settlement_currency.code}")


__all__ = [
    "BinanceTieredMarginModel",
    "FeeRecord",
    "FundingLedgerEntry",
    "FundingPrediction",
    "IsolatedUsdmLedger",
    "LiquidationTriggerResult",
    "UsdmOrderPlan",
    "compute_isolated_liquidation_price",
    "plan_usdm_isolated_order",
]
