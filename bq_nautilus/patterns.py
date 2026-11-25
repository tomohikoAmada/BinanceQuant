"""M-NT2 verified multi-strategy patterns and execution-boundary guards for NautilusTrader."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model import (
    BookType,
    ClientId,
    InstrumentId,
    InstrumentStatus,
    OrderBookDeltas,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    TimeInForce,
    TradeTick,
)
from nautilus_trader.trading import Strategy

from .custom_data import (
    BMD_FACTOR_QUOTE_DATATYPE,
    BMD_LIQUIDATION_DATATYPE,
    BMD_STREAM_QUALITY_DATATYPE,
    BmdFactorQuoteData,
    BmdLiquidationData,
    BmdStreamQualityData,
)

MAKER_QUEUE_BOUNDARY_NOTICE = (
    "Nautilus L2_MBP consumes public price-level depth aggregates without exchange FIFO "
    "queue-position state; LIMIT / POST_ONLY fills in BacktestEngine validate interface "
    "plumbing only. Queue-sensitive Maker alpha must be cross-checked with HftBacktest."
)


class AllowlistViolationError(PermissionError):
    """Raised when a strategy attempts to trade an unlisted or read-only factor instrument."""


@dataclass(slots=True)
class TradingAllowlistGuard:
    """Hard execution guard blocking orders outside the explicitly permitted instrument set."""

    allowed_instruments: frozenset[InstrumentId]
    blocked_attempts: list[str] = field(default_factory=list)
    raise_on_violation: bool = True

    def assert_allowed(self, instrument_id: InstrumentId) -> bool:
        if instrument_id in self.allowed_instruments:
            return True
        self.blocked_attempts.append(str(instrument_id))
        if self.raise_on_violation:
            raise AllowlistViolationError(
                f"Blocked order for {instrument_id}; allowlist={sorted(str(x) for x in self.allowed_instruments)}"
            )
        return False


def _route_id(client_id: str | None) -> ClientId | None:
    return ClientId(client_id) if client_id else None


def _submit_routed(strategy: Strategy, order: Any, exec_client_id: str | None) -> None:
    exec_route = _route_id(exec_client_id)
    if exec_route is not None:
        strategy.submit_order(order, client_id=exec_route)
    else:
        strategy.submit_order(order)


class CrossAssetFactorStrategyConfig(StrategyConfig):
    """Configuration for Pattern 2: read-only cross-asset factor + isolated target."""

    def __init__(
        self,
        *,
        factor_instrument_id: InstrumentId,
        target_instrument_id: InstrumentId,
        order_qty: str = "1.0",
        factor_jump_bps: float = 250.0,
        trade_target_on_signal: bool = False,
        use_custom_factor_stream: bool = False,
        probe_factor_order_blocked: bool = False,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        telemetry: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.factor_instrument_id = factor_instrument_id
        self.target_instrument_id = target_instrument_id
        self.order_qty = order_qty
        self.factor_jump_bps = factor_jump_bps
        self.trade_target_on_signal = trade_target_on_signal
        self.use_custom_factor_stream = use_custom_factor_stream
        self.probe_factor_order_blocked = probe_factor_order_blocked
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.telemetry = telemetry if telemetry is not None else {}


class CrossAssetFactorStrategy(Strategy):
    """Pattern 2: read-only cross-asset factor + isolated single-symbol tradable target."""

    def __init__(self, config: CrossAssetFactorStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.telemetry = config.telemetry
        self.guard = TradingAllowlistGuard(frozenset({config.target_instrument_id}))
        self._factor_ref_mid: float | None = None
        self._last_factor_mid: float | None = None
        self._last_target_mid: float | None = None
        self._probed_block = False
        self.telemetry.setdefault("factor_updates", 0)
        self.telemetry.setdefault("target_updates", 0)
        self.telemetry.setdefault("blocked_factor_orders", 0)
        self.telemetry.setdefault("target_orders", 0)

    def on_start(self) -> None:
        route = _route_id(self._cfg.data_client_id)
        self.subscribe_quotes(self._cfg.target_instrument_id, client_id=route)
        if self._cfg.use_custom_factor_stream:
            self.subscribe_data(BMD_FACTOR_QUOTE_DATATYPE, client_id=route)
        else:
            self.subscribe_quotes(self._cfg.factor_instrument_id, client_id=route)

    def _safe_submit(self, instrument_id: InstrumentId, side: OrderSide, qty: Quantity) -> bool:
        try:
            self.guard.assert_allowed(instrument_id)
        except AllowlistViolationError:
            self.telemetry["blocked_factor_orders"] += 1
            return False
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=qty,
        )
        self.telemetry["target_orders"] += 1
        _submit_routed(self, order, self._cfg.exec_client_id)
        return True

    def _on_factor_mid(self, mid: float) -> None:
        if self._factor_ref_mid is None:
            self._factor_ref_mid = mid
        self._last_factor_mid = mid
        self.telemetry["factor_updates"] += 1
        if self._cfg.probe_factor_order_blocked and not self._probed_block:
            self._probed_block = True
            self._safe_submit(
                self._cfg.factor_instrument_id,
                OrderSide.BUY,
                Quantity.from_str("0.01"),
            )
        if not self._cfg.trade_target_on_signal or self._last_target_mid is None:
            return
        move_bps = ((mid - self._factor_ref_mid) / self._factor_ref_mid) * 10_000.0
        if move_bps >= self._cfg.factor_jump_bps and self.portfolio.is_net_flat(
            self._cfg.target_instrument_id
        ):
            self._safe_submit(
                self._cfg.target_instrument_id,
                OrderSide.BUY,
                Quantity.from_str(self._cfg.order_qty),
            )

    def on_data(self, data: Any) -> None:
        payload = getattr(data, "data", data)
        if isinstance(payload, BmdFactorQuoteData):
            mid = (float(payload.bid_price) + float(payload.ask_price)) * 0.5
            self._on_factor_mid(mid)

    def on_quote(self, tick: QuoteTick) -> None:
        mid = (float(tick.bid_price) + float(tick.ask_price)) * 0.5
        if tick.instrument_id == self._cfg.factor_instrument_id:
            self._on_factor_mid(mid)
        elif tick.instrument_id == self._cfg.target_instrument_id:
            self._last_target_mid = mid
            self.telemetry["target_updates"] += 1


class L2MicrostructureStrategyConfig(StrategyConfig):
    """Configuration for Pattern 3: L2 OrderBookDeltas + TradeTick + GAP lockout."""

    def __init__(
        self,
        *,
        instrument_id: InstrumentId,
        order_qty: str = "0.01",
        imbalance_threshold: float = 0.25,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        telemetry: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.order_qty = order_qty
        self.imbalance_threshold = imbalance_threshold
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.telemetry = telemetry if telemetry is not None else {}


class L2MicrostructureStrategy(Strategy):
    """Pattern 3: multi-level L2 OrderBookDeltas + TradeTick with stream-scoped GAP/book lockout."""

    def __init__(self, config: L2MicrostructureStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.telemetry = config.telemetry
        self._venue_trading = True
        self._depth_healthy = True
        self._has_quality_feed = False
        self._pending_entry = False
        self._trade_imbalance = 0.0
        self.telemetry.setdefault("deltas_events", 0)
        self.telemetry.setdefault("trade_events", 0)
        self.telemetry.setdefault("halt_events", 0)
        self.telemetry.setdefault("recovery_events", 0)
        self.telemetry.setdefault("clear_events", 0)
        self.telemetry.setdefault("quality_events", [])
        self.telemetry.setdefault("trades_while_depth_bad", 0)
        self.telemetry.setdefault("top_levels_seen", [])
        self.telemetry.setdefault("orders_submitted", 0)
        self.telemetry.setdefault("orders_blocked_halt", 0)
        self.telemetry.setdefault("protective_exits_submitted", 0)

    def _can_open_from_book(self) -> bool:
        return self._venue_trading and self._depth_healthy

    def submit_protective_exit(self, qty: Quantity | None = None) -> bool:
        """Protective cancel/reduce-only exit MUST remain available even while L2 book is degraded."""
        self.cancel_all_orders(self._cfg.instrument_id)
        if self.portfolio.is_net_flat(self._cfg.instrument_id):
            return False
        close_qty = qty or Quantity.from_str(self._cfg.order_qty)
        side = OrderSide.SELL if self.portfolio.is_net_long(self._cfg.instrument_id) else OrderSide.BUY
        order = self.order_factory.market(
            instrument_id=self._cfg.instrument_id,
            order_side=side,
            quantity=close_qty,
            reduce_only=True,
        )
        self.telemetry["protective_exits_submitted"] += 1
        _submit_routed(self, order, self._cfg.exec_client_id)
        return True

    def on_start(self) -> None:
        route = _route_id(self._cfg.data_client_id)
        self.subscribe_book_deltas(
            self._cfg.instrument_id,
            BookType.L2_MBP,
            managed=True,
            client_id=route,
        )
        self.subscribe_trades(self._cfg.instrument_id, client_id=route)
        self.subscribe_instrument_status(self._cfg.instrument_id, client_id=route)
        self.subscribe_data(BMD_STREAM_QUALITY_DATATYPE, client_id=route)

    def _matches_instrument_meta(self, market: str, symbol: str) -> bool:
        want_sym = self._cfg.instrument_id.symbol.value
        is_usdm_id = want_sym.endswith("-PERP")
        expected_sym = want_sym[:-5] if is_usdm_id else want_sym
        expected_mkt = "USD_M_PERPETUAL" if is_usdm_id else "SPOT"
        return symbol == expected_sym and market in {expected_mkt, "USDM" if is_usdm_id else "SPOT"}

    def on_data(self, data: Any) -> None:
        payload = getattr(data, "data", data)
        if not isinstance(payload, BmdStreamQualityData):
            return
        if not self._matches_instrument_meta(payload.market, payload.symbol):
            return
        self.telemetry["quality_events"].append((payload.stream, payload.status, payload.reason))
        if payload.stream == "diff_depth":
            self._has_quality_feed = True
            if payload.status != "LIVE":
                if self._depth_healthy:
                    self._depth_healthy = False
                    self.telemetry["halt_events"] += 1
            elif not self._depth_healthy:
                self._depth_healthy = True
                self.telemetry["recovery_events"] += 1

    def on_instrument_status(self, status: InstrumentStatus) -> None:
        if status.instrument_id != self._cfg.instrument_id:
            return
        if not status.is_trading:
            self._venue_trading = False
            self.telemetry["halt_events"] += 1
        elif not self._venue_trading:
            self._venue_trading = True
            self.telemetry["recovery_events"] += 1

    def on_trade(self, trade: TradeTick) -> None:
        if trade.instrument_id != self._cfg.instrument_id:
            return
        self.telemetry["trade_events"] += 1
        if not self._depth_healthy:
            self.telemetry["trades_while_depth_bad"] += 1
        sign = 1.0 if trade.aggressor_side.name == "BUY" else -1.0
        self._trade_imbalance = 0.5 * self._trade_imbalance + sign * float(trade.size)

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        if deltas.instrument_id != self._cfg.instrument_id:
            return
        self.telemetry["deltas_events"] += 1
        # Pure single-CLEAR delta (e.g. Route B send_gap_clear sequence=0, where F_SNAPSHOT is set)
        # immediately invalidates the book until valid resync snapshot arrives.
        if len(deltas.deltas) == 1 and deltas.deltas[0].action.name == "CLEAR":
            self.telemetry["clear_events"] = self.telemetry.get("clear_events", 0) + 1
            self.telemetry["last_clear_ts_init_ns"] = int(deltas.ts_init)
            if self._depth_healthy:
                self._depth_healthy = False
                self.telemetry["halt_events"] += 1
                self.telemetry["last_halt_wall_ns"] = int(__import__("time").time_ns())
            return
        book = self.cache.order_book(self._cfg.instrument_id)
        if book is None or book.best_bid_price() is None or book.best_ask_price() is None:
            if not self._can_open_from_book():
                self.telemetry["orders_blocked_halt"] += 1
            return
        if (
            not self._depth_healthy
            and not self._has_quality_feed
            and deltas.is_snapshot
            and len(deltas.deltas) >= 2
        ):
            self._depth_healthy = True
            self.telemetry["recovery_events"] += 1
            self.telemetry["last_recovery_ts_init_ns"] = int(deltas.ts_init)
            self.telemetry["last_recovery_wall_ns"] = int(__import__("time").time_ns())
        bid_sz = float(book.best_bid_size() or 0.0)
        ask_sz = float(book.best_ask_size() or 0.0)
        self.telemetry["top_levels_seen"].append(
            (float(book.best_bid_price()), float(book.best_ask_price()), len(book.bids()), len(book.asks()))
        )
        if not self._can_open_from_book():
            self.telemetry["orders_blocked_halt"] += 1
            return
        if (bid_sz + ask_sz) <= 0.0 or self._pending_entry:
            return
        imb = (bid_sz - ask_sz) / (bid_sz + ask_sz)
        if imb >= self._cfg.imbalance_threshold and self.portfolio.is_net_flat(self._cfg.instrument_id):
            order = self.order_factory.market(
                instrument_id=self._cfg.instrument_id,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_str(self._cfg.order_qty),
            )
            self._pending_entry = True
            self.telemetry["orders_submitted"] += 1
            _submit_routed(self, order, self._cfg.exec_client_id)

    def on_order_filled(self, event: Any) -> None:
        self._pending_entry = False

    def on_order_rejected(self, event: Any) -> None:
        self._pending_entry = False


class MultiTimescaleHybridStrategyConfig(StrategyConfig):
    """Configuration for Pattern 4: 1Hz timer + intra-second stop + sparse liquidation stream."""

    def __init__(
        self,
        *,
        instrument_id: InstrumentId,
        order_qty: str = "0.01",
        timer_interval_ms: int = 1000,
        stop_loss_bps: float = 50.0,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        telemetry: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.order_qty = order_qty
        self.timer_interval_ms = timer_interval_ms
        self.stop_loss_bps = stop_loss_bps
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.telemetry = telemetry if telemetry is not None else {}


class MultiTimescaleHybridStrategy(Strategy):
    """Pattern 4: slow 1Hz timer decision + tick-by-tick intra-second stop + sparse liquidation stream."""

    def __init__(self, config: MultiTimescaleHybridStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.telemetry = config.telemetry
        self._entry_px: float | None = None
        self._last_mid: float | None = None
        self._closing = False
        self._healthy = True
        self.telemetry.setdefault("timer_ticks", [])
        self.telemetry.setdefault("entry_ts_ns", None)
        self.telemetry.setdefault("stop_loss_ts_ns", None)
        self.telemetry.setdefault("liquidations", [])
        self.telemetry.setdefault("stale_halts", 0)
        self.telemetry.setdefault("slow_stream_stale", 0)

    def on_start(self) -> None:
        route = _route_id(self._cfg.data_client_id)
        self.subscribe_quotes(self._cfg.instrument_id, client_id=route)
        self.subscribe_instrument_status(self._cfg.instrument_id, client_id=route)
        self.subscribe_data(BMD_STREAM_QUALITY_DATATYPE, client_id=route)
        self.subscribe_data(BMD_LIQUIDATION_DATATYPE, client_id=route)
        self.clock.set_timer(
            "hybrid_1hz",
            timedelta(milliseconds=self._cfg.timer_interval_ms),
        )

    def on_instrument_status(self, status: InstrumentStatus) -> None:
        if status.instrument_id != self._cfg.instrument_id:
            return
        if not status.is_trading:
            self._healthy = False
            if status.reason and "STALE" in str(status.reason):
                self.telemetry["stale_halts"] += 1
        else:
            self._healthy = True

    def on_data(self, data: Any) -> None:
        payload = getattr(data, "data", data)
        if isinstance(payload, BmdLiquidationData):
            self.telemetry["liquidations"].append(
                (payload.symbol, payload.side, payload.price, payload.ts_init)
            )
        elif isinstance(payload, BmdStreamQualityData):
            want_sym = self._cfg.instrument_id.symbol.value.removesuffix("-PERP")
            if payload.symbol != want_sym:
                return
            if payload.stream in ("liquidation", "reference_data"):
                if payload.status == "STALE":
                    self.telemetry["slow_stream_stale"] += 1
                # Slow/sparse stream timeout MUST NOT halt BBO execution or stop-loss exits.
                return
            if payload.stream == "book_ticker":
                if payload.status != "LIVE":
                    self._healthy = False
                    if payload.status == "STALE" or "STALE" in payload.reason:
                        self.telemetry["stale_halts"] += 1
                else:
                    self._healthy = True

    def on_time_event(self, event: Any) -> None:
        ts_ns = int(event.ts_event)
        self.telemetry["timer_ticks"].append(ts_ns)
        if not self._healthy or self._last_mid is None or self._closing:
            return
        if self._entry_px is None and self.portfolio.is_net_flat(self._cfg.instrument_id):
            self._entry_px = self._last_mid
            self.telemetry["entry_ts_ns"] = ts_ns
            order = self.order_factory.market(
                instrument_id=self._cfg.instrument_id,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_str(self._cfg.order_qty),
            )
            _submit_routed(self, order, self._cfg.exec_client_id)

    def on_quote(self, tick: QuoteTick) -> None:
        if tick.instrument_id != self._cfg.instrument_id:
            return
        mid = (float(tick.bid_price) + float(tick.ask_price)) * 0.5
        self._last_mid = mid
        if self._entry_px is not None and not self._closing and self.portfolio.is_net_long(
            self._cfg.instrument_id
        ):
            drop_bps = ((self._entry_px - mid) / self._entry_px) * 10_000.0
            if drop_bps >= self._cfg.stop_loss_bps:
                self._closing = True
                self.telemetry["stop_loss_ts_ns"] = int(tick.ts_init)
                order = self.order_factory.market(
                    instrument_id=self._cfg.instrument_id,
                    order_side=OrderSide.SELL,
                    quantity=Quantity.from_str(self._cfg.order_qty),
                )
                _submit_routed(self, order, self._cfg.exec_client_id)


class MakerLimitPostOnlyStrategyConfig(StrategyConfig):
    """Configuration for Pattern 1b: LIMIT / POST_ONLY Maker order interface."""

    def __init__(
        self,
        *,
        instrument_id: InstrumentId,
        order_qty: str = "0.01",
        bid_offset_ticks: int = 0,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        telemetry: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.order_qty = order_qty
        self.bid_offset_ticks = bid_offset_ticks
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.telemetry = telemetry if telemetry is not None else {}


class MakerLimitPostOnlyStrategy(Strategy):
    """Pattern 1b: LIMIT / POST_ONLY Maker order interface with explicit L2 queue disclaimer."""

    def __init__(self, config: MakerLimitPostOnlyStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.telemetry = config.telemetry
        self._submitted = False
        self.telemetry["queue_boundary_notice"] = MAKER_QUEUE_BOUNDARY_NOTICE
        self.telemetry.setdefault("orders_submitted", 0)
        self.telemetry.setdefault("orders_rejected", 0)
        self.telemetry.setdefault("orders_filled", 0)

    def on_start(self) -> None:
        route = _route_id(self._cfg.data_client_id)
        self.subscribe_quotes(self._cfg.instrument_id, client_id=route)

    def on_quote(self, tick: QuoteTick) -> None:
        if tick.instrument_id != self._cfg.instrument_id or self._submitted:
            return
        inst = self.cache.instrument(self._cfg.instrument_id)
        if inst is None:
            return
        px = tick.bid_price.as_decimal() - (inst.price_increment.as_decimal() * self._cfg.bid_offset_ticks)
        limit_px = Price.from_decimal_dp(px, inst.price_precision)
        order = self.order_factory.limit(
            instrument_id=self._cfg.instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str(self._cfg.order_qty),
            price=limit_px,
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        self._submitted = True
        self.telemetry["orders_submitted"] += 1
        _submit_routed(self, order, self._cfg.exec_client_id)

    def on_order_rejected(self, event: Any) -> None:
        self.telemetry["orders_rejected"] += 1

    def on_order_filled(self, event: Any) -> None:
        self.telemetry["orders_filled"] += 1


__all__ = [
    "AllowlistViolationError",
    "CrossAssetFactorStrategy",
    "CrossAssetFactorStrategyConfig",
    "L2MicrostructureStrategy",
    "L2MicrostructureStrategyConfig",
    "MAKER_QUEUE_BOUNDARY_NOTICE",
    "MakerLimitPostOnlyStrategy",
    "MakerLimitPostOnlyStrategyConfig",
    "MultiTimescaleHybridStrategy",
    "MultiTimescaleHybridStrategyConfig",
    "TradingAllowlistGuard",
]
