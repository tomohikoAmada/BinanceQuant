"""Strict Spot F1 Backtest, LiveNode Sandbox, Two-Stage Reconciliation, and Causality Gates."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
import gc
import hashlib
import json
import time
from typing import Any, Iterable, Literal

from nautilus_trader.adapters.sandbox import (
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig, StrategyConfig
from nautilus_trader.execution import MakerTakerFeeModel, StaticLatencyModel
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    AccountType,
    BookType,
    ClientId,
    Currency,
    Money,
    OmsType,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy

from bq_feed.events import BookTickerEvent, FeedEvent, Market, Stream
from bq_feed.parity import ParityReport, compare_overlapping_windows

from .custom_data import BMD_STREAM_QUALITY_DATATYPE, BmdStreamQualityData
from .live import BmdDataClientFactory, BmdGatewayConfig

VENUE = Venue("BINANCE")
USDT = Currency.from_str("USDT")


class BtcSpotBboStrategyConfig(StrategyConfig):
    """Configuration for deterministic BTCUSDT Spot F1 strategy."""

    def __init__(
        self,
        *,
        instrument_id: Any,
        rule_mode: Literal["imbalance", "buy_then_sell", "buy_once"] = "imbalance",
        order_qty: str = "0.000200",
        max_pos_qty: str = "0.000600",
        buy_imbalance_threshold: float = 0.25,
        sell_imbalance_threshold: float = 0.25,
        max_spread_bps: float = 20.0,
        sell_on_callback: int = 10,
        max_quotes: int | None = None,
        online: bool = False,
        record_callbacks: bool = True,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.rule_mode = rule_mode
        self.order_qty = order_qty
        self.max_pos_qty = max_pos_qty
        self.buy_imbalance_threshold = buy_imbalance_threshold
        self.sell_imbalance_threshold = sell_imbalance_threshold
        self.max_spread_bps = max_spread_bps
        self.sell_on_callback = sell_on_callback
        self.max_quotes = max_quotes
        self.online = online
        self.record_callbacks = record_callbacks
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id


class BtcSpotBboStrategy(Strategy):
    """Deterministic Spot F1 taker rule shared identically by BacktestEngine and LiveNode."""

    def __init__(self, config: BtcSpotBboStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.is_healthy = True
        self.blocked_by_health = 0
        self.callbacks_count = 0
        self._cb_hasher = hashlib.sha256()
        self.callbacks: list[dict[str, Any]] = []
        self.decision_times: list[int] = []
        self.intents: list[dict[str, Any]] = []
        self.fills: list[dict[str, Any]] = []
        self.health_transitions: list[dict[str, Any]] = []
        self.dequeue_latencies_ns: list[int] = []
        self._net_pos = Decimal(0)
        self._inflight_buy_qty = Decimal(0)
        self._inflight_sell_qty = Decimal(0)
        self._shutting_down = False

    def on_start(self) -> None:
        route = ClientId(self._cfg.data_client_id) if self._cfg.data_client_id else None
        self.subscribe_instrument_status(self._cfg.instrument_id, client_id=route)
        self.subscribe_data(BMD_STREAM_QUALITY_DATATYPE, client_id=route)
        self.subscribe_quotes(self._cfg.instrument_id, client_id=route)

    def on_data(self, data: Any) -> None:
        payload = getattr(data, "data", data)
        if isinstance(payload, BmdStreamQualityData) and payload.stream == "book_ticker":
            self.is_healthy = payload.status == "LIVE"
            self.health_transitions.append(
                {
                    "stream": payload.stream,
                    "is_trading": self.is_healthy,
                    "status": payload.status,
                    "reason": payload.reason,
                    "ts_event": int(payload.ts_event),
                    "ts_init": int(payload.ts_init),
                }
            )

    def on_instrument_status(self, status: Any) -> None:
        self.is_healthy = bool(status.is_trading)
        self.health_transitions.append(
            {
                "is_trading": self.is_healthy,
                "reason": str(status.reason),
                "ts_event": int(status.ts_event),
                "ts_init": int(status.ts_init),
            }
        )

    def _submit(self, side: OrderSide, qty: Decimal, reason: str) -> None:
        now_ns = time.time_ns() if self._cfg.online else int(self.clock.timestamp_ns())
        self.decision_times.append(now_ns)
        order = self.order_factory.market(
            self._cfg.instrument_id,
            side,
            Quantity.from_str(str(qty)),
        )
        self.intents.append(
            {
                "side": "BUY" if side == OrderSide.BUY else "SELL",
                "qty": str(qty),
                "decision_ns": now_ns,
                "callback_index": self.callbacks_count,
                "reason": reason,
            }
        )
        if side == OrderSide.BUY:
            self._inflight_buy_qty += qty
        else:
            self._inflight_sell_qty += qty
        exec_route = ClientId(self._cfg.exec_client_id) if self._cfg.exec_client_id else None
        if exec_route is not None:
            self.submit_order(order, client_id=exec_route)
        else:
            self.submit_order(order)

    def on_quote(self, quote: QuoteTick) -> None:
        cb_utc = time.time_ns() if self._cfg.online else int(self.clock.timestamp_ns())
        if self._cfg.online:
            self.dequeue_latencies_ns.append(max(0, cb_utc - int(quote.ts_init)))
        self.callbacks_count += 1
        cb_line = (
            f"{int(quote.ts_event)}|{int(quote.ts_init)}|{cb_utc}|"
            f"{quote.bid_price}|{quote.ask_price}|{quote.bid_size}|{quote.ask_size}\n"
        )
        self._cb_hasher.update(cb_line.encode("utf-8"))
        if self._cfg.record_callbacks:
            self.callbacks.append(
                {
                    "ts_event": int(quote.ts_event),
                    "ts_init": int(quote.ts_init),
                    "callback_ns": cb_utc,
                    "bid_price": str(quote.bid_price),
                    "ask_price": str(quote.ask_price),
                    "bid_size": str(quote.bid_size),
                    "ask_size": str(quote.ask_size),
                }
            )
        n = self.callbacks_count
        if not self.is_healthy:
            self.blocked_by_health += 1
            self._maybe_stop()
            return

        order_qty = Decimal(self._cfg.order_qty)
        max_pos = Decimal(self._cfg.max_pos_qty)

        if self._cfg.rule_mode == "buy_once":
            if n == 1:
                self._submit(OrderSide.BUY, order_qty, "buy_once")
        elif self._cfg.rule_mode == "buy_then_sell":
            if n == 1:
                self._submit(OrderSide.BUY, order_qty, "buy_then_sell:buy")
            elif n == self._cfg.sell_on_callback and self._net_pos > 0:
                self._submit(OrderSide.SELL, min(order_qty, self._net_pos), "buy_then_sell:sell")
        else:
            bid_p = Decimal(str(quote.bid_price))
            ask_p = Decimal(str(quote.ask_price))
            bid_q = Decimal(str(quote.bid_size))
            ask_q = Decimal(str(quote.ask_size))
            mid = (bid_p + ask_p) / Decimal(2)
            spread_bps = float(((ask_p - bid_p) / mid) * Decimal("10000")) if mid > 0 else 999999.0
            denom = bid_q + ask_q
            imb = float((bid_q - ask_q) / denom) if denom > 0 else 0.0
            if spread_bps <= self._cfg.max_spread_bps:
                effective_long = self._net_pos + self._inflight_buy_qty
                available_sell = max(Decimal(0), self._net_pos - self._inflight_sell_qty)
                if imb >= self._cfg.buy_imbalance_threshold and (effective_long + order_qty) <= max_pos:
                    can_afford = True
                    acc = self.portfolio.account(self._cfg.instrument_id.venue)
                    if acc is not None:
                        free_usdt = acc.balance_free(USDT)
                        if free_usdt is not None:
                            needed = (self._inflight_buy_qty + order_qty) * ask_p * Decimal("1.002")
                            can_afford = free_usdt.as_decimal() >= needed
                    if can_afford:
                        self._submit(OrderSide.BUY, order_qty, f"imb={imb:.4f}")
                elif imb <= -self._cfg.sell_imbalance_threshold and available_sell > 0:
                    self._submit(OrderSide.SELL, min(order_qty, available_sell), f"imb={imb:.4f}")
        self._maybe_stop()

    def on_order_filled(self, event: Any) -> None:
        side = str(event.order_side)
        qty = Decimal(str(event.last_qty))
        if "BUY" in side:
            self._net_pos += qty
            self._inflight_buy_qty = max(Decimal(0), self._inflight_buy_qty - qty)
        else:
            self._net_pos -= qty
            self._inflight_sell_qty = max(Decimal(0), self._inflight_sell_qty - qty)
        self.fills.append(
            {
                "side": side,
                "qty": str(event.last_qty),
                "px": str(event.last_px),
                "fee": str(event.commission),
                "ts_event": int(event.ts_event),
                "ts_init": int(event.ts_init),
            }
        )
        self._maybe_stop()

    def on_order_rejected(self, event: Any) -> None:
        self._release_inflight(event)

    def on_order_canceled(self, event: Any) -> None:
        self._release_inflight(event)

    def on_order_expired(self, event: Any) -> None:
        self._release_inflight(event)

    def _release_inflight(self, event: Any) -> None:
        side = str(getattr(event, "order_side", ""))
        qty = Decimal(str(getattr(event, "quantity", 0) or 0))
        if "BUY" in side:
            self._inflight_buy_qty = max(Decimal(0), self._inflight_buy_qty - qty)
        elif "SELL" in side:
            self._inflight_sell_qty = max(Decimal(0), self._inflight_sell_qty - qty)

    def _maybe_stop(self) -> None:
        if (
            self._cfg.online
            and not self._shutting_down
            and self._cfg.max_quotes is not None
            and self.callbacks_count >= self._cfg.max_quotes
            and (len(self.intents) == 0 or len(self.fills) >= len(self.intents))
        ):
            self._shutting_down = True
            self.shutdown_system("Finished bounded M-NT1 sandbox run")


def run_spot_backtest(
    *,
    instrument: Any,
    data: list[Any],
    strategy_config: BtcSpotBboStrategyConfig,
    starting_usdt: Decimal = Decimal("50"),
    fee_rate: Decimal = Decimal("0.001"),
    latency_ns: int = 0,
    liquidity_consumption: bool = True,
) -> dict[str, Any]:
    """Run strict Spot F1 BacktestEngine (L1_MBP, liquidity_consumption, bar_execution=False)."""
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("AUDIT-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            starting_balances=[Money.from_str(f"{starting_usdt} USDT")],
            book_type=BookType.L1_MBP,
            fee_model=MakerTakerFeeModel(maker_rate=fee_rate, taker_rate=fee_rate),
            latency_model=StaticLatencyModel(base_latency_nanos=latency_ns),
            liquidity_consumption=liquidity_consumption,
            bar_execution=False,
        )
        engine.add_instrument(instrument)
        engine.add_data(data)
        strat = BtcSpotBboStrategy(strategy_config)
        engine.add_strategy(strat)
        engine.run()

        acc = engine.portfolio.account(VENUE)
        base_ccy = instrument.base_currency
        base_bal = acc.balance_total(base_ccy)
        quote_bal = acc.balance_total(USDT)
        cb_hash = (
            hashlib.sha256(json.dumps(strat.callbacks, sort_keys=True).encode("utf-8")).hexdigest()
            if strat.callbacks
            else strat._cb_hasher.hexdigest()
        )
        return {
            "callbacks_count": strat.callbacks_count,
            "callbacks": strat.callbacks,
            "callback_sha256": cb_hash,
            "decision_times": strat.decision_times,
            "intents": strat.intents,
            "fills": strat.fills,
            "blocked_by_health": strat.blocked_by_health,
            "health_transitions": strat.health_transitions,
            "cash_usdt": str(quote_bal.as_decimal()) if quote_bal else "0",
            "base_qty": str(base_bal.as_decimal()) if base_bal else "0",
        }
    finally:
        engine.dispose()


def hand_verify_spot_ledger(
    *,
    starting_usdt: Decimal,
    fee_rate: Decimal,
    fills: list[dict[str, Any]],
    reported_cash_usdt: str,
    reported_base_qty: str,
) -> dict[str, Any]:
    """Independently hand-calculate Spot cash, base position, and fees against Nautilus."""
    cash = starting_usdt
    base = Decimal(0)
    total_fee_usdt = Decimal(0)
    per_fill: list[dict[str, Any]] = []
    for f in fills:
        qty = Decimal(f["qty"])
        px = Decimal(f["px"])
        notional = qty * px
        expected_fee = (notional * fee_rate).quantize(Decimal("0.00000001"))
        fee_parts = str(f["fee"]).split()
        actual_fee = Decimal(fee_parts[0])
        fee_asset = fee_parts[1] if len(fee_parts) > 1 else ""
        assert fee_asset == "USDT", f"Expected USDT fee asset, got {f['fee']}"
        assert actual_fee == expected_fee, f"Fee mismatch: {actual_fee} != {expected_fee}"
        is_buy = "BUY" in f["side"]
        if is_buy:
            cash -= notional + expected_fee
            base += qty
        else:
            cash += notional - expected_fee
            base -= qty
        total_fee_usdt += expected_fee
        per_fill.append(
            {
                "side": f["side"],
                "qty": str(qty),
                "px": str(px),
                "notional": str(notional),
                "fee_usdt": str(expected_fee),
                "cash_after": str(cash),
                "base_after": str(base),
            }
        )
    cash_match = Decimal(reported_cash_usdt) == cash
    base_match = Decimal(reported_base_qty) == base
    return {
        "expected_cash_usdt": str(cash),
        "reported_cash_usdt": reported_cash_usdt,
        "expected_base_qty": str(base),
        "reported_base_qty": reported_base_qty,
        "total_fee_usdt": str(total_fee_usdt),
        "cash_match": cash_match,
        "base_match": base_match,
        "passed": cash_match and base_match,
        "per_fill": per_fill,
    }


async def run_spot_sandbox_live(
    *,
    target: str,
    instrument: Any,
    market: Market,
    symbol: str,
    max_items: int = 20,
    rule_mode: Literal["imbalance", "buy_then_sell", "buy_once"] = "buy_once",
    order_qty: str = "0.000200",
    max_pos_qty: str = "0.000600",
    starting_usdt: Decimal = Decimal("50"),
    fee_rate: Decimal = Decimal("0.001"),
    stale_threshold_ns: int = 5_000_000_000,
    timeout_secs: float = 30.0,
    stub: Any | None = None,
) -> dict[str, Any]:
    """Run Nautilus LiveNode + SandboxExecutionClient against BMD Gateway gRPC."""
    stats: dict[str, Any] = {"accepted": 0, "rows": [], "health_events": [], "other_payloads": []}
    cfg = BmdGatewayConfig(
        target=target,
        instruments={instrument.id: (market, symbol, instrument)},
        stale_threshold_ns=stale_threshold_ns,
        max_items=max_items,
        rpc_timeout_secs=max(10.0, timeout_secs - 2.0),
        stats=stats,
    )
    node = None
    try:
        node = (
            LiveNode.builder("MNT1-SPOT-SANDBOX", TraderId("AUDIT-001"), Environment.SANDBOX)
            .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR))
            .with_timeout_connection(5)
            .with_timeout_disconnection_secs(2)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .with_reconciliation(reconciliation=False)
            .add_data_client("BMD", BmdDataClientFactory(stub=stub), cfg)
            .add_simulated_exec_client(
                "BINANCE",
                SandboxExecutionClientFactory(),
                SandboxExecutionClientConfig(
                    venue=VENUE,
                    account_id=AccountId("BINANCE-SANDBOX-001"),
                    account_type=AccountType.CASH,
                    oms_type=OmsType.NETTING,
                    book_type=BookType.L1_MBP,
                    starting_balances=[Money.from_str(f"{starting_usdt} USDT")],
                    fee_model=MakerTakerFeeModel(maker_rate=fee_rate, taker_rate=fee_rate),
                    liquidity_consumption=True,
                    bar_execution=False,
                ),
            )
            .build()
        )
        strat = BtcSpotBboStrategy(
            BtcSpotBboStrategyConfig(
                instrument_id=instrument.id,
                rule_mode=rule_mode,
                order_qty=order_qty,
                max_pos_qty=max_pos_qty,
                max_quotes=max_items,
                online=True,
                data_client_id="BMD",
                exec_client_id="BINANCE",
            )
        )
        node.add_strategy(strat)
        try:
            await asyncio.wait_for(node.run_async(), timeout=timeout_secs)
        except RuntimeError:
            err = stats.get("connect_error")
            if err is not None:
                raise err from None
            raise
    finally:
        if node is not None:
            node.dispose()
        node = None
        gc.collect()

    rows = stats["rows"]
    for row, cb in zip(rows, strat.callbacks, strict=True):
        row["strategy_visible_ns"] = cb["callback_ns"]
    sorted_lat = sorted(strat.dequeue_latencies_ns)
    p99_ns = sorted_lat[int(0.99 * (len(sorted_lat) - 1))] if sorted_lat else 0
    return {
        "market": market.value,
        "symbol": symbol,
        "accepted": stats["accepted"],
        "callbacks_count": strat.callbacks_count,
        "unique_update_ids": len({r["update_id"] for r in rows if r["update_id"] is not None}),
        "intents": strat.intents,
        "sandbox_fills": strat.fills,
        "blocked_by_health": strat.blocked_by_health,
        "health_events": stats["health_events"],
        "dequeue_latency_p99_ns": p99_ns,
        "dequeue_latency_max_ns": max(sorted_lat) if sorted_lat else 0,
        "rows": rows,
        "strategy_callbacks": strat.callbacks,
    }


@dataclass(slots=True)
class SpotWindowReconcileResult:
    stage1_passed: bool
    stage1_details: dict[str, Any]
    stage2_parity: ParityReport
    cross_boundary_paired_count: int
    accepted: bool


def reconcile_spot_window(
    *,
    instrument: Any,
    gateway_rows: list[dict[str, Any]],
    recorder_events: Iterable[FeedEvent],
    window_start_ns: int,
    window_end_ns: int,
    expected_fills: list[dict[str, Any]] | None = None,
    starting_usdt: Decimal = Decimal("50"),
    fee_rate: Decimal = Decimal("0.001"),
    rule_mode: Literal["imbalance", "buy_then_sell", "buy_once"] = "buy_once",
    order_qty: str = "0.000200",
    max_pos_qty: str = "0.000600",
    record_callbacks: bool = True,
) -> SpotWindowReconcileResult:
    """Two-stage M-NT1 reconciliation: Stage 1 callback replay+hand ledger, Stage 2 Recorder UID parity."""
    p_dp = instrument.price_precision
    s_dp = instrument.size_precision
    ticks: list[QuoteTick] = []
    gw_events: list[BookTickerEvent] = []
    for seq_idx, row in enumerate(gateway_rows, start=1):
        ts_event = int(row["ts_event"])
        ts_init = int(row.get("strategy_visible_ns") or row["gateway_receive_ns"])
        ticks.append(
            QuoteTick(
                instrument_id=instrument.id,
                bid_price=Price.from_decimal_dp(Decimal(row["bid"]), p_dp),
                ask_price=Price.from_decimal_dp(Decimal(row["ask"]), p_dp),
                bid_size=Quantity.from_decimal_dp(Decimal(row["bid_size"]), s_dp),
                ask_size=Quantity.from_decimal_dp(Decimal(row["ask_size"]), s_dp),
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
        gw_events.append(
            BookTickerEvent(
                venue="BINANCE",
                market=Market.SPOT,
                symbol=str(instrument.raw_symbol),
                best_bid_price=Decimal(row["bid"]),
                best_bid_quantity=Decimal(row["bid_size"]),
                best_ask_price=Decimal(row["ask"]),
                best_ask_quantity=Decimal(row["ask_size"]),
                update_id=row.get("update_id"),
                receive_time_utc_ns=int(row["gateway_receive_ns"]),
                source="gateway_grpc",
                delivery_seq=seq_idx,
            )
        )

    bt_res = run_spot_backtest(
        instrument=instrument,
        data=ticks,
        strategy_config=BtcSpotBboStrategyConfig(
            instrument_id=instrument.id,
            rule_mode=rule_mode,
            order_qty=order_qty,
            max_pos_qty=max_pos_qty,
            record_callbacks=record_callbacks,
        ),
        starting_usdt=starting_usdt,
        fee_rate=fee_rate,
    )
    del ticks
    gc.collect()
    hand = hand_verify_spot_ledger(
        starting_usdt=starting_usdt,
        fee_rate=fee_rate,
        fills=bt_res["fills"],
        reported_cash_usdt=bt_res["cash_usdt"],
        reported_base_qty=bt_res["base_qty"],
    )
    fills_match = True
    if expected_fills is not None:
        fills_match = [
            {"side": f["side"], "qty": f["qty"], "px": f["px"], "fee": f["fee"]}
            for f in bt_res["fills"]
        ] == [
            {"side": f["side"], "qty": f["qty"], "px": f["px"], "fee": f["fee"]}
            for f in expected_fills
        ]
    stage1_ok = (
        bt_res["callbacks_count"] == len(gateway_rows)
        and len(gateway_rows) > 0
        and hand["passed"]
        and fills_match
    )

    rec_list = list(recorder_events)
    parity = compare_overlapping_windows(
        rec_list,
        gw_events,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns,
    )
    # Count matched pairs where one side fell outside [window_start_ns, window_end_ns]
    rec_by_uid = {
        e.update_id: e.receive_time_utc_ns
        for e in rec_list
        if isinstance(e, BookTickerEvent) and e.update_id is not None
    }
    gw_by_uid = {
        e.update_id: e.receive_time_utc_ns
        for e in gw_events
        if e.update_id is not None
    }
    cross_boundary = 0
    for uid in set(rec_by_uid) & set(gw_by_uid):
        rt = rec_by_uid[uid]
        gt = gw_by_uid[uid]
        if rt is None or gt is None:
            continue
        r_in = window_start_ns <= rt <= window_end_ns
        g_in = window_start_ns <= gt <= window_end_ns
        if r_in != g_in:
            cross_boundary += 1

    stage2_ok = (
        parity.status == "PASSED"
        and parity.matched_count > 0
        and parity.recorder_coverage == 1.0
        and parity.gateway_coverage == 1.0
        and not parity.field_mismatches
    )
    return SpotWindowReconcileResult(
        stage1_passed=stage1_ok,
        stage1_details={
            "callbacks": bt_res["callbacks_count"],
            "fills_match_expected": fills_match,
            "hand_ledger": hand,
            "callback_sha256": bt_res["callback_sha256"],
        },
        stage2_parity=parity,
        cross_boundary_paired_count=cross_boundary,
        accepted=stage1_ok and stage2_ok,
    )


def verify_spot_causality_and_delay(
    *,
    instrument: Any,
    events: list[BookTickerEvent],
    split_index: int,
    strategy_config: BtcSpotBboStrategyConfig,
    starting_usdt: Decimal = Decimal("50"),
    fee_rate: Decimal = Decimal("0.001"),
    order_latency_ns: int = 2_000_000,
    backlog_delay_ns: int = 2_000_000_000,
) -> dict[str, Any]:
    """Verify prefix invariance under future mutation/append and 2s backlog delivery shift."""
    from .recorder import bmd_book_ticker_to_quote_tick

    base_ticks = [bmd_book_ticker_to_quote_tick(ev, instrument)[0] for ev in events]
    base_res = run_spot_backtest(
        instrument=instrument,
        data=base_ticks,
        strategy_config=strategy_config,
        starting_usdt=starting_usdt,
        fee_rate=fee_rate,
        latency_ns=order_latency_ns,
    )

    cutoff_ts = int(base_ticks[split_index].ts_init)
    mutated_ticks = list(base_ticks[: split_index + 1])
    for t in base_ticks[split_index + 1 :]:
        mutated_ticks.append(
            QuoteTick(
                instrument_id=t.instrument_id,
                bid_price=Price.from_decimal_dp(t.bid_price.as_decimal() * Decimal("2"), instrument.price_precision),
                ask_price=Price.from_decimal_dp(t.ask_price.as_decimal() * Decimal("2"), instrument.price_precision),
                bid_size=t.bid_size,
                ask_size=t.ask_size,
                ts_event=t.ts_event,
                ts_init=t.ts_init,
            )
        )
    # Also append an extra synthetic future quote at the end
    last = mutated_ticks[-1]
    mutated_ticks.append(
        QuoteTick(
            instrument_id=last.instrument_id,
            bid_price=last.bid_price,
            ask_price=last.ask_price,
            bid_size=last.bid_size,
            ask_size=last.ask_size,
            ts_event=last.ts_event + 1_000_000,
            ts_init=last.ts_init + 1_000_000,
        )
    )
    pert_res = run_spot_backtest(
        instrument=instrument,
        data=mutated_ticks,
        strategy_config=strategy_config,
        starting_usdt=starting_usdt,
        fee_rate=fee_rate,
        latency_ns=order_latency_ns,
    )

    base_prefix_cb = [c for c in base_res["callbacks"] if c["ts_init"] <= cutoff_ts]
    pert_prefix_cb = [c for c in pert_res["callbacks"] if c["ts_init"] <= cutoff_ts]
    base_prefix_int = [i for i in base_res["intents"] if i["decision_ns"] <= cutoff_ts]
    pert_prefix_int = [i for i in pert_res["intents"] if i["decision_ns"] <= cutoff_ts]
    base_prefix_fills = [f for f in base_res["fills"] if f["ts_event"] <= cutoff_ts]
    pert_prefix_fills = [f for f in pert_res["fills"] if f["ts_event"] <= cutoff_ts]
    prefix_invariant = (
        base_prefix_cb == pert_prefix_cb
        and base_prefix_int == pert_prefix_int
        and base_prefix_fills == pert_prefix_fills
    )

    delayed_ticks = [bmd_book_ticker_to_quote_tick(ev, instrument, delay_ns=backlog_delay_ns)[0] for ev in events]
    delayed_res = run_spot_backtest(
        instrument=instrument,
        data=delayed_ticks,
        strategy_config=strategy_config,
        starting_usdt=starting_usdt,
        fee_rate=fee_rate,
        latency_ns=order_latency_ns,
    )
    delay_shift_ok = (
        len(delayed_res["decision_times"]) == len(base_res["decision_times"])
        and all(
            d - b == backlog_delay_ns
            for d, b in zip(delayed_res["decision_times"], base_res["decision_times"], strict=True)
        )
        and all(
            f["ts_event"] >= delayed_res["decision_times"][idx] + order_latency_ns
            for idx, f in enumerate(delayed_res["fills"])
            if idx < len(delayed_res["decision_times"])
        )
    )
    return {
        "prefix_invariant": prefix_invariant,
        "delay_shift_ok": delay_shift_ok,
        "passed": prefix_invariant and delay_shift_ok,
        "base_decisions": len(base_res["decision_times"]),
        "delayed_decisions": len(delayed_res["decision_times"]),
    }


__all__ = [
    "BtcSpotBboStrategy",
    "BtcSpotBboStrategyConfig",
    "SpotWindowReconcileResult",
    "VENUE",
    "hand_verify_spot_ledger",
    "reconcile_spot_window",
    "run_spot_backtest",
    "run_spot_sandbox_live",
    "verify_spot_causality_and_delay",
]
