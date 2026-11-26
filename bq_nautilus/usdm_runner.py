"""Strict USD-M One-Way Isolated Perpetual Runner, Protection Lifecycle & Demo Reconciliation (M-NT3)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import hmac
import json
import os
import time
from typing import Any, Literal, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request

from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig, StrategyConfig
from nautilus_trader.execution import MakerTakerFeeModel, StaticLatencyModel
from nautilus_trader.model import (
    AccountType,
    BookType,
    ClientId,
    Currency,
    FundingRateUpdate,
    InstrumentId,
    MarkPriceUpdate,
    Money,
    OmsType,
    OrderSide,
    Price,
    Quantity,
    QuoteTick,
    TraderId,
    TriggerType,
    Venue,
)
from nautilus_trader.trading import Strategy

from bq_feed.events import Market

from .instruments import (
    BmdInstrumentSpec,
    DEFAULT_USDM_TIER1_BRACKETS,
    LeverageBracketTier,
)
from .usdm_accounting import (
    BinanceTieredMarginModel,
    IsolatedUsdmLedger,
    UsdmOrderPlan,
    plan_usdm_isolated_order,
)

BINANCE_VENUE = Venue("BINANCE")
USDT = Currency.from_str("USDT")


def redact_sensitive(obj: Any) -> Any:
    """Recursively mask API keys, secrets, and HMAC signatures for evidence serialization."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            kl = str(k).lower()
            if any(tok in kl for tok in ("api_key", "apikey", "api_secret", "secret", "signature", "listenkey", "x-mbx-apikey")):
                out[k] = "***REDACTED***"
            else:
                out[k] = redact_sensitive(v)
        return out
    if isinstance(obj, list):
        return [redact_sensitive(x) for x in obj]
    if isinstance(obj, str) and "signature=" in obj:
        base, _, _ = obj.partition("signature=")
        return base + "signature=***REDACTED***"
    return obj


class UsdmPerpAuditStrategyConfig(StrategyConfig):
    """Configuration for one-way isolated USD-M perpetual audit & protection lifecycle."""

    def __init__(
        self,
        *,
        instrument_id: Any,
        allowed_symbol: str = "SOLUSDT",
        leverage: int = 5,
        order_qty: str = "0.20",
        side: Literal["BUY", "SELL"] = "BUY",
        protection_mode: Literal["none", "quantity_reduce_only", "close_position"] = "quantity_reduce_only",
        working_type: Literal["MARK_PRICE", "CONTRACT_PRICE"] = "MARK_PRICE",
        stop_distance_pct: str = "0.02",
        entry_mode: Literal["market_on_first_quote", "plan_scripted"] = "market_on_first_quote",
        scripted_actions: tuple[dict[str, Any], ...] = (),
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        subscribe_funding: bool = True,
        subscribe_mark: bool = True,
        online: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.allowed_symbol = allowed_symbol
        self.leverage = int(leverage)
        self.order_qty = order_qty
        self.side = side
        self.protection_mode = protection_mode
        self.working_type = working_type
        self.stop_distance_pct = stop_distance_pct
        self.entry_mode = entry_mode
        self.scripted_actions = scripted_actions
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.subscribe_funding = subscribe_funding
        self.subscribe_mark = subscribe_mark
        self.online = online


class UsdmPerpAuditStrategy(Strategy):
    """Deterministic single-symbol one-way USD-M perpetual strategy shared across Backtest and Live/Demo."""

    def __init__(self, config: UsdmPerpAuditStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.callbacks_count = 0
        self.net_pos = Decimal(0)
        self.visible_predictions: list[dict[str, Any]] = []
        self.mark_updates: list[dict[str, Any]] = []
        self.intents: list[dict[str, Any]] = []
        self.fills: list[dict[str, Any]] = []
        self.order_events: list[dict[str, Any]] = []
        self.rejected_orders: list[dict[str, Any]] = []
        self.active_protection_client_id: Any = None
        self.active_protection_qty: Decimal = Decimal(0)
        self._last_quote_px: Decimal | None = None
        self._last_mark_px: Decimal | None = None
        self._entry_submitted = False
        self._scripted_done: set[int] = set()
        self._shutting_down = False

    def _route_data(self) -> ClientId | None:
        return ClientId(self._cfg.data_client_id) if self._cfg.data_client_id else None

    def _route_exec(self) -> ClientId | None:
        return ClientId(self._cfg.exec_client_id) if self._cfg.exec_client_id else None

    def _submit(self, order: Any, *, params: dict[str, Any] | None = None) -> None:
        route = self._route_exec()
        if route is not None and params is not None:
            self.submit_order(order, client_id=route, params=params)
        elif route is not None:
            self.submit_order(order, client_id=route)
        elif params is not None:
            self.submit_order(order, params=params)
        else:
            self.submit_order(order)

    def _assert_allowed_instrument(self, inst_id: Any) -> None:
        sym = inst_id.symbol.value.removesuffix("-PERP")
        if sym != self._cfg.allowed_symbol:
            raise RuntimeError(f"M-NT3 allowlist violation: {inst_id} != {self._cfg.allowed_symbol}")

    def on_start(self) -> None:
        self._assert_allowed_instrument(self._cfg.instrument_id)
        route = self._route_data()
        self.subscribe_quotes(self._cfg.instrument_id, client_id=route)
        if self._cfg.subscribe_mark:
            self.subscribe_mark_prices(self._cfg.instrument_id, client_id=route)
        if self._cfg.subscribe_funding:
            self.subscribe_funding_rates(self._cfg.instrument_id, client_id=route)

    def on_funding_rate(self, funding: FundingRateUpdate) -> None:
        self.visible_predictions.append(
            {
                "rate": str(funding.rate),
                "next_funding_ns": funding.next_funding_ns,
                "ts_event": int(funding.ts_event),
                "ts_init": int(funding.ts_init),
                "clock_ns": int(self.clock.timestamp_ns()),
            }
        )

    def on_mark_price(self, mark: MarkPriceUpdate) -> None:
        self._last_mark_px = Decimal(str(mark.value))
        self.mark_updates.append(
            {
                "mark_price": str(mark.value),
                "ts_event": int(mark.ts_event),
                "ts_init": int(mark.ts_init),
            }
        )

    def _quantize_price(self, px: Decimal) -> Price:
        inst = self.cache.instrument(self._cfg.instrument_id)
        if inst is None:
            return Price.from_str(f"{px:.2f}")
        inc = Decimal(str(inst.price_increment))
        dp = int(inst.price_precision)
        qpx = (px / inc).to_integral_value() * inc
        return Price.from_str(f"{qpx:.{dp}f}")

    def _quantize_qty(self, qty: Decimal) -> Quantity:
        inst = self.cache.instrument(self._cfg.instrument_id)
        if inst is None:
            return Quantity.from_str(str(qty))
        dp = int(inst.size_precision)
        return Quantity.from_str(f"{qty:.{dp}f}")

    def _ensure_protection_for_pos(self, ref_px: Decimal) -> None:
        if self._cfg.protection_mode == "none" or self.net_pos == 0:
            return
        abs_pos = abs(self.net_pos)
        if self.active_protection_client_id is not None:
            if self._cfg.protection_mode == "close_position":
                return
            if self.active_protection_qty == abs_pos:
                return
            # Exposure changed (e.g. partial fill or add): cancel previous quantity stop before re-arming
            old_order = self.cache.order(self.active_protection_client_id)
            if old_order is not None and not old_order.is_closed:
                route = self._route_exec()
                if route is not None:
                    self.cancel_order(old_order.client_order_id, client_id=route)
                else:
                    self.cancel_order(old_order.client_order_id)

        close_side = OrderSide.SELL if self.net_pos > 0 else OrderSide.BUY
        dist = Decimal(self._cfg.stop_distance_pct)
        trig_raw = ref_px * (Decimal(1) - dist) if self.net_pos > 0 else ref_px * (Decimal(1) + dist)
        trig_px = self._quantize_price(trig_raw)
        trig_type = (
            TriggerType.MARK_PRICE
            if self._cfg.working_type == "MARK_PRICE"
            else TriggerType.LAST_PRICE
        )
        stop_order = self.order_factory.stop_market(
            self._cfg.instrument_id,
            close_side,
            self._quantize_qty(abs_pos),
            trig_px,
            trigger_type=trig_type,
            reduce_only=True,
        )
        params = {"close_position": True} if self._cfg.protection_mode == "close_position" else None
        self.active_protection_client_id = stop_order.client_order_id
        self.active_protection_qty = abs_pos
        self.intents.append(
            {
                "kind": f"protection_{self._cfg.protection_mode}",
                "side": "SELL" if close_side == OrderSide.SELL else "BUY",
                "qty": str(abs_pos),
                "trigger_price": str(trig_px),
                "working_type": self._cfg.working_type,
                "ts_ns": int(self.clock.timestamp_ns()),
            }
        )
        self._submit(stop_order, params=params)

    def on_quote(self, quote: QuoteTick) -> None:
        self._assert_allowed_instrument(quote.instrument_id)
        self.callbacks_count += 1
        bid = Decimal(str(quote.bid_price))
        ask = Decimal(str(quote.ask_price))
        self._last_quote_px = (bid + ask) / Decimal(2)

        if self._cfg.entry_mode == "market_on_first_quote":
            if not self._entry_submitted:
                self._entry_submitted = True
                side = OrderSide.BUY if self._cfg.side == "BUY" else OrderSide.SELL
                qty = Decimal(self._cfg.order_qty)
                order = self.order_factory.market(
                    self._cfg.instrument_id,
                    side,
                    self._quantize_qty(qty),
                )
                self.intents.append(
                    {
                        "kind": "entry_market",
                        "side": self._cfg.side,
                        "qty": str(qty),
                        "callback": self.callbacks_count,
                        "ts_ns": int(self.clock.timestamp_ns()),
                        "predicted_rate_visible": self.visible_predictions[-1]["rate"] if self.visible_predictions else None,
                    }
                )
                self._submit(order)
            return

        # Scripted deterministic actions keyed by `callback` index
        for idx, act in enumerate(self._cfg.scripted_actions):
            if idx in self._scripted_done:
                continue
            if int(act["callback"]) != self.callbacks_count:
                continue
            self._scripted_done.add(idx)
            action_kind = act.get("action", "market")
            if action_kind == "market":
                s_str = str(act["side"])
                side = OrderSide.BUY if s_str == "BUY" else OrderSide.SELL
                qty = Decimal(str(act["qty"]))
                reduce_only = bool(act.get("reduce_only", False))
                order = self.order_factory.market(
                    self._cfg.instrument_id,
                    side,
                    self._quantize_qty(qty),
                    reduce_only=reduce_only,
                )
                self.intents.append(
                    {
                        "kind": "scripted_market",
                        "side": s_str,
                        "qty": str(qty),
                        "reduce_only": reduce_only,
                        "callback": self.callbacks_count,
                        "ts_ns": int(self.clock.timestamp_ns()),
                        "predicted_rate_visible": self.visible_predictions[-1]["rate"] if self.visible_predictions else None,
                    }
                )
                self._submit(order)

    def on_order_event(self, event: Any) -> None:
        self.order_events.append(
            {
                "type": type(event).__name__,
                "client_order_id": str(getattr(event, "client_order_id", "")),
                "ts_event": int(getattr(event, "ts_event", 0)),
            }
        )

    def on_order_rejected(self, event: Any) -> None:
        self.rejected_orders.append(
            {
                "client_order_id": str(event.client_order_id),
                "reason": str(getattr(event, "reason", "")),
                "ts_event": int(event.ts_event),
            }
        )

    def on_order_filled(self, event: Any) -> None:
        side = "BUY" if "BUY" in str(event.order_side) else "SELL"
        qty = Decimal(str(event.last_qty))
        px = Decimal(str(event.last_px))
        self.net_pos += qty if side == "BUY" else -qty
        self.fills.append(
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(getattr(event, "venue_order_id", "")),
                "trade_id": str(getattr(event, "trade_id", "")),
                "side": side,
                "qty": str(qty),
                "px": str(px),
                "fee": str(event.commission),
                "ts_event": int(event.ts_event),
                "ts_init": int(event.ts_init),
            }
        )
        if str(event.client_order_id) == str(self.active_protection_client_id) or self.net_pos == 0:
            if self.net_pos == 0:
                self.active_protection_client_id = None
                self.active_protection_qty = Decimal(0)
        elif self.net_pos != 0:
            self._ensure_protection_for_pos(px)


def run_usdm_backtest(
    *,
    spec: BmdInstrumentSpec,
    data: Sequence[Any],
    strategy_config: UsdmPerpAuditStrategyConfig,
    starting_usdt: Decimal = Decimal("50"),
    leverage: int = 5,
    brackets: Sequence[LeverageBracketTier] | None = None,
    maker_fee_rate: Decimal = Decimal("0.0002"),
    taker_fee_rate: Decimal = Decimal("0.0005"),
    latency_ns: int = 0,
    liquidation_enabled: bool = True,
    liquidation_trigger_ratio: float = 1.0,
) -> dict[str, Any]:
    """Run Nautilus BacktestEngine (MARGIN, NETTING, BinanceTieredMarginModel, liquidation_enabled) alongside IsolatedUsdmLedger."""
    if spec.market != Market.USDM:
        raise ValueError(f"Expected Market.USDM spec, got {spec.market}")
    inst = spec.to_nautilus_instrument()
    used_brackets = tuple(
        brackets
        or DEFAULT_USDM_TIER1_BRACKETS.get(
            spec.symbol,
            (LeverageBracketTier(1, 20, Decimal("0"), Decimal("100000"), Decimal("0.005"), Decimal("0")),),
        )
    )
    margin_model = BinanceTieredMarginModel(brackets_by_instrument={str(inst.id): used_brackets, spec.symbol: used_brackets})
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("MNT3-AUDIT-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
        )
    )
    try:
        engine.add_venue(
            venue=BINANCE_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money.from_str(f"{starting_usdt:.8f} USDT")],
            leverages={inst.id: Decimal(leverage)},
            margin_model=margin_model,
            fee_model=MakerTakerFeeModel(maker_fee_rate, taker_fee_rate),
            latency_model=StaticLatencyModel(latency_ns) if latency_ns > 0 else None,
            book_type=BookType.L1_MBP,
            liquidity_consumption=True,
            bar_execution=False,
            liquidation_enabled=liquidation_enabled,
            liquidation_trigger_ratio=liquidation_trigger_ratio,
        )
        engine.add_instrument(inst)
        strat = UsdmPerpAuditStrategy(strategy_config)
        engine.add_strategy(strat)

        # Feed only engine-compatible data chronologically; track FundingRateUpdate boundaries
        engine.add_data(list(data))
        engine.run()

        acc = engine.cache.account_for_venue(BINANCE_VENUE)
        total_usdt = Decimal(str(acc.balance_total(USDT).as_decimal())) if acc else Decimal(0)
        free_usdt = Decimal(str(acc.balance_free(USDT).as_decimal())) if acc else Decimal(0)
        locked_usdt = Decimal(str(acc.balance_locked(USDT).as_decimal())) if acc else Decimal(0)
        positions = list(engine.cache.positions())
        pos_summary = [
            {
                "id": str(p.id),
                "side": str(p.side),
                "quantity": str(p.quantity),
                "is_closed": bool(p.is_closed),
                "realized_pnl": str(p.realized_pnl) if p.realized_pnl is not None else None,
                "avg_px_open": str(p.avg_px_open),
                "avg_px_close": str(p.avg_px_close) if p.avg_px_close is not None else None,
            }
            for p in positions
        ]
        return {
            "instrument_id": str(inst.id),
            "starting_usdt": str(starting_usdt),
            "leverage": leverage,
            "final_total_usdt": str(total_usdt),
            "final_free_usdt": str(free_usdt),
            "final_locked_usdt": str(locked_usdt),
            "fills": strat.fills,
            "intents": strat.intents,
            "rejected_orders": strat.rejected_orders,
            "visible_predictions": strat.visible_predictions,
            "positions": pos_summary,
            "callbacks_count": strat.callbacks_count,
            "net_pos": str(strat.net_pos),
        }
    finally:
        engine.dispose()


def build_binance_usdm_demo_configs(
    *,
    account_id: str = "BINANCE-DEMO-USDM",
    symbols: Sequence[str] = ("SOLUSDT",),
    leverage: int = 5,
    api_key: str | None = None,
    api_secret: str | None = None,
    base_url_http: str | None = None,
    base_url_ws: str | None = None,
    demo: bool = True,
) -> tuple[Any, Any]:
    """Construct validated BinanceDataClientConfig & BinanceExecutionClientConfig for USD-M Demo or Mock Demo.

    Note: `use_ws_trading=False` is explicitly set because Nautilus `2.0.0rc9` routes Demo WS-Trading
    to `testnet.binancefuture.com` instead of `demo-fapi.binance.com`; REST execution hits `demo-fapi.binance.com` cleanly.
    """
    from nautilus_trader.adapters.binance import (
        BinanceDataClientConfig,
        BinanceEnvironment,
        BinanceExecutionClientConfig,
        BinanceInstrumentProviderConfig,
        BinanceMarginType,
        BinanceProductType,
    )
    from nautilus_trader.model import AccountId, OmsType

    env = BinanceEnvironment.DEMO if demo else BinanceEnvironment.TESTNET
    inst_ids = [f"{s}-PERP.BINANCE" for s in symbols]
    data_cfg = BinanceDataClientConfig(
        product_type=BinanceProductType.USD_M,
        environment=env,
        base_url_http=base_url_http,
        base_url_ws=base_url_ws,
        api_key=api_key,
        api_secret=api_secret,
        instrument_provider=BinanceInstrumentProviderConfig(load_ids=inst_ids),
        futures_diff_depth_interval_ms=100,
    )
    exec_cfg = BinanceExecutionClientConfig(
        account_id=AccountId(account_id),
        product_type=BinanceProductType.USD_M,
        environment=env,
        base_url_http=base_url_http,
        base_url_ws=base_url_ws,
        use_ws_trading=False,
        instrument_provider=BinanceInstrumentProviderConfig(load_ids=inst_ids),
        oms_type=OmsType.NETTING,
        use_position_ids=False,
        api_key=api_key,
        api_secret=api_secret,
        futures_leverages={str(s): int(leverage) for s in symbols},
        futures_margin_types={str(s): BinanceMarginType.ISOLATED for s in symbols},
    )
    return data_cfg, exec_cfg


class SignedUsdmRestClient:
    """Minimal HMAC-SHA256 client for Binance USD-M Demo endpoints (queries, income, algoOrder, leverageBracket)."""

    def __init__(
        self,
        *,
        base_url: str = "https://demo-fapi.binance.com",
        api_key: str | None = None,
        api_secret: str | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os.environ.get("BINANCE_DEMO_API_KEY", "")
        self.api_secret = api_secret if api_secret is not None else os.environ.get("BINANCE_DEMO_API_SECRET", "")
        self.timeout_s = timeout_s
        self.audit_log: list[dict[str, Any]] = []

    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _sign_params(self, params: Mapping[str, Any]) -> str:
        clean: list[tuple[str, str]] = []
        for k, v in params.items():
            if v is None:
                continue
            if isinstance(v, bool):
                clean.append((k, "true" if v else "false"))
            else:
                clean.append((k, str(v)))
        clean.append(("timestamp", str(int(time.time() * 1000))))
        query = urllib.parse.urlencode(clean)
        sig = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        signed: bool = True,
    ) -> Any:
        raw_params = dict(params or {})
        headers = {"Accept": "application/json", "User-Agent": "BinanceQuant/M-NT3"}
        if signed or self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        if signed:
            if not self.has_credentials():
                raise RuntimeError("BINANCE_DEMO_API_KEY and BINANCE_DEMO_API_SECRET are required for signed Demo calls")
            qs = self._sign_params(raw_params)
        else:
            qs = urllib.parse.urlencode({k: ("true" if v is True else ("false" if v is False else str(v))) for k, v in raw_params.items() if v is not None})

        url = f"{self.base_url}{path}"
        if method in ("GET", "DELETE") and qs:
            url = f"{url}?{qs}"
            body = None
        elif qs:
            body = qs.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            body = None

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        record: dict[str, Any] = {
            "method": method,
            "path": path,
            "params": redact_sensitive(raw_params),
            "ts_utc_ns": time.time_ns(),
        }
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw_text = resp.read().decode("utf-8")
                parsed = json.loads(raw_text) if raw_text else {}
                record["status"] = int(resp.status)
                self.audit_log.append(record)
                return parsed
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            record["status"] = int(exc.code)
            record["error"] = err_body[:500]
            self.audit_log.append(record)
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {err_body}") from exc

    def validate_algo_order_params(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        trigger_price: str,
        working_type: str = "MARK_PRICE",
        close_position: bool = False,
        quantity: str | None = None,
        reduce_only: bool | None = None,
        client_algo_id: str | None = None,
    ) -> dict[str, Any]:
        """Validate & format `/fapi/v1/algoOrder` request enforcing closePosition vs quantity/reduceOnly exclusion."""
        payload: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "triggerPrice": trigger_price,
            "workingType": working_type,
        }
        if client_algo_id:
            payload["clientAlgoId"] = client_algo_id
        if close_position:
            if quantity is not None or reduce_only is not None:
                raise ValueError("Binance USD-M algoOrder forbids quantity and reduceOnly when closePosition=true")
            payload["closePosition"] = True
        else:
            if not quantity:
                raise ValueError("Non-closePosition algoOrder requires explicit positive quantity")
            payload["quantity"] = quantity
            if reduce_only is not None:
                payload["reduceOnly"] = reduce_only
        return payload


__all__ = [
    "BINANCE_VENUE",
    "SignedUsdmRestClient",
    "USDT",
    "UsdmPerpAuditStrategy",
    "UsdmPerpAuditStrategyConfig",
    "build_binance_usdm_demo_configs",
    "redact_sensitive",
    "run_usdm_backtest",
]
