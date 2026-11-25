"""Causal ML/DL bridge tying Nautilus market feeds to bq_feature and bq_model."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
import time
from typing import Any, Sequence

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model import (
    AggressorSide,
    ClientId,
    InstrumentId,
    InstrumentStatus,
    MarkPriceUpdate,
    OrderSide,
    Quantity,
    QuoteTick,
    TradeTick,
)
from nautilus_trader.trading import Strategy

from bq_feed.events import (
    AggTradeEvent,
    BookTickerEvent,
    LiquidationEvent,
    Market,
    MarkPriceEvent,
    Stream,
    StreamHealthEvent,
)
from bq_feature.engine import FeatureEngine, FeatureState, FeatureVector
from bq_model.bundle import DEFAULT_FEATURES, ModelBundle, TrainReport, fit_walk_forward_model

from .custom_data import BmdLiquidationData


class CausalCutoffError(RuntimeError):
    """Raised when attempting inference/trading at or before model.trained_until_ns."""


@dataclass(slots=True)
class CausalTrainingSample:
    """Single causal observation bound to a contiguous healthy epoch of one product."""

    market: str
    symbol: str
    epoch_id: int
    feature_row: dict[str, Any]
    mid_price: float


class CausalFeatureBridge:
    """Translates Nautilus data objects into causal FeatureVectors via bq_feature.FeatureEngine."""

    def __init__(
        self,
        *,
        market: Market,
        symbol: str,
        strict_receive_time: bool = False,
    ) -> None:
        self.market = market
        self.symbol = symbol
        self.engine = FeatureEngine(strict_receive_time=strict_receive_time)
        self.state = FeatureState()
        self.epoch_id = 0
        self._seq = 0
        self._samples: list[CausalTrainingSample] = []

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def on_quote(self, tick: QuoteTick, *, record: bool = False) -> FeatureVector | None:
        ev = BookTickerEvent(
            venue="BINANCE",
            market=self.market,
            symbol=self.symbol,
            delivery_seq=self._next_seq(),
            exchange_event_time_ms=int(tick.ts_event // 1_000_000),
            receive_time_utc_ns=int(tick.ts_init),
            best_bid_price=tick.bid_price.as_decimal(),
            best_bid_quantity=tick.bid_size.as_decimal(),
            best_ask_price=tick.ask_price.as_decimal(),
            best_ask_quantity=tick.ask_size.as_decimal(),
        )
        fv = self.engine.update(self.state, ev)
        if record and fv is not None and fv.health_valid and fv.mid_price > 0:
            row = asdict(fv)
            self._samples.append(
                CausalTrainingSample(
                    market=self.market.value,
                    symbol=self.symbol,
                    epoch_id=self.epoch_id,
                    feature_row=row,
                    mid_price=fv.mid_price,
                )
            )
        return fv

    def on_trade(self, trade: TradeTick) -> FeatureVector | None:
        ev = AggTradeEvent(
            venue="BINANCE",
            market=self.market,
            symbol=self.symbol,
            delivery_seq=self._next_seq(),
            exchange_event_time_ms=int(trade.ts_event // 1_000_000),
            receive_time_utc_ns=int(trade.ts_init),
            aggregate_trade_id=int(str(trade.trade_id)) if str(trade.trade_id).isdigit() else self._seq,
            price=trade.price.as_decimal(),
            quantity=trade.size.as_decimal(),
            first_trade_id=0,
            last_trade_id=0,
            trade_time_ms=int(trade.ts_event // 1_000_000),
            buyer_is_maker=(trade.aggressor_side == AggressorSide.SELL),
        )
        return self.engine.update(self.state, ev)

    def on_mark_price(self, mp: MarkPriceUpdate, *, funding_rate: Decimal = Decimal("0")) -> None:
        ev = MarkPriceEvent(
            venue="BINANCE",
            market=self.market,
            symbol=self.symbol,
            delivery_seq=self._next_seq(),
            exchange_event_time_ms=int(mp.ts_event // 1_000_000),
            receive_time_utc_ns=int(mp.ts_init),
            mark_price=mp.value.as_decimal(),
            index_price=mp.value.as_decimal(),
            estimated_settle_price=mp.value.as_decimal(),
            funding_rate=funding_rate,
            next_funding_time_ms=int(mp.ts_event // 1_000_000) + 28_800_000,
        )
        self.engine.update(self.state, ev)

    def on_liquidation(self, liq: BmdLiquidationData) -> None:
        ev = LiquidationEvent(
            venue="BINANCE",
            market=Market(liq.market),
            symbol=liq.symbol,
            delivery_seq=self._next_seq(),
            exchange_event_time_ms=int(liq.ts_event // 1_000_000),
            receive_time_utc_ns=int(liq.ts_init),
            side=liq.side,
            order_type=liq.order_type,
            time_in_force=liq.time_in_force,
            original_quantity=Decimal(liq.original_quantity),
            price=Decimal(liq.price),
            average_price=Decimal(liq.average_price) if liq.average_price is not None else None,
            order_status=liq.order_status,
            last_filled_quantity=Decimal(liq.last_filled_quantity),
            accumulated_filled_quantity=Decimal(liq.accumulated_filled_quantity),
            order_trade_time_ms=liq.order_trade_time_ms,
        )
        self.engine.update(self.state, ev)

    def on_status(self, status: InstrumentStatus) -> None:
        sym_state = self.state.get(self.market, self.symbol)
        was_healthy = sym_state.health_valid
        ev = StreamHealthEvent(
            venue="BINANCE",
            market=self.market,
            symbol=self.symbol,
            stream=Stream.BOOK_TICKER,
            status="LIVE" if status.is_trading else "GAP",
            reason=str(status.reason or ("LIVE" if status.is_trading else "HALT")),
            observed_time_utc_ns=int(status.ts_init),
        )
        self.engine.on_health(self.state, ev)
        if not status.is_trading and was_healthy:
            self.epoch_id += 1
        elif status.is_trading and not was_healthy:
            self.epoch_id += 1

    def build_forward_horizon_dataset(
        self,
        *,
        horizon_steps: int = 1,
    ) -> tuple[list[dict[str, Any]], list[float], list[int]]:
        """Build forward-return labels strictly inside identical (market, symbol, epoch_id)."""
        if horizon_steps < 1:
            raise ValueError("horizon_steps must be >= 1")
        rows: list[dict[str, Any]] = []
        targets: list[float] = []
        label_end_times_ns: list[int] = []
        n = len(self._samples)
        for i in range(n - horizon_steps):
            s0 = self._samples[i]
            s1 = self._samples[i + horizon_steps]
            # Never construct labels across GAP/STALE epoch boundaries or across products.
            if (
                s0.epoch_id != s1.epoch_id
                or s0.market != s1.market
                or s0.symbol != s1.symbol
                or s0.mid_price <= 0.0
            ):
                continue
            ret = (s1.mid_price - s0.mid_price) / s0.mid_price
            rows.append(s0.feature_row)
            targets.append(float(ret))
            label_end_times_ns.append(int(s1.feature_row["timestamp_ns"]))
        return rows, targets, label_end_times_ns


def train_causal_nautilus_bundle(
    *,
    quotes: Sequence[QuoteTick],
    market: Market,
    symbol: str,
    models_root: Path,
    model_id: str,
    horizon_steps: int = 1,
    n_splits: int = 3,
    embargo_ns: int = 1_000_000_000,
    feature_names: tuple[str, ...] = DEFAULT_FEATURES,
    statuses: Sequence[InstrumentStatus] = (),
    trades: Sequence[TradeTick] = (),
    alpha: float = 1e-2,
) -> tuple[ModelBundle, TrainReport]:
    """Build causal features from Nautilus data and fit a Purged+Embargoed ModelBundle."""
    if embargo_ns <= 0:
        raise ValueError("embargo_ns must be > 0 to prevent label/feature overlap leakage")
    bridge = CausalFeatureBridge(market=market, symbol=symbol)
    timeline: list[tuple[int, int, Any]] = []
    for st in statuses:
        timeline.append((int(st.ts_init), 0, st))
    for tr in trades:
        timeline.append((int(tr.ts_init), 1, tr))
    for q in quotes:
        timeline.append((int(q.ts_init), 2, q))
    timeline.sort(key=lambda item: (item[0], item[1]))
    for _, kind, obj in timeline:
        if kind == 0:
            bridge.on_status(obj)
        elif kind == 1:
            bridge.on_trade(obj)
        else:
            bridge.on_quote(obj, record=True)

    rows, targets, label_end_times_ns = bridge.build_forward_horizon_dataset(
        horizon_steps=horizon_steps
    )
    if not rows:
        raise ValueError("No valid contiguous-epoch samples available for model training")
    return fit_walk_forward_model(
        rows,
        targets,
        label_end_times_ns,
        model_id=model_id,
        models_root=models_root,
        feature_names=feature_names,
        n_splits=n_splits,
        embargo_ns=embargo_ns,
        alpha=alpha,
        market=market.value,
        symbol=symbol,
    )


class CausalMlStrategyConfig(StrategyConfig):
    """Configuration for CausalMlStrategy."""

    def __init__(
        self,
        *,
        instrument_id: Any,
        market: str,
        symbol: str,
        model_bundle_dir: str,
        order_qty: str = "0.01",
        entry_threshold: float = 1e-4,
        raise_on_pre_cutoff: bool = False,
        data_client_id: str | None = None,
        exec_client_id: str | None = None,
        telemetry: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.instrument_id = instrument_id
        self.market = market
        self.symbol = symbol
        self.model_bundle_dir = model_bundle_dir
        self.order_qty = order_qty
        self.entry_threshold = entry_threshold
        self.raise_on_pre_cutoff = raise_on_pre_cutoff
        self.data_client_id = data_client_id
        self.exec_client_id = exec_client_id
        self.telemetry = telemetry if telemetry is not None else {}


class CausalMlStrategy(Strategy):
    """Nautilus strategy executing <1ms causal ModelBundle inference with fail-closed cutoff."""

    def __init__(self, config: CausalMlStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self.telemetry = config.telemetry
        self.bundle = ModelBundle.load(Path(config.model_bundle_dir))
        self.bridge = CausalFeatureBridge(
            market=Market(config.market),
            symbol=config.symbol,
        )
        self._qty: Quantity | None = None
        self.telemetry.setdefault("predictions", [])
        self.telemetry.setdefault("rejected_pre_cutoff", 0)
        self.telemetry.setdefault("rejected_unhealthy", 0)
        self.telemetry.setdefault("inference_latencies_ns", [])
        self.telemetry.setdefault("orders_submitted", [])
        self.telemetry.setdefault("trained_until_ns", self.bundle.trained_until_ns)

    def on_start(self) -> None:
        inst = self.cache.instrument(self._cfg.instrument_id)
        if inst is not None:
            self._qty = Quantity.from_str(self._cfg.order_qty)
        route = ClientId(self._cfg.data_client_id) if self._cfg.data_client_id else None
        self.subscribe_quotes(self._cfg.instrument_id, client_id=route)
        self.subscribe_trades(self._cfg.instrument_id, client_id=route)
        self.subscribe_instrument_status(self._cfg.instrument_id, client_id=route)

    def on_instrument_status(self, status: InstrumentStatus) -> None:
        if status.instrument_id == self._cfg.instrument_id:
            self.bridge.on_status(status)

    def on_trade(self, trade: TradeTick) -> None:
        if trade.instrument_id == self._cfg.instrument_id:
            self.bridge.on_trade(trade)

    def _submit_routed(self, order: Any) -> None:
        exec_route = ClientId(self._cfg.exec_client_id) if self._cfg.exec_client_id else None
        if exec_route is not None:
            self.submit_order(order, client_id=exec_route)
        else:
            self.submit_order(order)

    def on_quote(self, tick: QuoteTick) -> None:
        if tick.instrument_id != self._cfg.instrument_id:
            return
        fv = self.bridge.on_quote(tick, record=False)
        if fv is None:
            return
        # Always enforce strict causal cutoff: reject any event <= bundle.trained_until_ns.
        if int(tick.ts_init) <= self.bundle.trained_until_ns:
            self.telemetry["rejected_pre_cutoff"] += 1
            if self._cfg.raise_on_pre_cutoff:
                raise CausalCutoffError(
                    f"Rejected pre-cutoff tick ts_init={tick.ts_init} <= trained_until_ns={self.bundle.trained_until_ns}"
                )
            return
        if not fv.health_valid:
            self.telemetry["rejected_unhealthy"] += 1
            return

        t0 = time.perf_counter_ns()
        score = self.bundle.predict_one(fv)
        dt_ns = time.perf_counter_ns() - t0
        self.telemetry["inference_latencies_ns"].append(dt_ns)
        self.telemetry["predictions"].append((int(tick.ts_init), float(score)))

        if self._qty is None:
            return
        pos_open = self.portfolio.is_net_long(self._cfg.instrument_id)
        if score >= self._cfg.entry_threshold and not pos_open:
            order = self.order_factory.market(
                instrument_id=self._cfg.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self._qty,
            )
            self.telemetry["orders_submitted"].append((int(tick.ts_init), "BUY", float(score)))
            self._submit_routed(order)
        elif score <= -self._cfg.entry_threshold and pos_open:
            order = self.order_factory.market(
                instrument_id=self._cfg.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self._qty,
            )
            self.telemetry["orders_submitted"].append((int(tick.ts_init), "SELL", float(score)))
            self._submit_routed(order)


__all__ = [
    "CausalCutoffError",
    "CausalFeatureBridge",
    "CausalMlStrategy",
    "CausalMlStrategyConfig",
    "CausalTrainingSample",
    "train_causal_nautilus_bundle",
]
