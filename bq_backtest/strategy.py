"""Pure causal strategies shared cleanly across backtest, paper, and live."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from bq_feature.engine import FeatureVector
from bq_feed.events import Market, Stream


@dataclass(frozen=True, slots=True)
class StrategyAccountView:
    position_qty: Decimal
    cash_quote: Decimal
    last_trade_ts_ns: int = 0
    intent_counter: int = 0


@dataclass(frozen=True, slots=True)
class OrderIntent:
    intent_id: str
    market: Market
    symbol: str
    side: str  # "BUY" | "SELL"
    quantity: Decimal
    order_type: str  # "MARKET_TAKER"
    decision_seq: int
    decision_ts_ns: int
    decision_infer_ns: int
    reason: str
    expected_edge_bps: float = 0.0


class BaseStrategy(Protocol):
    strategy_id: str
    required_streams: tuple[tuple[Market, str, Stream], ...]

    def on_feature(
        self, feat: FeatureVector, account: StrategyAccountView
    ) -> OrderIntent | None: ...


class BtcSpotBboRule:
    """Deterministic Spot BTCUSDT BBO taker rule (Team C M9 starter strategy)."""

    strategy_id = "btc_spot_bbo_rule.v1"

    def __init__(
        self,
        *,
        symbol: str = "BTCUSDT",
        market: Market = Market.SPOT,
        order_qty: Decimal = Decimal("0.01"),
        max_pos_qty: Decimal = Decimal("0.05"),
        buy_imbalance_threshold: float = 0.25,
        sell_imbalance_threshold: float = 0.25,
        max_spread_bps: float = 15.0,
        cooldown_ns: int = 0,
        allow_short: bool = False,
        infer_ns: int = 100_000,
    ) -> None:
        self.symbol = symbol
        self.market = market
        self.required_streams = ((market, symbol, Stream.BOOK_TICKER),)
        self._order_qty = order_qty
        self._max_pos = max_pos_qty
        self._buy_th = buy_imbalance_threshold
        self._sell_th = sell_imbalance_threshold
        self._max_spread_bps = max_spread_bps
        self._cooldown_ns = cooldown_ns
        self._allow_short = allow_short
        self._infer_ns = infer_ns

    def on_feature(
        self, feat: FeatureVector, account: StrategyAccountView
    ) -> OrderIntent | None:
        if feat.symbol != self.symbol or not feat.health_valid:
            return None
        if feat.spread_bps > self._max_spread_bps:
            return None
        if self._cooldown_ns > 0 and account.last_trade_ts_ns > 0:
            if (feat.timestamp_ns - account.last_trade_ts_ns) < self._cooldown_ns:
                return None

        if feat.bbo_imbalance >= self._buy_th:
            headroom = self._max_pos - account.position_qty
            qty = min(self._order_qty, headroom)
            if qty > 0:
                return OrderIntent(
                    intent_id=f"{self.strategy_id}-{feat.delivery_seq}-B",
                    market=self.market,
                    symbol=self.symbol,
                    side="BUY",
                    quantity=qty,
                    order_type="MARKET_TAKER",
                    decision_seq=feat.delivery_seq,
                    decision_ts_ns=feat.timestamp_ns,
                    decision_infer_ns=self._infer_ns,
                    reason=f"imb={feat.bbo_imbalance:.4f}>={self._buy_th}",
                    expected_edge_bps=feat.bbo_imbalance * 10.0,
                )

        if feat.bbo_imbalance <= -self._sell_th:
            if self._allow_short:
                headroom = account.position_qty + self._max_pos
                qty = min(self._order_qty, headroom)
            else:
                qty = min(self._order_qty, account.position_qty)
            if qty > 0:
                return OrderIntent(
                    intent_id=f"{self.strategy_id}-{feat.delivery_seq}-S",
                    market=self.market,
                    symbol=self.symbol,
                    side="SELL",
                    quantity=qty,
                    order_type="MARKET_TAKER",
                    decision_seq=feat.delivery_seq,
                    decision_ts_ns=feat.timestamp_ns,
                    decision_infer_ns=self._infer_ns,
                    reason=f"imb={feat.bbo_imbalance:.4f}<=-{self._sell_th}",
                    expected_edge_bps=abs(feat.bbo_imbalance) * 10.0,
                )
        return None


class ThreeStageQuantStrategy:
    """Regime -> Alpha (ModelBundle or composite) -> MetaCostRisk causal strategy."""

    def __init__(
        self,
        *,
        strategy_id: str = "three_stage_quant.v1",
        market: Market = Market.SPOT,
        symbol: str = "BTCUSDT",
        required_streams: tuple[tuple[Market, str, Stream], ...] | None = None,
        model_bundle: object | None = None,
        order_qty: Decimal = Decimal("0.01"),
        max_pos_qty: Decimal = Decimal("0.05"),
        max_spread_bps: float = 20.0,
        max_jump: float = 0.85,
        max_fsi: float = 100.0,
        cost_buffer_bps: float = 5.0,
        allow_short: bool = False,
        infer_ns: int = 200_000,
    ) -> None:
        self.strategy_id = strategy_id
        self.market = market
        self.symbol = symbol
        self.required_streams = required_streams or ((market, symbol, Stream.BOOK_TICKER),)
        self._model = model_bundle
        self._order_qty = order_qty
        self._max_pos = max_pos_qty
        self._max_spread_bps = max_spread_bps
        self._max_jump = max_jump
        self._max_fsi = max_fsi
        self._cost_buffer_bps = cost_buffer_bps
        self._allow_short = allow_short
        self._infer_ns = infer_ns

    def _regime_allows(self, feat: FeatureVector) -> bool:
        return (
            feat.health_valid
            and feat.spread_bps <= self._max_spread_bps
            and feat.j_engine <= self._max_jump
            and feat.fsi <= self._max_fsi
        )

    def _alpha_bps(self, feat: FeatureVector) -> float:
        if self._model is not None:
            return float(self._model.predict_one(feat))
        micro_edge_bps = (
            ((feat.microprice - feat.mid_price) / feat.mid_price) * 10_000.0
            if feat.mid_price > 0
            else 0.0
        )
        return micro_edge_bps + feat.bbo_imbalance * 8.0 + feat.ofi * 0.5

    def on_feature(
        self, feat: FeatureVector, account: StrategyAccountView
    ) -> OrderIntent | None:
        if feat.symbol != self.symbol or not self._regime_allows(feat):
            return None
        alpha_bps = self._alpha_bps(feat)
        # MetaCostRisk: net edge must exceed half-spread + fee buffer
        net_cost_bps = 0.5 * feat.spread_bps + self._cost_buffer_bps
        if alpha_bps > net_cost_bps:
            qty = min(self._order_qty, self._max_pos - account.position_qty)
            if qty > 0:
                return OrderIntent(
                    intent_id=f"{self.strategy_id}-{feat.delivery_seq}-B",
                    market=self.market,
                    symbol=self.symbol,
                    side="BUY",
                    quantity=qty,
                    order_type="MARKET_TAKER",
                    decision_seq=feat.delivery_seq,
                    decision_ts_ns=feat.timestamp_ns,
                    decision_infer_ns=self._infer_ns,
                    reason=f"alpha={alpha_bps:.2f}>cost={net_cost_bps:.2f}",
                    expected_edge_bps=alpha_bps,
                )
        elif -alpha_bps > net_cost_bps:
            if self._allow_short:
                qty = min(self._order_qty, account.position_qty + self._max_pos)
            else:
                qty = min(self._order_qty, account.position_qty)
            if qty > 0:
                return OrderIntent(
                    intent_id=f"{self.strategy_id}-{feat.delivery_seq}-S",
                    market=self.market,
                    symbol=self.symbol,
                    side="SELL",
                    quantity=qty,
                    order_type="MARKET_TAKER",
                    decision_seq=feat.delivery_seq,
                    decision_ts_ns=feat.timestamp_ns,
                    decision_infer_ns=self._infer_ns,
                    reason=f"alpha={alpha_bps:.2f}<-cost={net_cost_bps:.2f}",
                    expected_edge_bps=-alpha_bps,
                )
        return None
