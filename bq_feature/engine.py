"""Pure causal stateful microstructure feature engine: update(state, event)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
import math

from bq_feed.events import (
    AggTradeEvent,
    BookTickerEvent,
    DepthUpdateEvent,
    LiquidationEvent,
    Market,
    MarketEvent,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    StreamHealthEvent,
)


def _event_ts_ns(ev: MarketEvent, *, strict_receive_time: bool = False) -> int | None:
    if ev.receive_time_utc_ns is not None:
        return ev.receive_time_utc_ns
    if strict_receive_time:
        return None
    exch = getattr(ev, "exchange_event_time_ms", None) or getattr(ev, "trade_time_ms", None) or getattr(ev, "order_trade_time_ms", None)
    return int(exch or 0) * 1_000_000


@dataclass(slots=True)
class FeatureVector:
    delivery_seq: int
    timestamp_ns: int
    market: str
    symbol: str
    health_valid: bool
    mid_price: float
    spread_bps: float
    microprice: float
    bbo_imbalance: float
    ofi: float
    net_limit_flow: float
    pcvr: float
    sweep_cluster: float
    fragmentation: float
    fsi: float
    j_engine: float
    volume_clock_bars: int
    kline_1s_close: float
    kline_5s_close: float


@dataclass(slots=True)
class _KlineBar:
    bucket_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int


@dataclass(slots=True)
class SymbolFeatureState:
    symbol: str
    health_valid: bool = True
    health_reason: str = "OK"
    best_bid_price: Decimal | None = None
    best_bid_qty: Decimal | None = None
    best_ask_price: Decimal | None = None
    best_ask_qty: Decimal | None = None
    prev_bid_price: float | None = None
    prev_bid_qty: float | None = None
    prev_ask_price: float | None = None
    prev_ask_qty: float | None = None
    last_ofi: float = 0.0
    rolling_ofi: deque[float] = field(default_factory=lambda: deque(maxlen=20))
    depth_bids: dict[Decimal, Decimal] = field(default_factory=dict)
    depth_asks: dict[Decimal, Decimal] = field(default_factory=dict)
    # R1 NetLimitFlow / PCVR
    resting_adds: deque[float] = field(default_factory=lambda: deque(maxlen=50))
    resting_cancels: deque[float] = field(default_factory=lambda: deque(maxlen=50))
    traded_vols: deque[float] = field(default_factory=lambda: deque(maxlen=50))
    # R7/N5 Sweep cluster & fragmentation
    last_trade_ts_ns: int = 0
    last_trade_is_buy: bool | None = None
    sweep_run: int = 0
    trade_sizes: deque[float] = field(default_factory=lambda: deque(maxlen=32))
    # R9 FSI: basis + funding + throttled forceOrder count proxy
    funding_rate: float = 0.0
    basis_bps: float = 0.0
    liq_events_window: deque[int] = field(default_factory=lambda: deque(maxlen=64))
    # R6 J_engine: log returns for bipower jump test
    prev_mid_for_return: float | None = None
    log_returns: deque[float] = field(default_factory=lambda: deque(maxlen=60))
    # Dual clocks: physical 1s / 5s & volume clock
    kline_1s: _KlineBar | None = None
    kline_5s: _KlineBar | None = None
    volume_acc: float = 0.0
    volume_clock_bars: int = 0


@dataclass(slots=True)
class FeatureState:
    symbols: dict[tuple[Market, str], SymbolFeatureState] = field(default_factory=dict)
    last_seq: int = 0

    def get(self, market: Market, symbol: str) -> SymbolFeatureState:
        key = (market, symbol)
        st = self.symbols.get(key)
        if st is None:
            st = SymbolFeatureState(symbol=symbol)
            self.symbols[key] = st
        return st


class FeatureEngine:
    """Stateless pure-causal operator transforming (FeatureState, Event) -> FeatureVector | None."""

    def __init__(
        self,
        *,
        volume_clock_threshold: float = 10.0,
        sweep_gap_ns: int = 50_000_000,
        strict_receive_time: bool = False,
    ) -> None:
        self._vol_thresh = volume_clock_threshold
        self._sweep_gap_ns = sweep_gap_ns
        self._strict_receive_time = strict_receive_time
        self.skipped_missing_receive_time_count = 0

    def on_health(self, state: FeatureState, health: StreamHealthEvent) -> None:
        st = state.get(health.market, health.symbol)
        state.last_seq = max(state.last_seq, health.delivery_seq)
        if health.status in ("GAP", "STALE", "DEGRADED", "CLOSED", "STOPPED"):
            st.health_valid = False
            st.health_reason = f"{health.status}:{health.reason}"
            # Do not keep stale L2/OFI state across gaps.
            st.prev_bid_price = None
            st.prev_ask_price = None
            st.rolling_ofi.clear()
        elif health.status == "LIVE":
            st.health_valid = True
            st.health_reason = "OK"

    def _update_klines(self, st: SymbolFeatureState, ts_ns: int, price: float, qty: float) -> None:
        b1 = ts_ns // 1_000_000_000
        b5 = ts_ns // 5_000_000_000
        if st.kline_1s is None or st.kline_1s.bucket_ns != b1:
            st.kline_1s = _KlineBar(b1, price, price, price, price, qty, 1)
        else:
            st.kline_1s.high = max(st.kline_1s.high, price)
            st.kline_1s.low = min(st.kline_1s.low, price)
            st.kline_1s.close = price
            st.kline_1s.volume += qty
            st.kline_1s.trades += 1
        if st.kline_5s is None or st.kline_5s.bucket_ns != b5:
            st.kline_5s = _KlineBar(b5, price, price, price, price, qty, 1)
        else:
            st.kline_5s.high = max(st.kline_5s.high, price)
            st.kline_5s.low = min(st.kline_5s.low, price)
            st.kline_5s.close = price
            st.kline_5s.volume += qty
            st.kline_5s.trades += 1

    def _update_bbo_metrics(
        self,
        st: SymbolFeatureState,
        ts_ns: int,
        bid_p: Decimal,
        bid_q: Decimal,
        ask_p: Decimal,
        ask_q: Decimal,
    ) -> None:
        st.best_bid_price = bid_p
        st.best_bid_qty = bid_q
        st.best_ask_price = ask_p
        st.best_ask_qty = ask_q

        bp = float(bid_p)
        bq = float(bid_q)
        ap = float(ask_p)
        aq = float(ask_q)

        # Crossed book check (>= 0 spread check matches Projection CROSSED_BOOK contract!)
        if bp >= ap:
            st.health_valid = False
            st.health_reason = "CROSSED_BOOK"
        elif st.health_reason == "CROSSED_BOOK":
            st.health_valid = True
            st.health_reason = "OK"

        # Cont's OFI increment at top level
        ofi_delta = 0.0
        if st.prev_bid_price is not None and st.prev_bid_qty is not None:
            if bp > st.prev_bid_price:
                ofi_delta += bq
            elif bp == st.prev_bid_price:
                ofi_delta += bq - st.prev_bid_qty
            else:
                ofi_delta -= st.prev_bid_qty
        if st.prev_ask_price is not None and st.prev_ask_qty is not None:
            if ap < st.prev_ask_price:
                ofi_delta -= aq
            elif ap == st.prev_ask_price:
                ofi_delta -= aq - st.prev_ask_qty
            else:
                ofi_delta += st.prev_ask_qty

        st.last_ofi = ofi_delta
        st.rolling_ofi.append(ofi_delta)
        st.prev_bid_price, st.prev_bid_qty = bp, bq
        st.prev_ask_price, st.prev_ask_qty = ap, aq

        # Resting add/cancel proxy for R1
        add = max(ofi_delta, 0.0)
        cancel = max(-ofi_delta, 0.0)
        st.resting_adds.append(add)
        st.resting_cancels.append(cancel)

        mid = 0.5 * (bp + ap)
        if st.prev_mid_for_return is not None and st.prev_mid_for_return > 0 and mid > 0:
            st.log_returns.append(math.log(mid / st.prev_mid_for_return))
        st.prev_mid_for_return = mid
        self._update_klines(st, ts_ns, mid, 0.0)

    def update(self, state: FeatureState, event: MarketEvent) -> FeatureVector | None:
        """Pure causal state transition driven exclusively by delivered event + current state."""
        ts_ns = _event_ts_ns(event, strict_receive_time=self._strict_receive_time)
        if ts_ns is None:
            self.skipped_missing_receive_time_count += 1
            return None
        state.last_seq = max(state.last_seq, event.delivery_seq)
        st = state.get(event.market, event.symbol)

        if isinstance(event, BookTickerEvent):
            # Healthy BBO packet after a stale/gap event restores BBO liveness if source isn't crossed
            if st.health_reason.startswith("STALE:"):
                st.health_valid = True
                st.health_reason = "OK"
            self._update_bbo_metrics(
                st,
                ts_ns,
                event.best_bid_price,
                event.best_bid_quantity,
                event.best_ask_price,
                event.best_ask_quantity,
            )
        elif isinstance(event, OrderBookSnapshotEvent):
            st.depth_bids = {p: q for p, q in event.bids if q > 0}
            st.depth_asks = {p: q for p, q in event.asks if q > 0}
            st.health_valid = True
            st.health_reason = "OK"
            if st.depth_bids and st.depth_asks:
                bb = max(st.depth_bids)
                ba = min(st.depth_asks)
                self._update_bbo_metrics(st, ts_ns, bb, st.depth_bids[bb], ba, st.depth_asks[ba])
        elif isinstance(event, DepthUpdateEvent):
            for p, q in event.bids:
                if q == 0:
                    st.depth_bids.pop(p, None)
                else:
                    st.depth_bids[p] = q
            for p, q in event.asks:
                if q == 0:
                    st.depth_asks.pop(p, None)
                else:
                    st.depth_asks[p] = q
            if st.depth_bids and st.depth_asks:
                bb = max(st.depth_bids)
                ba = min(st.depth_asks)
                self._update_bbo_metrics(st, ts_ns, bb, st.depth_bids[bb], ba, st.depth_asks[ba])
        elif isinstance(event, AggTradeEvent):
            px = float(event.price)
            qty = float(event.quantity)
            st.traded_vols.append(qty)
            st.trade_sizes.append(qty)
            st.volume_acc += qty
            while st.volume_acc >= self._vol_thresh:
                st.volume_acc -= self._vol_thresh
                st.volume_clock_bars += 1
            is_buy = not event.buyer_is_maker
            if (
                st.last_trade_is_buy == is_buy
                and ts_ns - st.last_trade_ts_ns <= self._sweep_gap_ns
            ):
                st.sweep_run += 1
            else:
                st.sweep_run = 1
            st.last_trade_ts_ns = ts_ns
            st.last_trade_is_buy = is_buy
            self._update_klines(st, ts_ns, px, qty)
        elif isinstance(event, MarkPriceEvent):
            st.funding_rate = float(event.funding_rate)
            idx_p = float(event.index_price)
            if idx_p > 0:
                st.basis_bps = ((float(event.mark_price) - idx_p) / idx_p) * 10_000.0
        elif isinstance(event, LiquidationEvent):
            st.liq_events_window.append(ts_ns)
            while st.liq_events_window and ts_ns - st.liq_events_window[0] > 60_000_000_000:
                st.liq_events_window.popleft()
        elif isinstance(event, ReferenceObservationEvent):
            if event.funding_rate_settled is not None:
                st.funding_rate = float(event.funding_rate_settled)
            return None

        return self.snapshot(state, event.market, event.symbol, event.delivery_seq, ts_ns)

    def snapshot(
        self,
        state: FeatureState,
        market: Market,
        symbol: str,
        delivery_seq: int,
        ts_ns: int,
    ) -> FeatureVector | None:
        st = state.get(market, symbol)
        if (
            st.best_bid_price is None
            or st.best_bid_qty is None
            or st.best_ask_price is None
            or st.best_ask_qty is None
        ):
            return None

        bp = float(st.best_bid_price)
        bq = float(st.best_bid_qty)
        ap = float(st.best_ask_price)
        aq = float(st.best_ask_qty)
        mid = 0.5 * (bp + ap)
        spread_bps = ((ap - bp) / mid) * 10_000.0 if mid > 0 else 0.0
        denom = bq + aq
        microprice = ((bp * aq + ap * bq) / denom) if denom > 0 else mid
        imbalance = ((bq - aq) / denom) if denom > 0 else 0.0
        ofi_sum = float(sum(st.rolling_ofi))

        net_limit = float(sum(st.resting_adds) - sum(st.resting_cancels))
        t_vol = float(sum(st.traded_vols))
        pcvr = (net_limit / t_vol) if t_vol > 1e-12 else 0.0

        frag = (
            float(len(st.trade_sizes)) / float(sum(st.trade_sizes))
            if st.trade_sizes and sum(st.trade_sizes) > 1e-12
            else 0.0
        )
        # FSI proxy combining funding (scaled to bps), basis bps, and throttled liquidation rate
        fsi = abs(st.funding_rate) * 10_000.0 + abs(st.basis_bps) + float(len(st.liq_events_window))

        # R6 J_engine: (RV - BV) / RV using causal log returns
        j_val = 0.0
        if len(st.log_returns) >= 4:
            rets = list(st.log_returns)
            rv = sum(r * r for r in rets)
            bv = (math.pi / 2.0) * sum(abs(rets[i]) * abs(rets[i - 1]) for i in range(1, len(rets)))
            if rv > 1e-18:
                j_val = max(0.0, (rv - bv) / rv)

        k1 = st.kline_1s.close if st.kline_1s is not None else mid
        k5 = st.kline_5s.close if st.kline_5s is not None else mid
        return FeatureVector(
            delivery_seq=delivery_seq,
            timestamp_ns=ts_ns,
            market=market.value,
            symbol=symbol,
            health_valid=st.health_valid,
            mid_price=mid,
            spread_bps=spread_bps,
            microprice=microprice,
            bbo_imbalance=imbalance,
            ofi=ofi_sum,
            net_limit_flow=net_limit,
            pcvr=pcvr,
            sweep_cluster=float(st.sweep_run),
            fragmentation=frag,
            fsi=fsi,
            j_engine=j_val,
            volume_clock_bars=st.volume_clock_bars,
            kline_1s_close=k1,
            kline_5s_close=k5,
        )
