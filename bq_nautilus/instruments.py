"""Contemporaneous BMD Instrument specification builders for NautilusTrader (D4 compliant)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import time
from typing import Any, Iterable, Sequence
import urllib.request

from bq_feed.events import Market, ReferenceObservationEvent

SUPPORTED_BMD_SYMBOLS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
)

BINANCE_SPOT_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_USDM_LIVE_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_USDM_DEMO_EXCHANGE_INFO_URL = "https://demo-fapi.binance.com/fapi/v1/exchangeInfo"


class UnknownInstrumentSpecError(ValueError):
    """Raised per D4 when instrument filters or observation timestamps are unknown."""


@dataclass(frozen=True, slots=True)
class LeverageBracketTier:
    """One Binance USD-M maintenance margin tier (`leverageBracket`)."""

    tier: int
    initial_leverage: int
    notional_floor: Decimal
    notional_cap: Decimal
    maint_margin_ratio: Decimal
    cum: Decimal


# Fallback brackets aligned to observed Binance USD-M Demo /fapi/v1/leverageBracket (2026-11-26).
# Live/authenticated flows should prefer signed `/fapi/v1/leverageBracket` observations
# with explicit `observe_time_utc_ns`.
DEFAULT_USDM_TIER1_BRACKETS: dict[str, tuple[LeverageBracketTier, ...]] = {
    "BTCUSDT": (
        LeverageBracketTier(1, 125, Decimal("0"), Decimal("50000"), Decimal("0.004"), Decimal("0")),
        LeverageBracketTier(2, 100, Decimal("50000"), Decimal("250000"), Decimal("0.005"), Decimal("50")),
        LeverageBracketTier(3, 50, Decimal("250000"), Decimal("3000000"), Decimal("0.01"), Decimal("1300")),
    ),
    "ETHUSDT": (
        LeverageBracketTier(1, 100, Decimal("0"), Decimal("100000"), Decimal("0.005"), Decimal("0")),
        LeverageBracketTier(2, 75, Decimal("100000"), Decimal("250000"), Decimal("0.0065"), Decimal("150")),
    ),
    "SOLUSDT": (
        LeverageBracketTier(1, 50, Decimal("0"), Decimal("50000"), Decimal("0.01"), Decimal("0")),
        LeverageBracketTier(2, 25, Decimal("50000"), Decimal("150000"), Decimal("0.02"), Decimal("500")),
    ),
    "BNBUSDT": (
        LeverageBracketTier(1, 75, Decimal("0"), Decimal("5000"), Decimal("0.005"), Decimal("0")),
        LeverageBracketTier(2, 50, Decimal("5000"), Decimal("10000"), Decimal("0.006"), Decimal("5")),
    ),
    "XRPUSDT": (
        LeverageBracketTier(1, 75, Decimal("0"), Decimal("5000"), Decimal("0.005"), Decimal("0")),
        LeverageBracketTier(2, 50, Decimal("5000"), Decimal("10000"), Decimal("0.006"), Decimal("5")),
    ),
}


def compute_tiered_maintenance_margin(
    notional: Decimal,
    brackets: Sequence[LeverageBracketTier],
) -> tuple[Decimal, LeverageBracketTier]:
    """Compute Binance USD-M maintenance margin `notional * MMR - cum` for positive notional."""
    abs_notional = abs(notional)
    if not brackets:
        raise UnknownInstrumentSpecError("Empty leverage brackets")
    ordered = sorted(brackets, key=lambda b: b.notional_floor)
    chosen = ordered[-1]
    for tier in ordered:
        if abs_notional >= tier.notional_floor and abs_notional <= tier.notional_cap:
            chosen = tier
            break
    if abs_notional == Decimal(0):
        chosen = ordered[0]
    mm = max(Decimal(0), abs_notional * chosen.maint_margin_ratio - chosen.cum)
    return mm, chosen


def bracket_tiers_from_binance_json(entry: dict[str, Any]) -> tuple[LeverageBracketTier, ...]:
    """Parse one symbol object from `/fapi/v1/leverageBracket`."""
    tiers: list[LeverageBracketTier] = []
    for raw in entry.get("brackets", []):
        tiers.append(
            LeverageBracketTier(
                tier=int(raw["bracket"]),
                initial_leverage=int(raw["initialLeverage"]),
                notional_floor=Decimal(str(raw["notionalFloor"])),
                notional_cap=Decimal(str(raw["notionalCap"])),
                maint_margin_ratio=Decimal(str(raw["maintMarginRatio"])),
                cum=Decimal(str(raw.get("cum", "0"))),
            )
        )
    if not tiers:
        raise UnknownInstrumentSpecError(f"No brackets present for {entry.get('symbol')}")
    return tuple(sorted(tiers, key=lambda t: t.notional_floor))


def _decimal_precision(step: Decimal) -> int:
    normalized = step.normalize()
    exp = normalized.as_tuple().exponent
    if not isinstance(exp, int):
        raise UnknownInstrumentSpecError(f"Non-finite decimal increment: {step!r}")
    return max(0, -exp)


def _format_dp(value: Decimal, dp: int) -> str:
    quant = Decimal(1).scaleb(-dp)
    return f"{value.quantize(quant):f}"


def _infer_base_quote(symbol: str, base_asset: str | None, quote_asset: str | None) -> tuple[str, str]:
    if base_asset and quote_asset:
        return base_asset, quote_asset
    for quote in ("USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH", "BNB"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    raise UnknownInstrumentSpecError(f"Unable to infer base/quote currency for {symbol}")


@dataclass(frozen=True, slots=True)
class BmdInstrumentSpec:
    market: Market
    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_notional: Decimal
    observe_time_utc_ns: int
    source_time_ms: int | None = None
    min_quantity: Decimal | None = None
    max_quantity: Decimal | None = None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    trading_status: str = "TRADING"
    source: str = "unknown"
    market_step_size: Decimal | None = None
    market_min_quantity: Decimal | None = None
    market_max_quantity: Decimal | None = None
    margin_init: Decimal | None = None
    margin_maint: Decimal | None = None
    contract_type: str | None = None

    @property
    def price_precision(self) -> int:
        return _decimal_precision(self.tick_size)

    @property
    def size_precision(self) -> int:
        return _decimal_precision(self.step_size)

    def to_nautilus_instrument(self) -> Any:
        from nautilus_trader.model import (
            Currency,
            CurrencyPair,
            CryptoPerpetual,
            InstrumentId,
            Money,
            Price,
            Quantity,
            Symbol,
            Venue,
        )

        if self.tick_size <= 0 or self.step_size <= 0 or self.min_notional < 0:
            raise UnknownInstrumentSpecError(
                f"Invalid filter bounds for {self.market.value}:{self.symbol}"
            )
        if self.observe_time_utc_ns <= 0:
            raise UnknownInstrumentSpecError(
                f"Missing observe_time_utc_ns for {self.market.value}:{self.symbol}"
            )

        p_dp = self.price_precision
        s_dp = self.size_precision
        base_ccy = Currency.from_str(self.base_asset)
        quote_ccy = Currency.from_str(self.quote_asset)
        venue = Venue("BINANCE")
        raw_sym = Symbol(self.symbol)
        price_inc = Price.from_str(_format_dp(self.tick_size, p_dp))
        size_inc = Quantity.from_str(_format_dp(self.step_size, s_dp))
        min_qty = Quantity.from_str(_format_dp(self.min_quantity or self.step_size, s_dp))
        max_qty = Quantity.from_str(_format_dp(self.max_quantity, s_dp)) if self.max_quantity else None
        min_px = Price.from_str(_format_dp(self.min_price or self.tick_size, p_dp))
        max_px = Price.from_str(_format_dp(self.max_price, p_dp)) if self.max_price else None
        min_notional_money = Money(float(self.min_notional), quote_ccy)
        info = {
            "bmd_source": self.source,
            "bmd_market": self.market.value,
            "observe_time_utc_ns": self.observe_time_utc_ns,
            "source_time_ms": self.source_time_ms,
            "trading_status": self.trading_status,
            "min_notional": str(self.min_notional),
        }

        if self.market == Market.SPOT:
            inst_id = InstrumentId(Symbol(self.symbol), venue)
            return CurrencyPair(
                instrument_id=inst_id,
                raw_symbol=raw_sym,
                base_currency=base_ccy,
                quote_currency=quote_ccy,
                price_precision=p_dp,
                size_precision=s_dp,
                price_increment=price_inc,
                size_increment=size_inc,
                ts_event=self.observe_time_utc_ns,
                ts_init=self.observe_time_utc_ns,
                min_quantity=min_qty,
                max_quantity=max_qty,
                min_notional=min_notional_money,
                max_price=max_px,
                min_price=min_px,
                info=info,
            )

        if self.market == Market.USDM:
            inst_id = InstrumentId(Symbol(f"{self.symbol}-PERP"), venue)
            return CryptoPerpetual(
                instrument_id=inst_id,
                raw_symbol=raw_sym,
                base_currency=base_ccy,
                quote_currency=quote_ccy,
                settlement_currency=quote_ccy,
                is_inverse=False,
                price_precision=p_dp,
                size_precision=s_dp,
                price_increment=price_inc,
                size_increment=size_inc,
                ts_event=self.observe_time_utc_ns,
                ts_init=self.observe_time_utc_ns,
                lot_size=size_inc,
                min_quantity=min_qty,
                max_quantity=max_qty,
                min_notional=min_notional_money,
                max_price=max_px,
                min_price=min_px,
                margin_init=self.margin_init,
                margin_maint=self.margin_maint,
                info=info,
            )

        raise UnknownInstrumentSpecError(f"Unsupported market for Nautilus instrument: {self.market.value}")


def spec_from_exchange_info_entry(
    entry: dict[str, Any],
    *,
    market: Market,
    observe_time_utc_ns: int,
    source_time_ms: int | None = None,
    source: str = "exchange_info",
) -> BmdInstrumentSpec:
    """Parse one Binance Spot or USD-M exchangeInfo symbol dict into BmdInstrumentSpec."""
    symbol = str(entry.get("symbol") or "")
    if not symbol:
        raise UnknownInstrumentSpecError("exchangeInfo symbol entry missing symbol")
    tick_size: Decimal | None = None
    step_size: Decimal | None = None
    market_step_size: Decimal | None = None
    market_min_qty: Decimal | None = None
    market_max_qty: Decimal | None = None
    min_notional: Decimal | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    min_px: Decimal | None = None
    max_px: Decimal | None = None

    for flt in entry.get("filters", []):
        ft = flt.get("filterType") or flt.get("filter_type")
        if ft == "PRICE_FILTER":
            raw_tick = flt.get("tickSize") or flt.get("tick_size")
            if raw_tick is not None:
                tick_size = Decimal(str(raw_tick))
            raw_minp = flt.get("minPrice") or flt.get("min_price")
            raw_maxp = flt.get("maxPrice") or flt.get("max_price")
            if raw_minp is not None and Decimal(str(raw_minp)) > 0:
                min_px = Decimal(str(raw_minp))
            if raw_maxp is not None and Decimal(str(raw_maxp)) > 0:
                max_px = Decimal(str(raw_maxp))
        elif ft == "LOT_SIZE":
            raw_step = flt.get("stepSize") or flt.get("step_size")
            if raw_step is not None:
                step_size = Decimal(str(raw_step))
            raw_minq = flt.get("minQty") or flt.get("min_qty")
            raw_maxq = flt.get("maxQty") or flt.get("max_qty")
            if raw_minq is not None and Decimal(str(raw_minq)) > 0:
                min_qty = Decimal(str(raw_minq))
            if raw_maxq is not None and Decimal(str(raw_maxq)) > 0:
                max_qty = Decimal(str(raw_maxq))
        elif ft == "MARKET_LOT_SIZE":
            raw_step = flt.get("stepSize") or flt.get("step_size")
            if raw_step is not None:
                market_step_size = Decimal(str(raw_step))
            raw_mmin = flt.get("minQty") or flt.get("min_qty")
            raw_mmax = flt.get("maxQty") or flt.get("max_qty")
            if raw_mmin is not None and Decimal(str(raw_mmin)) > 0:
                market_min_qty = Decimal(str(raw_mmin))
            if raw_mmax is not None and Decimal(str(raw_mmax)) > 0:
                market_max_qty = Decimal(str(raw_mmax))
        elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
            raw_not = flt.get("minNotional") or flt.get("notional") or flt.get("min_notional")
            if raw_not is not None:
                min_notional = Decimal(str(raw_not))

    if step_size is None:
        step_size = market_step_size
    if tick_size is None or step_size is None or min_notional is None:
        raise UnknownInstrumentSpecError(
            f"Incomplete filters for {market.value}:{symbol}: "
            f"tick={tick_size}, step={step_size}, min_notional={min_notional}"
        )
    base_asset, quote_asset = _infer_base_quote(
        symbol,
        entry.get("baseAsset") or entry.get("base_asset"),
        entry.get("quoteAsset") or entry.get("quote_asset"),
    )
    status = str(entry.get("status") or entry.get("contractStatus") or entry.get("trading_status") or "TRADING")
    raw_req_margin = entry.get("requiredMarginPercent") or entry.get("required_margin_percent")
    raw_maint_margin = entry.get("maintMarginPercent") or entry.get("maint_margin_percent")
    margin_init = Decimal(str(raw_req_margin)) / Decimal("100") if raw_req_margin is not None else None
    margin_maint = Decimal(str(raw_maint_margin)) / Decimal("100") if raw_maint_margin is not None else None
    contract_type = entry.get("contractType") or entry.get("contract_type")
    return BmdInstrumentSpec(
        market=market,
        symbol=symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=tick_size,
        step_size=step_size,
        min_notional=min_notional,
        observe_time_utc_ns=observe_time_utc_ns,
        source_time_ms=source_time_ms,
        min_quantity=min_qty,
        max_quantity=max_qty,
        min_price=min_px,
        max_price=max_px,
        trading_status=status,
        source=source,
        market_step_size=market_step_size,
        market_min_quantity=market_min_qty,
        market_max_quantity=market_max_qty,
        margin_init=margin_init,
        margin_maint=margin_maint,
        contract_type=str(contract_type) if contract_type is not None else None,
    )


def spec_from_reference_event(ev: ReferenceObservationEvent) -> BmdInstrumentSpec:
    """Build BmdInstrumentSpec from a BMD ReferenceObservationEvent (exchange_info/instrument)."""
    if ev.source_kind not in ("exchange_info", "instrument") or ev.status != "VALID":
        raise UnknownInstrumentSpecError(
            f"ReferenceObservationEvent is not a valid instrument spec: {ev.source_kind}/{ev.status}"
        )
    obs_ns = ev.observe_time_utc_ns or ev.receive_time_utc_ns
    if obs_ns is None or obs_ns <= 0:
        raise UnknownInstrumentSpecError(
            f"Instrument observation for {ev.market.value}:{ev.symbol} missing observe/receive timestamp"
        )

    payload: dict[str, Any] = {}
    if ev.payload_json:
        try:
            decoded = json.loads(ev.payload_json)
            if isinstance(decoded, dict):
                payload = decoded.get("instrument", decoded) if "instrument" in decoded else decoded
        except json.JSONDecodeError:
            payload = {}

    if payload.get("filters"):
        if not payload.get("symbol"):
            payload["symbol"] = ev.symbol
        if not payload.get("baseAsset") and ev.base_asset:
            payload["baseAsset"] = ev.base_asset
        if not payload.get("quoteAsset") and ev.quote_asset:
            payload["quoteAsset"] = ev.quote_asset
        if not payload.get("status") and ev.trading_status:
            payload["status"] = ev.trading_status
        return spec_from_exchange_info_entry(
            payload,
            market=ev.market,
            observe_time_utc_ns=obs_ns,
            source_time_ms=ev.source_time_ms,
            source=ev.source,
        )

    if ev.tick_size is None or ev.step_size is None or ev.min_notional is None:
        raise UnknownInstrumentSpecError(
            f"ReferenceObservationEvent for {ev.market.value}:{ev.symbol} lacks required filters"
        )
    base_asset, quote_asset = _infer_base_quote(ev.symbol, ev.base_asset, ev.quote_asset)
    return BmdInstrumentSpec(
        market=ev.market,
        symbol=ev.symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=ev.tick_size,
        step_size=ev.step_size,
        min_notional=ev.min_notional,
        observe_time_utc_ns=obs_ns,
        source_time_ms=ev.source_time_ms,
        trading_status=ev.trading_status or "TRADING",
        source=ev.source,
    )


def latest_instrument_spec_as_of(
    observations: Iterable[ReferenceObservationEvent],
    *,
    market: Market,
    symbol: str,
    as_of_utc_ns: int | None = None,
) -> BmdInstrumentSpec:
    """Return latest known valid instrument spec whose observation time <= as_of_utc_ns (D4)."""
    best: BmdInstrumentSpec | None = None
    for ev in observations:
        if ev.market != market or ev.symbol != symbol:
            continue
        if ev.source_kind not in ("exchange_info", "instrument") or ev.status != "VALID":
            continue
        spec = spec_from_reference_event(ev)
        if as_of_utc_ns is not None and spec.observe_time_utc_ns > as_of_utc_ns:
            continue
        if best is None or spec.observe_time_utc_ns >= best.observe_time_utc_ns:
            best = spec
    if best is None:
        raise UnknownInstrumentSpecError(
            f"No known instrument spec for {market.value}:{symbol} as_of={as_of_utc_ns}"
        )
    return best


def fetch_binance_exchange_info_specs(
    *,
    market: Market = Market.USDM,
    symbols: Sequence[str] = SUPPORTED_BMD_SYMBOLS,
    demo: bool = False,
    url: str | None = None,
    timeout_s: float = 10.0,
) -> dict[str, BmdInstrumentSpec]:
    """Fetch public Binance Spot/USD-M exchangeInfo and parse contemporaneous BmdInstrumentSpec entries."""
    target_url = url
    if target_url is None:
        if market == Market.USDM:
            target_url = BINANCE_USDM_DEMO_EXCHANGE_INFO_URL if demo else BINANCE_USDM_LIVE_EXCHANGE_INFO_URL
        elif market == Market.SPOT:
            target_url = BINANCE_SPOT_EXCHANGE_INFO_URL
        else:
            raise UnknownInstrumentSpecError(f"Unsupported market: {market.value}")
    req = urllib.request.Request(target_url, headers={"Accept": "application/json", "User-Agent": "BinanceQuant/M-NT3"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    observe_ns = time.time_ns()
    server_time_ms = int(payload["serverTime"]) if payload.get("serverTime") is not None else None
    wanted = set(symbols)
    out: dict[str, BmdInstrumentSpec] = {}
    src = f"{'demo' if demo else 'live'}_exchange_info"
    for entry in payload.get("symbols", []):
        sym = str(entry.get("symbol") or "")
        if wanted and sym not in wanted:
            continue
        out[sym] = spec_from_exchange_info_entry(
            entry,
            market=market,
            observe_time_utc_ns=observe_ns,
            source_time_ms=server_time_ms,
            source=src,
        )
    missing = [s for s in symbols if s not in out]
    if missing:
        raise UnknownInstrumentSpecError(f"Missing symbols in {src} ({market.value}): {missing}")
    return out


__all__ = [
    "BINANCE_SPOT_EXCHANGE_INFO_URL",
    "BINANCE_USDM_DEMO_EXCHANGE_INFO_URL",
    "BINANCE_USDM_LIVE_EXCHANGE_INFO_URL",
    "BmdInstrumentSpec",
    "DEFAULT_USDM_TIER1_BRACKETS",
    "LeverageBracketTier",
    "SUPPORTED_BMD_SYMBOLS",
    "UnknownInstrumentSpecError",
    "bracket_tiers_from_binance_json",
    "compute_tiered_maintenance_margin",
    "fetch_binance_exchange_info_specs",
    "latest_instrument_spec_as_of",
    "spec_from_exchange_info_entry",
    "spec_from_reference_event",
]
