"""Deployment configuration loader (deploy.toml) enforcing no exchange API keys."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tomllib
from typing import Any

from bq_backtest.fill import ExchangeFilters
from bq_feed.events import Market, Stream


@dataclass(frozen=True, slots=True)
class DeployConfig:
    gateway_target: str = "127.0.0.1:50051"
    market: Market = Market.SPOT
    symbol: str = "BTCUSDT"
    stream: Stream = Stream.BOOK_TICKER
    strategy_kind: str = "btc_spot_bbo_rule"
    model_id: str | None = None
    model_dir: str | None = None
    order_qty: Decimal = Decimal("0.01")
    max_pos_qty: Decimal = Decimal("0.05")
    buy_imbalance_threshold: float = 0.25
    sell_imbalance_threshold: float = 0.25
    max_spread_bps: float = 20.0
    taker_fee_bps: Decimal = Decimal("10")
    stale_threshold_ns: int = 5_000_000_000
    tick_size: Decimal | None = Decimal("0.01")
    step_size: Decimal | None = Decimal("0.00001")
    min_notional: Decimal | None = Decimal("10")
    filters_known: bool = True
    initial_cash_quote: Decimal = Decimal("100000")
    journal_path: str = "paper_journal.jsonl"
    config_hash: str = ""

    def to_filters(self) -> ExchangeFilters:
        return ExchangeFilters(
            tick_size=self.tick_size,
            step_size=self.step_size,
            min_notional=self.min_notional,
            is_known=self.filters_known,
        )


def load_deploy_config(path: Path) -> DeployConfig:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    # Enforce Team C / M10 boundary: Signal runner never holds exchange API credentials
    forbidden = {"api_key", "api_secret", "binance_api_key", "binance_secret_key"}
    if any(k.lower() in forbidden for k in raw) or "credentials" in raw:
        raise ValueError("bq_signal deploy.toml MUST NOT contain exchange API keys (reserved for M11 BQOrder)")

    cfg_dict: dict[str, Any] = {
        "gateway_target": str(raw.get("gateway_target", "127.0.0.1:50051")),
        "market": Market(raw.get("market", "SPOT")),
        "symbol": str(raw.get("symbol", "BTCUSDT")),
        "stream": Stream(raw.get("stream", "book_ticker")),
        "strategy_kind": str(raw.get("strategy_kind", "btc_spot_bbo_rule")),
        "model_id": raw.get("model_id"),
        "model_dir": raw.get("model_dir"),
        "order_qty": Decimal(str(raw.get("order_qty", "0.01"))),
        "max_pos_qty": Decimal(str(raw.get("max_pos_qty", "0.05"))),
        "buy_imbalance_threshold": float(raw.get("buy_imbalance_threshold", 0.25)),
        "sell_imbalance_threshold": float(raw.get("sell_imbalance_threshold", 0.25)),
        "max_spread_bps": float(raw.get("max_spread_bps", 20.0)),
        "taker_fee_bps": Decimal(str(raw.get("taker_fee_bps", "10"))),
        "stale_threshold_ns": int(raw.get("stale_threshold_ns", 5_000_000_000)),
        "tick_size": Decimal(str(raw["tick_size"])) if raw.get("tick_size") is not None else None,
        "step_size": Decimal(str(raw["step_size"])) if raw.get("step_size") is not None else None,
        "min_notional": Decimal(str(raw["min_notional"])) if raw.get("min_notional") is not None else None,
        "filters_known": bool(raw.get("filters_known", True)),
        "initial_cash_quote": Decimal(str(raw.get("initial_cash_quote", "100000"))),
        "journal_path": str(raw.get("journal_path", "paper_journal.jsonl")),
    }
    digest = hashlib.sha256(
        json.dumps(cfg_dict, default=str, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return DeployConfig(**cfg_dict, config_hash=digest)
