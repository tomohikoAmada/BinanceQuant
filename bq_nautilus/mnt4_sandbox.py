"""M-NT4 Frozen 72h Nautilus Sandbox Runner, Hourly UTC Sealing, Replay Auditor & BMDR Cross-Checker."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from typing import Any

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceEnvironment,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
    BinanceSpotMarketDataMode,
)
from nautilus_trader.adapters.sandbox import (
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig, StrategyConfig
from nautilus_trader.execution import MakerTakerFeeModel
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    AccountType,
    BookType,
    ClientId,
    Currency,
    InstrumentId,
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

from .custom_data import BMD_STREAM_QUALITY_DATATYPE, BmdStreamQualityData
from .mnt4_risk import (
    Mnt4RiskConfig,
    PersistentUtcRiskStateStore,
    RiskGuardedStrategyMixin,
    SingleKeyedProcessLock,
    SynchronousRiskGate,
)

VENUE = Venue("BINANCE")
USDT = Currency.from_str("USDT")


def _utc_hour_bucket(ts_ns: int | None = None) -> str:
    sec = (ts_ns / 1_000_000_000.0) if ts_ns is not None else time.time()
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y%m%dT%H")


def _utc_day(ts_ns: int | None = None) -> str:
    sec = (ts_ns / 1_000_000_000.0) if ts_ns is not None else time.time()
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%d")


def _read_rss_mib() -> float:
    p = Path("/proc/self/status")
    if p.is_file():
        for ln in p.read_text(encoding="utf-8").splitlines():
            if ln.startswith("VmRSS:"):
                return round(int(ln.split()[1]) / 1024.0, 2)
    div = (1024.0 * 1024.0) if sys.platform == "darwin" else 1024.0
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / div, 2)


def _git_sha(repo_root: Path) -> str:
    try:
        return subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        sha_file = Path(repo_root) / ".git_sha"
        if sha_file.is_file():
            return sha_file.read_text(encoding="utf-8").strip() or "unknown"
        return "unknown"


def _nautilus_pkg_version() -> str:
    import importlib.metadata

    try:
        return importlib.metadata.version("nautilus_trader")
    except Exception:
        return "unknown"


class SandboxHourlyAuditStrategyConfig(StrategyConfig):
    """Config for M-NT4 frozen zero-credential Sandbox runner with synchronous risk gate & hourly event capture."""

    def __init__(
        self,
        *,
        spot_symbol: str = "BTCUSDT",
        factor_usdm_symbol: str = "SOLUSDT",
        order_qty: str = "0.00020",
        max_pos_qty: str = "0.00060",
        trade_every_n_quotes: int = 250,
        risk_gate: SynchronousRiskGate,
        hourly_writer: Any | None,
        metrics: dict[str, Any],
        online: bool = True,
        subscribe_factor_usdm: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.spot_symbol = spot_symbol
        self.factor_usdm_symbol = factor_usdm_symbol
        self.order_qty = order_qty
        self.max_pos_qty = max_pos_qty
        self.trade_every_n_quotes = int(trade_every_n_quotes)
        self.risk_gate = risk_gate
        self.hourly_writer = hourly_writer
        self.metrics = metrics
        self.online = online
        self.subscribe_factor_usdm = subscribe_factor_usdm


class SandboxHourlyAuditStrategy(Strategy, RiskGuardedStrategyMixin):
    """Zero-credential Nautilus Sandbox strategy shared identically between Live Sandbox & Replay Auditor."""

    def __init__(self, config: SandboxHourlyAuditStrategyConfig) -> None:
        super().__init__(config)
        self._cfg = config
        self._init_risk_gate(config.risk_gate, market="SPOT", symbol=config.spot_symbol)
        self.spot_id = InstrumentId.from_str(f"{config.spot_symbol}.BINANCE")
        self.usdm_id = InstrumentId.from_str(f"{config.factor_usdm_symbol}-PERP.BINANCE")
        self.metrics = config.metrics
        self._last_spot_quote_ns: int | None = None
        self._seen_fills: set[tuple[str, str, str]] = set()
        self.metrics.setdefault("callbacks", 0)
        self.metrics.setdefault("spot_callbacks", 0)
        self.metrics.setdefault("gaps_count", 0)
        self.metrics.setdefault("packet_drops", 0)
        self.metrics.setdefault("vis_latencies_ms", [])
        self.metrics.setdefault("unprotected_exposure_ns", 0)
        self.metrics.setdefault("effective_market_ns", 0)
        self.metrics.setdefault("intents_hasher", hashlib.sha256())

    def on_start(self) -> None:
        init_day = _utc_day() if self._cfg.online else self._risk_gate.store.get_meta()["utc_day"]
        self._risk_gate.complete_reconciliation(utc_day=init_day)
        if self._cfg.online:
            sc = ClientId("BINANCE-SPOT")
            self.subscribe_quotes(self.spot_id, client_id=sc)
            if self._cfg.subscribe_factor_usdm:
                uc = ClientId("BINANCE-USDM")
                self.subscribe_quotes(self.usdm_id, client_id=uc)
        else:
            self.subscribe_quotes(self.spot_id)

    def on_data(self, data: Any) -> None:
        payload = getattr(data, "data", data)
        if isinstance(payload, BmdStreamQualityData) and payload.status != "LIVE":
            self.metrics["gaps_count"] += 1

    def on_quote(self, quote: QuoteTick) -> None:
        now_ns = time.time_ns() if self._cfg.online else int(self.clock.timestamp_ns())
        if quote.instrument_id == self.spot_id:
            if self._last_spot_quote_ns is not None:
                dt = max(0, now_ns - self._last_spot_quote_ns)
                if dt <= 10_000_000_000:
                    self.metrics["effective_market_ns"] += dt
                else:
                    self.metrics["gaps_count"] += 1
            self._last_spot_quote_ns = now_ns

        if self._cfg.hourly_writer is not None:
            self._cfg.hourly_writer.record_quote(quote, wall_ns=now_ns)

        self.metrics["callbacks"] += 1
        if self._cfg.online and len(self.metrics["vis_latencies_ms"]) < 200_000:
            self.metrics["vis_latencies_ms"].append(max(0.0, (now_ns - int(quote.ts_init)) / 1_000_000.0))

        if quote.instrument_id != self.spot_id:
            return

        self.metrics["spot_callbacks"] += 1
        bid = Decimal(str(quote.bid_price))
        ask = Decimal(str(quote.ask_price))
        mid = (bid + ask) / Decimal(2)
        cur_pos, avg_px = self._risk_gate.store.get_position("SPOT", self._cfg.spot_symbol)
        if cur_pos != 0 and avg_px > 0:
            self._risk_gate.store.update_mark_unrealized_pnl((mid - avg_px) * cur_pos)
        else:
            self._risk_gate.store.update_mark_unrealized_pnl(Decimal(0))

        if self._cfg.trade_every_n_quotes > 0 and self.metrics["spot_callbacks"] % self._cfg.trade_every_n_quotes == 0:
            exec_route = ClientId("BINANCE") if self._cfg.online else None
            evt_day = _utc_day(now_ns) if self._cfg.online else _utc_day(int(quote.ts_init))
            if self._cfg.online and self._risk_gate.store.get_meta()["utc_day"] != evt_day:
                open_pos = sum((Decimal(str(p.signed_qty)) for p in self.cache.positions_open(instrument_id=self.spot_id)), Decimal(0))
                if open_pos == cur_pos and not self.cache.orders_open(instrument_id=self.spot_id):
                    self._risk_gate.complete_reconciliation(utc_day=evt_day, ts_ns=now_ns)
                    self.metrics["utc_rollovers_reconciled"] = self.metrics.get("utc_rollovers_reconciled", 0) + 1
            inst = self.cache.instrument(self.spot_id)
            s_dp = int(inst.size_precision) if inst is not None else 5
            qty = Decimal(self._cfg.order_qty)
            if cur_pos == 0:
                order = self.order_factory.market(self.spot_id, OrderSide.BUY, Quantity.from_decimal_dp(qty, s_dp))
                dec = self.submit_guarded_order(
                    order,
                    reference_price=ask,
                    exec_client_id=exec_route,
                    now_utc_day=evt_day,
                )
            else:
                sell_qty = min(qty, cur_pos)
                order = self.order_factory.market(self.spot_id, OrderSide.SELL, Quantity.from_decimal_dp(sell_qty, s_dp))
                dec = self.submit_guarded_order(
                    order,
                    reference_price=bid,
                    reduce_only=True,
                    exec_client_id=exec_route,
                    now_utc_day=evt_day,
                )
            self.metrics["intents_hasher"].update(
                f"{self.metrics['spot_callbacks']}|{int(quote.ts_event)}|{dec.allowed}|{dec.reason}\n".encode("utf-8")
            )

    def on_order_denied(self, event: Any) -> None:
        self._risk_gate.store.remove_open_order(str(event.client_order_id))

    def on_order_rejected(self, event: Any) -> None:
        self._risk_gate.store.remove_open_order(str(event.client_order_id))

    def on_order_canceled(self, event: Any) -> None:
        self._risk_gate.store.remove_open_order(str(event.client_order_id))

    def on_order_filled(self, event: Any) -> None:
        cid = str(event.client_order_id)
        vid = str(getattr(event, "venue_order_id", ""))
        tid = str(getattr(event, "trade_id", ""))
        fkey = (cid, vid, tid)
        if tid and fkey in self._seen_fills:
            return
        if tid:
            self._seen_fills.add(fkey)
        side = "BUY" if "BUY" in str(event.order_side) else "SELL"
        qty = Decimal(str(event.last_qty))
        px = Decimal(str(event.last_px))
        fee_parts = str(event.commission).split()
        fee_amt = Decimal(fee_parts[0]) if fee_parts else Decimal(0)
        fee_ccy = fee_parts[1] if len(fee_parts) > 1 else "USDT"
        self._risk_gate.store.record_fill_idempotent(
            market="SPOT",
            symbol=self._cfg.spot_symbol,
            trade_id=tid or cid,
            client_order_id=cid,
            side=side,
            qty=qty,
            price=px,
            fee=fee_amt,
            fee_asset=fee_ccy,
            ts_ns=int(event.ts_init),
        )
        self._risk_gate.store.remove_open_order(cid)


class HourlySanitizedWriter:
    """Rotates sanitized quote JSONL.GZ files on UTC hour boundaries and writes sealed hour audit JSON."""

    def __init__(
        self,
        *,
        state_dir: Path,
        risk_gate: SynchronousRiskGate,
        metrics: dict[str, Any],
        spot_symbol: str,
        metadata: dict[str, Any],
    ) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.risk_gate = risk_gate
        self.metrics = metrics
        self.spot_symbol = spot_symbol
        self.metadata = metadata
        self.cur_hour: str | None = None
        self.cur_path: Path | None = None
        self._gz: Any = None
        self._hour_rows = 0
        self.sealed_paths: list[Path] = []
        self._t_hour_start = time.monotonic()

    def _ensure_open_for_hour(self, h: str) -> None:
        if self.cur_hour is None:
            self.cur_hour = h
            self.cur_path = self.state_dir / f"events_{self.cur_hour}.jsonl.gz"
            self._gz = gzip.open(self.cur_path, "wt", encoding="utf-8")
            self._hour_rows = 0
            self._t_hour_start = time.monotonic()
            return
        if h != self.cur_hour:
            self.seal_current_hour(reason="utc_hour_rollover", reset_window_counters=True)
            self.cur_hour = h
            self.cur_path = self.state_dir / f"events_{self.cur_hour}.jsonl.gz"
            self._gz = gzip.open(self.cur_path, "wt", encoding="utf-8")
            self._hour_rows = 0
            self._t_hour_start = time.monotonic()

    def record_quote(self, q: QuoteTick, *, wall_ns: int) -> None:
        self._ensure_open_for_hour(_utc_hour_bucket(wall_ns))
        row = {
            "instrument_id": str(q.instrument_id),
            "bid": str(q.bid_price),
            "ask": str(q.ask_price),
            "bid_sz": str(q.bid_size),
            "ask_sz": str(q.ask_size),
            "ts_event": int(q.ts_event),
            "ts_init": int(q.ts_init),
        }
        self._gz.write(json.dumps(row) + "\n")
        self._hour_rows += 1

    def seal_current_hour(self, *, reason: str = "manual_seal", reset_window_counters: bool = False) -> Path:
        if self.cur_hour is None:
            self._ensure_open_for_hour(_utc_hour_bucket())
        if self._gz is not None:
            self._gz.close()
            self._gz = None
        assert self.cur_path is not None and self.cur_hour is not None
        sha = hashlib.sha256(self.cur_path.read_bytes()).hexdigest() if self.cur_path.is_file() else ""
        lats = sorted(self.metrics.get("vis_latencies_ms") or [])
        pos_qty, _ = self.risk_gate.store.get_position("SPOT", self.spot_symbol)
        meta = self.risk_gate.store.get_meta()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        wall = max(1e-6, time.monotonic() - self._t_hour_start)
        audit = {
            "hour_utc": self.cur_hour,
            "seal_reason": reason,
            "metadata": self.metadata,
            "wall_secs": round(wall, 3),
            "effective_market_secs": round(self.metrics.get("effective_market_ns", 0) / 1e9, 3),
            "gaps_count": int(self.metrics.get("gaps_count", 0)),
            "packet_drops": int(self.metrics.get("packet_drops", 0)),
            "callbacks": int(self.metrics.get("callbacks", 0)),
            "hour_event_rows": self._hour_rows,
            "visibility_latency_ms": {
                "p50": round(lats[len(lats) // 2], 3) if lats else 0.0,
                "p95": round(lats[int(0.95 * (len(lats) - 1))], 3) if lats else 0.0,
                "p99": round(lats[int(0.99 * (len(lats) - 1))], 3) if lats else 0.0,
            },
            "process_rss_mib": _read_rss_mib(),
            "cpu_user_plus_sys_secs": round(ru.ru_utime + ru.ru_stime, 3),
            "position_qty": str(pos_qty),
            "marked_equity_usdt": str(self.risk_gate.store.marked_equity_usdt()),
            "daily_loss_usdt": str(self.risk_gate.store.daily_loss_usdt()),
            "kill_switch_active": bool(meta["kill_switch_active"]),
            "risk_rejections_count": int(
                self.risk_gate.store._conn.execute("SELECT COUNT(*) FROM risk_audit_log WHERE allowed = 0").fetchone()[0]
            ),
            "intents_sha256": self.metrics["intents_hasher"].hexdigest(),
            "hourly_events_path": str(self.cur_path),
            "hourly_events_sha256": sha,
            "unprotected_exposure_secs": round(self.metrics.get("unprotected_exposure_ns", 0) / 1e9, 3),
        }
        out_path = self.state_dir / f"audit_{self.cur_hour}_{reason}.hour-audit.json"
        out_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
        self.sealed_paths.append(out_path)
        if reset_window_counters:
            # Bound memory across multi-day 72h runs while keeping cumulative ledger persistent in SQLite
            self.metrics["vis_latencies_ms"].clear()
            self.metrics["effective_market_ns"] = 0
            self.metrics["gaps_count"] = 0
            self.metrics["callbacks"] = 0
            self.metrics["spot_callbacks"] = 0
            self.metrics["intents_hasher"] = hashlib.sha256()
        return out_path


def replay_sealed_hour(
    audit_path: Path,
    *,
    spot_instrument: Any,
    work_dir: Path,
    trade_every_n_quotes: int = 200,
) -> dict[str, Any]:
    """Deterministically replay a sealed hourly JSONL.GZ through the exact same SandboxHourlyAuditStrategy."""
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    events_path = Path(audit["hourly_events_path"])
    actual_sha = hashlib.sha256(events_path.read_bytes()).hexdigest()
    assert actual_sha == audit["hourly_events_sha256"], "Hourly events archive checksum mismatch"

    spot_id_str = str(spot_instrument.id)
    p_dp = spot_instrument.price_precision
    s_dp = spot_instrument.size_precision
    ticks: list[QuoteTick] = []
    total_rows = 0
    with gzip.open(events_path, "rt", encoding="utf-8") as fh:
        for ln in fh:
            row = json.loads(ln)
            total_rows += 1
            if row["instrument_id"] != spot_id_str:
                continue
            ticks.append(
                QuoteTick(
                    instrument_id=spot_instrument.id,
                    bid_price=Price.from_decimal_dp(Decimal(row["bid"]), p_dp),
                    ask_price=Price.from_decimal_dp(Decimal(row["ask"]), p_dp),
                    bid_size=Quantity.from_decimal_dp(Decimal(row["bid_sz"]), s_dp),
                    ask_size=Quantity.from_decimal_dp(Decimal(row["ask_sz"]), s_dp),
                    ts_event=int(row["ts_event"]),
                    ts_init=int(row["ts_init"]),
                )
            )

    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    replay_db = work_dir / f"replay_{audit['hour_utc']}.sqlite3"
    if replay_db.exists():
        replay_db.unlink()

    init_day = _utc_day(ticks[0].ts_init) if ticks else "2026-11-27"
    spot_sym = str(audit["metadata"].get("spot_symbol", "BTCUSDT"))
    cfg = Mnt4RiskConfig(
        environment="REPLAY",
        account_id="MNT4-REPLAY-001",
        trader_id="MNT4-REPLAY-001",
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({spot_sym}),
        starting_equity_usdt=Decimal(str(audit["metadata"].get("risk_starting_equity_usdt", "50"))),
        max_daily_loss_usdt=Decimal(str(audit["metadata"].get("risk_max_daily_loss_usdt", "2"))),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={spot_sym: Decimal("0.01")},
    )
    store = PersistentUtcRiskStateStore(
        replay_db,
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
        utc_day=init_day,
    )
    gate = SynchronousRiskGate(config=cfg, store=store)
    metrics: dict[str, Any] = {}

    if ticks:
        engine = BacktestEngine(
            BacktestEngineConfig(
                trader_id=TraderId(cfg.trader_id),
                logging=LoggerConfig(stdout_level=LogLevel.ERROR),
            )
        )
        try:
            engine.add_venue(
                venue=VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                starting_balances=[Money.from_str(f"{cfg.starting_equity_usdt:.8f} USDT")],
                book_type=BookType.L1_MBP,
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
                liquidity_consumption=True,
                bar_execution=False,
            )
            engine.add_instrument(spot_instrument)
            strat = SandboxHourlyAuditStrategy(
                SandboxHourlyAuditStrategyConfig(
                    spot_symbol=spot_sym,
                    trade_every_n_quotes=trade_every_n_quotes,
                    risk_gate=gate,
                    hourly_writer=None,
                    metrics=metrics,
                    online=False,
                    subscribe_factor_usdm=False,
                )
            )
            engine.add_strategy(strat)
            engine.add_data(ticks)
            engine.run()
        finally:
            engine.dispose()

    pos_qty, _ = store.get_position("SPOT", spot_sym)
    replayed_eq = store.marked_equity_usdt()
    replayed_intents_sha = metrics["intents_hasher"].hexdigest() if metrics.get("intents_hasher") else hashlib.sha256().hexdigest()
    store.close()

    return {
        "status": (
            "PASSED"
            if (
                actual_sha == audit["hourly_events_sha256"]
                and str(pos_qty) == str(audit["position_qty"])
            )
            else "MISMATCH"
        ),
        "hour_utc": audit["hour_utc"],
        "events_sha256_verified": actual_sha == audit["hourly_events_sha256"],
        "total_logged_rows": total_rows,
        "spot_ticks_replayed": len(ticks),
        "replayed_pos_qty": str(pos_qty),
        "live_reported_pos_qty": str(audit["position_qty"]),
        "replayed_equity_usdt": str(replayed_eq),
        "live_reported_equity_usdt": str(audit["marked_equity_usdt"]),
        "replayed_intents_sha256": replayed_intents_sha,
        "live_intents_sha256": str(audit["intents_sha256"]),
        "intents_exact_match": replayed_intents_sha == str(audit["intents_sha256"]),
    }


async def run_sandbox_session(
    *,
    state_dir: Path,
    duration_secs: float = 30.0,
    spot_symbol: str = "BTCUSDT",
    factor_usdm_symbol: str = "SOLUSDT",
    trade_every_n_quotes: int = 200,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Run zero-credential LiveNode(Environment.SANDBOX) for `duration_secs` and seal hourly audit + manifest."""
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    cfg = Mnt4RiskConfig(
        environment="SANDBOX",
        account_id="BINANCE-SANDBOX-72H",
        trader_id="MNT4-SANDBOX-001",
        allowed_markets=frozenset({"SPOT"}),
        allowed_symbols=frozenset({spot_symbol}),
        starting_equity_usdt=Decimal("50"),
        max_daily_loss_usdt=Decimal("2"),
        max_order_notional_usdt=Decimal("50"),
        max_abs_position_qty={spot_symbol: Decimal("0.01")},
    )
    lock_path = state_dir / "sandbox_keyed.lock"
    db_path = state_dir / "sandbox_risk.sqlite3"
    store = PersistentUtcRiskStateStore(
        db_path,
        environment=cfg.environment,
        account_id=cfg.account_id,
        trader_id=cfg.trader_id,
        starting_equity_usdt=cfg.starting_equity_usdt,
    )
    gate = SynchronousRiskGate(config=cfg, store=store)

    rroot = repo_root or Path(__file__).resolve().parents[1]
    metadata = {
        "git_sha": _git_sha(rroot),
        "nautilus_version": _nautilus_pkg_version(),
        "environment": "SANDBOX",
        "zero_live_credentials": True,
        "spot_symbol": spot_symbol,
        "factor_usdm_symbol": factor_usdm_symbol,
        "trade_every_n_quotes": trade_every_n_quotes,
        "risk_starting_equity_usdt": str(cfg.starting_equity_usdt),
        "risk_max_daily_loss_usdt": str(cfg.max_daily_loss_usdt),
        "pid": os.getpid(),
    }
    (state_dir / "run_manifest.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    metrics: dict[str, Any] = {}
    writer = HourlySanitizedWriter(
        state_dir=state_dir,
        risk_gate=gate,
        metrics=metrics,
        spot_symbol=spot_symbol,
        metadata=metadata,
    )

    spot_iid = f"{spot_symbol}.BINANCE"
    usdm_iid = f"{factor_usdm_symbol}-PERP.BINANCE"
    node = (
        LiveNode.builder("MNT4-SANDBOX-72H", TraderId(cfg.trader_id), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
        .with_risk_engine_config(cfg.to_nautilus_risk_engine_config())
        .with_timeout_connection(15)
        .with_timeout_disconnection_secs(3)
        .with_delay_post_stop_secs(1)
        .with_delay_shutdown_secs(1)
        .with_reconciliation(reconciliation=False)
        .add_data_client(
            "BINANCE-SPOT",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.SPOT,
                environment=BinanceEnvironment.LIVE,
                spot_market_data_mode=BinanceSpotMarketDataMode.Json,
                us=False,
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[spot_iid]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        .add_data_client(
            "BINANCE-USDM",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[usdm_iid]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
        .add_simulated_exec_client(
            "BINANCE",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=VENUE,
                account_id=AccountId(cfg.account_id),
                account_type=AccountType.CASH,
                oms_type=OmsType.NETTING,
                book_type=BookType.L1_MBP,
                starting_balances=[Money.from_str("50.00000000 USDT")],
                fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001")),
                liquidity_consumption=True,
                bar_execution=False,
            ),
        )
        .build()
    )
    strat = SandboxHourlyAuditStrategy(
        SandboxHourlyAuditStrategyConfig(
            spot_symbol=spot_symbol,
            factor_usdm_symbol=factor_usdm_symbol,
            trade_every_n_quotes=trade_every_n_quotes,
            risk_gate=gate,
            hourly_writer=writer,
            metrics=metrics,
            online=True,
        )
    )
    node.add_strategy(strat)

    async def _stopper() -> None:
        await asyncio.sleep(duration_secs)
        strat.shutdown_system("M-NT4 sandbox duration complete")

    with SingleKeyedProcessLock(lock_path, environment=cfg.environment, account_id=cfg.account_id, trader_id=cfg.trader_id):
        task = asyncio.create_task(_stopper())
        try:
            await asyncio.wait_for(node.run_async(), timeout=duration_secs + 30.0)
        finally:
            task.cancel()
            sealed_path = writer.seal_current_hour(reason="session_end")
            node.dispose()
            store.close()

    audit = json.loads(sealed_path.read_text(encoding="utf-8"))
    return {
        "status": "PASSED" if audit["callbacks"] > 0 else "FAILED",
        "sealed_audit_path": str(sealed_path),
        "all_sealed_paths": [str(p) for p in writer.sealed_paths],
        "audit": audit,
    }


__all__ = [
    "HourlySanitizedWriter",
    "SandboxHourlyAuditStrategy",
    "SandboxHourlyAuditStrategyConfig",
    "replay_sealed_hour",
    "run_sandbox_session",
]
