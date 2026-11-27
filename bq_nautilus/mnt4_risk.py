"""M-NT4 Single-Keyed Process Lock, Synchronous Account Risk Gate, and Persistent UTC Ledger."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Literal, Mapping, Sequence


class SingleInstanceLockError(RuntimeError):
    """Raised when another keyed process already holds the exclusive (environment, account_id) lock."""


class LedgerIdentityMismatchError(ValueError):
    """Raised when a persistent risk DB does not match the active (environment, account_id, trader_id)."""


def _d(v: Any) -> Decimal:
    if v is None or v == "":
        return Decimal(0)
    return v if isinstance(v, Decimal) else Decimal(str(v))


def _utc_day_str(ts_ns: int | None = None) -> str:
    sec = (ts_ns / 1_000_000_000.0) if ts_ns is not None else time.time()
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%d")


class SingleKeyedProcessLock:
    """Exclusive non-blocking flock + metadata file enforcing single keyed LiveNode per environment/account."""

    def __init__(
        self,
        lock_path: Path,
        *,
        environment: str,
        account_id: str,
        trader_id: str,
        service_user: str | None = None,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.environment = str(environment)
        self.account_id = str(account_id)
        self.trader_id = str(trader_id)
        self.service_user = service_user or os.environ.get("USER", "unknown")
        self._fd: int | None = None

    def acquire(self) -> dict[str, Any]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            holder = self.inspect_holder()
            raise SingleInstanceLockError(
                f"Keyed process lock already held at {self.lock_path}: {holder}"
            ) from exc
        meta = {
            "pid": os.getpid(),
            "environment": self.environment,
            "account_id": self.account_id,
            "trader_id": self.trader_id,
            "service_user": self.service_user,
            "started_utc_ns": time.time_ns(),
        }
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, (json.dumps(meta, sort_keys=True) + "\n").encode("utf-8"))
        os.fsync(fd)
        self._fd = fd
        return meta

    def inspect_holder(self) -> dict[str, Any] | None:
        if not self.lock_path.is_file():
            return None
        try:
            raw = self.lock_path.read_text(encoding="utf-8").strip()
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self) -> SingleKeyedProcessLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class Mnt4RiskConfig:
    """Account-wide risk & operational boundary configuration for M-NT4."""

    environment: str = "SANDBOX"
    account_id: str = "BINANCE-SANDBOX-001"
    trader_id: str = "MNT4-001"
    allowed_markets: frozenset[str] = frozenset({"SPOT", "USDM"})
    allowed_symbols: frozenset[str] = frozenset({"BTCUSDT", "SOLUSDT"})
    starting_equity_usdt: Decimal = Decimal("50")
    max_daily_loss_usdt: Decimal = Decimal("2")
    max_order_notional_usdt: Decimal = Decimal("50")
    max_abs_position_qty: Mapping[str, Decimal] = field(
        default_factory=lambda: {"BTCUSDT": Decimal("0.01"), "SOLUSDT": Decimal("1.0")}
    )
    max_open_orders: int = 4
    allow_short_spot: bool = False
    require_reconciliation_on_boot: bool = True
    require_daily_rollover_reconciliation: bool = True
    max_order_submit_rate: str = "10/00:00:01"
    max_order_modify_rate: str = "10/00:00:01"
    allowed_hosts: tuple[str, ...] = (
        "127.0.0.1",
        "localhost",
        "demo-fapi.binance.com",
        "testnet.binance.vision",
        "api.binance.com",
        "fapi.binance.com",
    )
    require_no_withdraw_permission: bool = True

    def to_nautilus_risk_engine_config(self) -> Any:
        from nautilus_trader.config import LiveRiskEngineConfig

        caps = {f"{sym}.BINANCE": str(self.max_order_notional_usdt) for sym in self.allowed_symbols}
        caps.update({f"{sym}-PERP.BINANCE": str(self.max_order_notional_usdt) for sym in self.allowed_symbols})
        return LiveRiskEngineConfig(
            bypass=False,
            max_order_submit_rate=self.max_order_submit_rate,
            max_order_modify_rate=self.max_order_modify_rate,
            max_notional_per_order=caps,
        )

    def to_nautilus_exec_engine_config(
        self,
        *,
        reconciliation: bool = True,
        lookback_mins: int = 60,
        inflight_check_interval_ms: int = 2000,
        open_check_interval_secs: float = 5.0,
    ) -> Any:
        from nautilus_trader.config import LiveExecutionEngineConfig

        return LiveExecutionEngineConfig(
            reconciliation=reconciliation,
            reconciliation_lookback_mins=lookback_mins,
            inflight_check_interval_ms=inflight_check_interval_ms,
            open_check_interval_secs=open_check_interval_secs,
            snapshot_orders=True,
            snapshot_positions=True,
        )


@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    is_risk_reducing: bool
    daily_loss_usdt: Decimal
    marked_equity_usdt: Decimal


class PersistentUtcRiskStateStore:
    """SQLite WAL state binding UTC day loss lock, kill switch, open orders, fills, fees & reconciliation."""

    def __init__(
        self,
        db_path: Path,
        *,
        environment: str,
        account_id: str,
        trader_id: str,
        starting_equity_usdt: Decimal,
        utc_day: str | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), isolation_level=None, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self.environment = str(environment)
        self.account_id = str(account_id)
        self.trader_id = str(trader_id)
        self._init_schema(starting_equity_usdt=_d(starting_equity_usdt), utc_day=utc_day or _utc_day_str())

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self, *, starting_equity_usdt: Decimal, utc_day: str) -> None:
        c = self._conn
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                environment TEXT NOT NULL,
                account_id TEXT NOT NULL,
                trader_id TEXT NOT NULL,
                utc_day TEXT NOT NULL,
                day_start_equity_usdt TEXT NOT NULL,
                external_cashflow_usdt TEXT NOT NULL DEFAULT '0',
                realized_pnl_usdt TEXT NOT NULL DEFAULT '0',
                mark_unrealized_pnl_usdt TEXT NOT NULL DEFAULT '0',
                usdt_fees_paid TEXT NOT NULL DEFAULT '0',
                kill_switch_active INTEGER NOT NULL DEFAULT 0,
                kill_switch_reason TEXT,
                last_reconciled_utc_day TEXT,
                last_reconcile_ts_ns INTEGER
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signed_qty TEXT NOT NULL DEFAULT '0',
                avg_entry_price TEXT NOT NULL DEFAULT '0',
                PRIMARY KEY (market, symbol)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS open_orders (
                client_order_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                qty TEXT NOT NULL,
                leaves_qty TEXT NOT NULL,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                close_position INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                updated_ts_ns INTEGER NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_fills (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                price TEXT NOT NULL,
                fee TEXT NOT NULL,
                fee_asset TEXT NOT NULL,
                realized_pnl_usdt TEXT NOT NULL DEFAULT '0',
                ts_ns INTEGER NOT NULL,
                PRIMARY KEY (market, symbol, trade_id)
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_fees (
                asset TEXT PRIMARY KEY,
                total_fee TEXT NOT NULL DEFAULT '0'
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_audit_log (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ns INTEGER NOT NULL,
                command_kind TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                allowed INTEGER NOT NULL,
                reason TEXT NOT NULL
            )
            """
        )
        row = c.execute("SELECT * FROM risk_meta WHERE id = 1").fetchone()
        if row is None:
            c.execute(
                """
                INSERT INTO risk_meta (
                    id, environment, account_id, trader_id, utc_day,
                    day_start_equity_usdt, external_cashflow_usdt, realized_pnl_usdt,
                    mark_unrealized_pnl_usdt, usdt_fees_paid, kill_switch_active
                ) VALUES (1, ?, ?, ?, ?, ?, '0', '0', '0', '0', 0)
                """,
                (self.environment, self.account_id, self.trader_id, utc_day, str(starting_equity_usdt)),
            )
        else:
            if (row["environment"], row["account_id"], row["trader_id"]) != (
                self.environment,
                self.account_id,
                self.trader_id,
            ):
                raise LedgerIdentityMismatchError(
                    "Risk DB identity mismatch: "
                    f"db={(row['environment'], row['account_id'], row['trader_id'])} != "
                    f"expected={(self.environment, self.account_id, self.trader_id)}"
                )

    def get_meta(self) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM risk_meta WHERE id = 1").fetchone()
        assert row is not None
        return dict(row)

    def marked_equity_usdt(self) -> Decimal:
        m = self.get_meta()
        return (
            _d(m["day_start_equity_usdt"])
            + _d(m["external_cashflow_usdt"])
            + _d(m["realized_pnl_usdt"])
            + _d(m["mark_unrealized_pnl_usdt"])
            - _d(m["usdt_fees_paid"])
        )

    def daily_loss_usdt(self) -> Decimal:
        m = self.get_meta()
        baseline = _d(m["day_start_equity_usdt"]) + _d(m["external_cashflow_usdt"])
        loss = baseline - self.marked_equity_usdt()
        return loss if loss > 0 else Decimal(0)

    def record_external_cashflow(self, delta_usdt: Decimal) -> None:
        m = self.get_meta()
        new_cf = _d(m["external_cashflow_usdt"]) + _d(delta_usdt)
        self._conn.execute(
            "UPDATE risk_meta SET external_cashflow_usdt = ? WHERE id = 1",
            (str(new_cf),),
        )

    def update_mark_unrealized_pnl(self, mark_unrealized_usdt: Decimal) -> None:
        self._conn.execute(
            "UPDATE risk_meta SET mark_unrealized_pnl_usdt = ? WHERE id = 1",
            (str(_d(mark_unrealized_usdt)),),
        )

    def record_realized_loss_or_pnl(self, pnl_delta_usdt: Decimal, *, usdt_fee: Decimal = Decimal(0)) -> None:
        m = self.get_meta()
        new_pnl = _d(m["realized_pnl_usdt"]) + _d(pnl_delta_usdt)
        new_fee = _d(m["usdt_fees_paid"]) + _d(usdt_fee)
        self._conn.execute(
            "UPDATE risk_meta SET realized_pnl_usdt = ?, usdt_fees_paid = ? WHERE id = 1",
            (str(new_pnl), str(new_fee)),
        )

    def set_kill_switch(self, active: bool, *, reason: str | None = None) -> None:
        self._conn.execute(
            "UPDATE risk_meta SET kill_switch_active = ?, kill_switch_reason = ? WHERE id = 1",
            (1 if active else 0, reason if active else None),
        )

    def mark_reconciled(self, *, utc_day: str, ts_ns: int | None = None) -> None:
        self._conn.execute(
            "UPDATE risk_meta SET last_reconciled_utc_day = ?, last_reconcile_ts_ns = ? WHERE id = 1",
            (utc_day, ts_ns or time.time_ns()),
        )

    def rollover_utc_day(self, new_utc_day: str, *, mark_reconciled: bool = False) -> None:
        eq = self.marked_equity_usdt()
        self._conn.execute(
            """
            UPDATE risk_meta SET
                utc_day = ?,
                day_start_equity_usdt = ?,
                external_cashflow_usdt = '0',
                realized_pnl_usdt = '0',
                usdt_fees_paid = '0',
                mark_unrealized_pnl_usdt = '0',
                last_reconciled_utc_day = CASE WHEN ? THEN ? ELSE last_reconciled_utc_day END
            WHERE id = 1
            """,
            (new_utc_day, str(eq), 1 if mark_reconciled else 0, new_utc_day),
        )

    def get_position(self, market: str, symbol: str) -> tuple[Decimal, Decimal]:
        row = self._conn.execute(
            "SELECT signed_qty, avg_entry_price FROM positions WHERE market = ? AND symbol = ?",
            (market, symbol),
        ).fetchone()
        if row is None:
            return Decimal(0), Decimal(0)
        return _d(row["signed_qty"]), _d(row["avg_entry_price"])

    def set_position(self, market: str, symbol: str, signed_qty: Decimal, avg_entry_price: Decimal = Decimal(0)) -> None:
        self._conn.execute(
            """
            INSERT INTO positions (market, symbol, signed_qty, avg_entry_price) VALUES (?, ?, ?, ?)
            ON CONFLICT(market, symbol) DO UPDATE SET signed_qty = excluded.signed_qty, avg_entry_price = excluded.avg_entry_price
            """,
            (market, symbol, str(_d(signed_qty)), str(_d(avg_entry_price))),
        )

    def record_fill_idempotent(
        self,
        *,
        market: str,
        symbol: str,
        trade_id: str,
        client_order_id: str,
        side: Literal["BUY", "SELL"],
        qty: Decimal,
        price: Decimal,
        fee: Decimal = Decimal(0),
        fee_asset: str = "USDT",
        ts_ns: int | None = None,
    ) -> bool:
        """Record trade execution idempotently keyed by (market, symbol, trade_id); updates pos & PnL once."""
        qty_d = _d(qty)
        px_d = _d(price)
        fee_d = _d(fee)
        now = ts_ns or time.time_ns()
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            exists = c.execute(
                "SELECT 1 FROM seen_fills WHERE market = ? AND symbol = ? AND trade_id = ?",
                (market, symbol, trade_id),
            ).fetchone()
            if exists is not None:
                c.execute("ROLLBACK")
                return False

            cur_pos, cur_avg = self.get_position(market, symbol)
            delta = qty_d if side == "BUY" else -qty_d
            new_pos = cur_pos + delta
            realized_pnl = Decimal(0)
            new_avg = cur_avg

            if cur_pos == 0 or (cur_pos > 0 and delta > 0) or (cur_pos < 0 and delta < 0):
                tot_abs = abs(cur_pos) + abs(delta)
                new_avg = ((abs(cur_pos) * cur_avg) + (abs(delta) * px_d)) / tot_abs if tot_abs > 0 else Decimal(0)
            else:
                closed = min(abs(cur_pos), abs(delta))
                if cur_pos > 0:
                    realized_pnl = (px_d - cur_avg) * closed
                else:
                    realized_pnl = (cur_avg - px_d) * closed
                if new_pos == 0:
                    new_avg = Decimal(0)
                elif (cur_pos > 0) != (new_pos > 0):
                    new_avg = px_d

            c.execute(
                """
                INSERT INTO seen_fills (
                    market, symbol, trade_id, client_order_id, side, qty, price, fee, fee_asset, realized_pnl_usdt, ts_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (market, symbol, trade_id, client_order_id, side, str(qty_d), str(px_d), str(fee_d), fee_asset, str(realized_pnl), now),
            )
            c.execute(
                """
                INSERT INTO positions (market, symbol, signed_qty, avg_entry_price) VALUES (?, ?, ?, ?)
                ON CONFLICT(market, symbol) DO UPDATE SET signed_qty = excluded.signed_qty, avg_entry_price = excluded.avg_entry_price
                """,
                (market, symbol, str(new_pos), str(new_avg)),
            )
            if fee_d != 0:
                cur_asset_fee_row = c.execute("SELECT total_fee FROM asset_fees WHERE asset = ?", (fee_asset,)).fetchone()
                prev_af = _d(cur_asset_fee_row["total_fee"]) if cur_asset_fee_row else Decimal(0)
                c.execute(
                    """
                    INSERT INTO asset_fees (asset, total_fee) VALUES (?, ?)
                    ON CONFLICT(asset) DO UPDATE SET total_fee = excluded.total_fee
                    """,
                    (fee_asset, str(prev_af + fee_d)),
                )
            usdt_fee_add = fee_d if fee_asset == "USDT" else Decimal(0)
            m = dict(c.execute("SELECT realized_pnl_usdt, usdt_fees_paid FROM risk_meta WHERE id = 1").fetchone())
            c.execute(
                "UPDATE risk_meta SET realized_pnl_usdt = ?, usdt_fees_paid = ? WHERE id = 1",
                (str(_d(m["realized_pnl_usdt"]) + realized_pnl), str(_d(m["usdt_fees_paid"]) + usdt_fee_add)),
            )
            c.execute("COMMIT")
            return True
        except Exception:
            c.execute("ROLLBACK")
            raise

    def fees_by_asset(self) -> dict[str, Decimal]:
        rows = self._conn.execute("SELECT asset, total_fee FROM asset_fees ORDER BY asset").fetchall()
        return {str(r["asset"]): _d(r["total_fee"]) for r in rows}

    def upsert_open_order(
        self,
        *,
        client_order_id: str,
        market: str,
        symbol: str,
        side: str,
        order_type: str,
        qty: Decimal,
        leaves_qty: Decimal,
        reduce_only: bool,
        close_position: bool,
        state: str,
        ts_ns: int | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO open_orders (
                client_order_id, market, symbol, side, order_type, qty, leaves_qty, reduce_only, close_position, state, updated_ts_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                leaves_qty = excluded.leaves_qty,
                state = excluded.state,
                updated_ts_ns = excluded.updated_ts_ns
            """,
            (
                client_order_id,
                market,
                symbol,
                side,
                order_type,
                str(_d(qty)),
                str(_d(leaves_qty)),
                1 if reduce_only else 0,
                1 if close_position else 0,
                state,
                ts_ns or time.time_ns(),
            ),
        )

    def remove_open_order(self, client_order_id: str) -> None:
        self._conn.execute("DELETE FROM open_orders WHERE client_order_id = ?", (client_order_id,))

    def list_open_orders(self, *, symbol: str | None = None) -> list[dict[str, Any]]:
        if symbol is None:
            rows = self._conn.execute("SELECT * FROM open_orders ORDER BY updated_ts_ns").fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM open_orders WHERE symbol = ? ORDER BY updated_ts_ns", (symbol,)).fetchall()
        return [dict(r) for r in rows]

    def append_audit(self, *, ts_ns: int, command_kind: str, symbol: str, side: str, qty: Decimal, allowed: bool, reason: str) -> None:
        self._conn.execute(
            "INSERT INTO risk_audit_log (ts_ns, command_kind, symbol, side, qty, allowed, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts_ns, command_kind, symbol, side, str(qty), 1 if allowed else 0, reason),
        )


class SynchronousRiskGate:
    """Single synchronous risk gate blocking risk-increasing SUBMIT/MODIFY/REVERSAL while keeping exits open."""

    def __init__(self, *, config: Mnt4RiskConfig, store: PersistentUtcRiskStateStore) -> None:
        self.config = config
        self.store = store
        self._boot_reconciled = not config.require_reconciliation_on_boot
        if self._boot_reconciled:
            m = store.get_meta()
            if not m["last_reconciled_utc_day"]:
                store.mark_reconciled(utc_day=m["utc_day"])

    @property
    def boot_reconciled(self) -> bool:
        return self._boot_reconciled

    def complete_reconciliation(self, *, utc_day: str | None = None, ts_ns: int | None = None) -> None:
        day = utc_day or _utc_day_str(ts_ns)
        m = self.store.get_meta()
        if m["utc_day"] != day:
            self.store.rollover_utc_day(day, mark_reconciled=True)
        else:
            self.store.mark_reconciled(utc_day=day, ts_ns=ts_ns)
        self._boot_reconciled = True

    def evaluate_command(
        self,
        *,
        command_kind: Literal["SUBMIT", "MODIFY", "CANCEL"],
        market: str,
        symbol: str,
        side: Literal["BUY", "SELL"],
        qty: Decimal,
        reference_price: Decimal,
        previous_qty: Decimal | None = None,
        reduce_only: bool = False,
        close_position: bool = False,
        now_utc_day: str | None = None,
        now_ns: int | None = None,
    ) -> RiskDecision:
        ts = now_ns or time.time_ns()
        day = now_utc_day or _utc_day_str(ts)
        meta = self.store.get_meta()
        daily_loss = self.store.daily_loss_usdt()
        equity = self.store.marked_equity_usdt()

        def _ret(ok: bool, reason: str, reducing: bool) -> RiskDecision:
            self.store.append_audit(
                ts_ns=ts,
                command_kind=command_kind,
                symbol=symbol,
                side=side,
                qty=_d(qty),
                allowed=ok,
                reason=reason,
            )
            return RiskDecision(
                allowed=ok,
                reason=reason,
                is_risk_reducing=reducing,
                daily_loss_usdt=daily_loss,
                marked_equity_usdt=equity,
            )

        if command_kind == "CANCEL":
            return _ret(True, "CANCEL_ALWAYS_ALLOWED", True)

        if market not in self.config.allowed_markets:
            return _ret(False, f"DISALLOWED_MARKET:{market}", False)
        if symbol not in self.config.allowed_symbols:
            return _ret(False, f"DISALLOWED_SYMBOL:{symbol}", False)

        cur_pos, _ = self.store.get_position(market, symbol)
        qty_d = _d(qty)
        if qty_d <= 0 and not close_position:
            return _ret(False, "NON_POSITIVE_QTY", False)

        effective_delta = qty_d if side == "BUY" else -qty_d
        if command_kind == "MODIFY":
            prev_d = _d(previous_qty)
            extra = qty_d - prev_d
            if extra <= 0 and not ((cur_pos > 0 and side == "SELL") or (cur_pos < 0 and side == "BUY")):
                # Shrinking an open order never increases max position exposure
                return _ret(True, "MODIFY_SHRINKS_EXPOSURE", True)
            effective_qty_for_pos = qty_d
            next_pos = cur_pos + (qty_d if side == "BUY" else -qty_d)
        else:
            effective_qty_for_pos = qty_d
            next_pos = cur_pos + effective_delta

        # A command is strictly risk-reducing only when flatting/reducing existing position without crossing zero
        is_opposite = (cur_pos > 0 and side == "SELL") or (cur_pos < 0 and side == "BUY")
        is_pure_reduction = cur_pos != 0 and is_opposite and (close_position or (qty_d <= abs(cur_pos)))
        increases_or_flips = not is_pure_reduction
        if command_kind == "MODIFY" and _d(previous_qty) > 0 and qty_d > _d(previous_qty) and not is_pure_reduction:
            increases_or_flips = True

        # Check startup & cross-UTC reconciliation gates BEFORE allowing risk-increasing actions
        if self.config.require_reconciliation_on_boot and not self._boot_reconciled and increases_or_flips:
            return _ret(False, "BOOT_RECONCILIATION_REQUIRED", False)
        if self.config.require_daily_rollover_reconciliation and meta["utc_day"] != day and increases_or_flips:
            return _ret(False, f"UTC_ROLLOVER_RECONCILIATION_REQUIRED:{meta['utc_day']}->{day}", False)
        if meta["last_reconciled_utc_day"] != day and self.config.require_daily_rollover_reconciliation and increases_or_flips:
            return _ret(False, "DAILY_RECONCILIATION_INCOMPLETE", False)

        if (reduce_only or close_position) and not is_pure_reduction:
            return _ret(False, "REDUCE_ONLY_WOULD_NOT_REDUCE_POSITION", False)

        # Manual Kill Switch and UTC Daily Loss Lock block ALL non-reducing orders (including modify qty-up & reversals!)
        if bool(meta["kill_switch_active"]):
            if is_pure_reduction:
                return _ret(True, "ALLOWED_REDUCE_DURING_KILL_SWITCH", True)
            return _ret(False, f"KILL_SWITCH_ACTIVE:{meta['kill_switch_reason'] or 'locked'}", False)

        if daily_loss >= self.config.max_daily_loss_usdt:
            if is_pure_reduction:
                return _ret(True, "ALLOWED_REDUCE_DURING_DAILY_LOSS_LOCK", True)
            return _ret(False, f"DAILY_LOSS_LOCK:{daily_loss}>={self.config.max_daily_loss_usdt}", False)

        if is_pure_reduction:
            return _ret(True, "ALLOWED_RISK_REDUCTION", True)

        if market == "SPOT" and not self.config.allow_short_spot and next_pos < 0:
            return _ret(False, f"SPOT_SHORT_BLOCKED:{next_pos}", False)

        # Cross-check unexecuted open BUY orders for SPOT cash/short semantics (prevents phantom sell of unfilled buy)
        if market == "SPOT" and side == "SELL":
            open_sell_reserved = sum(
                (_d(o["leaves_qty"]) for o in self.store.list_open_orders(symbol=symbol) if o["side"] == "SELL" and o["market"] == "SPOT"),
                Decimal(0),
            )
            if cur_pos - open_sell_reserved < qty_d and not self.config.allow_short_spot:
                return _ret(False, "SPOT_SELL_EXCEEDS_SETTLED_POSITION", False)

        notional = effective_qty_for_pos * _d(reference_price)
        if notional > self.config.max_order_notional_usdt:
            return _ret(False, f"ORDER_NOTIONAL_LIMIT:{notional}>{self.config.max_order_notional_usdt}", False)

        max_pos = self.config.max_abs_position_qty.get(symbol)
        if max_pos is not None and abs(next_pos) > _d(max_pos):
            return _ret(False, f"MAX_ABS_POSITION:{abs(next_pos)}>{max_pos}", False)

        open_orders = self.store.list_open_orders()
        if command_kind == "SUBMIT" and len(open_orders) >= self.config.max_open_orders:
            return _ret(False, f"MAX_OPEN_ORDERS:{len(open_orders)}>={self.config.max_open_orders}", False)

        return _ret(True, "ALLOWED", False)


class RiskGuardedStrategyMixin:
    """Synchronous pre-trade enforcement mixin for Nautilus Strategy classes (not a passive observer)."""

    def _init_risk_gate(self, gate: SynchronousRiskGate, *, market: str, symbol: str) -> None:
        self._risk_gate = gate
        self._risk_market = market
        self._risk_symbol = symbol
        self.risk_rejections: list[dict[str, Any]] = []
        self.allowed_risk_actions: list[dict[str, Any]] = []

    def submit_guarded_order(
        self,
        order: Any,
        *,
        reference_price: Decimal,
        reduce_only: bool = False,
        close_position: bool = False,
        exec_client_id: Any | None = None,
        params: dict[str, Any] | None = None,
        now_utc_day: str | None = None,
    ) -> RiskDecision:
        side = "BUY" if "BUY" in str(order.side) else "SELL"
        qty = Decimal(str(order.quantity))
        dec = self._risk_gate.evaluate_command(
            command_kind="SUBMIT",
            market=self._risk_market,
            symbol=self._risk_symbol,
            side=side,
            qty=qty,
            reference_price=reference_price,
            reduce_only=reduce_only,
            close_position=close_position,
            now_utc_day=now_utc_day,
            now_ns=int(self.clock.timestamp_ns()),
        )
        if not dec.allowed:
            self.risk_rejections.append(
                {
                    "kind": "SUBMIT",
                    "client_order_id": str(order.client_order_id),
                    "side": side,
                    "qty": str(qty),
                    "reason": dec.reason,
                }
            )
            return dec
        self._risk_gate.store.upsert_open_order(
            client_order_id=str(order.client_order_id),
            market=self._risk_market,
            symbol=self._risk_symbol,
            side=side,
            order_type=type(order).__name__,
            qty=qty,
            leaves_qty=qty,
            reduce_only=reduce_only,
            close_position=close_position,
            state="SUBMITTED",
            ts_ns=int(self.clock.timestamp_ns()),
        )
        self.allowed_risk_actions.append({"kind": "SUBMIT", "client_order_id": str(order.client_order_id), "reason": dec.reason})
        if exec_client_id is not None and params is not None:
            self.submit_order(order, client_id=exec_client_id, params=params)
        elif exec_client_id is not None:
            self.submit_order(order, client_id=exec_client_id)
        elif params is not None:
            self.submit_order(order, params=params)
        else:
            self.submit_order(order)
        return dec

    def modify_guarded_order(
        self,
        order: Any,
        *,
        new_quantity: Decimal,
        new_price: Any | None = None,
        reference_price: Decimal,
        exec_client_id: Any | None = None,
        now_utc_day: str | None = None,
    ) -> RiskDecision:
        side = "BUY" if "BUY" in str(order.side) else "SELL"
        old_qty = Decimal(str(order.quantity))
        dec = self._risk_gate.evaluate_command(
            command_kind="MODIFY",
            market=self._risk_market,
            symbol=self._risk_symbol,
            side=side,
            qty=new_quantity,
            previous_qty=old_qty,
            reference_price=reference_price,
            reduce_only=bool(getattr(order, "is_reduce_only", False)),
            now_utc_day=now_utc_day,
            now_ns=int(self.clock.timestamp_ns()),
        )
        if not dec.allowed:
            self.risk_rejections.append(
                {
                    "kind": "MODIFY",
                    "client_order_id": str(order.client_order_id),
                    "side": side,
                    "qty": str(new_quantity),
                    "reason": dec.reason,
                }
            )
            return dec
        from nautilus_trader.model import Quantity

        q = Quantity.from_str(str(new_quantity))
        if exec_client_id is not None:
            self.modify_order(order.client_order_id, quantity=q, price=new_price, client_id=exec_client_id)
        else:
            self.modify_order(order.client_order_id, quantity=q, price=new_price)
        return dec

    def cancel_guarded_order(self, order: Any, *, exec_client_id: Any | None = None) -> RiskDecision:
        side = "BUY" if "BUY" in str(order.side) else "SELL"
        qty = Decimal(str(order.leaves_qty))
        dec = self._risk_gate.evaluate_command(
            command_kind="CANCEL",
            market=self._risk_market,
            symbol=self._risk_symbol,
            side=side,
            qty=qty,
            reference_price=Decimal(1),
            now_ns=int(self.clock.timestamp_ns()),
        )
        if exec_client_id is not None:
            self.cancel_order(order.client_order_id, client_id=exec_client_id)
        else:
            self.cancel_order(order.client_order_id)
        return dec


__all__ = [
    "LedgerIdentityMismatchError",
    "Mnt4RiskConfig",
    "PersistentUtcRiskStateStore",
    "RiskDecision",
    "RiskGuardedStrategyMixin",
    "SingleInstanceLockError",
    "SingleKeyedProcessLock",
    "SynchronousRiskGate",
]
