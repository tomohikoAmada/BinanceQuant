"""Single-writer SQLite WAL transactional order ledger & persistent Kill Switch state."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sqlite3
import time
from typing import Literal

from .models import (
    ACTIVE_OR_UNKNOWN_STATES,
    ExchangeFill,
    ExchangeOrderSnapshot,
    KillSwitchState,
    LedgerOrderRecord,
    OrderState,
    TERMINAL_STATES,
)


def _d(val: str | Decimal | None) -> Decimal:
    if val is None or val == "":
        return Decimal(0)
    return Decimal(str(val))


class OrderLedger:
    """Crash-safe SQLite WAL ledger preventing duplicate outbound orders across disconnects & restarts."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path),
            isolation_level=None,
            timeout=30.0,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                intent_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL UNIQUE,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                requested_qty TEXT NOT NULL,
                limit_price TEXT,
                reference_price TEXT NOT NULL,
                state TEXT NOT NULL,
                exchange_order_id TEXT,
                cumulative_filled_qty TEXT NOT NULL DEFAULT '0',
                cumulative_quote_qty TEXT NOT NULL DEFAULT '0',
                avg_fill_price TEXT NOT NULL DEFAULT '0',
                cumulative_fee TEXT NOT NULL DEFAULT '0',
                fee_asset TEXT NOT NULL DEFAULT 'USDT',
                submit_attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_ts_ns INTEGER NOT NULL,
                updated_ts_ns INTEGER NOT NULL,
                terminal_ts_ns INTEGER
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS send_attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                intent_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                created_ts_ns INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                detail TEXT
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
                fill_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                price TEXT NOT NULL,
                fee TEXT NOT NULL,
                fee_asset TEXT NOT NULL,
                trade_time_ms INTEGER NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kill_switch (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                active INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                local_block_new_orders_ts_ns INTEGER,
                cancel_sent_ts_ns INTEGER,
                exchange_terminal_ts_ns INTEGER,
                cleared_by TEXT,
                cleared_ts_ns INTEGER
            )
            """
        )
        self._conn.execute("INSERT OR IGNORE INTO kill_switch (id, active) VALUES (1, 0)")

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> LedgerOrderRecord:
        return LedgerOrderRecord(
            intent_id=row["intent_id"],
            client_order_id=row["client_order_id"],
            market=row["market"],
            symbol=row["symbol"],
            side=row["side"],
            order_type=row["order_type"],
            requested_qty=Decimal(row["requested_qty"]),
            limit_price=Decimal(row["limit_price"]) if row["limit_price"] is not None else None,
            reference_price=Decimal(row["reference_price"]),
            state=OrderState(row["state"]),
            exchange_order_id=row["exchange_order_id"],
            cumulative_filled_qty=Decimal(row["cumulative_filled_qty"]),
            cumulative_quote_qty=Decimal(row["cumulative_quote_qty"]),
            avg_fill_price=Decimal(row["avg_fill_price"]),
            cumulative_fee=Decimal(row["cumulative_fee"]),
            fee_asset=row["fee_asset"],
            submit_attempts=int(row["submit_attempts"]),
            last_error=row["last_error"],
            created_ts_ns=int(row["created_ts_ns"]),
            updated_ts_ns=int(row["updated_ts_ns"]),
            terminal_ts_ns=int(row["terminal_ts_ns"]) if row["terminal_ts_ns"] is not None else None,
        )

    def get_order(self, intent_id: str) -> LedgerOrderRecord | None:
        row = self._conn.execute("SELECT * FROM orders WHERE intent_id = ?", (intent_id,)).fetchone()
        return self._row_to_record(row) if row is not None else None

    def get_order_by_client_id(self, client_order_id: str) -> LedgerOrderRecord | None:
        row = self._conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        return self._row_to_record(row) if row is not None else None

    def list_orders(self, *, states: frozenset[OrderState] | None = None) -> list[LedgerOrderRecord]:
        if states is None:
            rows = self._conn.execute("SELECT * FROM orders ORDER BY created_ts_ns ASC").fetchall()
        else:
            placeholders = ",".join("?" for _ in states)
            rows = self._conn.execute(
                f"SELECT * FROM orders WHERE state IN ({placeholders}) ORDER BY created_ts_ns ASC",
                tuple(s.value for s in states),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_active_or_unknown(self) -> list[LedgerOrderRecord]:
        return self.list_orders(states=ACTIVE_OR_UNKNOWN_STATES)

    def list_fills(self, client_order_id: str | None = None) -> list[ExchangeFill]:
        if client_order_id is None:
            rows = self._conn.execute("SELECT * FROM fills ORDER BY trade_time_ms ASC, fill_id ASC").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM fills WHERE client_order_id = ? ORDER BY trade_time_ms ASC, fill_id ASC",
                (client_order_id,),
            ).fetchall()
        return [
            ExchangeFill(
                fill_id=r["fill_id"],
                client_order_id=r["client_order_id"],
                symbol=r["symbol"],
                side=r["side"],
                qty=Decimal(r["qty"]),
                price=Decimal(r["price"]),
                fee=Decimal(r["fee"]),
                fee_asset=r["fee_asset"],
                trade_time_ms=int(r["trade_time_ms"]),
            )
            for r in rows
        ]

    def net_position_qty(self, symbol: str) -> Decimal:
        """Net executed + reserved open BUY/SELL exposure for risk checks."""
        rows = self._conn.execute("SELECT * FROM orders WHERE symbol = ?", (symbol,)).fetchall()
        pos = Decimal(0)
        for r in rows:
            st = OrderState(r["state"])
            filled = Decimal(r["cumulative_filled_qty"])
            req = Decimal(r["requested_qty"])
            if r["side"] == "BUY":
                pos += filled if st in TERMINAL_STATES else req
            else:
                pos -= filled if st in TERMINAL_STATES else req
        return pos

    def reserve_before_send(
        self,
        *,
        intent_id: str,
        client_order_id: str,
        market: str,
        symbol: str,
        side: Literal["BUY", "SELL"],
        order_type: Literal["MARKET", "LIMIT"],
        quantity: Decimal,
        limit_price: Decimal | None,
        reference_price: Decimal,
        now_ns: int | None = None,
    ) -> tuple[bool, LedgerOrderRecord]:
        """Atomically persist intent + send attempt BEFORE network IO.

        Returns (created_new, record). If created_new is False, the intent/client_order_id
        already existed and MUST NOT be transmitted again blindly.
        """
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            existing = self._conn.execute(
                "SELECT * FROM orders WHERE intent_id = ? OR client_order_id = ?",
                (intent_id, client_order_id),
            ).fetchone()
            if existing is not None:
                self._conn.execute("COMMIT")
                return False, self._row_to_record(existing)

            self._conn.execute(
                """
                INSERT INTO orders (
                    intent_id, client_order_id, market, symbol, side, order_type,
                    requested_qty, limit_price, reference_price, state, submit_attempts,
                    created_ts_ns, updated_ts_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    intent_id,
                    client_order_id,
                    market,
                    symbol,
                    side,
                    order_type,
                    str(quantity),
                    str(limit_price) if limit_price is not None else None,
                    str(reference_price),
                    OrderState.SENDING.value,
                    ts,
                    ts,
                ),
            )
            self._conn.execute(
                """
                INSERT INTO send_attempts (intent_id, client_order_id, created_ts_ns, outcome, detail)
                VALUES (?, ?, ?, 'SENDING', 'pre_wire_commit')
                """,
                (intent_id, client_order_id, ts),
            )
            row = self._conn.execute("SELECT * FROM orders WHERE intent_id = ?", (intent_id,)).fetchone()
            self._conn.execute("COMMIT")
            return True, self._row_to_record(row)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def mark_unknown_after_error(
        self,
        intent_id: str,
        *,
        error: str,
        now_ns: int | None = None,
    ) -> LedgerOrderRecord:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE orders
                SET state = ?, last_error = ?, updated_ts_ns = ?
                WHERE intent_id = ? AND state IN (?, ?)
                """,
                (
                    OrderState.UNKNOWN_PENDING_RECONCILE.value,
                    error,
                    ts,
                    intent_id,
                    OrderState.SENDING.value,
                    OrderState.NEW_PERSISTED.value,
                ),
            )
            row = self._conn.execute("SELECT * FROM orders WHERE intent_id = ?", (intent_id,)).fetchone()
            if row is not None:
                self._conn.execute(
                    """
                    INSERT INTO send_attempts (intent_id, client_order_id, created_ts_ns, outcome, detail)
                    VALUES (?, ?, ?, 'UNKNOWN_PENDING_RECONCILE', ?)
                    """,
                    (intent_id, row["client_order_id"], ts, error),
                )
            self._conn.execute("COMMIT")
            assert row is not None
            return self._row_to_record(row)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def apply_exchange_snapshot(
        self,
        snapshot: ExchangeOrderSnapshot,
        *,
        extra_fills: tuple[ExchangeFill, ...] = (),
        now_ns: int | None = None,
    ) -> LedgerOrderRecord:
        """Idempotently reconcile exchange order snapshot & fills into the local SQLite ledger."""
        ts = time.monotonic_ns() if now_ns is None else now_ns
        status_map = {
            "NEW": OrderState.ACKED_OPEN,
            "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
            "FILLED": OrderState.FILLED,
            "CANCELED": OrderState.CANCELED,
            "REJECTED": OrderState.REJECTED,
            "EXPIRED": OrderState.CANCELED,
        }
        next_state = status_map[snapshot.status]
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT * FROM orders WHERE client_order_id = ?", (snapshot.client_order_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown client_order_id in ledger: {snapshot.client_order_id}")

            all_fills = list(snapshot.fills) + list(extra_fills)
            for fl in all_fills:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO fills (
                        fill_id, client_order_id, symbol, side, qty, price, fee, fee_asset, trade_time_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fl.fill_id,
                        fl.client_order_id,
                        fl.symbol,
                        fl.side,
                        str(fl.qty),
                        str(fl.price),
                        str(fl.fee),
                        fl.fee_asset,
                        fl.trade_time_ms,
                    ),
                )

            fill_rows = self._conn.execute(
                "SELECT * FROM fills WHERE client_order_id = ?", (snapshot.client_order_id,)
            ).fetchall()
            if fill_rows:
                cum_qty = sum((Decimal(f["qty"]) for f in fill_rows), Decimal(0))
                cum_quote = sum((Decimal(f["qty"]) * Decimal(f["price"]) for f in fill_rows), Decimal(0))
                cum_fee = sum((Decimal(f["fee"]) for f in fill_rows), Decimal(0))
                fee_asset = fill_rows[-1]["fee_asset"]
            else:
                cum_qty = snapshot.executed_qty
                cum_quote = snapshot.cummulative_quote_qty
                cum_fee = snapshot.fee_quote
                fee_asset = snapshot.fee_asset

            avg_px = (cum_quote / cum_qty) if cum_qty > 0 else _d(snapshot.avg_price)
            term_ts = ts if next_state in TERMINAL_STATES else row["terminal_ts_ns"]
            err_text = snapshot.error_msg if snapshot.error_msg else row["last_error"]
            self._conn.execute(
                """
                UPDATE orders
                SET state = ?, exchange_order_id = COALESCE(?, exchange_order_id),
                    cumulative_filled_qty = ?, cumulative_quote_qty = ?, avg_fill_price = ?,
                    cumulative_fee = ?, fee_asset = ?, last_error = ?,
                    updated_ts_ns = ?, terminal_ts_ns = ?
                WHERE client_order_id = ?
                """,
                (
                    next_state.value,
                    snapshot.exchange_order_id,
                    str(cum_qty),
                    str(cum_quote),
                    str(avg_px),
                    str(cum_fee),
                    fee_asset,
                    err_text,
                    ts,
                    term_ts,
                    snapshot.client_order_id,
                ),
            )
            updated = self._conn.execute(
                "SELECT * FROM orders WHERE client_order_id = ?", (snapshot.client_order_id,)
            ).fetchone()
            self._conn.execute("COMMIT")
            return self._row_to_record(updated)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def mark_closed_not_found(self, client_order_id: str, *, reason: str, now_ns: int | None = None) -> LedgerOrderRecord:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE orders
                SET state = ?, last_error = ?, updated_ts_ns = ?, terminal_ts_ns = ?
                WHERE client_order_id = ?
                """,
                (OrderState.CLOSED_NOT_FOUND.value, reason, ts, ts, client_order_id),
            )
            row = self._conn.execute(
                "SELECT * FROM orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
            self._conn.execute("COMMIT")
            assert row is not None
            return self._row_to_record(row)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_kill_switch(self) -> KillSwitchState:
        row = self._conn.execute("SELECT * FROM kill_switch WHERE id = 1").fetchone()
        return KillSwitchState(
            active=bool(row["active"]),
            reason=row["reason"],
            local_block_new_orders_ts_ns=row["local_block_new_orders_ts_ns"],
            cancel_sent_ts_ns=row["cancel_sent_ts_ns"],
            exchange_terminal_ts_ns=row["exchange_terminal_ts_ns"],
            cleared_by=row["cleared_by"],
            cleared_ts_ns=row["cleared_ts_ns"],
        )

    def activate_kill_switch(self, *, reason: str, now_ns: int | None = None) -> KillSwitchState:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute("SELECT * FROM kill_switch WHERE id = 1").fetchone()
            block_ts = row["local_block_new_orders_ts_ns"] if (row["active"] and row["local_block_new_orders_ts_ns"]) else ts
            self._conn.execute(
                """
                UPDATE kill_switch
                SET active = 1, reason = ?, local_block_new_orders_ts_ns = ?,
                    cancel_sent_ts_ns = CASE WHEN active = 1 THEN cancel_sent_ts_ns ELSE NULL END,
                    exchange_terminal_ts_ns = CASE WHEN active = 1 THEN exchange_terminal_ts_ns ELSE NULL END,
                    cleared_by = NULL, cleared_ts_ns = NULL
                WHERE id = 1
                """,
                (reason, block_ts),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_kill_switch()

    def record_kill_switch_cancel_sent(self, *, now_ns: int | None = None) -> KillSwitchState:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE kill_switch
                SET cancel_sent_ts_ns = COALESCE(cancel_sent_ts_ns, ?)
                WHERE id = 1 AND active = 1
                """,
                (ts,),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_kill_switch()

    def record_kill_switch_terminal(self, *, now_ns: int | None = None) -> KillSwitchState:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE kill_switch
                SET exchange_terminal_ts_ns = COALESCE(exchange_terminal_ts_ns, ?)
                WHERE id = 1 AND active = 1
                """,
                (ts,),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_kill_switch()

    def clear_kill_switch(self, *, cleared_by: str, now_ns: int | None = None) -> KillSwitchState:
        ts = time.monotonic_ns() if now_ns is None else now_ns
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE kill_switch
                SET active = 0, cleared_by = ?, cleared_ts_ns = ?
                WHERE id = 1
                """,
                (cleared_by, ts),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_kill_switch()
