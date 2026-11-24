"""M11 BQOrder fault-injection, crash recovery, Kill Switch, IPC isolation, and M10 streaming tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from bq_backtest.strategy import OrderIntent
from bq_feed import BookTickerEvent, Market, Stream
from bq_order import (
    BinanceSpotRestClient,
    FakeSpotExchange,
    HardRiskGuard,
    HardRiskLimits,
    OrderExecutionGuard,
    OrderIpcClient,
    OrderIpcServer,
    OrderLedger,
    OrderState,
    SpotCredentials,
)
from bq_signal import DeployConfig, SignalRunner, reconcile_paper_journal


def _intent(
    intent_id: str,
    *,
    side: str = "BUY",
    qty: str = "0.01",
    order_type: str = "MARKET_TAKER",
    market: Market = Market.SPOT,
) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        market=market,
        symbol="BTCUSDT",
        side=side,
        quantity=Decimal(qty),
        order_type=order_type,
        decision_seq=1,
        decision_ts_ns=1_800_000_000_000_000_000,
        decision_infer_ns=0,
        reason="m11_test",
    )


def test_exchange_fill_dropped_ack_and_restart_recovery_single_submission(tmp_path: Path) -> None:
    """Team C mandatory fault: exchange filled -> dropped ACK -> restart -> 1 wire send & 1 local fill."""
    db_path = tmp_path / "orders.db"
    ex = FakeSpotExchange(fill_price=Decimal("60000"), fee_bps=Decimal("10"))
    ex.drop_next_ack_after_fill = True

    limits = HardRiskLimits()
    ledger1 = OrderLedger(db_path)
    guard1 = OrderExecutionGuard(ledger=ledger1, risk_guard=HardRiskGuard(limits), exchange=ex)

    intent = _intent("intent-drop-ack-001", qty="0.02")
    out1 = guard1.submit_intent(intent, reference_price=Decimal("60000"), now_ns=100)
    assert out1.accepted is False
    assert out1.state == OrderState.UNKNOWN_PENDING_RECONCILE
    assert ex.submit_wire_calls == 1
    assert ex.base_qty == Decimal("0.02")

    # Duplicate retry while UNKNOWN must be frozen locally without wire transmission
    out_dup = guard1.submit_intent(intent, reference_price=Decimal("60000"), now_ns=110)
    assert out_dup.accepted is False
    assert "FROZEN_PENDING_RECONCILE" in (out_dup.reason or "")
    assert ex.submit_wire_calls == 1
    ledger1.close()

    # Simulated crash & restart: reopen SQLite WAL ledger and recover pending orders first
    ledger2 = OrderLedger(db_path)
    guard2 = OrderExecutionGuard(ledger=ledger2, risk_guard=HardRiskGuard(limits), exchange=ex)
    recov = guard2.recover_pending(now_ns=200)
    assert recov.checked_count == 1
    assert recov.resolved_count == 1
    assert recov.still_pending_count == 0

    stored = ledger2.get_order("intent-drop-ack-001")
    assert stored is not None
    assert stored.state == OrderState.FILLED
    assert stored.cumulative_filled_qty == Decimal("0.02")
    # fee = 0.02 * 60000 * 10bps = 1.2 USDT
    assert stored.cumulative_fee == Decimal("1.2")

    # Even AFTER FILLED, replaying same intent_id cannot trigger Spot duplicate clientOrderId fill
    out_after = guard2.submit_intent(intent, reference_price=Decimal("60000"), now_ns=300)
    assert out_after.accepted is False
    assert "DUPLICATE_INTENT_IDEMPOTENT" in (out_after.reason or "")
    assert ex.submit_wire_calls == 1
    assert ex.base_qty == Decimal("0.02")
    ledger2.close()


def test_timeout_before_exchange_resolves_not_found(tmp_path: Path) -> None:
    db_path = tmp_path / "timeout.db"
    ex = FakeSpotExchange()
    ex.timeout_next_before_exchange = True

    ledger = OrderLedger(db_path)
    guard = OrderExecutionGuard(ledger=ledger, risk_guard=HardRiskGuard(HardRiskLimits()), exchange=ex)
    out = guard.submit_intent(_intent("intent-to-001"), reference_price=Decimal("50000"), now_ns=10)
    assert out.state == OrderState.UNKNOWN_PENDING_RECONCILE

    summary = guard.recover_pending(now_ns=20)
    assert summary.resolved_count == 1
    rec = ledger.get_order("intent-to-001")
    assert rec is not None
    assert rec.state == OrderState.CLOSED_NOT_FOUND
    ledger.close()


def test_kill_switch_partial_fill_cancel_race_network_outage_and_restart_lock(tmp_path: Path) -> None:
    """Kill Switch records 3 timestamps, survives restart, and reconciles late fills on cancel race."""
    db_path = tmp_path / "ks.db"
    ex = FakeSpotExchange(fill_price=Decimal("50000"), fee_bps=Decimal("10"))
    ex.next_partial_fill_ratio = Decimal("0.4")

    ledger = OrderLedger(db_path)
    guard = OrderExecutionGuard(ledger=ledger, risk_guard=HardRiskGuard(HardRiskLimits()), exchange=ex)
    out = guard.submit_intent(
        _intent("intent-part-001", qty="0.05", order_type="LIMIT"),
        reference_price=Decimal("49900"),
        limit_price=Decimal("49900"),
        now_ns=1000,
    )
    assert out.accepted is True
    assert out.state == OrderState.PARTIALLY_FILLED
    assert out.record is not None
    assert out.record.cumulative_filled_qty == Decimal("0.02")

    # Simulate network unreachable when Kill Switch triggers
    ex.network_unreachable = True
    ks1, sum1 = guard.trigger_kill_switch(reason="drawdown_breach", now_ns=2000)
    assert ks1.active is True
    assert ks1.local_block_new_orders_ts_ns == 2000
    assert ks1.cancel_sent_ts_ns == 2001
    assert ks1.exchange_terminal_ts_ns is None
    assert sum1.still_pending_count == 1
    assert sum1.kill_switch_terminal_completed is False
    ledger.close()

    # Restart while Kill Switch active: must reject new orders AND block clear while order non-terminal
    ledger_restarted = OrderLedger(db_path)
    guard_restarted = OrderExecutionGuard(
        ledger=ledger_restarted, risk_guard=HardRiskGuard(HardRiskLimits()), exchange=ex
    )
    blocked = guard_restarted.submit_intent(
        _intent("intent-blocked-002"), reference_price=Decimal("50000"), now_ns=3000
    )
    assert blocked.accepted is False
    assert "KILL_SWITCH_ACTIVE" in (blocked.reason or "")
    with pytest.raises(RuntimeError, match="non-terminal"):
        guard_restarted.clear_kill_switch(operator="ops")

    # Restore network with race where remainder fills right as cancel hits exchange
    ex.network_unreachable = False
    ex.next_cancel_race_fills_rest = True
    ks2, sum2 = guard_restarted.poll_kill_switch_reconcile(now_ns=4000)
    assert ks2.exchange_terminal_ts_ns == 4002
    assert sum2.kill_switch_terminal_completed is True

    final_rec = ledger_restarted.get_order("intent-part-001")
    assert final_rec is not None
    assert final_rec.state == OrderState.FILLED
    assert final_rec.cumulative_filled_qty == Decimal("0.05")
    assert len(ledger_restarted.list_fills("intent-part-001")) == 2

    # Explicit operator unlock succeeds once all tracked orders are terminal
    ks_cleared = guard_restarted.clear_kill_switch(operator="amanda", now_ns=5000)
    assert ks_cleared.active is False
    assert ks_cleared.cleared_by == "amanda"
    ledger_restarted.close()


def test_hard_risk_guard_and_mainnet_safety_gate(tmp_path: Path) -> None:
    ex = FakeSpotExchange(fill_price=Decimal("60000"))
    ledger = OrderLedger(tmp_path / "risk.db")
    limits = HardRiskLimits(
        max_order_qty=Decimal("0.05"),
        max_order_notional_quote=Decimal("2000"),
        max_abs_position_qty=Decimal("0.06"),
    )
    guard = OrderExecutionGuard(ledger=ledger, risk_guard=HardRiskGuard(limits), exchange=ex)

    assert "ORDER_QTY_LIMIT" in (
        guard.submit_intent(_intent("r1", qty="0.06"), reference_price=Decimal("60000")).reason or ""
    )
    assert "ORDER_NOTIONAL_LIMIT" in (
        guard.submit_intent(_intent("r2", qty="0.04"), reference_price=Decimal("60000")).reason or ""
    )
    assert "SPOT_SHORT_BLOCKED" in (
        guard.submit_intent(_intent("r3", side="SELL", qty="0.01"), reference_price=Decimal("60000")).reason or ""
    )
    assert "DAILY_LOSS_LIMIT" in (
        guard.submit_intent(
            _intent("r4", qty="0.01"),
            reference_price=Decimal("60000"),
            daily_realized_loss_quote=Decimal("600"),
        ).reason
        or ""
    )
    assert ex.submit_wire_calls == 0

    with pytest.raises(ValueError, match="allow_mainnet=True"):
        BinanceSpotRestClient(
            SpotCredentials(api_key="k", api_secret="s", base_url="https://api.binance.com", allow_mainnet=False)
        )
    ledger.close()


def test_ipc_live_boundary_and_windowed_streaming_reconcile(tmp_path: Path) -> None:
    """SignalRunner(mode='live') holds zero keys and dispatches intents to isolated BQOrder over UDS."""
    db_path = tmp_path / "ipc.db"
    with TemporaryDirectory(dir="/tmp") as short_dir:
        sock_path = Path(short_dir) / "bq.sock"
        ex = FakeSpotExchange(fill_price=Decimal("50000"))
        ledger = OrderLedger(db_path)
        guard = OrderExecutionGuard(ledger=ledger, risk_guard=HardRiskGuard(HardRiskLimits()), exchange=ex)
        server = OrderIpcServer(sock_path, guard, default_ref_price=Decimal("50000"))
        server.start()
        try:
            client = OrderIpcClient(sock_path, default_ref_price=Decimal("50000"))
            cfg = DeployConfig(order_qty=Decimal("0.01"), max_pos_qty=Decimal("0.05"))
            runner = SignalRunner(cfg, mode="live", intent_sink=client)

            t0 = 1_800_000_000_000_000_000
            events = [
                BookTickerEvent(
                    venue="BINANCE",
                    market=Market.SPOT,
                    symbol="BTCUSDT",
                    update_id=100 + i,
                    best_bid_price=Decimal("50000"),
                    best_bid_quantity=Decimal("8"),
                    best_ask_price=Decimal("50001"),
                    best_ask_quantity=Decimal("1"),
                    receive_time_utc_ns=t0 + i * 1_000_000,
                    delivery_seq=i + 1,
                    source="gateway_grpc",
                )
                for i in range(4)
            ]
            summary = runner.run_events(events)
            assert summary.intents_emitted >= 1
            assert len(client.last_responses) == summary.intents_emitted
            assert all(resp["ok"] for resp in client.last_responses)
            assert ex.submit_wire_calls == summary.intents_emitted
        finally:
            server.stop()
            ledger.close()

    # Verify streaming windowed reconcile on paper journal
    jpath = tmp_path / "win.jsonl"
    cfg_p = DeployConfig(order_qty=Decimal("0.01"), max_pos_qty=Decimal("0.05"))
    t0 = 1_800_000_000_000_000_000
    evs = [
        BookTickerEvent(
            venue="BINANCE",
            market=Market.SPOT,
            symbol="BTCUSDT",
            update_id=200 + i,
            best_bid_price=Decimal("50000"),
            best_bid_quantity=Decimal("8"),
            best_ask_price=Decimal("50001"),
            best_ask_quantity=Decimal("1"),
            receive_time_utc_ns=t0 + i * 1_000_000_000,
            delivery_seq=i + 1,
            source="gateway_grpc",
        )
        for i in range(4)
    ]
    SignalRunner(cfg_p, mode="paper").run_events(evs, journal_path=jpath)
    report = reconcile_paper_journal(
        jpath,
        cfg_p,
        recorder_events=evs[1:3],
        window_start_ns=t0 + 1_000_000_000,
        window_end_ns=t0 + 2_000_000_000,
    )
    assert report.accepted is True
    assert report.stage1_entries_checked == 4
    assert report.stage2_parity is not None
    assert report.stage2_parity.matched_count == 2
