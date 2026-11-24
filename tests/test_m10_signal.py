"""M10 tests: deploy.toml safety, paper/live signal runner, STALE/GAP fault injection, 2-stage reconciliation & bounded memory."""

from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import tracemalloc
from typing import Any

import pytest

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import gateway_messages_pb2

from bq_backtest import BacktestConfig, BacktestEngine, BtcSpotBboRule
from bq_feed import BookTickerEvent, GatewayLiveFeed, Market, Stream
from bq_signal import DeployConfig, SignalRunner, load_deploy_config, reconcile_paper_journal
from bq_signal.cli import main as cli_main


def _make_gateway_status() -> gateway_messages_pb2.GatewayStatusSnapshot:
    snap = gateway_messages_pb2.GatewayStatusSnapshot()
    row = snap.markets.add()
    row.venue = enums_pb2.VENUE_BINANCE
    row.market = enums_pb2.MARKET_SPOT
    row.symbol = "BTCUSDT"
    row.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
    row.enabled_streams.orderbook_rpc_enabled = False
    row.enabled_streams.streams.append(enums_pb2.STREAM_BOOK_TICKER)
    return snap


def _bt_env(uid: int, bid: str, bq: str, ask: str, aq: str, recv_ns: int, gen: int = 1) -> gateway_messages_pb2.GatewayEventEnvelope:
    env = gateway_messages_pb2.GatewayEventEnvelope()
    env.delivery_metadata.connection_generation = gen
    env.book_ticker.metadata.venue = enums_pb2.VENUE_BINANCE
    env.book_ticker.metadata.market = enums_pb2.MARKET_SPOT
    env.book_ticker.metadata.symbol = "BTCUSDT"
    env.book_ticker.metadata.receive_time_utc_ns = recv_ns
    env.book_ticker.metadata.receive_monotonic_ns = recv_ns
    env.book_ticker.update_id = uid
    env.book_ticker.best_bid_price = bid
    env.book_ticker.best_bid_quantity = bq
    env.book_ticker.best_ask_price = ask
    env.book_ticker.best_ask_quantity = aq
    return env


class _StubWithRotation:
    def __init__(self, envelopes: list[Any]) -> None:
        self._envelopes = envelopes

    def GetGatewayStatus(self, req: Any, timeout: float = 5.0) -> Any:
        return _make_gateway_status()

    def SubscribeEvents(self, req: Any) -> Any:
        return iter(self._envelopes)


def test_deploy_config_forbids_api_keys(tmp_path: Path) -> None:
    cfg = load_deploy_config(Path("deploy.toml"))
    assert cfg.market == Market.SPOT
    assert cfg.symbol == "BTCUSDT"
    assert len(cfg.config_hash) == 16

    bad_toml = tmp_path / "bad.toml"
    bad_toml.write_text('gateway_target = "127.0.0.1:50051"\napi_secret = "leak"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="API keys"):
        load_deploy_config(bad_toml)


def test_paper_fault_injection_stale_gap_and_two_stage_reconcile(tmp_path: Path) -> None:
    toml_path = tmp_path / "deploy.toml"
    journal_path = tmp_path / "paper_journal.jsonl"
    toml_path.write_text(
        f"""
gateway_target = "127.0.0.1:50051"
market = "SPOT"
symbol = "BTCUSDT"
stream = "book_ticker"
order_qty = "0.01"
max_pos_qty = "0.05"
buy_imbalance_threshold = 0.25
sell_imbalance_threshold = 0.25
max_spread_bps = 50.0
taker_fee_bps = "10"
stale_threshold_ns = 2000000000
tick_size = "0.01"
step_size = "0.00001"
min_notional = "10"
filters_known = true
initial_cash_quote = "10000"
journal_path = "{journal_path}"
""",
        encoding="utf-8",
    )
    cfg = load_deploy_config(toml_path)

    now = [1_000_000_000]
    acc = gateway_messages_pb2.GatewayEventEnvelope()
    acc.subscription_accepted.subscription_id = "sub-paper"

    gap = gateway_messages_pb2.GatewayEventEnvelope()
    gap.consumer_gap.market = enums_pb2.MARKET_SPOT
    gap.consumer_gap.symbol = "BTCUSDT"
    gap.consumer_gap.stream = enums_pb2.STREAM_BOOK_TICKER
    gap.consumer_gap.detected_time_utc_ns = 1_700_000_001_500_000_000
    gap.delivery_metadata.connection_generation = 2

    live_recovered = gateway_messages_pb2.GatewayEventEnvelope()
    live_recovered.stream_status.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
    live_recovered.stream_status.observed_time_utc_ns = 1_700_000_002_000_000_000

    envelopes = [
        acc,
        # 1. Healthy imbalance -> triggers BUY
        _bt_env(10, "30000.00", "5.0", "30001.00", "1.0", 1_700_000_001_002_000_000, gen=1),
        # 2. Gateway rotation/gap -> sets health_ok = False
        gap,
        # 3. During GAP state -> even strong imbalance must be BLOCKED (blocked_by_health_count += 1)
        _bt_env(11, "30000.00", "8.0", "30001.00", "1.0", 1_700_000_001_802_000_000, gen=2),
        # 4. Recovered LIVE + sell imbalance -> executes SELL
        live_recovered,
        _bt_env(12, "30005.00", "1.0", "30006.00", "5.0", 1_700_000_002_503_000_000, gen=2),
    ]

    live_feed = GatewayLiveFeed(stub=_StubWithRotation(envelopes), stale_threshold_ns=cfg.stale_threshold_ns, clock_ns=lambda: now[0])
    runner = SignalRunner(cfg, mode="paper", live_feed=live_feed)
    summary = runner.run_events(
        live_feed.subscribe_event_stream(cfg.market, cfg.symbol, cfg.stream),
        journal_path=journal_path,
    )
    assert summary.intents_emitted == 2
    assert summary.fills_count == 2
    assert summary.blocked_by_health_count == 1
    assert Decimal(summary.final_position_qty) == Decimal("0.00")

    # Independent Recorder window missing u=11 (to test UID cross-reconciliation & latency percentiles!)
    rec_events = [
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("30000.00"), Decimal("5.0"), Decimal("30001.00"), Decimal("1.0"), update_id=10, receive_time_utc_ns=1_700_000_001_000_000_000),
        BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", Decimal("30005.00"), Decimal("1.0"), Decimal("30006.00"), Decimal("5.0"), update_id=12, receive_time_utc_ns=1_700_000_002_500_000_000),
    ]
    rec_report = reconcile_paper_journal(journal_path, cfg, recorder_events=rec_events)
    assert rec_report.stage1_exact_replay_passed
    assert rec_report.stage1_entries_checked == 5
    assert rec_report.stage2_parity is not None
    assert rec_report.stage2_parity.matched_count == 2
    assert rec_report.stage2_parity.recorder_coverage == pytest.approx(1.0)
    assert rec_report.stage2_parity.gateway_coverage == pytest.approx(2.0 / 3.0)
    assert rec_report.stage2_parity.latency_p50_ms == pytest.approx(2.5)
    assert rec_report.gap_attribution["paper_health_gaps"] == 1
    assert rec_report.gap_attribution["paper_only_missing_in_recorder"] == 1


def test_live_mode_emits_intents_and_memory_stays_bounded(tmp_path: Path) -> None:
    cfg = load_deploy_config(Path("deploy.toml"))
    emitted: list[Any] = []
    runner = SignalRunner(cfg, mode="live", intent_sink=emitted.append)

    def _gen_events(count: int) -> Any:
        for i in range(count):
            bq, aq = (Decimal("5.0"), Decimal("1.0")) if i % 2 == 0 else (Decimal("1.0"), Decimal("5.0"))
            yield BookTickerEvent(
                "BINANCE", Market.SPOT, "BTCUSDT",
                Decimal("30000.00"), bq, Decimal("30001.00"), aq,
                update_id=1000 + i,
                receive_time_utc_ns=1_700_000_000_000_000_000 + i * 1_000_000,
                delivery_seq=i + 1,
            )

    tracemalloc.start()
    summary = runner.run_events(_gen_events(5000))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert summary.mode == "live"
    assert summary.processed_events == 5000
    assert len(emitted) == summary.intents_emitted
    assert summary.intents_emitted > 0
    # Streaming runner keeps only bounded rolling state (< 5 MiB even for 5k events!)
    assert peak < 5 * 1024 * 1024


def test_signal_runner_parity_with_backtest_watchdog_append_and_reconcile_guards(tmp_path: Path) -> None:
    t0 = 1_800_000_000_000_000_000
    ms = 1_000_000

    # 1. Fail-closed live mode without intent_sink
    with pytest.raises(ValueError, match="intent_sink"):
        SignalRunner(DeployConfig(), mode="live")

    # 2. Same arrival delay -> SignalRunner paper and BacktestEngine match identically!
    def q(seq: int, dt_ns: int, ask: str, bq: str = "40") -> BookTickerEvent:
        px = Decimal(ask)
        return BookTickerEvent("BINANCE", Market.SPOT, "BTCUSDT", px - Decimal("0.01"), Decimal(bq), px, Decimal("10"), update_id=seq, receive_time_utc_ns=t0 + dt_ns, delivery_seq=seq)

    traj = [q(1, 0, "100", "40"), q(2, ms // 2, "101", "10"), q(3, 2 * ms, "101", "10")]
    bt = BacktestEngine().run(traj, BtcSpotBboRule(order_qty=Decimal("1"), max_pos_qty=Decimal("1")), BacktestConfig(network_rtt_ms=1.0))
    jpath = tmp_path / "sync.jsonl"
    cfg_delay = DeployConfig(order_qty=Decimal("1"), max_pos_qty=Decimal("1"), model_infer_ms=0.1, network_rtt_ms=1.0)
    pr = SignalRunner(cfg_delay, mode="paper").run_events(traj, journal_path=jpath)
    jrows = [json.loads(l) for l in jpath.read_text().splitlines()]
    settled = [r["fill"] for r in jrows if r["fill"]]
    assert settled[0]["avg_fill_price"] == bt.ledger[0].avg_fill_price == "101"
    assert pr.final_cash_quote == bt.metrics["final_cash_quote"]

    # 3. 10s silence watchdog + UTC vs monotonic separation + append-safe restart
    time_ns = [100]
    def _env(uid: int) -> gateway_messages_pb2.GatewayEventEnvelope:
        e = gateway_messages_pb2.GatewayEventEnvelope()
        bt_p = e.book_ticker
        bt_p.metadata.market = enums_pb2.MARKET_SPOT
        bt_p.metadata.venue = enums_pb2.VENUE_BINANCE
        bt_p.metadata.symbol = "BTCUSDT"
        bt_p.metadata.receive_time_utc_ns = t0 + time_ns[0]
        bt_p.update_id = uid
        bt_p.best_bid_price, bt_p.best_bid_quantity, bt_p.best_ask_price, bt_p.best_ask_quantity = "99.99", "40", "100", "10"
        return e

    class _SilentStub:
        def GetGatewayStatus(self, req: Any, timeout: float = 5.0) -> Any:
            return _make_gateway_status()
        def SubscribeEvents(self, req: Any) -> Any:
            yield _env(1)
            time_ns[0] += 10_000 * ms
            yield _env(2)

    feed = GatewayLiveFeed(stub=_SilentStub(), clock_ns=lambda: time_ns[0], stale_threshold_ns=5_000 * ms)
    w_journal = tmp_path / "wd_append.jsonl"
    SignalRunner(DeployConfig(), live_feed=feed).run_events(
        feed.subscribe_event_stream(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER),
        journal_path=w_journal,
    )
    rows1 = [json.loads(l) for l in w_journal.read_text().splitlines()]
    assert any(r["event"].get("status") == "STALE" for r in rows1)
    time_ns[0] += 10_000 * ms
    exp = feed.poll_watchdog()[0]
    assert exp.observed_time_utc_ns == t0 + time_ns[0]

    # Restart appends cleanly instead of overwriting!
    SignalRunner(DeployConfig()).run_events([q(3, 0, "100", "10")], journal_path=w_journal)
    rows2 = [json.loads(l) for l in w_journal.read_text().splitlines()]
    assert len(rows2) == len(rows1) + 1
    assert [r["journal_seq"] for r in rows2] == list(range(1, len(rows2) + 1))

    # 4. Empty journal & disjoint Stage 2 reconciliation fail closed (non-accepted) & CLI exit code 1
    empty_j = tmp_path / "empty.jsonl"
    empty_j.touch()
    emp = reconcile_paper_journal(empty_j, DeployConfig(), recorder_events=[])
    assert not emp.stage1_exact_replay_passed and not emp.accepted

    # 5. Truncated/gap journal sequence rejected in Stage 1
    bad_j = tmp_path / "tampered.jsonl"
    tampered_rows = [json.loads(l) for l in w_journal.read_text().splitlines()]
    tampered_rows[1]["journal_seq"] = 99
    bad_j.write_text("\n".join(json.dumps(r) for r in tampered_rows) + "\n", encoding="utf-8")
    tampered_rep = reconcile_paper_journal(bad_j, DeployConfig())
    assert not tampered_rep.stage1_exact_replay_passed

    # 6. Hand-calculated trajectory: buy 1 @ ask 101 after 1ms jump (cash 120 -> 120 - 101 - 0.101 = 18.899)
    hand_cfg = DeployConfig(order_qty=Decimal("1"), max_pos_qty=Decimal("1"), initial_cash_quote=Decimal("120"), taker_fee_bps=Decimal("10"), model_infer_ms=0.1, network_rtt_ms=1.0)
    hand_j = tmp_path / "hand.jsonl"
    hand_summary = SignalRunner(hand_cfg, mode="paper").run_events(traj, journal_path=hand_j)
    assert Decimal(hand_summary.final_position_qty) == Decimal("1")
    assert Decimal(hand_summary.final_cash_quote) == Decimal("18.8990")
    assert reconcile_paper_journal(hand_j, hand_cfg).stage1_exact_replay_passed
    toml_f = tmp_path / "deploy.toml"
    toml_f.write_text(f'market = "SPOT"\nsymbol = "BTCUSDT"\nstream = "book_ticker"\njournal_path = "{empty_j}"\n', encoding="utf-8")
    assert cli_main(["reconcile", "--config", str(toml_f), "--journal", str(empty_j)]) == 1
