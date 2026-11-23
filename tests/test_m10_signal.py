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

from bq_feed import BookTickerEvent, GatewayLiveFeed, Market
from bq_signal import SignalRunner, load_deploy_config, reconcile_paper_journal


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
