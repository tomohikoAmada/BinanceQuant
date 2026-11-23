"""M9 tests: hand-calculated F1/F2 fills, causality perturbation, purged WF, <1ms inference, MCP 6 tools & cheat guards."""

from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from bq_backtest import (
    BacktestConfig,
    BacktestEngine,
    BtcSpotBboRule,
    ExchangeFilters,
    FillSimulator,
    OrderBookView,
    OrderIntent,
    ThreeStageQuantStrategy,
    run_sensitivity_matrix,
    verify_causality_perturbation,
)
from bq_feature import FeatureEngine, FeatureState, export_features_parquet, extract_feature_rows
from bq_feed import (
    AggTradeEvent,
    BookTickerEvent,
    DepthUpdateEvent,
    LiquidationEvent,
    Market,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    ReferenceObservationEvent,
    Stream,
    StreamHealthEvent,
    event_to_dict,
)
from bq_mcp import MAX_RESPONSE_BYTES, QuantitativeMcpServer, validate_strategy_code
from bq_model import ModelBundle, fit_walk_forward_model, purged_embargoed_splits


def test_hand_calculated_f1_partial_shared_gap_and_f2_funding() -> None:
    flt = ExchangeFilters(tick_size=Decimal("0.01"), step_size=Decimal("0.01"), min_notional=Decimal("10"), is_known=True)
    sim = FillSimulator(mode="F1", taker_fee_bps=Decimal("10"))
    book = OrderBookView(
        symbol="BTCUSDT",
        book_version=1,
        bids=((Decimal("99.90"), Decimal("2.00")),),
        asks=((Decimal("100.00"), Decimal("1.00")), (Decimal("101.00"), Decimal("2.00"))),
        health_valid=True,
    )
    i1 = OrderIntent("i1", Market.SPOT, "BTCUSDT", "BUY", Decimal("0.50"), "MARKET_TAKER", 1, 100, 0, "t1")
    r1 = sim.execute(i1, book, flt)
    # 0.50 * 100.00 = 50.00 USDT; 10 bps fee = 0.05 USDT
    assert r1.status == "FILLED"
    assert r1.filled_qty == Decimal("0.50")
    assert r1.avg_fill_price == Decimal("100.00")
    assert r1.fee_quote == Decimal("0.05")

    # Shared remaining top-level qty on same book_version: only 0.50 remains of 1.00!
    i2 = OrderIntent("i2", Market.SPOT, "BTCUSDT", "BUY", Decimal("0.80"), "MARKET_TAKER", 2, 100, 0, "t2")
    r2 = sim.execute(i2, book, flt)
    assert r2.status == "PARTIAL"
    assert r2.filled_qty == Decimal("0.50")

    # GAP book skips unconditionally
    gap_book = OrderBookView("BTCUSDT", 2, book.bids, book.asks, health_valid=False)
    r_gap = sim.execute(i1, gap_book, flt)
    assert r_gap.status == "SKIPPED_GAP"
    assert r_gap.filled_qty == Decimal(0)

    # D4 unknown filters reject
    r_unk = sim.execute(i1, book, ExchangeFilters(is_known=False))
    assert r_unk.status == "REJECTED_UNKNOWN_FILTERS"

    # F2 sweeps 2 levels: 1.00 @ 100.00 + 0.50 @ 101.00 -> notional 150.50, avg = 150.50 / 1.50
    sim_f2 = FillSimulator(mode="F2", taker_fee_bps=Decimal("10"), max_sweep_levels=5)
    i3 = OrderIntent("i3", Market.USDM, "BTCUSDT", "BUY", Decimal("1.50"), "MARKET_TAKER", 3, 200, 0, "t3")
    r_f2 = sim_f2.execute(i3, book, flt)
    assert r_f2.status == "FILLED"
    assert r_f2.levels_consumed == 2
    assert r_f2.notional_quote == Decimal("150.50")
    assert r_f2.fee_quote == Decimal("0.1505")


def test_backtest_gap_skip_and_usdm_real_funding_settlement(tmp_path: Path) -> None:
    events = [
        BookTickerEvent("BINANCE", Market.USDM, "BTCUSDT", Decimal("100"), Decimal("8"), Decimal("101"), Decimal("1"), update_id=1, receive_time_utc_ns=1_000_000_000, delivery_seq=1),
        MarkPriceEvent("BINANCE", Market.USDM, "BTCUSDT", Decimal("100"), Decimal("100"), Decimal("100"), Decimal("0.001"), 2_000, receive_time_utc_ns=1_500_000_000, delivery_seq=2),
        # Real funding event at fundingTime=2000ms: settles strictly on current position (1 * 100 * 0.001 = 0.10 paid if long)
        ReferenceObservationEvent("BINANCE", Market.USDM, "BTCUSDT", "funding_rate", funding_rate_settled=Decimal("0.001"), funding_time_ms=2000, receive_time_utc_ns=2_000_000_000, delivery_seq=3),
        # Health GAP then recovery
        StreamHealthEvent("BINANCE", Market.USDM, "BTCUSDT", Stream.BOOK_TICKER, "GAP", "lost", 2_500_000_000, delivery_seq=4),
        BookTickerEvent("BINANCE", Market.USDM, "BTCUSDT", Decimal("102"), Decimal("1"), Decimal("103"), Decimal("8"), update_id=2, receive_time_utc_ns=3_000_000_000, delivery_seq=5),
    ]
    strat = BtcSpotBboRule(market=Market.USDM, order_qty=Decimal("1"), max_pos_qty=Decimal("1"), buy_imbalance_threshold=0.2, sell_imbalance_threshold=0.2, max_spread_bps=200.0)
    cfg = BacktestConfig(
        run_id="usdm-funding-check",
        initial_cash_quote=Decimal("1000"),
        taker_fee_bps=Decimal("10"),
        default_filters=ExchangeFilters(Decimal("0.01"), Decimal("0.01"), Decimal("1"), is_known=True),
    )
    eng = BacktestEngine()
    res1 = eng.run(events, strat, cfg, runs_root=tmp_path / "runs")
    res2 = eng.run(events, strat, cfg)
    assert res1.semantic_digest == res2.semantic_digest
    assert Decimal(res1.metrics["total_funding_quote"]) == Decimal("-0.100")
    assert (tmp_path / "runs" / "usdm-funding-check" / "metrics.json").exists()


def _sample_stream(n: int = 30) -> list[Any]:
    out: list[Any] = []
    for i in range(n):
        px = Decimal("30000") + Decimal(i)
        if i % 3 == 0:
            bq, aq = Decimal("5.0"), Decimal("1.0")
        elif i % 3 == 1:
            bq, aq = Decimal("1.0"), Decimal("5.0")
        else:
            bq, aq = Decimal("2.0"), Decimal("2.0")
        out.append(
            BookTickerEvent(
                "BINANCE",
                Market.SPOT,
                "BTCUSDT",
                px,
                bq,
                px + Decimal("1.0"),
                aq,
                update_id=100 + i,
                receive_time_utc_ns=1_700_000_000_000_000_000 + i * 500_000_000,
                delivery_seq=i + 1,
            )
        )
    return out


def test_causality_perturbation_and_sensitivity_and_parquet(tmp_path: Path) -> None:
    stream = _sample_stream(24)
    strat = BtcSpotBboRule(order_qty=Decimal("0.1"), max_pos_qty=Decimal("0.5"), max_spread_bps=50.0)
    cfg = BacktestConfig(
        run_id="causality",
        default_filters=ExchangeFilters(Decimal("0.01"), Decimal("0.01"), Decimal("10"), is_known=True),
    )
    assert verify_causality_perturbation(stream, strat, split_index=10, config=cfg)

    pq_path = tmp_path / "features.parquet"
    n_rows = export_features_parquet(stream, pq_path)
    assert n_rows == 24
    tbl = pq.read_table(pq_path)
    assert tbl.num_rows == 24
    assert "ofi" in tbl.column_names and "j_engine" in tbl.column_names

    mat = run_sensitivity_matrix(stream, strat, rtt_ms_grid=(0.0, 600.0), fee_bps_grid=(Decimal("5"), Decimal("20")), base_config=cfg)
    assert len(mat) == 4


def test_purged_embargoed_walk_forward_and_sub_1ms_inference(tmp_path: Path) -> None:
    stream = _sample_stream(40)
    rows = extract_feature_rows(stream)
    targets = [float(i % 5 - 2) for i in range(len(rows))]
    # Label horizon spans 2 seconds forward (4 events at 0.5s step)
    end_times = [int(r["timestamp_ns"]) + 2_000_000_000 for r in rows]
    timestamps = [int(r["timestamp_ns"]) for r in rows]

    folds = purged_embargoed_splits(timestamps, end_times, n_splits=3, embargo_ns=1_000_000_000, walk_forward_only=False)
    for f in folds:
        v_start = f.val_start_ns
        v_end = f.val_end_ns
        for tr_i in f.train_indices:
            # No training label overlaps [v_start, v_end] nor enters embargo
            assert end_times[tr_i] < v_start or timestamps[tr_i] > v_end + 1_000_000_000

    bundle, report = fit_walk_forward_model(rows, targets, end_times, model_id="m-wf-1", models_root=tmp_path / "models", n_splits=3)
    assert report.folds_evaluated >= 2
    loaded = ModelBundle.load(tmp_path / "models" / "m-wf-1")
    t0 = time.perf_counter()
    for _ in range(100):
        score = loaded.predict_one(rows[0])
    per_call_ms = ((time.perf_counter() - t0) * 1000.0) / 100.0
    assert isinstance(score, float)
    assert per_call_ms < 1.0

    # Three-stage strategy using ModelBundle obeys causality perturbation too
    strat_3 = ThreeStageQuantStrategy(model_bundle=loaded, max_spread_bps=50.0, cost_buffer_bps=0.1)
    assert verify_causality_perturbation(stream, strat_3, split_index=15)


def test_mcp_guard_rejects_3_cheat_strategies_and_6_tools_under_64kib(tmp_path: Path) -> None:
    # Cheat 1: filesystem IO
    r1 = validate_strategy_code(
        source_code="def run():\n    return open('/etc/passwd').read()\n",
        required_streams=[(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER)],
    )
    assert not r1["valid"] and r1["stage"] == "ast_audit"

    # Cheat 2: wall-clock / network access
    r2 = validate_strategy_code(
        source_code="import time\ndef run():\n    return time.time()\n",
        required_streams=[(Market.SPOT, "BTCUSDT", Stream.BOOK_TICKER)],
    )
    assert not r2["valid"] and r2["stage"] == "ast_audit"

    # Cheat 3: live/paper parity check on D2 frozen COIN-M
    r3 = validate_strategy_code(
        source_code="def run(x):\n    return x\n",
        required_streams=[(Market.COINM, "BTCUSD_PERP", Stream.BOOK_TICKER)],
        mode="paper",
    )
    assert not r3["valid"] and r3["stage"] == "capability_parity"

    # Clean strategy + 6 MCP stdio tools + < 64 KiB cap
    srv = QuantitativeMcpServer(tmp_path / "ws")
    events_payload = [event_to_dict(e) for e in _sample_stream(30)]

    tools_reply = json.loads(srv.handle_rpc_line(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})) or "{}")
    assert len(tools_reply["result"]["tools"]) == 6

    for tool_name, tool_args in [
        ("inspect_datasets", {}),
        ("export_features", {"events": events_payload}),
        ("train_model", {"events": events_payload, "model_id": "m1"}),
        ("validate_strategy", {"source_code": "def decide(f):\n    return f.bbo_imbalance\n", "events": events_payload}),
        ("run_backtest", {"events": events_payload, "run_id": "r1", "include_sensitivity": True}),
        ("compare_runs", {"run_ids": ["r1"]}),
    ]:
        resp = srv.handle_rpc_line(
            json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": tool_name, "arguments": tool_args}})
        )
        assert resp is not None
        parsed = json.loads(resp)
        assert "result" in parsed
        text_payload = parsed["result"]["content"][0]["text"]
        assert len(text_payload.encode("utf-8")) < MAX_RESPONSE_BYTES
