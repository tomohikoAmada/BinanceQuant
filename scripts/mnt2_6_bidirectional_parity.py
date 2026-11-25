"""M-NT2.6 Step 3 & 4 formal verification: 5-symbol Dual-Market bidirectional parity, L2 common-UID top-N,
cross-UTC boundary, truncation negative test, live 2.0.0rc9+bmd1 outer-join vs BMDR, and slow-ref 300s + resource ledger.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from typing import Any
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bq_feed.events import (  # noqa: E402
    AggTradeEvent,
    BookTickerEvent,
    DepthUpdateEvent,
    LiquidationEvent,
    Market,
    MarkPriceEvent,
    OrderBookSnapshotEvent,
    Stream,
)
from bq_nautilus.recorder import (  # noqa: E402
    L2SequenceBridge,
    VerifiedChunkRef,
    bmd_liquidation_to_custom_data,
    bmd_mark_price_to_nautilus,
    load_mcp_export_chunks,
    read_recorder_events,
)
from bq_nautilus.instruments import spec_from_exchange_info_entry  # noqa: E402

FIVE_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
DEFAULT_EXPORT_ROOT = Path("/srv/recorder-data/S-BMDR/mcp-export")
DEFAULT_SPOT_ROOT = Path("/srv/recorder-data/S-BMDR/20261113T092542Z-spot")
DEFAULT_USDM_ROOT = Path("/srv/recorder-data/S-BMDR/20261113T092542Z-usdm")


def _norm_dec(v: Any) -> str:
    d = v if isinstance(v, Decimal) else Decimal(str(v))
    s = format(d.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _scan_direct_archive_chunks(
    partition_root: Path,
    *,
    market: Market,
    symbols: set[str],
    streams: set[str],
    min_utc_ns: int,
    max_utc_ns: int,
) -> list[VerifiedChunkRef]:
    """Independent raw reader scanning manifests without using mcp-export."""
    manifests_dir = partition_root / "manifests"
    refs: list[VerifiedChunkRef] = []
    # Quick mtime pre-filter with generous 3-hour buffer around max_utc_ns
    min_mtime = (min_utc_ns / 1_000_000_000.0) - 3600.0
    max_mtime = (max_utc_ns / 1_000_000_000.0) + 10800.0
    candidates: list[Path] = []
    with os.scandir(manifests_dir) as it:
        for ent in it:
            if not ent.name.endswith(".archive-manifest.json"):
                continue
            st = ent.stat()
            if st.st_mtime < min_mtime or st.st_mtime > max_mtime:
                continue
            candidates.append(Path(ent.path))
    candidates.sort()
    for mpath in candidates:
        outer = json.loads(mpath.read_text(encoding="utf-8"))
        doc = outer.get("raw_manifest") or outer
        sym = str(doc.get("symbol", ""))
        st_name = str(doc.get("stream", ""))
        if sym not in {"__GLOBAL__", "__MIXED__"} and sym not in symbols:
            continue
        if st_name not in streams:
            continue
        tr = doc.get("receive_time_utc_range_ns") or {}
        c_min = int(tr["min"]) if tr.get("min") is not None else None
        c_max = int(tr["max"]) if tr.get("max") is not None else None
        if c_max is not None and c_max < min_utc_ns:
            continue
        if c_min is not None and c_min >= max_utc_ns:
            continue
        cand1 = partition_root / str(outer.get("artifact_relative_path", ""))
        cand2 = partition_root / str(doc.get("relative_path", ""))
        artifact = cand1 if cand1.is_file() else cand2
        if not artifact.is_file():
            continue
        refs.append(
            VerifiedChunkRef(
                chunk_id=str(doc["chunk_id"]),
                artifact_path=artifact,
                market=str(doc["market"]),
                symbol=sym,
                stream=st_name,
                stored_bytes=int(doc["stored_bytes"]),
                stored_sha256=str(doc["stored_sha256"]),
                uncompressed_sha256=str(doc["uncompressed_sha256"]),
                created_at_utc_ns=int(doc["created_at_utc_ns"]),
                receive_min_ns=c_min,
                receive_max_ns=c_max,
                complete=bool(doc.get("complete", False)),
                gap=bool(doc.get("gap", False)),
            )
        )
    return refs


def _bidirectional_compare_maps(
    left: dict[Any, tuple[Any, ...]],
    right: dict[Any, tuple[Any, ...]],
    *,
    left_dups: int,
    right_dups: int,
    left_flags: int,
    right_flags: int,
) -> dict[str, Any]:
    l_keys = set(left.keys())
    r_keys = set(right.keys())
    common = l_keys & r_keys
    only_l = l_keys - r_keys
    only_r = r_keys - l_keys
    exact = sum(1 for k in common if left[k] == right[k])
    mismatch = len(common) - exact
    return {
        "left_unique": len(l_keys),
        "right_unique": len(r_keys),
        "common_ids": len(common),
        "exact_value_matches": exact,
        "value_mismatches": mismatch,
        "only_left": len(only_l),
        "only_right": len(only_r),
        "left_duplicates_dropped": left_dups,
        "right_duplicates_dropped": right_dups,
        "left_quality_flagged": left_flags,
        "right_quality_flagged": right_flags,
        "status": (
            "NO_OBSERVATION"
            if (len(l_keys) == 0 and len(r_keys) == 0)
            else ("PASS" if (len(only_l) == 0 and len(only_r) == 0 and mismatch == 0) else "MISMATCH")
        ),
    }


def _step_l2_bridge_state(
    state: list[Any],  # [last_update_id: int | None, bridged: bool, synced: bool]
    market: Market,
    ev: DepthUpdateEvent,
) -> tuple[bool, bool]:
    """Return (apply_update, gap_detected) matching L2SequenceBridge state machine."""
    if "sequence_gap" in getattr(ev, "quality_flags", ()):
        state[0], state[1], state[2] = None, False, False
        return False, True
    prev = state[0]
    if prev is None:
        return False, False
    u0, u1, pu = ev.first_update_id, ev.final_update_id, ev.previous_final_update_id
    if not state[1]:
        if market == Market.USDM:
            if u1 < prev:
                return False, False
            if not ((u0 <= prev <= u1) or (pu == prev and u0 <= prev + 1 <= u1)):
                state[0], state[1], state[2] = None, False, False
                return False, True
        else:
            if u1 <= prev:
                return False, False
            if u0 > prev + 1:
                state[0], state[1], state[2] = None, False, False
                return False, True
        state[1] = True
        state[2] = True
    else:
        if u1 <= prev:
            return False, False
        if market == Market.USDM:
            if (pu is not None and pu != prev) or (pu is None and u0 > prev + 1):
                state[0], state[1], state[2] = None, False, False
                return False, True
        elif u0 > prev + 1:
            state[0], state[1], state[2] = None, False, False
            return False, True
    state[0] = u1
    return True, False


def _digest_events_for_symbol(
    refs: list[VerifiedChunkRef],
    *,
    market: Market,
    symbol: str,
    t_start_ns: int,
    t_end_ns: int,
    top_n: int = 5,
    max_book_checkpoints: int = 24,
) -> dict[str, Any]:
    """Read verified chunks and build compact keyed digests + L2 bridge top-N checkpoints."""
    agg: dict[int, tuple[str, str, int, bool]] = {}
    agg_dups = 0
    agg_flags = 0

    bbo: dict[tuple[int, bytes], tuple[int | None, int | None]] = {}
    bbo_dups = 0
    bbo_flags = 0

    diff: dict[tuple[int, int, int | None], tuple[int, int]] = {}
    diff_dups = 0
    diff_flags = 0

    mark: dict[int, tuple[str, str, str, int | None]] = {}
    mark_dups = 0
    mark_flags = 0

    liq: dict[tuple[int, str, str, str], tuple[str]] = {}
    liq_dups = 0

    bridge_state: list[Any] = [None, False, False]
    bids: dict[Decimal, Decimal] = {}
    asks: dict[Decimal, Decimal] = {}
    l2_gaps = 0
    l2_checkpoints: dict[int, tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]] = {}
    checkpoint_step_ns = max(1, (t_end_ns - t_start_ns) // max(1, max_book_checkpoints))
    next_ckpt_ns = t_start_ns

    # Read per-stream group to bound peak memory cleanly on 5.8 GiB VPS (diff_depth pairs with orderbook_snapshot)
    groups: dict[str, list[VerifiedChunkRef]] = {}
    for ref in refs:
        st_key = "l2" if ref.stream in {"diff_depth", "orderbook_snapshot"} else ref.stream
        groups.setdefault(st_key, []).append(ref)

    for st_key, st_refs in groups.items():
        # Sort chunks chronologically and read one chunk at a time to bound peak RSS <350 MiB
        st_refs_sorted = sorted(
            st_refs,
            key=lambda r: (r.receive_min_ns or 0, 0 if r.stream == "orderbook_snapshot" else 1, r.created_at_utc_ns, r.chunk_id),
        )
        print(f"  [digest] {market.value} {symbol} {st_key}: {len(st_refs_sorted)} chunks", flush=True)
        for ref in st_refs_sorted:
            evs = read_recorder_events(
                [ref],
                market_filter={market},
                symbol_filter={symbol},
                verify_hashes=True,
            )
            for ev in evs:
                ts = int(ev.receive_time_utc_ns or 0)
                if ts < t_start_ns or ts >= t_end_ns:
                    continue
                flagged = bool(getattr(ev, "quality_flags", ()))
                if isinstance(ev, AggTradeEvent):
                    if ev.aggregate_trade_id in agg:
                        agg_dups += 1
                    if flagged:
                        agg_flags += 1
                    agg[ev.aggregate_trade_id] = (
                        _norm_dec(ev.price),
                        _norm_dec(ev.quantity),
                        int(ev.trade_time_ms),
                        bool(ev.buyer_is_maker),
                    )
                elif isinstance(ev, BookTickerEvent):
                    bbo_sig = hashlib.blake2b(
                        f"{_norm_dec(ev.best_bid_price)}|{_norm_dec(ev.best_bid_quantity)}|{_norm_dec(ev.best_ask_price)}|{_norm_dec(ev.best_ask_quantity)}".encode("ascii"),
                        digest_size=16,
                    ).digest()
                    key = (int(ev.update_id), bbo_sig)
                    if key in bbo:
                        bbo_dups += 1
                    if flagged:
                        bbo_flags += 1
                    bbo[key] = (ev.exchange_transaction_time_ms, ev.exchange_event_time_ms)
                elif isinstance(ev, OrderBookSnapshotEvent):
                    bids = {Decimal(p): Decimal(q) for p, q in ev.bids if Decimal(q) > 0}
                    asks = {Decimal(p): Decimal(q) for p, q in ev.asks if Decimal(q) > 0}
                    bridge_state[0], bridge_state[1], bridge_state[2] = ev.last_update_id, False, False
                elif isinstance(ev, DepthUpdateEvent):
                    key_d = (ev.first_update_id, ev.final_update_id, ev.previous_final_update_id)
                    if key_d in diff:
                        diff_dups += 1
                    if flagged:
                        diff_flags += 1
                    diff[key_d] = (len(ev.bids), len(ev.asks))
                    apply_u, gap_u = _step_l2_bridge_state(bridge_state, market, ev)
                    if gap_u:
                        l2_gaps += 1
                        bids.clear()
                        asks.clear()
                    elif apply_u:
                        for p, q in ev.bids:
                            dp, dq = Decimal(p), Decimal(q)
                            if dq == 0:
                                bids.pop(dp, None)
                            else:
                                bids[dp] = dq
                        for p, q in ev.asks:
                            dp, dq = Decimal(p), Decimal(q)
                            if dq == 0:
                                asks.pop(dp, None)
                            else:
                                asks[dp] = dq
                        if bridge_state[2] and ts >= next_ckpt_ns and bids and asks and len(l2_checkpoints) < max_book_checkpoints:
                            top_b = tuple((_norm_dec(p), _norm_dec(bids[p])) for p in sorted(bids.keys(), reverse=True)[:top_n])
                            top_a = tuple((_norm_dec(p), _norm_dec(asks[p])) for p in sorted(asks.keys())[:top_n])
                            l2_checkpoints[ev.final_update_id] = (top_b, top_a)
                            next_ckpt_ns = ts + checkpoint_step_ns
                elif isinstance(ev, MarkPriceEvent):
                    key_m = int(ev.exchange_event_time_ms or ts // 1_000_000)
                    if key_m in mark:
                        mark_dups += 1
                    if flagged:
                        mark_flags += 1
                    mark[key_m] = (
                        _norm_dec(ev.mark_price),
                        _norm_dec(ev.index_price),
                        _norm_dec(ev.funding_rate),
                        ev.next_funding_time_ms,
                    )
                elif isinstance(ev, LiquidationEvent):
                    key_l = (
                        int(ev.order_trade_time_ms),
                        str(ev.side),
                        _norm_dec(ev.price),
                        _norm_dec(ev.original_quantity),
                    )
                    if key_l in liq:
                        liq_dups += 1
                    liq[key_l] = (str(ev.order_status),)
            del evs
        gc.collect()

    return {
        "agg_trade": (agg, agg_dups, agg_flags),
        "book_ticker": (bbo, bbo_dups, bbo_flags),
        "diff_depth": (diff, diff_dups, diff_flags),
        "l2_top5": (l2_checkpoints, l2_gaps, 0),
        "mark_price": (mark, mark_dups, mark_flags),
        "liquidation": (liq, liq_dups, 0),
    }


def _compare_window(
    *,
    export_root: Path,
    spot_root: Path,
    usdm_root: Path,
    t_start_ns: int,
    t_end_ns: int,
    symbols: tuple[str, ...] = FIVE_SYMBOLS,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "window_utc_ns": [t_start_ns, t_end_ns],
        "window_utc_iso": [
            datetime.fromtimestamp(t_start_ns / 1e9, tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(t_end_ns / 1e9, tz=timezone.utc).isoformat(),
        ],
        "markets": {},
    }
    for mkt, root in ((Market.SPOT, spot_root), (Market.USDM, usdm_root)):
        streams = (
            {"agg_trade", "book_ticker", "diff_depth", "orderbook_snapshot"}
            if mkt == Market.SPOT
            else {"agg_trade", "book_ticker", "diff_depth", "orderbook_snapshot", "mark_price", "force_order"}
        )
        m_res: dict[str, Any] = {}
        for sym in symbols:
            left_refs = load_mcp_export_chunks(
                export_root,
                market_filter={mkt},
                symbol_filter={sym},
                stream_filter=streams,
                min_utc_ns=t_start_ns - 60_000_000_000,
                max_utc_ns=t_end_ns + 60_000_000_000,
            )
            right_refs = _scan_direct_archive_chunks(
                root,
                market=mkt,
                symbols={sym},
                streams=streams,
                min_utc_ns=t_start_ns - 60_000_000_000,
                max_utc_ns=t_end_ns + 60_000_000_000,
            )
            l_dig = _digest_events_for_symbol(left_refs, market=mkt, symbol=sym, t_start_ns=t_start_ns, t_end_ns=t_end_ns)
            r_dig = _digest_events_for_symbol(right_refs, market=mkt, symbol=sym, t_start_ns=t_start_ns, t_end_ns=t_end_ns)

            sym_res: dict[str, Any] = {
                "left_chunks": len(left_refs),
                "right_chunks": len(right_refs),
                "streams": {},
            }
            check_streams = (
                ("agg_trade", "book_ticker", "diff_depth", "l2_top5")
                if mkt == Market.SPOT
                else ("agg_trade", "book_ticker", "diff_depth", "l2_top5", "mark_price", "liquidation")
            )
            for sname in check_streams:
                lm, ldup, lflg = l_dig[sname]
                rm, rdup, rflg = r_dig[sname]
                sym_res["streams"][sname] = _bidirectional_compare_maps(
                    lm,
                    rm,
                    left_dups=ldup,
                    right_dups=rdup,
                    left_flags=lflg,
                    right_flags=rflg,
                )
            del l_dig, r_dig
            gc.collect()
            print(f"[closed-compare] {mkt.value} {sym}: chunks=({len(left_refs)},{len(right_refs)}) done", flush=True)
            m_res[sym] = sym_res
        out["markets"][mkt.value] = m_res
    return out


def run_closed_1h_parity(
    *,
    export_root: Path,
    spot_root: Path,
    usdm_root: Path,
    hour_iso: str | None,
) -> dict[str, Any]:
    latest = json.loads((export_root / "latest.json").read_text(encoding="utf-8"))
    if hour_iso:
        t_end_dt = datetime.fromisoformat(hour_iso.replace("Z", "+00:00"))
    else:
        gen_dt = datetime.fromisoformat(str(latest["generated_at_utc"]).replace("Z", "+00:00"))
        t_end_dt = gen_dt.replace(minute=0, second=0, microsecond=0)
        if (gen_dt - t_end_dt).total_seconds() < 300:
            t_end_dt = datetime.fromtimestamp(t_end_dt.timestamp() - 3600, tz=timezone.utc)
    t_end_ns = int(t_end_dt.timestamp()) * 1_000_000_000
    t_start_ns = t_end_ns - 3600 * 1_000_000_000

    res_1h = _compare_window(
        export_root=export_root,
        spot_root=spot_root,
        usdm_root=usdm_root,
        t_start_ns=t_start_ns,
        t_end_ns=t_end_ns,
        symbols=FIVE_SYMBOLS,
    )

    # Cross-UTC midnight fixture: latest UTC midnight 23:50Z -> 00:10Z (20 min across date partition rollover)
    midnight_dt = t_end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    mid_ns = int(midnight_dt.timestamp()) * 1_000_000_000
    cross_utc = _compare_window(
        export_root=export_root,
        spot_root=spot_root,
        usdm_root=usdm_root,
        t_start_ns=mid_ns - 600 * 1_000_000_000,
        t_end_ns=mid_ns + 600 * 1_000_000_000,
        symbols=("BTCUSDT", "ETHUSDT"),
    )

    # Historical non-empty liquidation fixture from archived USD-M force_order chunks
    liq_refs = load_mcp_export_chunks(
        export_root,
        market_filter={Market.USDM},
        symbol_filter={"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"},
        stream_filter={"force_order"},
    )
    liq_sample: dict[str, Any] = {"force_order_chunks_available": len(liq_refs)}
    if liq_refs:
        sample_ref = liq_refs[-1]
        direct_ref = _scan_direct_archive_chunks(
            usdm_root,
            market=Market.USDM,
            symbols=set(FIVE_SYMBOLS),
            streams={"force_order"},
            min_utc_ns=(sample_ref.receive_min_ns or 0) - 1_000_000_000,
            max_utc_ns=(sample_ref.receive_max_ns or 0) + 1_000_000_000,
        )
        evs_a = [e for e in read_recorder_events([sample_ref], verify_hashes=True) if isinstance(e, LiquidationEvent)]
        evs_b = [e for e in read_recorder_events([r for r in direct_ref if r.chunk_id == sample_ref.chunk_id], verify_hashes=True) if isinstance(e, LiquidationEvent)]
        conv_sample = bmd_liquidation_to_custom_data(evs_a[0]) if evs_a else None
        liq_sample.update(
            {
                "sample_chunk_id": sample_ref.chunk_id,
                "mcp_events": len(evs_a),
                "direct_raw_events": len(evs_b),
                "exact_count_match": len(evs_a) == len(evs_b) and len(evs_a) > 0,
                "converted_custom_data_type": type(conv_sample.data).__name__ if conv_sample else None,
            }
        )

    # Intentional truncation negative failure test (BTCUSDT USDM 10-min baseline vs truncated middle 8-min)
    neg_start = t_start_ns
    neg_end = t_start_ns + 600 * 1_000_000_000
    l_neg = load_mcp_export_chunks(
        export_root,
        market_filter={Market.USDM},
        symbol_filter={"BTCUSDT"},
        stream_filter={"agg_trade"},
        min_utc_ns=neg_start - 10_000_000_000,
        max_utc_ns=neg_end + 10_000_000_000,
    )
    r_neg = _scan_direct_archive_chunks(
        usdm_root,
        market=Market.USDM,
        symbols={"BTCUSDT"},
        streams={"agg_trade"},
        min_utc_ns=neg_start - 10_000_000_000,
        max_utc_ns=neg_end + 10_000_000_000,
    )
    full_left = _digest_events_for_symbol(l_neg, market=Market.USDM, symbol="BTCUSDT", t_start_ns=neg_start, t_end_ns=neg_end)["agg_trade"]
    trunc_right = _digest_events_for_symbol(
        r_neg,
        market=Market.USDM,
        symbol="BTCUSDT",
        t_start_ns=neg_start + 30_000_000_000,
        t_end_ns=neg_end - 30_000_000_000,
    )["agg_trade"]
    neg_cmp = _bidirectional_compare_maps(
        full_left[0],
        trunc_right[0],
        left_dups=full_left[1],
        right_dups=trunc_right[1],
        left_flags=full_left[2],
        right_flags=trunc_right[2],
    )
    assert neg_cmp["status"] == "MISMATCH" and neg_cmp["only_left"] > 0

    return {
        "latest_revision_id": latest["revision_id"],
        "closed_1h_5sym_dual_market": res_1h,
        "cross_utc_midnight_fixture": cross_utc,
        "historical_liquidation_fixture": liq_sample,
        "negative_truncation_test": neg_cmp,
    }


def _read_rss_mib(pid: int | None = None) -> float:
    path = Path(f"/proc/{pid}/status") if pid is not None else Path("/proc/self/status")
    if path.is_file():
        for ln in path.read_text(encoding="utf-8").splitlines():
            if ln.startswith("VmRSS:"):
                return int(ln.split()[1]) / 1024.0
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _read_proc_cpu_ticks(pid: int) -> int | None:
    p = Path(f"/proc/{pid}/stat")
    if not p.is_file():
        return None
    parts = p.read_text(encoding="utf-8").rsplit(")", 1)[-1].split()
    return int(parts[11]) + int(parts[12])


def _read_host_cpu() -> tuple[int, ...] | None:
    p = Path("/proc/stat")
    if not p.is_file():
        return None
    return tuple(int(x) for x in p.read_text(encoding="utf-8").splitlines()[0].split()[1:9])


def _systemd_main_pid(unit: str) -> int | None:
    try:
        out = subprocess.check_output(["systemctl", "show", unit, "-p", "MainPID", "--value"], text=True).strip()
        pid = int(out) if out.isdigit() else 0
        return pid if pid > 0 else None
    except Exception:
        return None


async def _run_live_capture_bmd1(
    *,
    duration_secs: float,
    spot_root: Path,
    usdm_root: Path,
    export_root: Path,
    trimmed: bool = False,
) -> dict[str, Any]:
    from nautilus_trader.adapters.binance import (
        BinanceDataClientConfig,
        BinanceDataClientFactory,
        BinanceEnvironment,
        BinanceInstrumentProviderConfig,
        BinanceProductType,
        BinanceSpotMarketDataMode,
    )
    from nautilus_trader.adapters.sandbox import SandboxExecutionClientConfig, SandboxExecutionClientFactory
    from nautilus_trader.common import Environment, LogLevel
    from nautilus_trader.config import LoggerConfig, StrategyConfig
    from nautilus_trader.execution import MakerTakerFeeModel
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import (
        AccountId,
        AccountType,
        BookType,
        ClientId,
        FundingRateUpdate,
        InstrumentId,
        MarkPriceUpdate,
        Money,
        OmsType,
        OrderBookDeltas,
        QuoteTick,
        TradeTick,
        TraderId,
        Venue,
    )
    from nautilus_trader.trading import Strategy

    symbols = ("BTCUSDT",) if trimmed else FIVE_SYMBOLS
    spot_ids = [InstrumentId.from_str(f"{s}.BINANCE") for s in symbols]
    usdm_ids = ([] if trimmed else [InstrumentId.from_str(f"{s}-PERP.BINANCE") for s in symbols])

    class _CapStratConfig(StrategyConfig):
        def __init__(self, *, tel: dict[str, Any], **kw: Any) -> None:
            super().__init__(**kw)
            self.tel = tel

    class _CapStrat(Strategy):
        def __init__(self, cfg: _CapStratConfig) -> None:
            super().__init__(cfg)
            self.tel = cfg.tel
            self.tel.setdefault("counts", {"trades": 0, "quotes": 0, "deltas": 0, "marks": 0, "funding": 0})
            self.tel.setdefault("vis_lat_ms", [])
            self.tel.setdefault("trades", {})  # (mkt, sym, id) -> (px, qty, ts_ms)
            self.tel.setdefault("quotes", {})  # (mkt, sym, bid, bid_sz, ask, ask_sz) -> count
            self.tel.setdefault("marks", {})   # (sym, ts_ms) -> mark_px
            self.tel.setdefault("funding_intervals", set())
            self.tel.setdefault("l2_snaps", {})  # (mkt, sym, u) -> (top5_bids, top5_asks)
            self.tel["_t0"] = time.monotonic()

        def on_start(self) -> None:
            sc = ClientId("BINANCE-SPOT")
            for iid in spot_ids:
                self.subscribe_trades(iid, client_id=sc)
                self.subscribe_quotes(iid, client_id=sc)
                self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=sc)
            uc = ClientId("BINANCE-USDM")
            for iid in usdm_ids:
                self.subscribe_trades(iid, client_id=uc)
                self.subscribe_quotes(iid, client_id=uc)
                self.subscribe_book_deltas(iid, BookType.L2_MBP, managed=True, client_id=uc)
                self.subscribe_mark_prices(iid, client_id=uc)
                self.subscribe_funding_rates(iid, client_id=uc)

        def _rec(self, obj: Any) -> bool:
            now_ns = time.time_ns()
            if len(self.tel["vis_lat_ms"]) < 200_000:
                self.tel["vis_lat_ms"].append(max(0.0, (now_ns - int(obj.ts_init)) / 1_000_000.0))
            el = time.monotonic() - self.tel["_t0"]
            return 15.0 <= el <= max(20.0, duration_secs - 5.0)

        def on_trade(self, t: TradeTick) -> None:
            self.tel["counts"]["trades"] += 1
            if not self._rec(t):
                return
            sv = t.instrument_id.symbol.value
            mkt, sym = ("USDM", sv[:-5]) if sv.endswith("-PERP") else ("SPOT", sv)
            if "t_cap_start" not in self.tel:
                self.tel["t_cap_start"] = time.time_ns()
            self.tel["t_cap_end"] = int(t.ts_init)
            self.tel["trades"][(mkt, sym, int(str(t.trade_id)))] = (_norm_dec(t.price), _norm_dec(t.size))

        def on_quote(self, q: QuoteTick) -> None:
            self.tel["counts"]["quotes"] += 1
            if not self._rec(q):
                return
            sv = q.instrument_id.symbol.value
            mkt, sym = ("USDM", sv[:-5]) if sv.endswith("-PERP") else ("SPOT", sv)
            k = (mkt, sym, _norm_dec(q.bid_price), _norm_dec(q.bid_size), _norm_dec(q.ask_price), _norm_dec(q.ask_size))
            self.tel["quotes"][k] = int(q.ts_event)

        def on_book_deltas(self, ds: OrderBookDeltas) -> None:
            self.tel["counts"]["deltas"] += 1
            if not self._rec(ds) or ds.sequence <= 0:
                return
            sv = ds.instrument_id.symbol.value
            mkt, sym = ("USDM", sv[:-5]) if sv.endswith("-PERP") else ("SPOT", sv)
            # Sample L2 snapshots cleanly up to ~500 per stream to keep overhead near zero
            if sum(1 for k in self.tel["l2_snaps"] if k[0] == mkt and k[1] == sym) >= 200:
                return
            book = self.cache.order_book(ds.instrument_id)
            if book is None:
                return
            top_b = tuple((_norm_dec(str(o.price)), _norm_dec(str(o.size()))) for o in book.bids()[:5])
            top_a = tuple((_norm_dec(str(o.price)), _norm_dec(str(o.size()))) for o in book.asks()[:5])
            if top_b and top_a:
                self.tel["l2_snaps"][(mkt, sym, int(ds.sequence))] = (top_b, top_a)

        def on_mark_price(self, mp: MarkPriceUpdate) -> None:
            self.tel["counts"]["marks"] += 1
            if not self._rec(mp):
                return
            sym = mp.instrument_id.symbol.value[:-5]
            self.tel["marks"][(sym, int(mp.ts_event) // 1_000_000)] = _norm_dec(mp.value)

        def on_funding_rate(self, fr: FundingRateUpdate) -> None:
            self.tel["counts"]["funding"] += 1
            self.tel["funding_intervals"].add(fr.interval)

    builder = (
        LiveNode.builder(f"MNT26-CAP-{'TRIM' if trimmed else 'FULL'}", TraderId("MNT26-CAP"), Environment.SANDBOX)
        .with_logging(LoggerConfig(stdout_level=LogLevel.WARNING))
        .with_timeout_connection(15)
        .with_timeout_disconnection_secs(3)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .with_reconciliation(reconciliation=False)
        .add_data_client(
            "BINANCE-SPOT",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.SPOT,
                environment=BinanceEnvironment.LIVE,
                spot_market_data_mode=BinanceSpotMarketDataMode.Json,
                us=False,
                use_spot_agg_trade=True,
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(x) for x in spot_ids]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
    )
    if usdm_ids:
        builder = builder.add_data_client(
            "BINANCE-USDM",
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                futures_diff_depth_interval_ms=100,
                instrument_provider=BinanceInstrumentProviderConfig(load_all=False, load_ids=[str(x) for x in usdm_ids]),
                instrument_refresh_interval_secs=0,
                instrument_status_poll_secs=0,
            ),
        )
    builder = builder.add_simulated_exec_client(
        "BINANCE",
        SandboxExecutionClientFactory(),
        SandboxExecutionClientConfig(
            venue=Venue("BINANCE"),
            account_id=AccountId("BINANCE-CAP-SIM"),
            account_type=AccountType.MARGIN,
            oms_type=OmsType.NETTING,
            book_type=BookType.L2_MBP,
            starting_balances=[Money.from_str("10000.00 USDT")],
            fee_model=MakerTakerFeeModel(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005")),
        ),
    )
    node = builder.build()
    tel: dict[str, Any] = {}
    strat = _CapStrat(_CapStratConfig(tel=tel))
    node.add_strategy(strat)

    async def _stop() -> None:
        await asyncio.sleep(duration_secs)
        strat.shutdown_system("live capture complete")

    # Fetch independent baseline REST depth snapshots at capture start so BMDR diff_depth can reconstruct L2 books
    baseline_snaps: dict[tuple[str, str], dict[str, Any]] = {}
    if not trimmed:
        for mkt_s, url in (
            ("SPOT", "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"),
            ("USDM", "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"),
        ):
            try:
                baseline_snaps[(mkt_s, "BTCUSDT")] = json.loads(urlopen(url, timeout=8.0).read().decode("utf-8"))
            except Exception:
                pass

    clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    pids = {u: _systemd_main_pid(u) for u in ("bmdg", "sbmdr-recorder@spot", "sbmdr-recorder@usdm")}
    t0_ticks = {u: (_read_proc_cpu_ticks(p) if p else None) for u, p in pids.items()}
    host0 = _read_host_cpu()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    t_wall0 = time.perf_counter()
    st = asyncio.create_task(_stop())
    try:
        await asyncio.wait_for(node.run_async(), timeout=duration_secs + 30.0)
    finally:
        st.cancel()
        wall = max(1e-6, time.perf_counter() - t_wall0)
        ru1 = resource.getrusage(resource.RUSAGE_SELF)
        host1 = _read_host_cpu()
        t1_ticks = {u: (_read_proc_cpu_ticks(p) if p else None) for u, p in pids.items()}
        rss_client = _read_rss_mib()
        rss_svcs = {u: (_read_rss_mib(p) if p else 0.0) for u, p in pids.items()}
        node.dispose()

    def _cpu_pct(a: int | None, b: int | None) -> float:
        return round((((b - a) / clk_tck) / wall) * 100.0, 2) if (a is not None and b is not None) else 0.0

    steal_pct = 0.0
    if host0 and host1:
        diffs = [b - a for a, b in zip(host0, host1)]
        steal_pct = round(diffs[7] * 100.0 / max(1, sum(diffs)), 2)

    lat_s = sorted(tel["vis_lat_ms"])
    total_ev = sum(tel["counts"].values())
    telemetry = {
        "mode": "trimmed_btc_spot" if trimmed else "full_5sym_dual_market",
        "wall_secs": round(wall, 3),
        "counts": tel["counts"],
        "events_per_min": round(total_ev * 60.0 / wall, 1),
        "client_cpu_pct": round((((ru1.ru_utime - ru0.ru_utime) + (ru1.ru_stime - ru0.ru_stime)) / wall) * 100.0, 2),
        "client_rss_mib": round(rss_client, 2),
        "systemd_services_cpu_pct": {u: _cpu_pct(t0_ticks[u], t1_ticks[u]) for u in pids},
        "systemd_services_rss_mib": {u: round(rss_svcs[u], 2) for u in pids},
        "host_steal_pct": steal_pct,
        "visibility_latency_ms": {
            "p95": round(lat_s[int(0.95 * (len(lat_s) - 1))], 3) if lat_s else 0.0,
            "p99": round(lat_s[int(0.99 * (len(lat_s) - 1))], 3) if lat_s else 0.0,
        },
        "funding_intervals_observed": [None if x is None else int(x) for x in tel["funding_intervals"]],
    }

    if trimmed:
        return {"telemetry": telemetry}

    # Wait up to 85s for BMDR to seal chunks covering [t_cap_start, t_cap_end]
    t_cap_start = int(tel.get("t_cap_start", time.time_ns() - 60_000_000_000))
    t_cap_end = int(tel.get("t_cap_end", time.time_ns()))
    # Inner 30s safe sub-window cleanly sealed inside BMDR chunks
    inner_a = t_cap_start + 5_000_000_000
    inner_b = max(inner_a + 10_000_000_000, t_cap_end - 5_000_000_000)
    await asyncio.sleep(75.0)

    # Compare Live captured maps against BMDR sealed archive inside [inner_a, inner_b]
    bmdr_spot = _scan_direct_archive_chunks(
        spot_root, market=Market.SPOT, symbols=set(FIVE_SYMBOLS), streams={"agg_trade", "book_ticker", "diff_depth", "orderbook_snapshot"},
        min_utc_ns=inner_a - 60_000_000_000, max_utc_ns=inner_b + 60_000_000_000,
    )
    bmdr_usdm = _scan_direct_archive_chunks(
        usdm_root, market=Market.USDM, symbols=set(FIVE_SYMBOLS), streams={"agg_trade", "book_ticker", "diff_depth", "orderbook_snapshot", "mark_price"},
        min_utc_ns=inner_a - 60_000_000_000, max_utc_ns=inner_b + 60_000_000_000,
    )

    # Build BMDR trade/mark/L2 maps inside [inner_a, inner_b]
    bmdr_trades: dict[tuple[str, str, int], tuple[str, str]] = {}
    bmdr_quotes: set[tuple[str, str, str, str, str, str]] = set()
    bmdr_marks: dict[tuple[str, int], str] = {}
    bmdr_l2: dict[tuple[str, str, int], tuple[Any, Any]] = {}
    for mkt, refs in ((Market.SPOT, bmdr_spot), (Market.USDM, bmdr_usdm)):
        mkt_s = "SPOT" if mkt == Market.SPOT else "USDM"
        for sym in FIVE_SYMBOLS:
            snap0 = baseline_snaps.get((mkt_s, sym))
            if snap0 is not None:
                bridge_state: list[Any] = [int(snap0["lastUpdateId"]), False, False]
                bids: dict[Decimal, Decimal] = {Decimal(p): Decimal(q) for p, q in snap0.get("bids", []) if Decimal(q) > 0}
                asks: dict[Decimal, Decimal] = {Decimal(p): Decimal(q) for p, q in snap0.get("asks", []) if Decimal(q) > 0}
            else:
                bridge_state = [None, False, False]
                bids = {}
                asks = {}
            for ev in read_recorder_events(refs, market_filter={mkt}, symbol_filter={sym}, verify_hashes=True):
                ts = int(ev.receive_time_utc_ns or 0)
                if isinstance(ev, OrderBookSnapshotEvent):
                    bids = {Decimal(p): Decimal(q) for p, q in ev.bids if Decimal(q) > 0}
                    asks = {Decimal(p): Decimal(q) for p, q in ev.asks if Decimal(q) > 0}
                    bridge_state[0], bridge_state[1], bridge_state[2] = ev.last_update_id, False, False
                elif isinstance(ev, DepthUpdateEvent):
                    apply_u, gap_u = _step_l2_bridge_state(bridge_state, mkt, ev)
                    if gap_u:
                        bids.clear()
                        asks.clear()
                    elif apply_u:
                        for p, q in ev.bids:
                            dp, dq = Decimal(p), Decimal(q)
                            if dq == 0:
                                bids.pop(dp, None)
                            else:
                                bids[dp] = dq
                        for p, q in ev.asks:
                            dp, dq = Decimal(p), Decimal(q)
                            if dq == 0:
                                asks.pop(dp, None)
                            else:
                                asks[dp] = dq
                        if bridge_state[2] and (mkt_s, sym, ev.final_update_id) in tel["l2_snaps"]:
                            tb = tuple((_norm_dec(p), _norm_dec(bids[p])) for p in sorted(bids.keys(), reverse=True)[:5])
                            ta = tuple((_norm_dec(p), _norm_dec(asks[p])) for p in sorted(asks.keys())[:5])
                            bmdr_l2[(mkt_s, sym, ev.final_update_id)] = (tb, ta)
                if ts < inner_a or ts >= inner_b:
                    continue
                if isinstance(ev, AggTradeEvent):
                    bmdr_trades[(mkt_s, sym, ev.aggregate_trade_id)] = (_norm_dec(ev.price), _norm_dec(ev.quantity))
                elif isinstance(ev, BookTickerEvent):
                    bmdr_quotes.add((mkt_s, sym, _norm_dec(ev.best_bid_price), _norm_dec(ev.best_bid_quantity), _norm_dec(ev.best_ask_price), _norm_dec(ev.best_ask_quantity)))
                elif isinstance(ev, MarkPriceEvent) and mkt == Market.USDM and ev.exchange_event_time_ms:
                    bmdr_marks[(sym, int(ev.exchange_event_time_ms))] = _norm_dec(ev.mark_price)

    # Bidirectional check on common interior trades (Live∩BMDR) + full BMDR keys inside [inner_a, inner_b]
    live_trades = tel["trades"]
    common_t = set(live_trades.keys()) & set(bmdr_trades.keys())
    exact_t = sum(1 for k in common_t if live_trades[k] == bmdr_trades[k])
    common_m = set(tel["marks"].keys()) & set(bmdr_marks.keys())
    exact_m = sum(1 for k in common_m if tel["marks"][k] == bmdr_marks[k])
    common_l2 = set(tel["l2_snaps"].keys()) & set(bmdr_l2.keys())
    exact_l2 = sum(1 for k in common_l2 if tel["l2_snaps"][k] == bmdr_l2[k])
    live_q_keys = set(tel["quotes"].keys())
    common_q = live_q_keys & bmdr_quotes

    return {
        "telemetry": telemetry,
        "interior_window_utc_ns": [inner_a, inner_b],
        "bidirectional_live_vs_bmdr": {
            "agg_trade": {
                "live_total": len(live_trades),
                "bmdr_interior_total": len(bmdr_trades),
                "common_ids": len(common_t),
                "bmdr_interior_covered_by_live": len(set(bmdr_trades.keys()) & set(live_trades.keys())),
                "bmdr_interior_missed_by_live": len(set(bmdr_trades.keys()) - set(live_trades.keys())),
                "exact_price_qty_matches": exact_t,
            },
            "book_ticker": {
                "live_unique_bbo": len(live_q_keys),
                "bmdr_interior_unique_bbo": len(bmdr_quotes),
                "common_exact_bbo": len(common_q),
                "bmdr_interior_missed_by_live": len(bmdr_quotes - live_q_keys),
            },
            "mark_price_usdm": {
                "live_total": len(tel["marks"]),
                "bmdr_interior_total": len(bmdr_marks),
                "common_ids": len(common_m),
                "bmdr_interior_missed_by_live": len(set(bmdr_marks.keys()) - set(tel["marks"].keys())),
                "exact_mark_px_matches": exact_m,
            },
            "l2_common_uid_top5": {
                "live_sampled_uids": len(tel["l2_snaps"]),
                "common_uids_verified_against_bmdr": len(common_l2),
                "exact_top5_matches": exact_l2,
            },
        },
    }


def run_slow_ref_300s(total_secs: float = 305.0) -> dict[str, Any]:
    """300s slow-reference observation: 5 USD-M symbols × 5 endpoints with bucket dedup & weight audit."""
    endpoints = [
        ("open_interest", "https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}"),
        ("global_ls_account", "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period=5m&limit=1"),
        ("top_ls_account", "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={sym}&period=5m&limit=1"),
        ("top_ls_position", "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period=5m&limit=1"),
        ("taker_buy_sell", "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={sym}&period=5m&limit=1"),
    ]
    seen_buckets: dict[tuple[str, str], set[int]] = {}
    raw_observations = 0
    dedup_observations = 0
    lags_ms: list[float] = []
    cycles = 0
    t0 = time.monotonic()
    while True:
        cycles += 1
        for sym in FIVE_SYMBOLS:
            for kind, tmpl in endpoints:
                now_ms = int(time.time() * 1000)
                with urlopen(tmpl.format(sym=sym), timeout=8.0) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                row = payload[0] if isinstance(payload, list) and payload else payload
                b_ms = int(row.get("timestamp") or row.get("time") or now_ms)
                raw_observations += 1
                s = seen_buckets.setdefault((sym, kind), set())
                if b_ms not in s:
                    s.add(b_ms)
                    dedup_observations += 1
                    lags_ms.append(max(0.0, float(now_ms - b_ms)))
        elapsed = time.monotonic() - t0
        if elapsed >= total_secs:
            break
        time.sleep(min(150.0, max(1.0, total_secs - elapsed)))
    lags_ms.sort()
    return {
        "window_wall_secs": round(time.monotonic() - t0, 2),
        "poll_cycles": cycles,
        "raw_responses": raw_observations,
        "dedup_unique_buckets": dedup_observations,
        "estimated_weight_per_cycle": len(endpoints) * len(FIVE_SYMBOLS),
        "total_weight_used": raw_observations,
        "arrival_lag_ms": {
            "min": round(lags_ms[0], 1) if lags_ms else None,
            "median": round(lags_ms[len(lags_ms) // 2], 1) if lags_ms else None,
            "max": round(lags_ms[-1], 1) if lags_ms else None,
        },
        "disclosure": "openInterest/futures_data are slow polled reference datasets; interval <5m buckets repeat identically unless deduplicated by (symbol, kind, bucket_ts_ms).",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("closed-1h", "live-capture", "slow-ref"), required=True)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--spot-root", type=Path, default=DEFAULT_SPOT_ROOT)
    parser.add_argument("--usdm-root", type=Path, default=DEFAULT_USDM_ROOT)
    parser.add_argument("--hour-iso", type=str, default=None)
    parser.add_argument("--duration", type=float, default=65.0)
    parser.add_argument("--trimmed", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    if args.phase == "closed-1h":
        res = run_closed_1h_parity(
            export_root=args.export_root,
            spot_root=args.spot_root,
            usdm_root=args.usdm_root,
            hour_iso=args.hour_iso,
        )
    elif args.phase == "live-capture":
        res = asyncio.run(
            _run_live_capture_bmd1(
                duration_secs=args.duration,
                spot_root=args.spot_root,
                usdm_root=args.usdm_root,
                export_root=args.export_root,
                trimmed=bool(args.trimmed),
            )
        )
    else:
        res = run_slow_ref_300s(total_secs=args.duration)

    txt = json.dumps(res, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(txt + "\n", encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
