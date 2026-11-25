"""Compare Route B live trade samples against contemporaneous BMDR sealed partitions on VPS."""

from __future__ import annotations

import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bq_feed.events import AggTradeEvent, Market, Stream  # noqa: E402
from bq_nautilus.recorder import VerifiedChunkRef, read_recorder_events  # noqa: E402


def _scan_recent_archive_chunks(
    partition_root: Path,
    *,
    market: Market,
    min_utc_ns: int,
    max_utc_ns: int,
) -> list[VerifiedChunkRef]:
    manifests_dir = partition_root / "manifests"
    cutoff_mtime = (min_utc_ns / 1_000_000_000.0) - 600.0
    entries: list[tuple[float, Path]] = []
    with os.scandir(manifests_dir) as it:
        for ent in it:
            if not ent.name.endswith(".archive-manifest.json"):
                continue
            st = ent.stat()
            if st.st_mtime >= cutoff_mtime:
                entries.append((st.st_mtime, Path(ent.path)))
    entries.sort()
    refs: list[VerifiedChunkRef] = []
    for _, mpath in entries:
        outer = json.loads(mpath.read_text(encoding="utf-8"))
        doc = outer.get("raw_manifest") or outer
        if str(doc.get("stream", "")) != "agg_trade":
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
                symbol=str(doc["symbol"]),
                stream=str(doc["stream"]),
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


def compare_live_vs_bmdr(
    *,
    live_json_path: Path,
    spot_partition: Path,
    usdm_partition: Path,
) -> dict[str, Any]:
    live_doc = json.loads(live_json_path.read_text(encoding="utf-8"))
    t_start, t_end = [int(x) for x in live_doc["window_utc_ns"]]
    samples = live_doc.get("sample_trades", [])

    live_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in samples:
        iid = str(row["instrument_id"])
        sym_raw = iid.split(".", 1)[0]
        if sym_raw.endswith("-PERP"):
            mkt = "USDM"
            sym = sym_raw[:-5]
        else:
            mkt = "SPOT"
            sym = sym_raw
        live_by_key.setdefault((mkt, sym), []).append(row)

    pad_ns = 30_000_000_000
    spot_refs = _scan_recent_archive_chunks(
        spot_partition,
        market=Market.SPOT,
        min_utc_ns=t_start - pad_ns,
        max_utc_ns=t_end + pad_ns,
    )
    usdm_refs = _scan_recent_archive_chunks(
        usdm_partition,
        market=Market.USDM,
        min_utc_ns=t_start - pad_ns,
        max_utc_ns=t_end + pad_ns,
    )

    bmdr_usdm: dict[tuple[str, int], tuple[Decimal, Decimal, int]] = {}
    for ref in usdm_refs:
        for ev in read_recorder_events([ref], verify_hashes=True):
            if isinstance(ev, AggTradeEvent):
                bmdr_usdm[(ev.symbol, ev.aggregate_trade_id)] = (
                    ev.price,
                    ev.quantity,
                    ev.trade_time_ms,
                )

    bmdr_spot_trades: dict[tuple[str, int], Decimal] = {}
    bmdr_spot_count: dict[str, int] = {}
    for ref in spot_refs:
        for ev in read_recorder_events([ref], verify_hashes=True):
            if isinstance(ev, AggTradeEvent):
                bmdr_spot_count[ev.symbol] = bmdr_spot_count.get(ev.symbol, 0) + 1
                for tid in range(ev.first_trade_id, ev.last_trade_id + 1):
                    bmdr_spot_trades[(ev.symbol, tid)] = ev.price

    usdm_matched = 0
    usdm_total = 0
    usdm_exact_px = 0
    usdm_symbols_matched: dict[str, int] = {}
    for (mkt, sym), rows in live_by_key.items():
        if mkt != "USDM":
            continue
        for r in rows:
            usdm_total += 1
            aid = int(r["trade_id"])
            rec = bmdr_usdm.get((sym, aid))
            if rec is not None:
                usdm_matched += 1
                if Decimal(r["price"]) == rec[0] and Decimal(r["size"]) == rec[1]:
                    usdm_exact_px += 1
                    usdm_symbols_matched[sym] = usdm_symbols_matched.get(sym, 0) + 1

    spot_matched = 0
    spot_total = 0
    spot_exact_px = 0
    spot_symbols_matched: dict[str, int] = {}
    for (mkt, sym), rows in live_by_key.items():
        if mkt != "SPOT":
            continue
        for r in rows:
            spot_total += 1
            tid = int(r["trade_id"])
            px = bmdr_spot_trades.get((sym, tid))
            if px is not None:
                spot_matched += 1
                if Decimal(r["price"]) == px:
                    spot_exact_px += 1
                    spot_symbols_matched[sym] = spot_symbols_matched.get(sym, 0) + 1

    return {
        "window_utc_ns": [t_start, t_end],
        "spot_chunks_inspected": len(spot_refs),
        "usdm_chunks_inspected": len(usdm_refs),
        "usdm_agg_trade_sample": {
            "live_samples": usdm_total,
            "bmdr_total_indexed": len(bmdr_usdm),
            "matched_by_agg_trade_id": usdm_matched,
            "exact_price_qty_matches": usdm_exact_px,
            "match_rate": round(usdm_matched / usdm_total, 4) if usdm_total else None,
            "exact_rate_of_matched": round(usdm_exact_px / usdm_matched, 4) if usdm_matched else None,
            "by_symbol": usdm_symbols_matched,
        },
        "spot_trade_sample": {
            "note": "Route B Spot subscribes @trade (matched against BMDR @aggTrade [first_trade_id..last_trade_id] & price)",
            "live_samples": spot_total,
            "bmdr_agg_trades_in_window": bmdr_spot_count,
            "matched_inside_bmdr_agg_range": spot_matched,
            "exact_price_matches": spot_exact_px,
            "match_rate": round(spot_matched / spot_total, 4) if spot_total else None,
            "exact_rate_of_matched": round(spot_exact_px / spot_matched, 4) if spot_matched else None,
            "by_symbol": spot_symbols_matched,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-json", type=Path, required=True)
    parser.add_argument("--spot-partition", type=Path, default=Path("/srv/recorder-data/S-BMDR/20261113T092542Z-spot"))
    parser.add_argument("--usdm-partition", type=Path, default=Path("/srv/recorder-data/S-BMDR/20261113T092542Z-usdm"))
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    res = compare_live_vs_bmdr(
        live_json_path=args.live_json,
        spot_partition=args.spot_partition,
        usdm_partition=args.usdm_partition,
    )
    txt = json.dumps(res, indent=2)
    if args.out is not None:
        args.out.write_text(txt + "\n", encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
