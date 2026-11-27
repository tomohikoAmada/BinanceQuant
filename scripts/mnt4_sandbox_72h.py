"""M-NT4 72h Frozen Sandbox Runner, Hourly Replay Auditor, and BMDR Archive Cross-Checker."""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bq_feed.events import Market  # noqa: E402
from bq_nautilus.instruments import BmdInstrumentSpec  # noqa: E402
from bq_nautilus.mnt4_sandbox import replay_sealed_hour, run_sandbox_session  # noqa: E402
from bq_nautilus.recorder import load_mcp_export_chunks, read_recorder_events  # noqa: E402


def _default_btc_spot_instrument():
    from decimal import Decimal

    spec = BmdInstrumentSpec(
        market=Market.SPOT,
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.00001"),
        min_notional=Decimal("5.0"),
        observe_time_utc_ns=1_795_700_000_000_000_000,
        min_quantity=Decimal("0.00001"),
        max_quantity=Decimal("9000.0"),
    )
    return spec.to_nautilus_instrument()


def cross_check_hour_against_bmdr(
    audit_path: Path,
    *,
    export_root: Path = Path("/srv/recorder-data/S-BMDR/mcp-export"),
) -> dict:
    """Spot-check unique sanitized sandbox BBO quotes against sealed S-BMDR archive."""
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    events_path = Path(audit["hourly_events_path"])
    spot_sym = str(audit["metadata"].get("spot_symbol", "BTCUSDT"))
    sandbox_quotes: set[tuple[str, str, str, str]] = set()
    t_min = None
    t_max = None
    with gzip.open(events_path, "rt", encoding="utf-8") as fh:
        for ln in fh:
            row = json.loads(ln)
            if not row["instrument_id"].startswith(f"{spot_sym}."):
                continue
            sandbox_quotes.add((row["bid"], row["bid_sz"], row["ask"], row["ask_sz"]))
            ts = int(row["ts_init"])
            t_min = ts if t_min is None else min(t_min, ts)
            t_max = ts if t_max is None else max(t_max, ts)

    if t_min is None or t_max is None or not export_root.exists():
        return {"status": "PENDING_BMDR_ARCHIVE", "sandbox_unique_quotes": len(sandbox_quotes)}

    refs = load_mcp_export_chunks(
        export_root,
        market_filter={Market.SPOT},
        symbol_filter={spot_sym},
        stream_filter={"book_ticker"},
        min_utc_ns=t_min - 5_000_000_000,
        max_utc_ns=t_max + 5_000_000_000,
    )
    bmdr_quotes: set[tuple[str, str, str, str]] = set()
    for ev in read_recorder_events(refs, verify_hashes=True):
        if getattr(ev, "receive_time_utc_ns", None) is None:
            continue
        if t_min <= int(ev.receive_time_utc_ns) <= t_max:
            bmdr_quotes.add(
                (str(ev.best_bid_price), str(ev.best_bid_quantity), str(ev.best_ask_price), str(ev.best_ask_quantity))
            )
    common = sandbox_quotes & bmdr_quotes
    return {
        "status": "PENDING_BMDR_SEAL" if not bmdr_quotes else ("PASS" if len(bmdr_quotes - sandbox_quotes) == 0 else "PARTIAL"),
        "sandbox_unique_quotes": len(sandbox_quotes),
        "bmdr_unique_quotes": len(bmdr_quotes),
        "common_exact_quotes": len(common),
        "bmdr_missed_by_sandbox": len(bmdr_quotes - sandbox_quotes),
        "chunks_checked": len(refs),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run zero-credential Nautilus Sandbox session (e.g. 259200s for 72h)")
    p_run.add_argument("--state-dir", type=Path, required=True)
    p_run.add_argument("--duration-secs", type=float, default=259200.0)
    p_run.add_argument("--spot-symbol", type=str, default="BTCUSDT")
    p_run.add_argument("--factor-usdm-symbol", type=str, default="SOLUSDT")
    p_run.add_argument("--trade-every-n-quotes", type=int, default=200)
    p_run.add_argument("--out", type=Path, default=None)

    p_rep = sub.add_parser("replay-hour", help="Replay a sealed .hour-audit.json deterministically")
    p_rep.add_argument("--audit", type=Path, required=True)
    p_rep.add_argument("--work-dir", type=Path, required=True)
    p_rep.add_argument("--trade-every-n-quotes", type=int, default=200)

    p_chk = sub.add_parser("cross-check-bmdr", help="Cross-check sealed hour events against S-BMDR archive")
    p_chk.add_argument("--audit", type=Path, required=True)
    p_chk.add_argument("--export-root", type=Path, default=Path("/srv/recorder-data/S-BMDR/mcp-export"))

    args = parser.parse_args()
    if args.cmd == "run":
        res = asyncio.run(
            run_sandbox_session(
                state_dir=args.state_dir,
                duration_secs=float(args.duration_secs),
                spot_symbol=args.spot_symbol,
                factor_usdm_symbol=args.factor_usdm_symbol,
                trade_every_n_quotes=int(args.trade_every_n_quotes),
                repo_root=REPO_ROOT,
            )
        )
        txt = json.dumps(res, indent=2)
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(txt + "\n", encoding="utf-8")
        print(txt)
    elif args.cmd == "replay-hour":
        res = replay_sealed_hour(
            args.audit,
            spot_instrument=_default_btc_spot_instrument(),
            work_dir=args.work_dir,
            trade_every_n_quotes=int(args.trade_every_n_quotes),
        )
        print(json.dumps(res, indent=2))
        if res.get("status") != "PASSED" or not res.get("intents_exact_match"):
            raise SystemExit(1)
    else:
        res = cross_check_hour_against_bmdr(args.audit, export_root=args.export_root)
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
