"""Local stdio MCP server exposing 6 quantitative research tools (< 64 KiB responses)."""

from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any

from bq_backtest.engine import BacktestConfig, BacktestEngine, run_sensitivity_matrix
from bq_backtest.strategy import BtcSpotBboRule, ThreeStageQuantStrategy
from bq_feature.export import export_features_parquet, extract_feature_rows
from bq_feed.events import Market, Stream, event_from_dict
from bq_feed.parity import LIVE_CAPABILITY_MATRIX
from bq_feed.replay import RecorderReplayFeed
from bq_model.bundle import DEFAULT_FEATURES, ModelBundle, fit_walk_forward_model

from .guard import validate_strategy_code

MAX_RESPONSE_BYTES = 65_536


def _cap_json_bytes(obj: dict[str, Any], max_bytes: int = MAX_RESPONSE_BYTES) -> dict[str, Any]:
    encoded = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) < max_bytes:
        return obj
    # Deterministically trim list fields until strictly < max_bytes
    trimmed = dict(obj)
    trimmed["truncated"] = True
    for k, v in list(trimmed.items()):
        if isinstance(v, list):
            lo, hi = 0, len(v)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                trimmed[k] = v[:mid]
                if len(json.dumps(trimmed, separators=(",", ":"), sort_keys=True).encode("utf-8")) < max_bytes:
                    lo = mid
                else:
                    hi = mid - 1
            trimmed[k] = v[:lo]
            trimmed[f"{k}_total_count"] = len(v)
            if len(json.dumps(trimmed, separators=(",", ":"), sort_keys=True).encode("utf-8")) < max_bytes:
                return trimmed
    # Fallback minimal summary guaranteed < max_bytes
    minimal = {
        "truncated": True,
        "summary": json.dumps(trimmed, default=str)[: max_bytes - 256],
    }
    return minimal


TOOL_SCHEMAS = [
    {"name": "inspect_datasets", "description": "Inspect Recorder partitions, cached datasets, and LIVE_CAPABILITY_MATRIX."},
    {"name": "export_features", "description": "Compute causal features and export features.parquet."},
    {"name": "train_model", "description": "Train a Purged+Embargoed Walk-Forward ModelBundle (<1ms inference)."},
    {"name": "validate_strategy", "description": "Verify strategy AST safety, BMDG parity, and causality perturbation."},
    {"name": "run_backtest", "description": "Execute F1/F2 causal backtest with sensitivity matrix under runs/<run_id>/."},
    {"name": "compare_runs", "description": "Compare saved backtest runs or paper reconciliation digests."},
]


class QuantitativeMcpServer:
    """6-tool stdio JSON-RPC research MCP server with strict <64 KiB output cap."""

    def __init__(self, workspace_root: Path) -> None:
        self._root = workspace_root
        self._root.mkdir(parents=True, exist_ok=True)
        self._models_dir = self._root / "models"
        self._runs_dir = self._root / "runs"

    def _load_events(self, args: dict[str, Any]) -> list[Any]:
        if "events" in args:
            return [event_from_dict(d) for d in args["events"]]
        if "partition_root" in args:
            mkt_filter = {Market(m) for m in args["market_filter"]} if args.get("market_filter") else None
            sym_filter = {str(s) for s in args["symbol_filter"]} if args.get("symbol_filter") else None
            strm_filter = {str(s) for s in args["stream_filter"]} if args.get("stream_filter") else None
            min_ns = int(args["min_utc_ns"]) if args.get("min_utc_ns") is not None else None
            max_ns = int(args["max_utc_ns"]) if args.get("max_utc_ns") is not None else None
            strict_ts = bool(args.get("strict_receive_time", False))
            feed = RecorderReplayFeed.from_recorder_root(
                Path(args["partition_root"]),
                market_filter=mkt_filter,
                symbol_filter=sym_filter,
                stream_filter=strm_filter,
                min_utc_ns=min_ns,
                max_utc_ns=max_ns,
            )
            return list(feed.iter_events(strict_receive_time=strict_ts))
        if "raw_records" in args:
            feed = RecorderReplayFeed.from_raw_records(args["raw_records"])
            return list(feed.iter_events(strict_receive_time=bool(args.get("strict_receive_time", False))))
        raise ValueError("Must provide 'events', 'partition_root', or 'raw_records'")

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        args = arguments or {}
        if name == "inspect_datasets":
            data_dir = Path(args.get("data_dir", self._root))
            partitions = sorted(p.name for p in data_dir.iterdir() if p.is_dir()) if data_dir.exists() else []
            runs = sorted(p.name for p in self._runs_dir.iterdir() if p.is_dir()) if self._runs_dir.exists() else []
            models = sorted(p.name for p in self._models_dir.iterdir() if p.is_dir()) if self._models_dir.exists() else []
            caps = [
                {
                    "market": k[0].value,
                    "stream": k[1].value,
                    "research": v.research_available,
                    "live": v.live_gateway_available,
                    "status": v.status,
                }
                for k, v in sorted(LIVE_CAPABILITY_MATRIX.items(), key=lambda x: (x[0][0].value, x[0][1].value))
            ]
            return _cap_json_bytes({"partitions": partitions, "runs": runs, "models": models, "capabilities": caps})

        if name == "export_features":
            events = self._load_events(args)
            out_path = Path(args.get("output_path", self._root / "features.parquet"))
            count = export_features_parquet(events, out_path)
            preview = extract_feature_rows(events[:10])
            return _cap_json_bytes({"row_count": count, "output_path": str(out_path), "preview": preview[:5]})

        if name == "train_model":
            events = self._load_events(args)
            rows = extract_feature_rows(events)
            horizon = int(args.get("horizon_steps", 1))
            if horizon <= 0:
                raise ValueError("horizon_steps must be positive")
            # Construct forward targets strictly per (market, symbol) & contiguous healthy epoch
            epochs: dict[tuple[str, str], list[list[dict[str, Any]]]] = {}
            for r in rows:
                pkey = (str(r["market"]), str(r["symbol"]))
                if not r.get("health_valid", True):
                    epochs.setdefault(pkey, []).append([])
                    continue
                plist = epochs.setdefault(pkey, [[]])
                if not plist:
                    plist.append([])
                plist[-1].append(r)

            target_mkt = str(args.get("market", rows[0]["market"] if rows else "SPOT"))
            target_sym = str(args.get("symbol", rows[0]["symbol"] if rows else "BTCUSDT"))
            train_rows: list[dict[str, Any]] = []
            targets: list[float] = []
            end_times: list[int] = []
            for seg in epochs.get((target_mkt, target_sym), []):
                for idx in range(len(seg) - horizon):
                    p0 = seg[idx]["mid_price"]
                    p1 = seg[idx + horizon]["mid_price"]
                    ret_bps = ((p1 - p0) / p0) * 10_000.0 if p0 > 0 else 0.0
                    train_rows.append(seg[idx])
                    targets.append(ret_bps)
                    end_times.append(int(seg[idx + horizon]["timestamp_ns"]))

            if len(train_rows) < 6:
                raise ValueError("Need at least 6 contiguous healthy feature rows to run Walk-Forward training")
            bundle, report = fit_walk_forward_model(
                train_rows,
                targets,
                end_times,
                model_id=str(args.get("model_id", "model-v1")),
                models_root=self._models_dir,
                feature_names=tuple(args.get("feature_names", DEFAULT_FEATURES)),
                n_splits=int(args.get("n_splits", 3)),
                embargo_ns=int(args.get("embargo_ns", 0)),
                market=target_mkt,
                symbol=target_sym,
            )
            return _cap_json_bytes({
                "model_id": bundle.model_id,
                "schema_hash": bundle.schema_hash,
                "trained_until_ns": report.trained_until_ns,
                "model_digest": report.model_digest,
                "folds_evaluated": report.folds_evaluated,
                "oos_mse": report.oos_mse,
                "oos_corr": report.oos_corr,
                "bundle_dir": report.bundle_dir,
            })

        if name == "validate_strategy":
            src = str(args["source_code"])
            reqs = [
                (Market(r["market"]), str(r["symbol"]), Stream(r["stream"]))
                for r in args.get("required_streams", [{"market": "SPOT", "symbol": "BTCUSDT", "stream": "book_ticker"}])
            ]
            events = self._load_events(args) if ("events" in args or "partition_root" in args or "raw_records" in args) else None
            strat = BtcSpotBboRule() if events is not None else None
            res = validate_strategy_code(
                source_code=src,
                required_streams=reqs,
                mode=str(args.get("mode", "paper")),
                sample_events=events,
                strategy_instance=strat,
            )
            return _cap_json_bytes(res)

        if name == "run_backtest":
            events = self._load_events(args)
            kind = str(args.get("strategy", "btc_spot_bbo_rule"))
            if kind == "three_stage":
                model_id = args.get("model_id")
                bundle = ModelBundle.load(self._models_dir / model_id) if model_id else None
                strat: Any = ThreeStageQuantStrategy(model_bundle=bundle)
            else:
                strat = BtcSpotBboRule(
                    order_qty=Decimal(str(args.get("order_qty", "0.01"))),
                    buy_imbalance_threshold=float(args.get("buy_threshold", 0.25)),
                    sell_imbalance_threshold=float(args.get("sell_threshold", 0.25)),
                )
            cfg = BacktestConfig(
                run_id=str(args.get("run_id", "run-1")),
                fill_mode=str(args.get("fill_mode", "F1")),  # type: ignore[arg-type]
                taker_fee_bps=Decimal(str(args.get("taker_fee_bps", "10"))),
                network_rtt_ms=float(args.get("network_rtt_ms", 0.0)),
            )
            res = BacktestEngine().run(events, strat, cfg, runs_root=self._runs_dir)
            payload: dict[str, Any] = {
                "run_id": res.run_id,
                "metrics": res.metrics,
                "semantic_digest": res.semantic_digest,
                "ledger_count": len(res.ledger),
                "run_dir": str(self._runs_dir / res.run_id),
            }
            if args.get("include_sensitivity", False):
                payload["sensitivity"] = run_sensitivity_matrix(events, strat, base_config=cfg)
            return _cap_json_bytes(payload)

        if name == "compare_runs":
            run_ids = list(args.get("run_ids", []))
            compared: list[dict[str, Any]] = []
            for rid in run_ids:
                mpath = self._runs_dir / str(rid) / "metrics.json"
                if not mpath.exists():
                    compared.append({"run_id": rid, "missing": True})
                else:
                    compared.append(json.loads(mpath.read_text(encoding="utf-8")))
            return _cap_json_bytes({"runs": compared})

        raise KeyError(f"Unknown MCP tool: {name}")

    def handle_rpc_line(self, line: str) -> str | None:
        req = json.loads(line)
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params") or {}
        try:
            if method == "initialize":
                res: Any = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "bq-mcp", "version": "0.1.0"}}
            elif method == "tools/list":
                res = {"tools": TOOL_SCHEMAS}
            elif method == "tools/call":
                tool_res = self.call_tool(str(params["name"]), params.get("arguments"))
                res = {"content": [{"type": "text", "text": json.dumps(tool_res)}]}
            elif method == "notifications/initialized":
                return None
            else:
                raise KeyError(f"Unsupported method: {method}")
            return json.dumps({"jsonrpc": "2.0", "id": req_id, "result": res})
        except Exception as exc:
            return json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(exc)}})

    def serve_stdio(self) -> None:
        for line in sys.stdin:
            stripped = line.strip()
            if not stripped:
                continue
            reply = self.handle_rpc_line(stripped)
            if reply is not None:
                sys.stdout.write(reply + "\n")
                sys.stdout.flush()


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd() / "bq_workspace"
    QuantitativeMcpServer(root).serve_stdio()
