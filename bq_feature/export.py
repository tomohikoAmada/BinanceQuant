"""Deterministic feature extraction and Parquet export."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from bq_feed.events import FeedEvent, StreamHealthEvent

from .engine import FeatureEngine, FeatureState


def extract_feature_rows(
    events: Iterable[FeedEvent],
    *,
    engine: FeatureEngine | None = None,
) -> list[dict[str, Any]]:
    eng = engine or FeatureEngine()
    state = FeatureState()
    rows: list[dict[str, Any]] = []
    for ev in events:
        if isinstance(ev, StreamHealthEvent):
            eng.on_health(state, ev)
        else:
            vec = eng.update(state, ev)
            if vec is not None:
                rows.append(asdict(vec))
    return rows


_SCHEMA = pa.schema([
    ("delivery_seq", pa.int64()),
    ("timestamp_ns", pa.int64()),
    ("market", pa.string()),
    ("symbol", pa.string()),
    ("health_valid", pa.bool_()),
    ("mid_price", pa.float64()),
    ("spread_bps", pa.float64()),
    ("microprice", pa.float64()),
    ("bbo_imbalance", pa.float64()),
    ("ofi", pa.float64()),
    ("net_limit_flow", pa.float64()),
    ("pcvr", pa.float64()),
    ("sweep_cluster", pa.float64()),
    ("fragmentation", pa.float64()),
    ("fsi", pa.float64()),
    ("j_engine", pa.float64()),
    ("volume_clock_bars", pa.int64()),
    ("kline_1s_close", pa.float64()),
    ("kline_5s_close", pa.float64()),
])


def export_features_parquet(
    events: Iterable[FeedEvent],
    output_path: Path,
    *,
    engine: FeatureEngine | None = None,
) -> int:
    """Compute causal features over stream and persist features.parquet."""
    rows = extract_feature_rows(events, engine=engine)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        cols = {field.name: [r[field.name] for r in rows] for field in _SCHEMA}
        table = pa.table(cols, schema=_SCHEMA)
    else:
        table = pa.table({field.name: pa.array([], type=field.type) for field in _SCHEMA}, schema=_SCHEMA)
    pq.write_table(table, output_path, compression="zstd")
    return len(rows)
