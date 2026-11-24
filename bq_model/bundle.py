"""Sub-millisecond ModelBundle persistence, train-only normalization, and inference."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .split import purged_embargoed_splits


DEFAULT_FEATURES: tuple[str, ...] = (
    "bbo_imbalance",
    "ofi",
    "net_limit_flow",
    "pcvr",
    "sweep_cluster",
    "fragmentation",
    "fsi",
    "j_engine",
)


@dataclass(frozen=True, slots=True)
class ModelMetadata:
    model_id: str
    kind: str
    feature_names: tuple[str, ...]
    schema_hash: str
    train_mean: tuple[float, ...]
    train_std: tuple[float, ...]
    weights: tuple[float, ...]
    bias: float
    alpha: float
    market: str = "SPOT"
    symbol: str = "BTCUSDT"
    feature_version: str = "v1"
    input_digest: str = ""
    trained_until_ns: int = 0
    model_digest: str = ""


@dataclass(frozen=True, slots=True)
class TrainReport:
    model_id: str
    folds_evaluated: int
    oos_mse: float
    oos_corr: float
    bundle_dir: str
    trained_until_ns: int = 0
    model_digest: str = ""


class ModelBundle:
    """Self-contained <1ms causal inference bundle."""

    def __init__(self, metadata: ModelMetadata) -> None:
        self.metadata = metadata
        self._feature_names = metadata.feature_names
        self._mean = np.asarray(metadata.train_mean, dtype=np.float64)
        self._std = np.asarray(metadata.train_std, dtype=np.float64)
        self._weights = np.asarray(metadata.weights, dtype=np.float64)
        self._bias = float(metadata.bias)

    @property
    def model_id(self) -> str:
        return self.metadata.model_id

    @property
    def schema_hash(self) -> str:
        return self.metadata.schema_hash

    @property
    def trained_until_ns(self) -> int:
        return self.metadata.trained_until_ns

    def predict_one(self, features: dict[str, float] | Any) -> float:
        """Pure in-memory <1ms inference on causal feature snapshot."""
        vec = np.empty(len(self._feature_names), dtype=np.float64)
        if isinstance(features, dict):
            for idx, name in enumerate(self._feature_names):
                vec[idx] = float(features[name])
        else:
            for idx, name in enumerate(self._feature_names):
                vec[idx] = float(getattr(features, name))
        normed = (vec - self._mean) / self._std
        return float(np.dot(normed, self._weights) + self._bias)

    def predict_batch(self, x_matrix: np.ndarray) -> np.ndarray:
        normed = (x_matrix - self._mean) / self._std
        return normed @ self._weights + self._bias

    def save(self, models_root: Path) -> Path:
        out_dir = models_root / self.metadata.model_id
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = asdict(self.metadata)
        (out_dir / "bundle.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return out_dir

    @classmethod
    def load(cls, bundle_dir: Path) -> "ModelBundle":
        raw = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
        meta = ModelMetadata(
            model_id=str(raw["model_id"]),
            kind=str(raw["kind"]),
            feature_names=tuple(raw["feature_names"]),
            schema_hash=str(raw["schema_hash"]),
            train_mean=tuple(float(x) for x in raw["train_mean"]),
            train_std=tuple(float(x) for x in raw["train_std"]),
            weights=tuple(float(x) for x in raw["weights"]),
            bias=float(raw["bias"]),
            alpha=float(raw["alpha"]),
            market=str(raw.get("market", "SPOT")),
            symbol=str(raw.get("symbol", "BTCUSDT")),
            feature_version=str(raw.get("feature_version", "v1")),
            input_digest=str(raw.get("input_digest", "")),
            trained_until_ns=int(raw.get("trained_until_ns", 0)),
            model_digest=str(raw.get("model_digest", "")),
        )
        return cls(meta)


def _fit_ridge(
    x_train: np.ndarray, y_train: np.ndarray, alpha: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    mean = x_train.mean(axis=0)
    std = x_train.std(axis=0)
    std = np.where(std < 1e-9, 1.0, std)
    xn = (x_train - mean) / std
    y_mean = float(y_train.mean())
    yc = y_train - y_mean
    d = xn.shape[1]
    gram = xn.T @ xn + alpha * np.eye(d, dtype=np.float64)
    w = np.linalg.solve(gram, xn.T @ yc)
    return mean, std, w, y_mean


def _schema_hash(feature_names: tuple[str, ...]) -> str:
    return hashlib.sha256("|".join(feature_names).encode("utf-8")).hexdigest()[:16]


def fit_walk_forward_model(
    rows: Sequence[dict[str, Any]],
    targets: Sequence[float],
    label_end_times_ns: Sequence[int],
    *,
    model_id: str,
    models_root: Path,
    feature_names: tuple[str, ...] = DEFAULT_FEATURES,
    n_splits: int = 3,
    embargo_ns: int = 0,
    alpha: float = 1.0,
    market: str | None = None,
    symbol: str | None = None,
    feature_version: str = "v1",
) -> tuple[ModelBundle, TrainReport]:
    """Fit ModelBundle using Purged+Embargoed Walk-Forward with provenance & causal cutoff."""
    if not rows:
        raise ValueError("Cannot fit model on empty feature rows")
    timestamps = [int(r["timestamp_ns"]) for r in rows]
    resolved_market = market or str(rows[0].get("market", "SPOT"))
    resolved_symbol = symbol or str(rows[0].get("symbol", "BTCUSDT"))
    input_digest = hashlib.sha256(
        json.dumps(
            {
                "market": resolved_market,
                "symbol": resolved_symbol,
                "timestamps": [timestamps[0], timestamps[-1], len(rows)],
                "label_end_max": max(int(x) for x in label_end_times_ns),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    folds = purged_embargoed_splits(
        timestamps,
        label_end_times_ns,
        n_splits=n_splits,
        embargo_ns=embargo_ns,
        walk_forward_only=True,
    )
    x_all = np.asarray(
        [[float(r[name]) for name in feature_names] for r in rows],
        dtype=np.float64,
    )
    y_all = np.asarray(targets, dtype=np.float64)

    oos_preds: list[float] = []
    oos_true: list[float] = []
    used_folds = 0
    for fold in folds:
        if not fold.train_indices or not fold.val_indices:
            continue
        used_folds += 1
        tr = list(fold.train_indices)
        va = list(fold.val_indices)
        mean, std, w, b = _fit_ridge(x_all[tr], y_all[tr], alpha)
        preds = ((x_all[va] - mean) / std) @ w + b
        oos_preds.extend(preds.tolist())
        oos_true.extend(y_all[va].tolist())

    if oos_preds:
        p_arr = np.asarray(oos_preds, dtype=np.float64)
        t_arr = np.asarray(oos_true, dtype=np.float64)
        oos_mse = float(np.mean((p_arr - t_arr) ** 2))
        if p_arr.std() > 1e-12 and t_arr.std() > 1e-12:
            oos_corr = float(np.corrcoef(p_arr, t_arr)[0, 1])
        else:
            oos_corr = 0.0
    else:
        oos_mse = 0.0
        oos_corr = 0.0

    # Fit final causal deployment bundle on all data available at training cutoff;
    # enforce trained_until_ns = max(label_end_times_ns) so pre-cutoff trading is rejected.
    mean_f, std_f, w_f, b_f = _fit_ridge(x_all, y_all, alpha)
    trained_until_ns = max(int(t) for t in label_end_times_ns)
    model_digest = hashlib.sha256(
        json.dumps(
            {
                "model_id": model_id,
                "market": resolved_market,
                "symbol": resolved_symbol,
                "feature_names": list(feature_names),
                "weights": [float(v) for v in w_f],
                "bias": float(b_f),
                "trained_until_ns": trained_until_ns,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    meta = ModelMetadata(
        model_id=model_id,
        kind="linear_ridge",
        feature_names=feature_names,
        schema_hash=_schema_hash(feature_names),
        train_mean=tuple(float(v) for v in mean_f),
        train_std=tuple(float(v) for v in std_f),
        weights=tuple(float(v) for v in w_f),
        bias=float(b_f),
        alpha=alpha,
        market=resolved_market,
        symbol=resolved_symbol,
        feature_version=feature_version,
        input_digest=input_digest,
        trained_until_ns=trained_until_ns,
        model_digest=model_digest,
    )
    bundle = ModelBundle(meta)
    saved_dir = bundle.save(models_root)
    return bundle, TrainReport(
        model_id=model_id,
        folds_evaluated=used_folds,
        oos_mse=oos_mse,
        oos_corr=oos_corr,
        bundle_dir=str(saved_dir),
        trained_until_ns=trained_until_ns,
        model_digest=model_digest,
    )
