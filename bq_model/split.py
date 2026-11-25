"""Purged and Embargoed Walk-Forward / Cross-Validation splitter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    fold_index: int
    train_indices: tuple[int, ...]
    val_indices: tuple[int, ...]
    val_start_ns: int
    val_end_ns: int


def purged_embargoed_splits(
    timestamps_ns: Sequence[int],
    label_end_times_ns: Sequence[int],
    *,
    n_splits: int = 3,
    embargo_ns: int = 0,
    walk_forward_only: bool = True,
) -> list[WalkForwardFold]:
    """Generate zero-leakage folds purging overlapping labels and enforcing embargo_ns."""
    n = len(timestamps_ns)
    if len(label_end_times_ns) != n:
        raise ValueError("timestamps_ns and label_end_times_ns must match in length")
    if n_splits < 1 or n < n_splits + 1:
        raise ValueError("Insufficient samples for requested n_splits")
    for i in range(n):
        if label_end_times_ns[i] < timestamps_ns[i]:
            raise ValueError(f"label_end_times_ns[{i}] < timestamps_ns[{i}]")
        if i > 0 and timestamps_ns[i] < timestamps_ns[i - 1]:
            raise ValueError("timestamps_ns must be monotonically non-decreasing")

    folds: list[WalkForwardFold] = []
    if walk_forward_only:
        # Anchored/expanding strictly causal walk-forward folds over last n_splits chunks
        chunk = n // (n_splits + 1)
        if chunk < 1:
            raise ValueError("Chunk size too small for walk_forward_only")
        for f in range(n_splits):
            val_start_idx = (f + 1) * chunk
            val_end_idx = n if f == n_splits - 1 else (f + 2) * chunk
            val_idx = tuple(range(val_start_idx, val_end_idx))
            val_start_ns = timestamps_ns[val_start_idx]
            val_end_ns = max(label_end_times_ns[i] for i in val_idx)
            # Purge overlapping labels and honor embargo_ns gap before val_start_ns
            train_idx = tuple(
                i
                for i in range(0, val_start_idx)
                if label_end_times_ns[i] < val_start_ns
                and (embargo_ns <= 0 or (label_end_times_ns[i] + embargo_ns) <= val_start_ns)
            )
            folds.append(
                WalkForwardFold(
                    fold_index=f,
                    train_indices=train_idx,
                    val_indices=val_idx,
                    val_start_ns=val_start_ns,
                    val_end_ns=val_end_ns,
                )
            )
        return folds

    # Symmetric Purged + Embargoed K-Fold CV
    chunk = n // n_splits
    for f in range(n_splits):
        val_start_idx = f * chunk
        val_end_idx = n if f == n_splits - 1 else (f + 1) * chunk
        val_idx = tuple(range(val_start_idx, val_end_idx))
        val_start_ns = timestamps_ns[val_start_idx]
        val_end_ns = max(label_end_times_ns[i] for i in val_idx)
        train_idx_list: list[int] = []
        for i in range(n):
            if val_start_idx <= i < val_end_idx:
                continue
            t_start = timestamps_ns[i]
            t_end = label_end_times_ns[i]
            # Purge any overlapping label [t_start, t_end] with [val_start_ns, val_end_ns]
            if t_start <= val_end_ns and t_end >= val_start_ns:
                continue
            # Embargo post-validation training samples inside (val_end_ns, val_end_ns + embargo_ns]
            if t_start >= val_end_ns and t_start <= val_end_ns + embargo_ns:
                continue
            train_idx_list.append(i)
        folds.append(
            WalkForwardFold(
                fold_index=f,
                train_indices=tuple(train_idx_list),
                val_indices=val_idx,
                val_start_ns=val_start_ns,
                val_end_ns=val_end_ns,
            )
        )
    return folds
