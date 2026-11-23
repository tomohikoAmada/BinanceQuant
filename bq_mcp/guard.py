"""AST safety analyzer, capability parity check, and causality guard for strategies."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any, Sequence

from bq_backtest.engine import BacktestConfig, verify_causality_perturbation
from bq_backtest.strategy import BaseStrategy
from bq_feed.events import FeedEvent, Market, Stream
from bq_feed.parity import check_strategy_capabilities

FORBIDDEN_MODULES = frozenset({
    "os",
    "sys",
    "pathlib",
    "io",
    "socket",
    "http",
    "urllib",
    "requests",
    "httpx",
    "aiohttp",
    "grpc",
    "subprocess",
    "shutil",
    "sqlite3",
    "duckdb",
    "time",
    "datetime",
    "random",
    "pickle",
    "ctypes",
    "importlib",
    "multiprocessing",
    "threading",
})

FORBIDDEN_CALLS = frozenset({
    "open",
    "eval",
    "exec",
    "compile",
    "__import__",
    "globals",
    "locals",
    "vars",
    "input",
})


@dataclass(frozen=True, slots=True)
class AuditResult:
    passed: bool
    violations: tuple[str, ...]


def ast_audit_strategy_source(source_code: str) -> AuditResult:
    """Inspect strategy source AST; block filesystem IO, network, wall-clock, dynamic exec."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError as exc:
        return AuditResult(False, (f"SyntaxError: {exc}",))

    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in FORBIDDEN_MODULES:
                    violations.append(f"forbidden import '{alias.name}' at line {node.lineno}")
        elif isinstance(node, ast.ImportFrom):
            mod = (node.module or "").split(".")[0]
            if mod in FORBIDDEN_MODULES:
                violations.append(f"forbidden from-import '{node.module}' at line {node.lineno}")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALLS:
                violations.append(f"forbidden builtin call '{node.func.id}()' at line {node.lineno}")
            elif isinstance(node.func, ast.Attribute):
                attr = node.func.attr
                if attr in ("open", "read_text", "write_text", "time", "monotonic", "time_ns", "now", "utcnow", "urlopen", "get", "post"):
                    # Block wall-clock / IO helpers even if imported through aliases
                    if isinstance(node.func.value, ast.Name) and node.func.value.id in (
                        "time", "datetime", "os", "pathlib", "Path", "urllib", "requests", "socket"
                    ):
                        violations.append(
                            f"forbidden IO/wall-clock call '{node.func.value.id}.{attr}()' at line {node.lineno}"
                        )
    return AuditResult(len(violations) == 0, tuple(violations))


def validate_strategy_code(
    *,
    source_code: str,
    required_streams: Sequence[tuple[Market, str, Stream]],
    mode: str = "paper",
    sample_events: Sequence[FeedEvent] | None = None,
    strategy_instance: BaseStrategy | None = None,
    split_index: int | None = None,
) -> dict[str, Any]:
    """Run full 3-gate strategy validation: AST safety + capability parity + causality perturbation."""
    ast_res = ast_audit_strategy_source(source_code)
    if not ast_res.passed:
        return {
            "valid": False,
            "stage": "ast_audit",
            "violations": list(ast_res.violations),
        }

    try:
        check_strategy_capabilities(required_streams, mode=mode)
    except Exception as exc:
        return {
            "valid": False,
            "stage": "capability_parity",
            "violations": [str(exc)],
        }

    if sample_events is not None and strategy_instance is not None:
        split = split_index if split_index is not None else max(0, len(sample_events) // 2 - 1)
        if len(sample_events) >= 2 and 0 <= split < len(sample_events) - 1:
            ok = verify_causality_perturbation(
                sample_events,
                strategy_instance,
                split,
                config=BacktestConfig(run_id="validate-causality"),
            )
            if not ok:
                return {
                    "valid": False,
                    "stage": "causality_perturbation",
                    "violations": ["prefix outputs diverged when future events were perturbed"],
                }

    return {"valid": True, "stage": "all_passed", "violations": []}
