"""Score candidate pipelines during eval using Airflow run metrics."""

from __future__ import annotations

from typing import Any


def compute_total_score(scores_by_category: dict[str, int]) -> int:
    return int(sum(scores_by_category.values()))


def _score_reliability(metrics: dict[str, Any]) -> int:
    state = str(metrics.get("state", "")).lower()
    if state == "success":
        return 25
    if state == "failed":
        return 5
    return 10


def _score_dependency_robustness(metrics: dict[str, Any]) -> int:
    tasks = metrics.get("tasks", {})
    total = max(1, int(tasks.get("total", 0)))
    success = int(tasks.get("success", 0))
    ratio = success / total
    return int(round(ratio * 20))


def _score_time_behavior(metrics: dict[str, Any]) -> int:
    duration = metrics.get("duration_seconds")
    if duration is None:
        return 8
    if duration <= 60:
        return 15
    if duration <= 300:
        return 12
    if duration <= 900:
        return 9
    return 6


def _score_safety(metrics: dict[str, Any]) -> int:
    failed = int(metrics.get("tasks", {}).get("failed", 0))
    if failed == 0:
        return 15
    if failed == 1:
        return 8
    return 4


def run_scoring(candidate_metrics: dict[str, Any], profile_weights: dict[str, Any]) -> dict[str, Any]:
    """Compute a starter category score map from runtime metrics.

    This keeps scoring external to DAG code and deterministic for CI/CD.
    """
    scores = {
        "reliability": _score_reliability(candidate_metrics),
        "dependency_robustness": _score_dependency_robustness(candidate_metrics),
        "control_flow": 14,
        "time_behavior": _score_time_behavior(candidate_metrics),
        "safety": _score_safety(candidate_metrics),
        "observability": 8,
        "efficiency": 5,
    }
    return {
        "scores": scores,
        "total_score": compute_total_score(scores),
        "hard_gates_passed": bool(candidate_metrics.get("hard_gates_passed", False)),
        "profile_used": profile_weights.get("profile_name", "unknown"),
    }
