"""Promotion decision policy dispatcher."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_policy(root: Path, change_type: str) -> dict[str, Any]:
    policy_map = {
        "config_only": "config_only_policy.json",
        "logic_change": "logic_change_policy.json",
        "connector_change": "connector_change_policy.json",
        "destructive_change": "destructive_change_policy.json",
    }
    filename = policy_map.get(change_type, "logic_change_policy.json")
    with (root / "scoring" / "policies" / filename).open("r", encoding="utf-8") as f:
        return json.load(f)


def decide_promotion(
    *,
    root: Path,
    change_type: str,
    total_score: int,
    champion_score: int,
    hard_gates_passed: bool,
) -> str:
    policy = load_policy(root, change_type)
    minimum_threshold = int(policy.get("minimum_threshold", 75))
    requires_beating = bool(policy.get("requires_beating_champion", False))
    allow_equal = bool(policy.get("allow_equal_to_champion", False))

    if bool(policy.get("requires_hard_gates", True)) and not hard_gates_passed:
        return "reject"
    if total_score < minimum_threshold:
        return "reject"

    if requires_beating:
        if allow_equal:
            if total_score < champion_score:
                return "reject"
        else:
            if total_score <= champion_score:
                return "reject"

    return "promote"
