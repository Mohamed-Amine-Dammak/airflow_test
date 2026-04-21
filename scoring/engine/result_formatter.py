"""Build stable scoring artifacts."""

from __future__ import annotations


def build_eval_result(pipeline_id: str, candidate_version: int, champion_version: int, payload: dict) -> dict:
    """Create persisted evaluation artifact payload."""
    return {
        "pipeline_id": pipeline_id,
        "candidate_version": candidate_version,
        "champion_version": champion_version,
        **payload,
    }
