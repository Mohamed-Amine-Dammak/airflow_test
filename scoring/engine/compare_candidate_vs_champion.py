"""Helpers for candidate vs champion comparisons."""

from __future__ import annotations


def compare_scores(candidate_score: int, champion_score: int) -> dict:
    """Return comparison summary used by decision logic."""
    return {
        "candidate_score": candidate_score,
        "champion_score": champion_score,
        "delta": candidate_score - champion_score,
        "beats_champion": candidate_score > champion_score,
        "matches_champion": candidate_score == champion_score,
    }
