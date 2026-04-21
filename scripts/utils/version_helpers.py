"""Version helper functions for pipeline manifests."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any


def current_eval_version(manifest: dict[str, Any]) -> int:
    return int(manifest["current_eval_version"])


def champion_version(manifest: dict[str, Any]) -> int:
    return int(manifest["champion_version"])


def current_prod_version(manifest: dict[str, Any]) -> int:
    return int(manifest["current_prod_version"])


def version_entry(manifest: dict[str, Any], version: int) -> dict[str, Any]:
    for item in manifest.get("versions", []):
        if int(item.get("version", -1)) == version:
            return item
    raise KeyError(f"Version {version} not found in manifest versions[]")


def best_promotable_version(eval_results: list[dict[str, Any]]) -> int | None:
    promotable = [r for r in eval_results if r.get("decision") == "promote" and r.get("hard_gates_passed")]
    if not promotable:
        return None
    promotable.sort(
        key=lambda r: (
            int(r.get("total_score", 0)),
            int(r.get("candidate_version", -1)),
        ),
        reverse=True,
    )
    return int(promotable[0]["candidate_version"])


def run(root: Path) -> int:
    print(f"version_helpers ready. root={root}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="version_helpers")
    parser.add_argument("--root", default=".", help="Repository root")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
