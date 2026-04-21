"""Promote the best accepted candidate to production deployment view."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scoring.engine.decision_engine import decide_promotion
from scripts.utils.metadata_loader import (
    list_pipeline_ids,
    load_eval_history,
    load_manifest,
    save_manifest,
)
from scripts.utils.version_helpers import best_promotable_version, champion_version, version_entry


def _candidate_results(root: Path, pipeline_id: str, manifest: dict) -> list[dict]:
    out: list[dict] = []
    for item in manifest.get("versions", []):
        version = int(item.get("version", -1))
        payload = load_eval_history(root, pipeline_id, version)
        if payload:
            out.append(payload)
    return out


def _publish_prod_dag(root: Path, pipeline_id: str, manifest: dict, version: int) -> None:
    entry = version_entry(manifest, version)
    source = root / "pipelines" / pipeline_id / "versions" / entry["file"]
    target = root / "dags" / "prod" / f"{manifest['logical_dag_id']}.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def process_pipeline(root: Path, pipeline_id: str, enforce_policy: bool) -> int:
    manifest = load_manifest(root, pipeline_id)
    eval_results = _candidate_results(root, pipeline_id, manifest)
    best = best_promotable_version(eval_results)

    if best is None:
        print(f"[{pipeline_id}] No promotable candidate found in eval history.")
        return 0

    best_result = next(r for r in eval_results if int(r["candidate_version"]) == best)
    champ_v = champion_version(manifest)
    champ_result = load_eval_history(root, pipeline_id, champ_v) or {"total_score": 0}

    decision = decide_promotion(
        root=root,
        change_type=manifest.get("change_type", "logic_change"),
        total_score=int(best_result.get("total_score", 0)),
        champion_score=int(champ_result.get("total_score", 0)),
        hard_gates_passed=bool(best_result.get("hard_gates_passed", False)),
    )

    if enforce_policy and decision != "promote":
        print(f"[{pipeline_id}] Best candidate v{best} rejected by policy at promotion step.")
        return 1

    previous_prod = int(manifest.get("current_prod_version", champ_v))
    manifest["previous_prod_version"] = previous_prod
    manifest["current_prod_version"] = best
    manifest["champion_version"] = best
    manifest["status"] = "promoted"

    for item in manifest.get("versions", []):
        version = int(item.get("version", -1))
        if version == best:
            item["status"] = "champion"
        elif version == previous_prod:
            item["status"] = "previous_champion"

    _publish_prod_dag(root, pipeline_id, manifest, best)
    save_manifest(root, pipeline_id, manifest)

    print(
        f"[{pipeline_id}] promoted candidate v{best} (score={best_result.get('total_score')}) over champion v{champ_v}"
    )
    return 0


def _targets(root: Path, pipeline_id: str | None) -> list[str]:
    if pipeline_id:
        return [pipeline_id]
    return list_pipeline_ids(root)


def run(root: Path, pipeline_id: str | None, enforce_policy: bool) -> int:
    rc = 0
    for pid in _targets(root, pipeline_id):
        try:
            rc = max(rc, process_pipeline(root, pid, enforce_policy))
        except FileNotFoundError:
            continue
    return rc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="promote_candidate")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--pipeline-id", default=None, help="Single pipeline id; default all")
    parser.add_argument(
        "--skip-policy-check",
        action="store_true",
        help="Allow promotion even if policy decision is reject",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve(), args.pipeline_id, not args.skip_policy_check)


if __name__ == "__main__":
    raise SystemExit(main())
