"""Publish generated eval DAG artifacts into dags/eval for Airflow scanning."""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.deployments.build_eval_deployment import build_eval_artifact
from scripts.utils.metadata_loader import list_pipeline_ids, load_manifest

EVAL_FILENAME_PATTERN = re.compile(r"^(?P<logical>.+)_eval_v(?P<version>\d+)\.py$")


def _cleanup_old_eval_versions(eval_dir: Path, logical_dag_id: str, keep_version: int) -> None:
    for candidate in eval_dir.glob(f"{logical_dag_id}_eval_v*.py"):
        m = EVAL_FILENAME_PATTERN.match(candidate.name)
        if not m:
            continue
        version = int(m.group("version"))
        if version != keep_version:
            candidate.unlink(missing_ok=True)


def publish_pipeline(root: Path, pipeline_id: str) -> Path:
    manifest = load_manifest(root, pipeline_id)
    logical = manifest["logical_dag_id"]
    eval_version = int(manifest["current_eval_version"])

    artifact = build_eval_artifact(root, pipeline_id)
    eval_dir = root / "dags" / "eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    destination = eval_dir / artifact.name
    shutil.copyfile(artifact, destination)
    _cleanup_old_eval_versions(eval_dir, logical, eval_version)

    print(f"[{pipeline_id}] published eval DAG: {destination}")
    return destination


def _targets(root: Path, pipeline_id: str | None) -> list[str]:
    if pipeline_id:
        return [pipeline_id]
    return list_pipeline_ids(root)


def run(root: Path, pipeline_id: str | None) -> int:
    rc = 0
    for pid in _targets(root, pipeline_id):
        try:
            publish_pipeline(root, pid)
        except FileNotFoundError as exc:
            rc = 1
            print(f"[{pid}] publish skipped: {exc}")
    return rc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="publish_eval_dag")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--pipeline-id", default=None, help="Single pipeline id; default all")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve(), args.pipeline_id)


if __name__ == "__main__":
    raise SystemExit(main())
