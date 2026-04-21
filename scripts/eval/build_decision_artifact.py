"""Build and persist eval decision artifacts from latest metrics/scoring output."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.utils.metadata_loader import load_latest_eval, save_eval_results


def run(root: Path, pipeline_id: str) -> int:
    latest = load_latest_eval(root, pipeline_id)
    version = int(latest["candidate_version"])
    save_eval_results(root, pipeline_id, version, latest)
    print(f"[{pipeline_id}] decision artifact persisted for v{version}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="build_decision_artifact")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--pipeline-id", required=True, help="Pipeline id")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve(), args.pipeline_id)


if __name__ == "__main__":
    raise SystemExit(main())
