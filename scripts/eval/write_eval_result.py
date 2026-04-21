"""Write eval result payload to latest + history artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.utils.metadata_loader import save_eval_results


def run(root: Path, pipeline_id: str, input_file: Path) -> int:
    payload = json.loads(input_file.read_text(encoding="utf-8"))
    version = int(payload["candidate_version"])
    save_eval_results(root, pipeline_id, version, payload)
    print(f"[{pipeline_id}] wrote eval result for v{version}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="write_eval_result")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--pipeline-id", required=True, help="Pipeline id")
    parser.add_argument("--input-file", required=True, help="JSON eval result file")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve(), args.pipeline_id, Path(args.input_file).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
