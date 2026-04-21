"""Repository metadata helpers for manifests and eval artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def list_pipeline_ids(root: Path) -> list[str]:
    pipelines_root = root / "pipelines"
    if not pipelines_root.exists():
        return []
    return sorted([p.name for p in pipelines_root.iterdir() if p.is_dir()])


def pipeline_root(root: Path, pipeline_id: str) -> Path:
    return root / "pipelines" / pipeline_id


def load_manifest(root: Path, pipeline_id: str) -> dict[str, Any]:
    return load_json(pipeline_root(root, pipeline_id) / "manifest" / "manifest.json")


def save_manifest(root: Path, pipeline_id: str, manifest: dict[str, Any]) -> None:
    dump_json(pipeline_root(root, pipeline_id) / "manifest" / "manifest.json", manifest)


def runtime_config(root: Path, pipeline_id: str) -> dict[str, Any]:
    return load_json(pipeline_root(root, pipeline_id) / "config" / "runtime_config.json")


def latest_eval_path(root: Path, pipeline_id: str) -> Path:
    return pipeline_root(root, pipeline_id) / "evaluation" / "latest_eval_result.json"


def history_eval_path(root: Path, pipeline_id: str, version: int) -> Path:
    return pipeline_root(root, pipeline_id) / "evaluation" / "history" / f"eval_v{version}.json"


def load_latest_eval(root: Path, pipeline_id: str) -> dict[str, Any]:
    path = latest_eval_path(root, pipeline_id)
    return load_json(path)


def save_eval_results(root: Path, pipeline_id: str, version: int, result: dict[str, Any]) -> None:
    dump_json(latest_eval_path(root, pipeline_id), result)
    dump_json(history_eval_path(root, pipeline_id, version), result)


def load_eval_history(root: Path, pipeline_id: str, version: int) -> dict[str, Any] | None:
    path = history_eval_path(root, pipeline_id, version)
    if not path.exists():
        return None
    return load_json(path)


def run(root: Path) -> int:
    print(f"metadata_loader ready. pipelines={len(list_pipeline_ids(root))}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="metadata_loader")
    parser.add_argument("--root", default=".", help="Repository root")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
