"""Starter script: validate_manifest."""

from __future__ import annotations

import argparse
from pathlib import Path


def run(root: Path) -> int:
    """TODO: implement validate_manifest workflow."""
    print(f"[TODO] {root} :: validate_manifest not fully implemented yet")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="validate_manifest")
    parser.add_argument("--root", default=".", help="Repository root")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve())


if __name__ == "__main__":
    raise SystemExit(main())


