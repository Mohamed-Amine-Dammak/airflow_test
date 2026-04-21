"""Collect eval metrics from Airflow API, score candidate, and persist decision artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scoring.engine.decision_engine import decide_promotion, load_policy
from scoring.engine.scoring_engine import run_scoring
from scripts.utils.airflow_api_client import AirflowApiClient, AirflowApiError
from scripts.utils.metadata_loader import (
    list_pipeline_ids,
    load_eval_history,
    load_manifest,
    save_eval_results,
    save_manifest,
)
from scripts.utils.version_helpers import champion_version, current_eval_version


def _load_profile(root: Path, pipeline_id: str) -> dict[str, Any]:
    score_inputs = root / "pipelines" / pipeline_id / "evaluation" / "score_inputs.json"
    profile_name = "trigger_pipeline_profile"
    if score_inputs.exists():
        payload = json.loads(score_inputs.read_text(encoding="utf-8"))
        profile_name = payload.get("profile", profile_name)
    profile_path = root / "scoring" / "profiles" / f"{profile_name}.json"
    if not profile_path.exists():
        profile_path = root / "scoring" / "profiles" / "trigger_pipeline_profile.json"
    return json.loads(profile_path.read_text(encoding="utf-8"))


def _candidate_dag_id(manifest: dict[str, Any], candidate_v: int) -> str:
    logical = manifest["logical_dag_id"]
    return f"{logical}_eval_v{candidate_v}"


def _champion_score(
    root: Path,
    pipeline_id: str,
    manifest: dict[str, Any],
    champion_v: int,
    client: AirflowApiClient,
    profile: dict[str, Any],
) -> int:
    hist = load_eval_history(root, pipeline_id, champion_v)
    if hist:
        return int(hist.get("total_score", 0))
    try:
        prod_dag_id = manifest["logical_dag_id"]
        latest_prod = client.get_latest_dag_run(prod_dag_id)
        if latest_prod:
            prod_metrics = client.collect_run_metrics(prod_dag_id, latest_prod)
            scored = run_scoring(prod_metrics, profile)
            return int(scored.get("total_score", 0))
    except AirflowApiError:
        pass
    return 0


def process_pipeline(
    root: Path,
    pipeline_id: str,
    client: AirflowApiClient,
    timeout_seconds: int,
    poll_seconds: int,
) -> int:
    manifest = load_manifest(root, pipeline_id)
    cand_v = current_eval_version(manifest)
    champ_v = champion_version(manifest)
    dag_id = _candidate_dag_id(manifest, cand_v)

    # Wait until the candidate eval DAG run reaches terminal state.
    dag_run = client.wait_for_terminal_dag_run(
        dag_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    candidate_metrics = client.collect_run_metrics(dag_id, dag_run)

    profile = _load_profile(root, pipeline_id)
    scoring_payload = run_scoring(candidate_metrics, profile)
    total_score = int(scoring_payload["total_score"])
    champion_score = _champion_score(root, pipeline_id, manifest, champ_v, client, profile)

    change_type = manifest.get("change_type", "logic_change")
    policy = load_policy(root, change_type)
    minimum_threshold = int(policy.get("minimum_threshold", 75))
    decision = decide_promotion(
        root=root,
        change_type=change_type,
        total_score=total_score,
        champion_score=champion_score,
        hard_gates_passed=bool(scoring_payload["hard_gates_passed"]),
    )

    eval_result = {
        "pipeline_id": pipeline_id,
        "candidate_version": cand_v,
        "champion_version": champ_v,
        "change_type": change_type,
        "hard_gates_passed": bool(scoring_payload["hard_gates_passed"]),
        "scores": scoring_payload["scores"],
        "total_score": total_score,
        "minimum_threshold": minimum_threshold,
        "champion_score": champion_score,
        "decision": decision,
        "airflow_metrics": candidate_metrics,
        "evaluated_at": datetime.utcnow().isoformat() + "Z",
    }

    save_eval_results(root, pipeline_id, cand_v, eval_result)

    for item in manifest.get("versions", []):
        if int(item.get("version", -1)) == cand_v:
            item["status"] = "evaluated"
            item["score_result_file"] = f"evaluation/history/eval_v{cand_v}.json"
    manifest["status"] = "evaluated"
    save_manifest(root, pipeline_id, manifest)

    print(
        f"[{pipeline_id}] eval version v{cand_v} => score={total_score} vs champion={champion_score}, decision={decision}"
    )
    return 0


def _target_pipelines(root: Path, pipeline_id: str | None) -> list[str]:
    if pipeline_id:
        return [pipeline_id]
    selected: list[str] = []
    for pid in list_pipeline_ids(root):
        manifest = load_manifest(root, pid)
        if int(manifest.get("current_eval_version", -1)) != int(manifest.get("current_prod_version", -1)):
            selected.append(pid)
    return selected


def run(
    root: Path,
    pipeline_id: str | None,
    airflow_url: str,
    airflow_api_prefix: str,
    airflow_username: str | None,
    airflow_password: str | None,
    airflow_token: str | None,
    timeout_seconds: int,
    poll_seconds: int,
) -> int:
    targets = _target_pipelines(root, pipeline_id)
    if not targets:
        print("No pipeline with pending eval candidate found.")
        return 0

    client = AirflowApiClient(
        airflow_url,
        api_prefix=airflow_api_prefix,
        username=airflow_username,
        password=airflow_password,
        token=airflow_token,
    )

    rc = 0
    for pid in targets:
        try:
            process_pipeline(root, pid, client, timeout_seconds, poll_seconds)
        except AirflowApiError as exc:
            rc = 1
            print(f"[{pid}] Airflow API error: {exc}")
    return rc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="collect_eval_metrics")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--pipeline-id", default=None, help="Single pipeline id; default auto-detect")
    parser.add_argument("--airflow-url", default="http://localhost:8081", help="Airflow base URL")
    parser.add_argument("--airflow-api-prefix", default="/api/v2", help="Airflow API prefix, e.g. /api/v2")
    parser.add_argument("--airflow-username", default=None, help="Airflow API username")
    parser.add_argument("--airflow-password", default=None, help="Airflow API password")
    parser.add_argument("--airflow-token", default=None, help="Bearer token")
    parser.add_argument("--timeout-seconds", type=int, default=900, help="Wait timeout for eval run")
    parser.add_argument("--poll-seconds", type=int, default=15, help="Poll interval")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(
        root=Path(args.root).resolve(),
        pipeline_id=args.pipeline_id,
        airflow_url=args.airflow_url,
        airflow_api_prefix=args.airflow_api_prefix,
        airflow_username=args.airflow_username,
        airflow_password=args.airflow_password,
        airflow_token=args.airflow_token,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
