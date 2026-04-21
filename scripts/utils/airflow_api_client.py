"""Airflow API client for collecting DAG run metrics (Airflow 3.x compatible)."""

from __future__ import annotations

import argparse
import base64
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class AirflowApiError(RuntimeError):
    """Raised when Airflow API interaction fails."""


class AirflowApiClient:
    """Small HTTP client for Airflow stable REST API."""

    def __init__(
        self,
        base_url: str,
        *,
        api_prefix: str = "/api/v2",
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_prefix = api_prefix if api_prefix.startswith("/") else f"/{api_prefix}"
        self.username = username
        self.password = password
        self.token = token
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.username and self.password:
            raw = f"{self.username}:{self.password}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
        return headers

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        body = None
        headers = self._headers()
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = Request(url=url, method=method, data=body, headers=headers)
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise AirflowApiError(f"HTTP {exc.code} on {url}: {body}") from exc
        except URLError as exc:
            raise AirflowApiError(f"Could not reach Airflow API at {url}: {exc}") from exc

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)

    def list_dag_runs(self, dag_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        dag = quote(dag_id, safe="")
        payload = self._request("GET", f"{self.api_prefix}/dags/{dag}/dagRuns?limit={limit}&order_by=-start_date")
        return payload.get("dag_runs", [])

    def get_latest_dag_run(self, dag_id: str) -> dict[str, Any] | None:
        runs = self.list_dag_runs(dag_id, limit=1)
        return runs[0] if runs else None

    def wait_for_terminal_dag_run(
        self,
        dag_id: str,
        *,
        timeout_seconds: int = 900,
        poll_seconds: int = 15,
        min_start_time: datetime | None = None,
    ) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        terminal_states = {"success", "failed"}

        while time.time() < deadline:
            latest = self.get_latest_dag_run(dag_id)
            if latest:
                state = (latest.get("state") or "").lower()
                start_dt = self._parse_dt(latest.get("start_date"))
                is_new_enough = min_start_time is None or (start_dt and start_dt >= min_start_time)
                if is_new_enough and state in terminal_states:
                    return latest
            time.sleep(poll_seconds)

        raise AirflowApiError(
            f"Timed out waiting for terminal DAG run for '{dag_id}' after {timeout_seconds}s"
        )

    def list_task_instances(self, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
        dag = quote(dag_id, safe="")
        run = quote(dag_run_id, safe="")
        payload = self._request("GET", f"{self.api_prefix}/dags/{dag}/dagRuns/{run}/taskInstances")
        return payload.get("task_instances", [])

    def collect_run_metrics(self, dag_id: str, dag_run: dict[str, Any]) -> dict[str, Any]:
        run_id = dag_run.get("dag_run_id") or dag_run.get("run_id")
        if not run_id:
            raise AirflowApiError(f"No dag_run_id returned by Airflow for DAG '{dag_id}'")

        task_instances = self.list_task_instances(dag_id, run_id)
        total_tasks = len(task_instances)
        success_tasks = sum(1 for t in task_instances if (t.get("state") or "").lower() == "success")
        failed_tasks = sum(1 for t in task_instances if (t.get("state") or "").lower() == "failed")

        started = self._parse_dt(dag_run.get("start_date"))
        ended = self._parse_dt(dag_run.get("end_date"))
        duration_seconds = None
        if started and ended:
            duration_seconds = max(0.0, (ended - started).total_seconds())

        return {
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "state": dag_run.get("state", "unknown"),
            "start_date": dag_run.get("start_date"),
            "end_date": dag_run.get("end_date"),
            "duration_seconds": duration_seconds,
            "tasks": {
                "total": total_tasks,
                "success": success_tasks,
                "failed": failed_tasks,
            },
            "hard_gates_passed": str(dag_run.get("state", "")).lower() == "success",
        }


def run(root: Path) -> int:
    """CLI placeholder to verify module importability."""
    print(f"Airflow API client module ready. root={root}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="airflow_api_client")
    parser.add_argument("--root", default=".", help="Repository root")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(Path(args.root).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
