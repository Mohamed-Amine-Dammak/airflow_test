from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import time


def custom_failure_email(email_list):
    def _callback(context):
        print(f"Failure/retry callback triggered. Emails: {email_list}")
    return _callback


def build_talend_dag(config: dict):
    dag_id = config["dag_id"]
    start_date = datetime.fromisoformat(config["start_date"])
    tags = config.get("tags", ["talend"])
    connection_id = config.get("connection_id", "talend_cloud")
    email_list = config.get("email_list", [])
    retries = config.get("retries", 3)
    retry_delay_minutes = config.get("retry_delay_minutes", 5)
    jobs = config.get("jobs", [])
    schedule = config.get("schedule")

    default_args = {
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "on_failure_callback": custom_failure_email(email_list),
        "on_retry_callback": custom_failure_email(email_list),
    }

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        default_args=default_args,
        tags=tags,
    )
    def talend_cloud_job_dag():

        def _pause(label: str, seconds: int = 5):
            print(f"{label} -> sleeping for {seconds} seconds...")
            time.sleep(seconds)
            print(f"{label} -> resumed.")

        @task
        def get_executable_id(job_name: str, sleep_after: int = 5) -> str:
            conn = BaseHook.get_connection(connection_id)
            base_url = conn.host.rstrip("/")

            headers = {
                "Authorization": f"Bearer {conn.password}",
                "Content-Type": "application/json",
            }

            url = f"{base_url}/orchestration/executables/tasks"
            params = {
                "name": job_name,
                "limit": 100,
                "offset": 0,
            }

            response = requests.get(url, headers=headers, params=params, timeout=60)

            if response.status_code != 200:
                raise AirflowException(
                    f"Failed to fetch Talend executables: "
                    f"{response.status_code} - {response.text}"
                )

            payload = response.json()
            items = payload.get("items", []) if isinstance(payload, dict) else []

            exact_matches = [
                item for item in items
                if str(item.get("name", "")).strip() == str(job_name).strip()
            ]

            if not exact_matches:
                sample_names = [item.get("name") for item in items[:20]]
                raise AirflowException(
                    f"No executable found for job_name='{job_name}'. "
                    f"Returned names: {sample_names}"
                )

            if len(exact_matches) > 1:
                executables = [item.get("executable") for item in exact_matches]
                raise AirflowException(
                    f"Multiple executables found for job_name='{job_name}'. "
                    f"Executables: {executables}"
                )

            executable_id = exact_matches[0].get("executable")
            if not executable_id:
                raise AirflowException(
                    f"Task found for job_name='{job_name}' but no executable returned."
                )

            print(f"Resolved job_name='{job_name}' -> executable_id='{executable_id}'")
            _pause(f"After get_executable_id [{job_name}]", sleep_after)
            return executable_id

        @task
        def trigger_job(executable_id: str, sleep_after: int = 5) -> str:
            conn = BaseHook.get_connection(connection_id)
            base_url = conn.host.rstrip("/")

            headers = {
                "Authorization": f"Bearer {conn.password}",
                "Content-Type": "application/json",
            }
            payload = {"executable": executable_id}

            url = f"{base_url}/processing/executions"
            response = requests.post(url, headers=headers, json=payload, timeout=60)

            if response.status_code not in (200, 201):
                raise AirflowException(
                    f"Failed to trigger Talend job: {response.status_code} - {response.text}"
                )

            body = response.json()
            execution_id = body.get("executionId") or body.get("id")

            if not execution_id:
                raise AirflowException(
                    f"Talend trigger succeeded but no execution ID was returned: {body}"
                )

            print(f"Talend job triggered. Execution ID: {execution_id}")
            _pause(f"After trigger_job [execution_id={execution_id}]", sleep_after)
            return execution_id

        @task
        def monitor_job(
            execution_id: str,
            poll_interval: int = 10,
            timeout: int = 50,
            sleep_after: int = 5,
        ):
            """
            Monitor Talend Cloud job execution logs and fetch component metrics
            dynamically using the execution ID.
            """
            conn = BaseHook.get_connection(connection_id)
            base_url = conn.host.rstrip("/")
            headers = {"Authorization": f"Bearer {conn.password}"}

            logs_url = f"{base_url}/monitoring/executions/{execution_id}/logs"
            metrics_url = (
                f"{base_url}/monitoring/observability/executions/"
                f"{execution_id}/component"
            )

            print(f"DEBUG logs URL = {logs_url}")
            print(f"DEBUG metrics URL = {metrics_url}")

            start_time = time.time()

            seen_logs = set()
            seen_metrics = set()

            all_logs = []
            all_metrics = []

            while True:
                finished = False
                failed = False

                # FETCH LOGS
                log_resp = requests.get(
                    logs_url,
                    headers=headers,
                    params={"count": 50, "order": "DESC"},
                    timeout=60,
                )
                if log_resp.status_code != 200:
                    raise AirflowException(f"Failed fetching logs: {log_resp.text}")

                logs = log_resp.json().get("data", [])
                new_logs = []

                if logs:
                    for log in logs:
                        key = (
                            log.get("logTimestamp"),
                            log.get("severity"),
                            log.get("logMessage"),
                        )
                        if key not in seen_logs:
                            seen_logs.add(key)
                            all_logs.append(log)
                            new_logs.append(log)

                    if new_logs:
                        print("\n[LOGS] Latest new entries:")
                        for log in sorted(new_logs, key=lambda x: x.get("logTimestamp", 0))[-30:]:
                            ts = log.get("logTimestamp")
                            sev = log.get("severity")
                            msg = log.get("logMessage")
                            print(f"[{ts}] {sev}: {msg}")

                            msg_text = str(msg or "")
                            if "EXECUTION_SUCCESS" in msg_text or " - Done." in msg_text:
                                finished = True
                            if (
                                "EXECUTION_ERROR" in msg_text
                                or "EXECUTION_FAILED" in msg_text
                                or "returnCode: 1" in msg_text
                                or "status EXECUTION_FAILED" in msg_text
                            ):
                                failed = True

                # FETCH METRICS
                metrics_resp = requests.get(
                    metrics_url,
                    headers=headers,
                    params={"limit": 200, "offset": 0},
                    timeout=60,
                )

                if metrics_resp.status_code == 200:
                    metrics_data = metrics_resp.json()
                    items = metrics_data.get("metrics", {}).get("items", [])
                    new_metrics = []

                    if items:
                        for item in items:
                            metric_key = (
                                item.get("pid"),
                                item.get("connector_id"),
                                item.get("component_start_time_seconds"),
                                item.get("component_execution_duration_milliseconds"),
                                item.get("component_connection_rows_total"),
                            )
                            if metric_key not in seen_metrics:
                                seen_metrics.add(metric_key)
                                all_metrics.append(item)
                                new_metrics.append(item)

                        if new_metrics:
                            print("\n[METRICS] Component-level Observability:")
                            for item in new_metrics:
                                print(
                                    f"component={item.get('connector_label', 'N/A')} | "
                                    f"duration_ms={item.get('component_execution_duration_milliseconds', 'N/A')} | "
                                    f"rows_processed={item.get('component_connection_rows_total', 'N/A')}"
                                )
                else:
                    print(f"Failed fetching metrics: {metrics_resp.text}")

                if failed:
                    raise AirflowException(
                        f"Talend execution {execution_id} failed according to execution logs."
                    )

                if finished:
                    print(f"\n[STATUS] Execution {execution_id} finished successfully.")
                    break

                if time.time() - start_time > timeout:
                    raise AirflowException(
                        f"Monitoring timed out for execution_id={execution_id}"
                    )

                time.sleep(poll_interval)

            _pause(f"After monitor_job [execution_id={execution_id}]", sleep_after)

            return {
                "execution_id": execution_id,
                "status": "SUCCESS",
                "logs": all_logs,
                "metrics": all_metrics,
            }

        task_map = {}

        for job_cfg in jobs:
            job_id = job_cfg["id"]
            job_name = job_cfg["job_name"]

            executable_id = get_executable_id.override(
                task_id=f"get_executable_id__{job_id}"
            )(
                job_name=job_name,
                sleep_after=job_cfg.get("sleep_after_lookup", 5),
            )

            execution_id = trigger_job.override(
                task_id=f"trigger_job__{job_id}"
            )(
                executable_id=executable_id,
                sleep_after=job_cfg.get("sleep_after_trigger", 5),
            )

            monitor = monitor_job.override(
                task_id=f"monitor_job__{job_id}"
            )(
                execution_id=execution_id,
                poll_interval=job_cfg.get("poll_interval", 10),
                timeout=job_cfg.get("timeout", 50),
                sleep_after=job_cfg.get("sleep_after_monitor", 5),
            )

            task_map[job_id] = {
                "get_executable_id": executable_id,
                "trigger_job": execution_id,
                "monitor": monitor,
            }

        for job_cfg in jobs:
            current_job_id = job_cfg["id"]
            for upstream_id in job_cfg.get("depends_on", []):
                task_map[upstream_id]["monitor"] >> task_map[current_job_id]["trigger_job"]

    return talend_cloud_job_dag()