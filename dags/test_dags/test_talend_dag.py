from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import time
from utils.etl_tasks import (
    trigger_n8n_workflow, monitor_n8n_workflow,
    trigger_talend_job, monitor_talend_execution, custom_failure_email
)

# --- DEFAULT ARGS START ---
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "tags": ['etl', 'dynamic', 'monitoring'],
}
# --- DEFAULT ARGS END ---





@dag(
    dag_id="talend_cloud_job_dynamic_test",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["talend", "cloud", "etl", "dynamic"],
)
def talend_dynamic_jobs_orchestration():

    @task
    def trigger_talend_job(executable_id: str) -> str:
        """Trigger one Talend Cloud job execution"""
        conn = BaseHook.get_connection("talend_cloud")
        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        payload = {"executable": executable_id}

        url = f"{conn.host.rstrip('/')}/processing/executions"

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            execution_id = data.get("executionId") or data.get("id")
            if not execution_id:
                raise ValueError("No executionId returned from Talend")
            print(f"Triggered Talend job {executable_id} → execution ID: {execution_id}")
            return execution_id
        except Exception as e:
            raise Exception(f"Failed to trigger Talend job {executable_id}: {str(e)}")

    @task
    def monitor_talend_execution(execution_id: str, poll_interval: int = 10, timeout_minutes: int = 45):
        conn = BaseHook.get_connection("talend_cloud")
        headers = {"Authorization": f"Bearer {conn.password}"}

        status_url  = f"{conn.host.rstrip('/')}/processing/executions/{execution_id}"
        logs_url    = f"{conn.host.rstrip('/')}/monitoring/executions/{execution_id}/logs"
        metrics_url = f"{conn.host.rstrip('/')}/monitoring/observability/executions/{execution_id}/component"

        start_time = time.time()
        timeout_sec = timeout_minutes * 60

        collected_logs = []
        collected_metrics = []
        final_status = "UNKNOWN"
        last_log_count = 0

        def _fetch_logs_and_metrics(fetch_number: int = 1):
            nonlocal collected_logs, collected_metrics, last_log_count

            print(f"  Fetching logs & metrics (attempt {fetch_number})...")

            # ── Logs ────────────────────────────────────────────────────────────
            try:
                log_resp = requests.get(
                    logs_url,
                    headers=headers,
                    params={"count": 100, "order": "DESC"},
                    timeout=10
                )
                print(f"  Logs HTTP status: {log_resp.status_code}")
                if log_resp.status_code == 200:
                    raw = log_resp.json()
                    print(f"  Raw logs keys: {list(raw.keys())} | data length: {len(raw.get('data', []))}")
                    new_logs = raw.get("data", [])
                    if len(new_logs) > last_log_count:
                        print(f"  → Found {len(new_logs) - last_log_count} new log entries")
                        for log in new_logs[last_log_count:]:
                            ts = log.get("logTimestamp", "–")
                            sev = log.get("severity", "?")
                            msg = log.get("logMessage", "").strip()
                            print(f"    [{ts}] {sev:<8} {msg}")
                        collected_logs.extend(new_logs)
                        last_log_count = len(new_logs)
                    else:
                        print("  No new logs since last fetch")
                else:
                    print(f"  Logs fetch failed: {log_resp.text[:200]}...")
            except Exception as e:
                print(f"  Logs error: {str(e)}")

            # ── Metrics ─────────────────────────────────────────────────────────
            try:
                m_resp = requests.get(metrics_url, headers=headers, params={"limit": 200}, timeout=10)
                print(f"  Metrics HTTP status: {m_resp.status_code}")
                if m_resp.status_code == 200:
                    raw = m_resp.json()
                    print(f"  Raw metrics keys: {list(raw.keys())}")
                    items = raw.get("metrics", {}).get("items", [])
                    print(f"  Metrics items count: {len(items)}")
                    if items:
                        collected_metrics = items
                        print("\n  Latest metrics:")
                        print("  " + "-" * 80)
                        print(f"  {'Component':<40} {'Duration ms':<12} {'Rows Processed':<15}")
                        print("  " + "-" * 80)
                        for item in items:
                            comp = item.get("connector_label") or item.get("component_name", "–")
                            dur  = item.get("component_execution_duration_milliseconds", "–")
                            rows = item.get("component_connection_rows_total", "–")
                            print(f"  {comp:<40} {dur:<12} {rows:<15}")
                        print("  " + "-" * 80 + "\n")
                else:
                    print(f"  Metrics fetch failed: {m_resp.text[:200]}...")
            except Exception as e:
                print(f"  Metrics error: {str(e)}")


        print(f"→ Starting monitoring for {execution_id} (timeout: {timeout_minutes} min)")

        # Main polling loop
        while time.time() - start_time < timeout_sec:
            try:
                r = requests.get(status_url, headers=headers, timeout=12)
                r.raise_for_status()
                data = r.json()
                current_status = data.get("status", "UNKNOWN").upper()
                final_status = current_status

                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] Status: {current_status}")

                if current_status in [
                    "EXECUTION_SUCCESSFUL", "COMPLETED", "SUCCESS",
                    "FAILED", "ERROR", "KILLED", "CANCELED"
                ]:
                    print(f"   → Terminal state reached: {current_status}")
                    # Give Talend API some time + multiple attempts
                    time.sleep(3)
                    _fetch_logs_and_metrics(1)
                    time.sleep(4)
                    _fetch_logs_and_metrics(2)
                    time.sleep(4)
                    _fetch_logs_and_metrics(3)
                    break

            except Exception as e:
                print(f"Status request failed: {str(e)}")

            # Normal fetch during polling
            _fetch_logs_and_metrics()
            time.sleep(poll_interval)

        # Final safety
        if final_status == "UNKNOWN":
            raise Exception("Monitoring timed out without detecting any status")

        if final_status not in ["EXECUTION_SUCCESSFUL", "COMPLETED", "SUCCESS"]:
            raise Exception(f"Job finished with unsuccessful status: {final_status}")

        print(f"Monitoring complete → {execution_id} → {final_status}")
        print(f"Total logs collected: {len(collected_logs)}")
        print(f"Total metrics snapshots: {len(collected_metrics)}")

        return {
            "execution_id": execution_id,
            "final_status": final_status,
            "logs_count": len(collected_logs),
            "metrics_count": len(collected_metrics),
            "logs": collected_logs[-50:],
            "metrics": collected_metrics
        }


    # ────────────────────────────────────────────────────────────────
    #   CONFIG - this list will be replaced by the Flask backend
    # ────────────────────────────────────────────────────────────────

    TALEND_JOB_IDS = [
        # --- TALEND JOB IDS START ---
        "69aab1120a9c058ed6a9131b",
        "69aab1040a9c058ed6a91315",
    # --- TALEND JOB IDS END ---
    ]

    # ────────────────────────────────────────────────────────────────

    previous_monitor = None

    for idx, job_id in enumerate(TALEND_JOB_IDS, start=1):
        trigger = trigger_talend_job.override(task_id=f"trigger_job_{idx}")(job_id)
        monitor = monitor_talend_execution.override(task_id=f"monitor_job_{idx}")(trigger)

        if previous_monitor:
            previous_monitor >> trigger

        previous_monitor = monitor


talend_dynamic_dag = talend_dynamic_jobs_orchestration()