from airflow.decorators import task
from airflow.hooks.base import BaseHook
import requests
import time
from airflow.exceptions import AirflowException
from datetime import datetime
from airflow.utils.email import send_email

@task
def trigger_n8n_workflow(
    workflow_id: str,
    webhook_path: str,
    file_name: str = "input.csv",
    environment: str = "dev",
    conn_id: str = "n8n_local"
) -> dict:
    """
    Triggers an n8n webhook and returns basic trigger info.
    """
    conn = BaseHook.get_connection(conn_id)
    webhook_url = f"{conn.host.rstrip('/')}/webhook-test/{webhook_path}"

    headers = {"Authorization": f"Bearer {conn.password}"}
    params = {"file": file_name, "env": environment}

    try:
        response = requests.get(webhook_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        print(f"✅ Workflow {workflow_id} triggered successfully via webhook {webhook_path}")
        return {
            "workflow_id": workflow_id,
            "webhook_path": webhook_path,
            "triggered_at": datetime.utcnow().isoformat(),
            "http_status": response.status_code
        }
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger n8n workflow {workflow_id}: {str(e)}")

@task
def monitor_n8n_workflow(
    trigger_result: dict,
    poll_interval: int = 5,
    timeout: int = 300,          # increased default timeout
    max_attempts: int = 60,
    conn_id: str = "n8n_local"
) -> str:
    """
    Monitors the latest execution of a specific n8n workflow.
    Uses trigger_result only to know which workflow we're watching.
    """
    workflow_id = trigger_result["workflow_id"]

    conn = BaseHook.get_connection(conn_id)
    api_key = conn.password

    headers = {
        "X-N8N-API-KEY": api_key,
        "Accept": "application/json"
    }

    executions_url = (
        f"{conn.host.rstrip('/')}/api/v1/executions"
        f"?workflowId={workflow_id}"
        f"&limit=1"
        f"&includeData=true"
    )

    print(f"→ Monitoring workflow {workflow_id} ...")

    start_time = time.time()
    attempt = 0
    latest_exec = None

    while attempt < max_attempts and (time.time() - start_time) < timeout:
        attempt += 1
        print(f"  attempt {attempt}/{max_attempts} ... ", end="")

        try:
            resp = requests.get(executions_url, headers=headers, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            executions = data.get("data", [])

            if executions:
                latest_exec = executions[0]
                status = latest_exec["status"]
                print(f"FOUND → status = {status}")
                break
            else:
                print("still not visible")

        except requests.exceptions.RequestException as e:
            print(f"request error: {str(e)}")

        time.sleep(poll_interval)

    if not latest_exec:
        raise AirflowException(
            f"Could not find any recent execution for workflow {workflow_id} "
            f"after {attempt} attempts (~{int(time.time()-start_time)}s)"
        )

    # ── Print nice summary + detailed logs ───────────────────────────────
    _print_execution_details(latest_exec, workflow_id)

    final_status = latest_exec["status"]

    if final_status in ["error", "failed", "crashed"]:
        raise AirflowException(
            f"n8n workflow {workflow_id} finished with bad status: {final_status}"
        )

    print(f"Workflow {workflow_id} completed → status = {final_status}")
    return final_status

def _print_execution_details(exec_data: dict, workflow_id: str):
    """Helper - verbose printing of execution details"""
    print("\n" + "═" * 70)
    print(f"EXECUTION DETAILS — Workflow {workflow_id}")
    print("═" * 70)

    fields = [
        ("ID", "id"),
        ("Status", "status"),
        ("Mode", "mode"),
        ("Started", "startedAt"),
        ("Stopped", "stoppedAt"),
        ("Finished", "finished"),
        ("Retry of", "retryOf"),
    ]

    for label, key in fields:
        val = exec_data.get(key, "N/A")
        if val is None:
            val = "None"
        print(f"{label:12} : {val}")

    print("\nNode execution data:")
    print("-" * 60)

    run_data = exec_data.get("data", {}).get("resultData", {}).get("runData", {})

    if not run_data:
        print("No node run data available.")
        return

    for node_name, runs in run_data.items():
        print(f"• {node_name}  (runs: {len(runs)})")
        for i, run in enumerate(runs, 1):
            print(f"  ├─ Run #{i:2}  status: {run.get('status','?')}")
            if run.get("error"):
                err = run["error"]
                print(f"  │   ERROR: {err.get('message','<no message>')}")
            if run.get("data", {}).get("main"):
                print(f"  │   → {len(run['data']['main'])} output branch(es)")

    print("-" * 60 + "\n")



################################# TALEND ###############################################################################


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


def custom_failure_email(recipients):
    def _callback(context):
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        try_number = context["task_instance"].try_number
        exception = context.get("exception", "No exception details available")
        log_url = context["task_instance"].log_url
        execution_date = context.get("execution_date", "N/A")

        subject = f"❌ Airflow Alert | DAG '{dag_id}' Failed"

        body = f"""
        <html>
        <body style="margin:0; padding:0; background-color:#f4f6f8; font-family:Arial, sans-serif; color:#333;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:30px 0;">
                <tr>
                    <td align="center">
                        <table width="700" cellpadding="0" cellspacing="0" style="background:#ffffff; border-radius:12px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.08);">
                            
                            <!-- Header -->
                            <tr>
                                <td style="background:#d32f2f; color:#ffffff; padding:24px 32px;">
                                    <h1 style="margin:0; font-size:24px;">❌ Task Failure Detected</h1>
                                    <p style="margin:8px 0 0; font-size:14px; opacity:0.95;">
                                        Airflow detected a task failure that requires attention.
                                    </p>
                                </td>
                            </tr>

                            <!-- Body -->
                            <tr>
                                <td style="padding:32px;">
                                    <p style="font-size:16px; margin-top:0;">
                                        Hello Team,
                                    </p>

                                    <p style="font-size:15px; line-height:1.6;">
                                        A task has failed during a DAG run. Below is a summary of what happened:
                                    </p>

                                    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; margin:24px 0; font-size:14px;">
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb; width:180px;"><strong>DAG ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{dag_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Task ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{task_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Run ID</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{run_id}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Execution Date</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{execution_date}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Try Number</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb;">{try_number}</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:12px; background:#f8f9fb; border:1px solid #e5e7eb;"><strong>Exception</strong></td>
                                            <td style="padding:12px; border:1px solid #e5e7eb; color:#b42318;">{exception}</td>
                                        </tr>
                                    </table>

                                    <p style="font-size:15px; line-height:1.6;">
                                        You can review the task logs for more details by clicking the button below:
                                    </p>

                                    <p style="text-align:center; margin:30px 0;">
                                        <a href="{log_url}"
                                           style="background:#d32f2f; color:#ffffff; text-decoration:none; padding:14px 24px; border-radius:8px; font-size:15px; font-weight:bold; display:inline-block;">
                                            View Task Logs
                                        </a>
                                    </p>

                                    <p style="font-size:15px; line-height:1.6;">
                                        Please investigate this failure as soon as possible.
                                    </p>

                                    <p style="font-size:15px; margin-bottom:0;">
                                        Regards,<br>
                                        <strong>Airflow Monitoring</strong>
                                    </p>
                                </td>
                            </tr>

                            <!-- Footer -->
                            <tr>
                                <td style="background:#f8f9fb; padding:16px 32px; font-size:12px; color:#667085; text-align:center; border-top:1px solid #e5e7eb;">
                                    This is an automated notification from Apache Airflow.
                                </td>
                            </tr>

                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

        send_email(to=recipients, subject=subject, html_content=body)

    return _callback