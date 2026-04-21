from airflow.sdk import dag
from datetime import datetime, timedelta
from templates.n8n_ops import (
    make_resolve_workflow_task,
    make_trigger_n8n_task,
    make_monitor_n8n_task,
)
from templates.talend_ops import (
    make_get_executable_id_task,
    make_trigger_job_task,
    make_monitor_job_task,
)


def custom_failure_email(email_list):
    def _callback(context):
        print(f"Failure/retry callback triggered. Emails: {email_list}")
    return _callback


def build_hybrid_dag(config: dict):
    dag_id = config["dag_id"]
    start_date = datetime.fromisoformat(config["start_date"])
    tags = config.get("tags", ["hybrid", "talend", "n8n"])
    talend_connection_id = config.get("talend_connection_id", "talend_cloud")
    n8n_connection_id = config.get("n8n_connection_id", "n8n_local")
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
    def hybrid_dynamic_workflow_dag():
        resolve_workflow = make_resolve_workflow_task(n8n_connection_id)
        trigger_n8n_workflow = make_trigger_n8n_task(n8n_connection_id)
        monitor_n8n_workflow = make_monitor_n8n_task(n8n_connection_id)

        get_executable_id = make_get_executable_id_task(talend_connection_id)
        trigger_job = make_trigger_job_task(talend_connection_id)
        monitor_job = make_monitor_job_task(talend_connection_id)

        task_map = {}

        for job_cfg in jobs:
            job_id = job_cfg["id"]
            step_type = job_cfg["type"]

            if step_type == "n8n":
                resolved = resolve_workflow.override(
                    task_id=f"resolve_workflow__{job_id}"
                )(
                    workflow_name=job_cfg["workflow_name"],
                    sleep_after=job_cfg.get("sleep_after_lookup", 10),
                )

                triggered = trigger_n8n_workflow.override(
                    task_id=f"trigger_n8n_workflow__{job_id}"
                )(
                    resolved=resolved,
                    file_name=job_cfg.get("file_name", "input.csv"),
                    environment=job_cfg.get("environment", "dev"),
                    use_test_webhook=job_cfg.get("use_test_webhook", True),
                    sleep_after=job_cfg.get("sleep_after_trigger", 10),
                )

                monitored = monitor_n8n_workflow.override(
                    task_id=f"monitor_n8n_workflow__{job_id}"
                )(
                    trigger_result=triggered,
                    poll_interval=job_cfg.get("poll_interval", 5),
                    timeout=job_cfg.get("timeout", 300),
                    max_attempts=job_cfg.get("max_attempts", 60),
                    sleep_after=job_cfg.get("sleep_after_monitor", 10),
                )

                task_map[job_id] = {
                    "entry": resolved,
                    "monitor": monitored,
                }

            elif step_type == "talend":
                executable_id = get_executable_id.override(
                    task_id=f"get_executable_id__{job_id}"
                )(
                    job_name=job_cfg["job_name"],
                    sleep_after=job_cfg.get("sleep_after_lookup", 5),
                )

                execution_id = trigger_job.override(
                    task_id=f"trigger_job__{job_id}"
                )(
                    executable_id=executable_id,
                    sleep_after=job_cfg.get("sleep_after_trigger", 5),
                )

                monitored = monitor_job.override(
                    task_id=f"monitor_job__{job_id}"
                )(
                    execution_id=execution_id,
                    poll_interval=job_cfg.get("poll_interval", 10),
                    timeout=job_cfg.get("timeout", 50),
                    sleep_after=job_cfg.get("sleep_after_monitor", 5),
                )

                task_map[job_id] = {
                    "entry": executable_id,
                    "monitor": monitored,
                }

        for job_cfg in jobs:
            current_job_id = job_cfg["id"]
            for upstream_id in job_cfg.get("depends_on", []):
                task_map[upstream_id]["monitor"] >> task_map[current_job_id]["entry"]

    return hybrid_dynamic_workflow_dag()