from airflow.decorators import dag, task
from utils.etl_tasks import (
    trigger_n8n_workflow, monitor_n8n_workflow,
    trigger_talend_job, monitor_talend_execution, custom_failure_email
)
from datetime import datetime, timedelta

# ... import your n8n and talend trigger/monitor functions or copy them here ...
SEQUENCE = [
# --- HYBRID SEQUENCE START ---
    {"type": "n8n", "workflow_id": "mplQ2xiNS2ERfHTU", "webhook_path": "d4727c48-61aa-4d22-a785-725bc3eff140"},
    {"type": "talend", "executable_id": "69aab1040a9c058ed6a91315"},
    {"type": "n8n", "workflow_id": "iUKeCvkK6t0JBWjB", "webhook_path": "d89dad44-3737-49cd-a6cf-327185304d35"},
    {"type": "talend", "executable_id": "69aab1120a9c058ed6a9131b"}
# --- HYBRID SEQUENCE END ---
]

# --- DEFAULT ARGS START ---
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "tags": ['etl', 'dynamic', 'monitoring'],
    "email_on_failure": True,
    "on_failure_callback": custom_failure_email(['amineelkpfe@gmail.com'])
}
# --- DEFAULT ARGS END ---


@dag(
    dag_id="hybrid_n8n_talend_orchestrator",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["talend", "n8n", "hybrid", "dynamic"],
)
def hybrid_orchestrator():

    previous_task = None

    for idx, step in enumerate(SEQUENCE, 1):
        if step["type"] == "n8n":
            trigger = trigger_n8n_workflow.override(task_id=f"n8n_trigger_{idx}")(
                workflow_id=step["workflow_id"],
                webhook_path=step["webhook_path"]
            )
            monitor = monitor_n8n_workflow.override(task_id=f"n8n_monitor_{idx}")(trigger)

        elif step["type"] == "talend":
            trigger = trigger_talend_job.override(task_id=f"talend_trigger_{idx}")(
                executable_id=step["executable_id"]
            )
            monitor = monitor_talend_execution.override(task_id=f"talend_monitor_{idx}")(trigger)

        if previous_task:
            previous_task >> trigger

        previous_task = monitor

hybrid_dag = hybrid_orchestrator()