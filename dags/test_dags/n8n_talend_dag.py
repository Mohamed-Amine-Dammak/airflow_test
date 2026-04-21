from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="master_etl_pipeline",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["master", "orchestration"]
)
def master_etl_pipeline():

    # 1️⃣ Trigger n8n DAG
    task1 = TriggerDagRunOperator(
        task_id="trigger_n8n_dag", trigger_dag_id="n8n_etl_with_monitoring",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 2️⃣ Trigger Talend DAG
    task2 = TriggerDagRunOperator(
        task_id="trigger_talend_dag", trigger_dag_id="talend_cloud_job_dynamic",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    task1 >> task2


dag = master_etl_pipeline()