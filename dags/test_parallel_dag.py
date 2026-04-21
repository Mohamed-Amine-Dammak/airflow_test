import fnmatch
import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator


@dag(
    dag_id="parallel_sensor_route_example",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["builder", "parallel", "branching"],
    description="Parallel upload + sensor, then route by filename prefix",
)
def parallel_sensor_route_example():
    # Control-flow nodes
    start = EmptyOperator(task_id="start")
    parallel_fork = EmptyOperator(task_id="parallel_fork")
    parallel_join = EmptyOperator(
        task_id="parallel_join",
        trigger_rule="all_success",
    )
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # Branch A: download then upload
    @task(task_id="azure_download")
    def azure_download() -> dict:
        filename = "cus_20260401.csv"
        local_path = f"/tmp/{filename}"
        print(f"Downloaded file to {local_path}")
        return {"filename": filename, "local_path": local_path}

    @task(task_id="sftp_upload")
    def sftp_upload(file_info: dict) -> dict:
        print(f"Uploading {file_info['local_path']} to SFTP")
        return {
            "uploaded": True,
            "filename": file_info["filename"],
            "remote_path": f"/incoming/{file_info['filename']}",
        }

    # Branch B: sensor waits for file detection
    @task(task_id="sftp_sensor")
    def sftp_sensor() -> dict:
        detected_filename = "cus_20260401.csv"
        print(f"Detected on SFTP: {detected_filename}")
        return {
            "found": True,
            "filename": detected_filename,
            "remote_path": f"/incoming/{detected_filename}",
        }

    # Conditional router
    @task.branch(task_id="conditional_router")
    def conditional_router(sensor_result: dict) -> str:
        filename = sensor_result["filename"]

        if fnmatch.fnmatch(filename, "cus*"):
            return "trigger_n8n"
        if fnmatch.fnmatch(filename, "cli*"):
            return "trigger_talend"

        # No-match path: skip all downstream branch tasks
        return None

    @task(task_id="trigger_n8n")
    def trigger_n8n(sensor_result: dict):
        print(f"Triggering n8n for file: {sensor_result['filename']}")

    @task(task_id="trigger_talend")
    def trigger_talend(sensor_result: dict):
        print(f"Triggering Talend for file: {sensor_result['filename']}")

    # Instantiate tasks
    azure_download_task = azure_download()
    sftp_upload_task = sftp_upload(azure_download_task)
    sftp_sensor_task = sftp_sensor()

    conditional_router_task = conditional_router(sftp_sensor_task)

    trigger_n8n_task = trigger_n8n(sftp_sensor_task)
    trigger_talend_task = trigger_talend(sftp_sensor_task)

    # Dependencies
    start >> parallel_fork

    parallel_fork >> azure_download_task
    azure_download_task >> sftp_upload_task >> parallel_join

    parallel_fork >> sftp_sensor_task
    sftp_sensor_task >> parallel_join

    parallel_join >> conditional_router_task

    conditional_router_task >> trigger_n8n_task >> end
    conditional_router_task >> trigger_talend_task >> end


parallel_sensor_route_example()