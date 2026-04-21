from airflow import DAG
from airflow.decorators import task
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
from pathlib import Path
import fnmatch
import os

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="y2_sftp_to_azure_blob",
    start_date=datetime(2026, 3, 17),
    schedule=None,   # ou un schedule si tu veux le lancer périodiquement
    catchup=False,
    default_args=default_args,
    tags=["y2", "sftp", "azure", "file-ingestion"],
) as dag:

    # =========================
    # VARIABLES
    # =========================
    sftp_conn_id = "y2_sftp"
    wasb_conn_id = "azure_blob_default"

    remote_folder = "/y2/outbound"
    file_pattern = "CUSTOMER_EXPORT_*.csv"

    local_dir = "/opt/airflow/tmp/y2"
    azure_container = "raw"
    azure_blob_prefix = "y2/customer_exports"

    wait_for_file = SFTPSensor(
        task_id="wait_for_file",
        sftp_conn_id=sftp_conn_id,
        path=remote_folder,
        file_pattern=file_pattern,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        deferrable=True,
    )

    @task
    def find_matching_file() -> dict:
        """
        Cherche le premier fichier correspondant au pattern dans le dossier SFTP.
        Retourne le chemin distant complet et le nom de fichier.
        """
        hook = SFTPHook(ssh_conn_id=sftp_conn_id)

        with hook.get_managed_conn() as conn:
            sftp = conn
            entries = sftp.listdir(remote_folder)

        matches = sorted([f for f in entries if fnmatch.fnmatch(f, file_pattern)])

        if not matches:
            raise AirflowException(
                f"Aucun fichier trouvé dans {remote_folder} avec le pattern {file_pattern}"
            )

        filename = matches[0]
        remote_path = f"{remote_folder}/{filename}"

        return {
            "filename": filename,
            "remote_path": remote_path,
        }

    @task
    def download_file(file_info: dict) -> dict:
        """
        Télécharge le fichier SFTP vers un répertoire local Airflow.
        """
        Path(local_dir).mkdir(parents=True, exist_ok=True)

        filename = file_info["filename"]
        remote_path = file_info["remote_path"]
        local_path = os.path.join(local_dir, filename)

        hook = SFTPHook(ssh_conn_id=sftp_conn_id)
        hook.retrieve_file(remote_full_path=remote_path, local_full_path=local_path)

        return {
            "filename": filename,
            "remote_path": remote_path,
            "local_path": local_path,
        }

    @task
    def upload_to_azure(file_info: dict) -> dict:
        """
        Envoie le fichier local vers Azure Blob Storage.
        """
        filename = file_info["filename"]
        local_path = file_info["local_path"]
        blob_name = f"{azure_blob_prefix}/{filename}"

        wasb = WasbHook(wasb_conn_id=wasb_conn_id)

        with open(local_path, "rb") as data:
            wasb.upload(
                container_name=azure_container,
                blob_name=blob_name,
                data=data,
                overwrite=True,
            )

        return {
            "filename": filename,
            "local_path": local_path,
            "container": azure_container,
            "blob_name": blob_name,
        }

    @task
    def cleanup_local_file(file_info: dict):
        """
        Supprime le fichier local après upload si besoin.
        """
        local_path = file_info["local_path"]
        if os.path.exists(local_path):
            os.remove(local_path)

    matched_file = find_matching_file()
    downloaded = download_file(matched_file)
    uploaded = upload_to_azure(downloaded)
    cleanup = cleanup_local_file(downloaded)

    wait_for_file >> matched_file >> downloaded >> uploaded >> cleanup