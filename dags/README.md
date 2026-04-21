# DAG Deployment Views

Airflow reads this folder (`/opt/airflow/dags` in containerized setups).

- `dags/eval/`: active candidate DAGs under evaluation.
- `dags/prod/`: active champion DAGs serving production.

Historical DAG logic is not stored here. Version history lives in `pipelines/<logical_dag_id>/versions/`.
