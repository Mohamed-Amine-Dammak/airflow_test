# Rollback Strategy

Rollback uses `previous_prod_version` and version metadata in manifest.
Rebuild `dags/prod/<logical_dag_id>.py` from prior accepted version.
