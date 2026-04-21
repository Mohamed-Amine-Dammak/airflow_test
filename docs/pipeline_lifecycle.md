# Pipeline Lifecycle

1. Add new version in `pipelines/<id>/versions`.
2. Update manifest and runtime config.
3. Publish candidate to `dags/eval`.
4. Evaluate and score.
5. Promote accepted best candidate to `dags/prod`.
6. Roll back with manifest metadata if needed.
