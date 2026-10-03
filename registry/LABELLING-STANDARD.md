Labelling Standard
==================

Every executable unit in this environment carries these four identifiers,
plus a propagated `run_id` where the unit is one instance of a pipeline run
(batch jobs) rather than a long-lived streaming service.

| Key           | Example         | Where set in this environment |
|---------------|------------------|--------------------------------|
| `pipeline_id` | `sparkpi_sample` | SparkApplication CR label + `spark.kubernetes.{driver,executor}.label.pipeline_id` conf; FlinkDeployment CR label + `podTemplate.metadata.labels`; Airflow DAG tag `pipeline_id=<id>`; NiFi Process Group variable `pipeline_id` |
| `domain`      | `platform`       | same mechanism as above |
| `tier`        | `P1`/`P2`/`P3`   | same |
| `owner`       | `team-platform`  | same (drives Alertmanager routing via the `owner` label) |
| `run_id`      | Airflow `run_id` / Spark app UID / Flink job_id | Batch jobs (SparkApplication, NiFi single-shot flow runs) mint one run_id per execution. Long-lived streaming jobs (Flink `sample_statemachine`) do not have a run_id in the same sense - they are tracked as one continuously-running `component_run` row instead, re-evaluated on every collector poll rather than opened/closed per run. |

Applied to the pipelines actually running in this environment (see
`registry/*.yaml`):

- `sparkpi_sample` (SparkApplication, namespace `default`) - batch, has run_id
- `nifi_ingest_sample` (NiFi process group, namespace `nifi`) - continuous flow, tracked as a live component
- `trigger_flink_job` (Airflow DAG, namespace `airflow`) - batch, has run_id (dag_run_id)
- `sample_statemachine` (FlinkDeployment, namespace `flink`) - long-lived streaming job, no run_id
- `kafka_to_iceberg_demo` (FlinkDeployment, namespace `flink`) - **registered but broken** (ErrImagePull / no backing Kafka broker exists in this environment). Deliberately left registered-and-failing so the registry-reconciliation and SLA-breach alerting paths have a real target to catch, rather than only ever seeing green.

Enforcement (`pipeline_unregistered` alert): the Control Tower's registry
reconciler compares every SparkApplication/FlinkDeployment/DAG it observes
via the Kubernetes API and Airflow DB against `registry/*.yaml`. Anything
observed but not registered fires `pipeline_unregistered`. See
`control-tower/app/collectors/registry_reconciler.py`.
