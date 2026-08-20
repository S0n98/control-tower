from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="trigger_flink_job",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[
        "pipeline_id=trigger_flink_job",
        "domain=platform",
        "tier=P2",
        "owner=team-platform",
    ],
)
def trigger_flink_job():
    @task
    def extract():
        return {"rows": 42}

    @task
    def submit_flink_job(payload: dict):
        # Placeholder for the real Flink job submission (kubectl/REST call to the
        # Flink Kubernetes Operator). Kept trivial here just to produce a real DAG run.
        print(f"submitting flink job with payload: {payload}")
        return True

    @task
    def notify(success: bool):
        print(f"flink job submission result: {success}")

    notify(submit_flink_job(extract()))


trigger_flink_job()
