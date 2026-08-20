"""Real end-to-end orchestration across NiFi, Spark, and Flink, all
triggered from a single Airflow DAG run - the shape the original plan
describes (Airflow orchestrating NiFi -> Spark -> Flink), not just the
placeholder submit_flink_job task the earlier sample DAG had.

Each task talks to the real system directly:
  - check_nifi_flow: REST call to NiFi's own API, asserts the ingest flow
    is actually running and reports live flowfile/queue counts.
  - submit_spark_job: deletes+recreates the sparkpi-sample SparkApplication
    CR via the Kubernetes API - a genuinely fresh Spark run, not a status
    check.
  - restart_flink_job: bumps sample-statemachine's spec.restartNonce, which
    the Flink Kubernetes Operator treats as a real trigger to restart the
    streaming job (this is the operator's documented way to force a
    redeploy without changing the job's jar/config).
  - notify: summarizes what happened.

Requires the `kubernetes` package (see the _PIP_ADDITIONAL_REQUIREMENTS
patch on the scheduler Deployment) and the airflow-spark-launcher-role /
airflow-flink-launcher-role RBAC (dq/airflow-pipeline-rbac.yaml).
"""
import time
from datetime import datetime

import urllib3
from airflow.sdk import dag, task

urllib3.disable_warnings()

NIFI_BASE_URL = "https://nifi.nifi.svc.cluster.local:8443"
NIFI_USERNAME = "admin"
NIFI_PASSWORD = "PipelineObsNiFi2026!"

SPARK_APP_MANIFEST = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "sparkpi-sample",
        "namespace": "default",
        "labels": {"system": "spark", "pipeline_id": "sparkpi_sample",
                   "domain": "platform", "tier": "P3", "owner": "team-platform"},
    },
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "apache/spark:3.5.1",
        "imagePullPolicy": "IfNotPresent",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar",
        "arguments": ["300000"],
        "sparkVersion": "3.5.1",
        "restartPolicy": {"type": "OnFailure", "onFailureRetries": 2, "onFailureRetryInterval": 30,
                           "onSubmissionFailureRetries": 2, "onSubmissionFailureRetryInterval": 30},
        "sparkConf": {
            "spark.ui.prometheus.enabled": "true",
            "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
            "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
        },
        "driver": {"cores": 1, "memory": "1024m", "serviceAccount": "spark",
                   "labels": {"system": "spark", "pipeline_id": "sparkpi_sample",
                              "domain": "platform", "tier": "P3", "owner": "team-platform"}},
        "executor": {"cores": 1, "instances": 2,
                     "labels": {"system": "spark", "pipeline_id": "sparkpi_sample",
                                "domain": "platform", "tier": "P3", "owner": "team-platform"}},
    },
}


@dag(
    dag_id="full_pipeline_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipeline_id=full_pipeline_demo", "domain=platform", "tier=P1", "owner=team-platform"],
)
def full_pipeline_demo():

    @task
    def check_nifi_flow():
        import httpx
        with httpx.Client(verify=False, timeout=10) as h:
            token = h.post(f"{NIFI_BASE_URL}/nifi-api/access/token",
                            data={"username": NIFI_USERNAME, "password": NIFI_PASSWORD}).text
            headers = {"Authorization": f"Bearer {token}"}
            status = h.get(f"{NIFI_BASE_URL}/nifi-api/flow/status", headers=headers).json()["controllerStatus"]
            print(f"NiFi controller status: running={status['runningCount']} "
                  f"stopped={status['stoppedCount']} invalid={status['invalidCount']} "
                  f"queued_flowfiles={status['flowFilesQueued']}")
            assert status["runningCount"] > 0, "NiFi ingest flow is not running"
            return status

    @task
    def submit_spark_job():
        from kubernetes import client, config
        config.load_incluster_config()
        api = client.CustomObjectsApi()
        group, version, plural, ns, name = "sparkoperator.k8s.io", "v1beta2", "sparkapplications", "default", "sparkpi-sample"
        try:
            api.delete_namespaced_custom_object(group, version, ns, plural, name)
            for _ in range(30):
                try:
                    api.get_namespaced_custom_object(group, version, ns, plural, name)
                    time.sleep(2)
                except Exception:
                    break
        except Exception:
            pass
        created = api.create_namespaced_custom_object(group, version, ns, plural, SPARK_APP_MANIFEST)
        print(f"submitted fresh SparkApplication: {created['metadata']['name']} uid={created['metadata']['uid']}")
        return created["metadata"]["uid"]

    @task
    def restart_flink_job():
        from kubernetes import client, config
        config.load_incluster_config()
        api = client.CustomObjectsApi()
        group, version, plural, ns, name = "flink.apache.org", "v1beta1", "flinkdeployments", "flink", "sample-statemachine"
        nonce = int(time.time())
        # spec.job.state must be "running" for restartNonce to actually do
        # anything - if the job was previously suspended (e.g. by the
        # Control Tower's sample-cleanup routine), bumping the nonce alone
        # leaves it suspended forever. Always un-suspend explicitly.
        patch = {"spec": {"job": {"state": "running"}, "restartNonce": nonce}}
        api.patch_namespaced_custom_object(group, version, ns, plural, name, patch)
        print(f"triggered restart of FlinkDeployment/{name} via restartNonce={nonce} (state=running)")
        return nonce

    @task
    def notify(nifi_status: dict, spark_uid: str, flink_nonce: int):
        print("Full pipeline demo run complete:")
        print(f"  NiFi: {nifi_status['runningCount']} processors running, "
              f"{nifi_status['flowFilesQueued']} flowfiles queued")
        print(f"  Spark: submitted sparkpi-sample uid={spark_uid}")
        print(f"  Flink: restarted sample-statemachine nonce={flink_nonce}")

    n = check_nifi_flow()
    s = submit_spark_job()
    f = restart_flink_job()
    notify(n, s, f)


full_pipeline_demo()
