"""Polls the K8s API for FlinkDeployment CRs. Streaming jobs are long-lived
(no natural per-run boundary), so each one is tracked as a single
continuously-updated component_run/pipeline_run pair keyed by the Flink
jobId, re-evaluated on every poll - see registry/LABELLING-STANDARD.md."""
import logging
from datetime import datetime, timezone

from kubernetes import client

from ..db import ct_conn
from ..registry import get_registry_components

log = logging.getLogger("control_tower.collectors.flink")

GROUP, VERSION, PLURAL = "flink.apache.org", "v1beta1", "flinkdeployments"

STATUS_MAP = {
    "RUNNING": "running", "DEPLOYING": "running",
    "CREATED": "running", "FINISHED": "success", "FAILED": "failed",
    "CANCELED": "failed", "SUSPENDED": "failed",
    # RECONCILING is ambiguous on its own (can mean "about to start" or
    # "stuck reconciling forever", e.g. kafka_to_iceberg_demo which has no
    # backing Kafka broker) - jobManagerDeploymentStatus disambiguates it below.
    "RECONCILING": "unknown",
}

UPSERT_RUN_SQL = """
INSERT INTO pipeline_run (run_id, pipeline_id, status, started_at, ended_at, trigger)
VALUES (%(run_id)s, %(pipeline_id)s, %(status)s, %(started_at)s, %(ended_at)s, 'flink-operator')
ON CONFLICT (run_id) DO UPDATE SET
    status = EXCLUDED.status, ended_at = EXCLUDED.ended_at;
"""

UPSERT_COMPONENT_SQL = """
INSERT INTO component_run (component_run_id, run_id, pipeline_id, type, external_id, status,
                            started_at, last_seen_at)
VALUES (%(component_run_id)s, %(run_id)s, %(pipeline_id)s, 'flink', %(external_id)s, %(status)s,
        %(started_at)s, now())
ON CONFLICT (component_run_id) DO UPDATE SET
    status = EXCLUDED.status, last_seen_at = now();
"""


def collect() -> list[dict]:
    reg = get_registry_components()
    observed = []
    try:
        api = client.CustomObjectsApi()
        items = api.list_cluster_custom_object(GROUP, VERSION, PLURAL).get("items", [])
    except Exception:
        log.exception("flink collector: failed to list FlinkDeployments")
        return observed

    with ct_conn() as conn, conn.cursor() as cur:
        for item in items:
            meta = item.get("metadata", {})
            ns, name = meta.get("namespace"), meta.get("name")
            observed.append({"type": "flink", "id": name, "namespace": ns})
            pipeline_id = reg.get(("flink", name))
            if not pipeline_id:
                continue

            status = item.get("status", {})
            job_status = status.get("jobStatus", {})
            state = job_status.get("state")
            deploy_status = status.get("jobManagerDeploymentStatus")
            spec_job_state = item.get("spec", {}).get("job", {}).get("state")

            if spec_job_state == "suspended":
                # A deliberate suspend (spec.job.state=suspended) tears down
                # the JobManager, which makes jobManagerDeploymentStatus read
                # MISSING - identical to a genuinely broken deployment. Check
                # spec intent first so a clean stop isn't reported as failed.
                mapped_status = "unknown"
                job_id = job_status.get("jobId", name)
            elif deploy_status in ("ERROR", "MISSING"):
                # jobManagerDeploymentStatus is the authoritative "is this
                # actually broken" signal - e.g. kafka_to_iceberg_demo has no
                # backing Kafka broker and sits in jobStatus.state=RECONCILING
                # forever while deployment status is ERROR. Surface it as
                # failed rather than trusting the ambiguous job-level state.
                mapped_status = "failed"
                job_id = job_status.get("jobId", name)
            elif not state:
                mapped_status = "unknown"
                job_id = name
            else:
                mapped_status = STATUS_MAP.get(state, "unknown")
                job_id = job_status.get("jobId", name)

            run_id = f"flink:{ns}:{name}:{job_id}"
            started_at = None
            start_time_ms = job_status.get("startTime")
            if start_time_ms and start_time_ms != "0":
                try:
                    started_at = datetime.fromtimestamp(int(start_time_ms) / 1000, tz=timezone.utc)
                except (ValueError, OverflowError):
                    pass
            ended_at = None if mapped_status == "running" else datetime.now(timezone.utc)

            cur.execute(UPSERT_RUN_SQL, {
                "run_id": run_id, "pipeline_id": pipeline_id, "status": mapped_status,
                "started_at": started_at, "ended_at": ended_at,
            })
            cur.execute(UPSERT_COMPONENT_SQL, {
                "component_run_id": run_id, "run_id": run_id, "pipeline_id": pipeline_id,
                "external_id": f"{ns}/{name}", "status": mapped_status, "started_at": started_at,
            })
    log.info("flink collector: observed %d FlinkDeployments", len(observed))
    return observed
