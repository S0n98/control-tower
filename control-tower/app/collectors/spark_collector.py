"""Polls the K8s API for SparkApplication CRs (plan §3.2: KSM CR metrics give
Prometheus a gauge of .status.applicationState.state; per-run detail - the
thing Prometheus is bad at - goes to Postgres here instead)."""
import logging

from kubernetes import client

from ..db import ct_conn
from ..registry import get_registry_components

log = logging.getLogger("control_tower.collectors.spark")

GROUP, VERSION, PLURAL = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"

STATUS_MAP = {
    "RUNNING": "running", "SUBMITTED": "running", "PENDING_RERUN": "running",
    "COMPLETED": "success", "FAILED": "failed", "FAILING": "failed",
    "SUBMISSION_FAILED": "failed", "INVALIDATING": "unknown", "UNKNOWN": "unknown",
}

UPSERT_RUN_SQL = """
INSERT INTO pipeline_run (run_id, pipeline_id, status, started_at, ended_at, duration_s, trigger)
VALUES (%(run_id)s, %(pipeline_id)s, %(status)s, %(started_at)s, %(ended_at)s, %(duration_s)s, 'spark-operator')
ON CONFLICT (run_id) DO UPDATE SET
    status = EXCLUDED.status, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
    duration_s = EXCLUDED.duration_s;
"""

UPSERT_COMPONENT_SQL = """
INSERT INTO component_run (component_run_id, run_id, pipeline_id, type, external_id, status,
                            started_at, ended_at, retry_count, ui_url, last_seen_at)
VALUES (%(component_run_id)s, %(run_id)s, %(pipeline_id)s, 'spark', %(external_id)s, %(status)s,
        %(started_at)s, %(ended_at)s, %(retry_count)s, %(ui_url)s, now())
ON CONFLICT (component_run_id) DO UPDATE SET
    status = EXCLUDED.status, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
    retry_count = EXCLUDED.retry_count, ui_url = EXCLUDED.ui_url, last_seen_at = now();
"""

# If a SparkApplication CR is deleted (not just completed), the collector
# simply stops observing it - nothing above ever writes a new row for it, so
# a pipeline whose CR disappeared mid-run stays stuck showing "running"
# forever. Resolve that here: for any registered spark pipeline NOT observed
# this cycle, if its latest known run is still "running", the true outcome
# is unknowable (the resource is gone, not completed) - mark it "unknown"
# rather than leaving a stale "running" status that misrepresents reality.
RESOLVE_ORPHANED_RUNNING_SQL = """
UPDATE pipeline_run pr
SET status = 'unknown', ended_at = now()
WHERE pr.pipeline_id = %(pipeline_id)s
  AND pr.status = 'running'
  AND pr.run_id = (
      SELECT run_id FROM pipeline_run WHERE pipeline_id = %(pipeline_id)s
      ORDER BY started_at DESC NULLS LAST LIMIT 1
  );
"""


def collect() -> list[dict]:
    """Returns observed [{type, id, namespace}] for the registry reconciler,
    and writes run facts to Postgres for anything that IS registered."""
    reg = get_registry_components()
    observed = []
    try:
        api = client.CustomObjectsApi()
        items = api.list_cluster_custom_object(GROUP, VERSION, PLURAL).get("items", [])
    except Exception:
        log.exception("spark collector: failed to list SparkApplications")
        return observed

    observed_spark_ids = {item.get("metadata", {}).get("name") for item in items}
    with ct_conn() as conn, conn.cursor() as cur:
        for (comp_type, comp_id), pipeline_id in reg.items():
            if comp_type == "spark" and comp_id not in observed_spark_ids:
                cur.execute(RESOLVE_ORPHANED_RUNNING_SQL, {"pipeline_id": pipeline_id})

        for item in items:
            meta = item.get("metadata", {})
            ns, name = meta.get("namespace"), meta.get("name")
            observed.append({"type": "spark", "id": name, "namespace": ns})
            pipeline_id = reg.get(("spark", name))
            if not pipeline_id:
                continue

            status = item.get("status", {})
            state = status.get("applicationState", {}).get("state", "UNKNOWN")
            uid = meta.get("uid", name)
            run_id = f"spark:{ns}:{name}:{uid}"
            started = status.get("lastSubmissionAttemptTime") or status.get("submissionTime")
            ended = status.get("terminationTime")
            duration_s = None
            if started and ended:
                from datetime import datetime
                try:
                    s = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    e = datetime.fromisoformat(ended.replace("Z", "+00:00"))
                    duration_s = (e - s).total_seconds()
                except ValueError:
                    pass

            cur.execute(UPSERT_RUN_SQL, {
                "run_id": run_id, "pipeline_id": pipeline_id,
                "status": STATUS_MAP.get(state, "unknown"),
                "started_at": started, "ended_at": ended, "duration_s": duration_s,
            })
            cur.execute(UPSERT_COMPONENT_SQL, {
                "component_run_id": run_id, "run_id": run_id, "pipeline_id": pipeline_id,
                "external_id": f"{ns}/{name}", "status": STATUS_MAP.get(state, "unknown"),
                "started_at": started, "ended_at": ended,
                "retry_count": status.get("executionAttempts", 0),
                "ui_url": status.get("driverInfo", {}).get("webUIAddress"),
            })
    log.info("spark collector: observed %d SparkApplications", len(observed))
    return observed
