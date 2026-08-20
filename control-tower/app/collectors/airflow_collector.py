"""Polls the Airflow metadata DB directly (read-only role, see plan §3.2:
'the DB is the authoritative source for run status and duration - do not try
to rebuild it from metrics'). Writes pipeline_run + component_run rows for
every dag_run tagged with a pipeline_id in the registry."""
import logging

from ..db import airflow_conn, ct_conn

log = logging.getLogger("control_tower.collectors.airflow")

# Airflow 3.x dag table stores tags as JSON in dag_tag; we match on the
# "pipeline_id=<id>" tag convention from registry/LABELLING-STANDARD.md.
DAG_RUNS_SQL = """
SELECT dr.id, dr.dag_id, dr.run_id, dr.state, dr.start_date, dr.end_date,
       dr.run_type
FROM dag_run dr
WHERE dr.dag_id IN (
    SELECT DISTINCT dag_id FROM dag_tag WHERE name LIKE 'pipeline_id=%%'
)
ORDER BY dr.start_date DESC NULLS LAST
LIMIT 200;
"""

TAGS_SQL = "SELECT dag_id, name FROM dag_tag WHERE name LIKE 'pipeline_id=%%'"

TASK_INSTANCES_SQL = """
SELECT ti.task_id, ti.state, ti.start_date, ti.end_date, ti.try_number
FROM task_instance ti
WHERE ti.dag_id = %(dag_id)s AND ti.run_id = %(run_id)s;
"""

UPSERT_RUN_SQL = """
INSERT INTO pipeline_run (run_id, pipeline_id, status, started_at, ended_at, duration_s, trigger)
VALUES (%(run_id)s, %(pipeline_id)s, %(status)s, %(started_at)s, %(ended_at)s, %(duration_s)s, %(trigger)s)
ON CONFLICT (run_id) DO UPDATE SET
    status = EXCLUDED.status, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
    duration_s = EXCLUDED.duration_s, trigger = EXCLUDED.trigger;
"""

UPSERT_COMPONENT_SQL = """
INSERT INTO component_run (component_run_id, run_id, pipeline_id, type, external_id, status,
                            started_at, ended_at, retry_count, last_seen_at)
VALUES (%(component_run_id)s, %(run_id)s, %(pipeline_id)s, 'airflow', %(external_id)s, %(status)s,
        %(started_at)s, %(ended_at)s, %(retry_count)s, now())
ON CONFLICT (component_run_id) DO UPDATE SET
    status = EXCLUDED.status, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
    retry_count = EXCLUDED.retry_count, last_seen_at = now();
"""

STATUS_MAP = {
    "success": "success", "failed": "failed", "running": "running",
    "queued": "running", "up_for_retry": "running", "up_for_reschedule": "running",
}


def collect() -> int:
    tags: dict[str, str] = {}
    try:
        with airflow_conn() as aconn, aconn.cursor() as acur:
            acur.execute(TAGS_SQL)
            for row in acur.fetchall():
                tags[row["dag_id"]] = row["name"].split("=", 1)[1]

            if not tags:
                log.info("no dags tagged with pipeline_id= found")
                return 0

            acur.execute(DAG_RUNS_SQL)
            runs = acur.fetchall()

            n = 0
            with ct_conn() as cconn, cconn.cursor() as ccur:
                for r in runs:
                    pipeline_id = tags.get(r["dag_id"])
                    if not pipeline_id:
                        continue
                    run_id = f"airflow:{r['dag_id']}:{r['run_id']}"
                    duration_s = None
                    if r["start_date"] and r["end_date"]:
                        duration_s = (r["end_date"] - r["start_date"]).total_seconds()
                    ccur.execute(UPSERT_RUN_SQL, {
                        "run_id": run_id, "pipeline_id": pipeline_id,
                        "status": STATUS_MAP.get(r["state"], "unknown"),
                        "started_at": r["start_date"], "ended_at": r["end_date"],
                        "duration_s": duration_s, "trigger": r["run_type"],
                    })

                    acur.execute(TASK_INSTANCES_SQL, {"dag_id": r["dag_id"], "run_id": r["run_id"]})
                    for ti in acur.fetchall():
                        ccur.execute(UPSERT_COMPONENT_SQL, {
                            "component_run_id": f"{run_id}:{ti['task_id']}",
                            "run_id": run_id, "pipeline_id": pipeline_id,
                            "external_id": f"{r['dag_id']}.{ti['task_id']}",
                            "status": STATUS_MAP.get(ti["state"], "unknown"),
                            "started_at": ti["start_date"], "ended_at": ti["end_date"],
                            "retry_count": ti["try_number"] or 0,
                        })
                    n += 1
            log.info("airflow collector: synced %d dag_runs across %d tagged dags", n, len(tags))
            return n
    except Exception:
        log.exception("airflow collector failed")
        return 0
