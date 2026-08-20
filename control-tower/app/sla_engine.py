"""SLA engine (plan §4.4). Evaluates continuously, not just at end-of-run.
Each rule writes a row into sla_evaluation (history/reporting - the reason
these facts live in Postgres, not Prometheus, see plan §1) and the latest
per-pipeline result also drives the bounded-cardinality /metrics gauges in
metrics.py."""
import logging
from datetime import datetime, timedelta, timezone

from croniter import croniter

from .db import ct_conn

log = logging.getLogger("control_tower.sla_engine")

INSERT_EVAL_SQL = """
INSERT INTO sla_evaluation (pipeline_id, run_id, sla_type, expected_at, actual_at, breach_minutes, result)
VALUES (%(pipeline_id)s, %(run_id)s, %(sla_type)s, %(expected_at)s, %(actual_at)s, %(breach_minutes)s, %(result)s);
"""


def _fetch_pipelines(cur):
    cur.execute("SELECT pipeline_id, tier, owner, schedule, sla_json, upstream_json FROM pipeline WHERE enabled")
    return cur.fetchall()


def _latest_run(cur, pipeline_id):
    cur.execute("""
        SELECT run_id, status, started_at, ended_at
        FROM pipeline_run WHERE pipeline_id = %s
        ORDER BY started_at DESC NULLS LAST LIMIT 1
    """, (pipeline_id,))
    return cur.fetchone()


def _eval_not_started(cur, p, now):
    # Every rule below always writes a row (ok or breached) whenever it
    # APPLIES to the pipeline, not just when breaching - a rule that only
    # ever inserts on breach leaves "most recent row" stuck on a stale
    # breach forever after the pipeline recovers, since nothing ever writes
    # a resolving 'ok' row. Every consumer (metrics.py, dashboards) reads
    # "most recent row" as current truth, so that invariant has to hold here.
    schedule = p["schedule"]
    sla = p["sla_json"] or {}
    if not schedule or schedule in ("manual", "continuous") or "start_by" not in sla:
        return
    try:
        itr = croniter(schedule, now)
        expected = itr.get_prev(datetime)
    except (ValueError, KeyError):
        return
    grace_minutes = sla.get("start_grace_minutes", 15)
    if now < expected + timedelta(minutes=grace_minutes):
        return
    cur.execute("""
        SELECT 1 FROM pipeline_run WHERE pipeline_id = %s AND started_at >= %s LIMIT 1
    """, (p["pipeline_id"], expected))
    if cur.fetchone():
        cur.execute(INSERT_EVAL_SQL, {
            "pipeline_id": p["pipeline_id"], "run_id": None, "sla_type": "not_started",
            "expected_at": expected, "actual_at": now, "breach_minutes": None, "result": "ok",
        })
        return
    breach_minutes = (now - expected).total_seconds() / 60
    cur.execute(INSERT_EVAL_SQL, {
        "pipeline_id": p["pipeline_id"], "run_id": None, "sla_type": "not_started",
        "expected_at": expected, "actual_at": None, "breach_minutes": breach_minutes,
        "result": "breached",
    })


def _eval_running_too_long(cur, p, now, run):
    sla = p["sla_json"] or {}
    max_dur = sla.get("max_duration_minutes")
    if not run or not max_dur:
        return
    if run["status"] != "running" or not run["started_at"]:
        # Nothing currently running for this pipeline, so "running too long"
        # is trivially not violated right now - resolves any prior breach.
        cur.execute(INSERT_EVAL_SQL, {
            "pipeline_id": p["pipeline_id"], "run_id": run["run_id"], "sla_type": "running_too_long",
            "expected_at": None, "actual_at": now, "breach_minutes": None, "result": "ok",
        })
        return
    running_minutes = (now - run["started_at"]).total_seconds() / 60
    if running_minutes <= max_dur:
        cur.execute(INSERT_EVAL_SQL, {
            "pipeline_id": p["pipeline_id"], "run_id": run["run_id"], "sla_type": "running_too_long",
            "expected_at": run["started_at"] + timedelta(minutes=max_dur), "actual_at": now,
            "breach_minutes": None, "result": "ok",
        })
        return
    cur.execute(INSERT_EVAL_SQL, {
        "pipeline_id": p["pipeline_id"], "run_id": run["run_id"], "sla_type": "running_too_long",
        "expected_at": run["started_at"] + timedelta(minutes=max_dur), "actual_at": now,
        "breach_minutes": running_minutes - max_dur, "result": "breached",
    })


def _eval_finished_late(cur, p, now, run):
    sla = p["sla_json"] or {}
    finish_by = sla.get("finish_by")
    if not run or not finish_by or run["status"] != "success" or not run["ended_at"]:
        return
    finish_dt = run["ended_at"].replace(
        hour=int(finish_by.split(":")[0]), minute=int(finish_by.split(":")[1]), second=0, microsecond=0)
    if run["ended_at"] <= finish_dt:
        cur.execute(INSERT_EVAL_SQL, {
            "pipeline_id": p["pipeline_id"], "run_id": run["run_id"], "sla_type": "finished_late",
            "expected_at": finish_dt, "actual_at": run["ended_at"], "breach_minutes": None, "result": "ok",
        })
        return
    breach_minutes = (run["ended_at"] - finish_dt).total_seconds() / 60
    cur.execute(INSERT_EVAL_SQL, {
        "pipeline_id": p["pipeline_id"], "run_id": run["run_id"], "sla_type": "finished_late",
        "expected_at": finish_dt, "actual_at": run["ended_at"],
        "breach_minutes": breach_minutes, "result": "breached",
    })


def _eval_freshness(cur, p, now):
    sla = p["sla_json"] or {}
    target = sla.get("freshness_target_minutes")
    if not target:
        return
    cur.execute("""
        SELECT outputs_json FROM pipeline WHERE pipeline_id = %s
    """, (p["pipeline_id"],))
    row = cur.fetchone()
    outputs = row["outputs_json"] if row else []
    for out in outputs:
        dataset = out.get("iceberg")
        if not dataset:
            continue
        cur.execute("SELECT freshness_s FROM dataset_state WHERE dataset = %s", (dataset,))
        ds = cur.fetchone()
        if not ds or ds["freshness_s"] is None:
            continue
        freshness_minutes = ds["freshness_s"] / 60
        result = "breached" if freshness_minutes > target else "ok"
        cur.execute(INSERT_EVAL_SQL, {
            "pipeline_id": p["pipeline_id"], "run_id": None, "sla_type": "freshness_breach",
            "expected_at": None, "actual_at": now,
            "breach_minutes": (freshness_minutes - target) if result == "breached" else None, "result": result,
        })


def _eval_dq_failed(cur, p, now):
    cur.execute("""
        SELECT check_name FROM dq_result
        WHERE pipeline_id = %s AND status = 'fail' AND evaluated_at > now() - interval '1 hour'
        LIMIT 1
    """, (p["pipeline_id"],))
    result = "breached" if cur.fetchone() else "ok"
    cur.execute(INSERT_EVAL_SQL, {
        "pipeline_id": p["pipeline_id"], "run_id": None, "sla_type": "dq_failed",
        "expected_at": None, "actual_at": now, "breach_minutes": None, "result": result,
    })


def _eval_upstream_blocked(cur, p, now):
    upstreams = [u if isinstance(u, str) else u.get("pipeline_id") for u in (p["upstream_json"] or [])]
    upstreams = [u for u in upstreams if u]
    if not upstreams:
        return
    blocked = False
    for up_id in upstreams:
        cur.execute("""
            SELECT status FROM pipeline_run WHERE pipeline_id = %s
            ORDER BY started_at DESC NULLS LAST LIMIT 1
        """, (up_id,))
        up_run = cur.fetchone()
        if up_run and up_run["status"] == "failed":
            blocked = True
            break
    cur.execute(INSERT_EVAL_SQL, {
        "pipeline_id": p["pipeline_id"], "run_id": None, "sla_type": "upstream_blocked",
        "expected_at": None, "actual_at": now, "breach_minutes": None,
        "result": "breached" if blocked else "ok",
    })


def evaluate() -> int:
    now = datetime.now(timezone.utc)
    n = 0
    with ct_conn() as conn, conn.cursor() as cur:
        pipelines = _fetch_pipelines(cur)
        for p in pipelines:
            run = _latest_run(cur, p["pipeline_id"])
            _eval_not_started(cur, p, now)
            _eval_running_too_long(cur, p, now, run)
            _eval_finished_late(cur, p, now, run)
            _eval_freshness(cur, p, now)
            _eval_dq_failed(cur, p, now)
            _eval_upstream_blocked(cur, p, now)
            n += 1
    log.info("sla engine: evaluated %d pipelines", n)
    return n
