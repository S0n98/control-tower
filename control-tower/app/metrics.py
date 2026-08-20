"""Bounded-cardinality Prometheus gauges (plan §4.4): ~140 pipelines x ~5
series is well under 1k series - safe, unlike labelling by per-run Spark
app UUID (plan §3.2 / §11 cardinality risk)."""
import logging

from prometheus_client import Gauge, CollectorRegistry

from .db import ct_conn

log = logging.getLogger("control_tower.metrics")

registry = CollectorRegistry()

pipeline_sla_status = Gauge(
    "pipeline_sla_status", "0=ok 1=at_risk 2=breached",
    ["pipeline_id", "tier", "domain", "owner"], registry=registry)
pipeline_last_success_timestamp_seconds = Gauge(
    "pipeline_last_success_timestamp_seconds", "Unix timestamp of last successful run",
    ["pipeline_id"], registry=registry)
pipeline_current_state = Gauge(
    "pipeline_current_state", "0=idle 1=running 2=failed",
    ["pipeline_id", "tier", "domain", "owner"], registry=registry)
dataset_freshness_seconds = Gauge(
    "dataset_freshness_seconds", "Seconds since last dataset snapshot",
    ["dataset", "domain"], registry=registry)
dq_score = Gauge(
    "dq_score", "Fraction of DQ checks passing in the last 24h",
    ["domain"], registry=registry)
pipeline_unregistered = Gauge(
    "pipeline_unregistered", "1 if a component was observed but is not in the registry",
    ["component_type", "external_id", "namespace"], registry=registry)
control_tower_up = Gauge(
    "control_tower_up", "1 if the last refresh cycle completed without raising", registry=registry)


STATE_MAP = {"running": 1, "failed": 2}


def refresh():
    try:
        with ct_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT pipeline_id, tier, domain, owner FROM pipeline WHERE enabled")
            pipelines = cur.fetchall()

            for p in pipelines:
                labels = (p["pipeline_id"], p["tier"], p["domain"], p["owner"])

                # Read via pipeline_current_sla_status (schema.sql), not
                # sla_evaluation directly - see that view's comment for why
                # "most recent row overall" is unsafe once every rule type
                # writes a row every cycle.
                cur.execute("""
                    SELECT overall_result FROM pipeline_current_sla_status
                    WHERE pipeline_id = %s AND last_evaluated_at > now() - interval '90 seconds'
                """, (p["pipeline_id"],))
                row = cur.fetchone()
                sla_val = {"ok": 0, "at_risk": 1, "breached": 2}.get(row["overall_result"], 0) if row else 0
                pipeline_sla_status.labels(*labels).set(sla_val)

                cur.execute("""
                    SELECT status, EXTRACT(EPOCH FROM started_at) AS ts FROM pipeline_run
                    WHERE pipeline_id = %s ORDER BY started_at DESC NULLS LAST LIMIT 1
                """, (p["pipeline_id"],))
                run = cur.fetchone()
                pipeline_current_state.labels(*labels).set(STATE_MAP.get(run["status"], 0) if run else 0)

                cur.execute("""
                    SELECT EXTRACT(EPOCH FROM MAX(ended_at)) AS ts FROM pipeline_run
                    WHERE pipeline_id = %s AND status = 'success'
                """, (p["pipeline_id"],))
                last_success = cur.fetchone()
                if last_success and last_success["ts"]:
                    pipeline_last_success_timestamp_seconds.labels(p["pipeline_id"]).set(last_success["ts"])

            cur.execute("SELECT dataset, freshness_s FROM dataset_state")
            for d in cur.fetchall():
                cur.execute("SELECT domain FROM pipeline p, dataset_state ds WHERE ds.dataset = %s LIMIT 1", (d["dataset"],))
                dom = cur.fetchone()
                dataset_freshness_seconds.labels(d["dataset"], (dom["domain"] if dom else "unknown")).set(d["freshness_s"] or 0)

            cur.execute("SELECT domain FROM pipeline GROUP BY domain")
            for dom in cur.fetchall():
                cur.execute("""
                    SELECT
                        count(*) FILTER (WHERE status = 'pass')::float / GREATEST(count(*), 1) AS score
                    FROM dq_result dq JOIN pipeline p ON p.pipeline_id = dq.pipeline_id
                    WHERE p.domain = %s AND dq.evaluated_at > now() - interval '24 hours'
                """, (dom["domain"],))
                score_row = cur.fetchone()
                dq_score.labels(dom["domain"]).set(score_row["score"] if score_row and score_row["score"] is not None else 1.0)

            cur.execute("""
                SELECT component_type, external_id, namespace FROM registry_reconcile_finding
                WHERE last_seen > now() - interval '10 minutes'
            """)
            pipeline_unregistered.clear()
            for f in cur.fetchall():
                pipeline_unregistered.labels(f["component_type"], f["external_id"], f["namespace"]).set(1)

        control_tower_up.set(1)
    except Exception:
        log.exception("metrics refresh failed")
        control_tower_up.set(0)
