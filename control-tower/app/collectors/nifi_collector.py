"""Polls the NiFi REST API directly (plan §3.2: REST API gives per-processor
state, back-pressure, and bulletins - the ERROR/WARN signal the built-in
PrometheusReportingTask doesn't cover). Treated as one continuously-running
component, same as Flink streaming jobs - see registry/LABELLING-STANDARD.md."""
import logging
import os
from datetime import datetime, timezone

import httpx

from ..db import ct_conn
from ..registry import get_registry_components

log = logging.getLogger("control_tower.collectors.nifi")

NIFI_BASE_URL = os.environ.get("NIFI_BASE_URL", "https://nifi.nifi.svc.cluster.local:8443")
NIFI_USERNAME = os.environ.get("NIFI_USERNAME", "")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD", "")

# NiFi's single-user provider issues short-lived tokens (they've expired
# twice in <24h during this build) - cached in-process and refreshed on 401
# rather than depending on someone manually rotating the nifi-prometheus-token
# Secret, which is what broke the Grafana/Postgres side previously.
_token_cache = {"token": os.environ.get("NIFI_TOKEN", "")}


def _fetch_fresh_token(h: httpx.Client) -> str:
    resp = h.post(f"{NIFI_BASE_URL}/nifi-api/access/token",
                   data={"username": NIFI_USERNAME, "password": NIFI_PASSWORD})
    resp.raise_for_status()
    return resp.text

UPSERT_RUN_SQL = """
INSERT INTO pipeline_run (run_id, pipeline_id, status, started_at, ended_at, trigger)
VALUES (%(run_id)s, %(pipeline_id)s, %(status)s, %(started_at)s, NULL, 'nifi')
ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status;
"""

UPSERT_COMPONENT_SQL = """
INSERT INTO component_run (component_run_id, run_id, pipeline_id, type, external_id, status,
                            started_at, last_seen_at)
VALUES (%(component_run_id)s, %(run_id)s, %(pipeline_id)s, 'nifi', %(external_id)s, %(status)s,
        %(started_at)s, now())
ON CONFLICT (component_run_id) DO UPDATE SET status = EXCLUDED.status, last_seen_at = now();
"""

INSERT_BULLETIN_DQ_SQL = """
INSERT INTO dq_result (dataset, check_name, dimension, pipeline_id, status, observed, details)
VALUES (%(dataset)s, 'nifi_bulletin_error_count', 'custom', %(pipeline_id)s,
        CASE WHEN %(count)s > 0 THEN 'fail' ELSE 'pass' END, %(count)s, %(details)s);
"""


def collect() -> list[dict]:
    reg = get_registry_components()
    observed = [{"type": "nifi", "id": "nifi-0", "namespace": "nifi"}]
    pipeline_id = reg.get(("nifi", "nifi-0"))
    if not pipeline_id:
        return observed

    try:
        with httpx.Client(verify=False, timeout=10) as h:
            def _get(path):
                headers = {"Authorization": f"Bearer {_token_cache['token']}"}
                r = h.get(f"{NIFI_BASE_URL}{path}", headers=headers)
                if r.status_code == 401 and NIFI_USERNAME and NIFI_PASSWORD:
                    log.warning("nifi collector: token rejected (401), re-authenticating")
                    _token_cache["token"] = _fetch_fresh_token(h)
                    r = h.get(f"{NIFI_BASE_URL}{path}", headers={"Authorization": f"Bearer {_token_cache['token']}"})
                r.raise_for_status()
                return r

            cs = _get("/nifi-api/flow/status").json()["controllerStatus"]
            bulletins = _get("/nifi-api/flow/bulletin-board").json()["bulletinBoard"]["bulletins"]
    except Exception:
        log.exception("nifi collector: REST call failed")
        return observed

    error_bulletins = [b for b in bulletins if b.get("bulletin", {}).get("level") == "ERROR"]
    status = "failed" if cs["invalidCount"] > 0 or error_bulletins else "running"
    run_id = "nifi:nifi-0"
    now = datetime.now(timezone.utc)

    with ct_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_RUN_SQL, {"run_id": run_id, "pipeline_id": pipeline_id, "status": status, "started_at": now})
        cur.execute(UPSERT_COMPONENT_SQL, {
            "component_run_id": run_id, "run_id": run_id, "pipeline_id": pipeline_id,
            "external_id": "nifi-0", "status": status, "started_at": now,
        })
        import json
        cur.execute(INSERT_BULLETIN_DQ_SQL, {
            "dataset": "nifi:nifi-0", "pipeline_id": pipeline_id, "count": len(error_bulletins),
            "details": json.dumps({"controller_status": cs, "error_bulletins": error_bulletins[:5]}),
        })
    log.info("nifi collector: running=%d stopped=%d invalid=%d error_bulletins=%d",
              cs["runningCount"], cs["stoppedCount"], cs["invalidCount"], len(error_bulletins))
    return observed
