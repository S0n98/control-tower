"""Enforces the labelling standard (plan §2): anything observed running in
the cluster/Airflow DB that isn't in the registry fires pipeline_unregistered
via metrics.py. This is what stops the registry silently drifting from
reality as new jobs get added."""
import logging

from ..db import ct_conn
from ..registry import get_registry_components

log = logging.getLogger("control_tower.collectors.registry_reconciler")

UPSERT_FINDING_SQL = """
INSERT INTO registry_reconcile_finding (kind, component_type, external_id, namespace)
VALUES ('unregistered', %(type)s, %(id)s, %(namespace)s)
ON CONFLICT (component_type, external_id, namespace) DO UPDATE SET last_seen = now();
"""

DELETE_STALE_SQL = """
DELETE FROM registry_reconcile_finding
WHERE (component_type, external_id, namespace) NOT IN (
    SELECT unnest(%(types)s::text[]), unnest(%(ids)s::text[]), unnest(%(namespaces)s::text[])
) AND last_seen < now() - interval '1 hour';
"""


def reconcile(observed: list[dict]) -> int:
    reg = get_registry_components()
    unregistered = [o for o in observed if (o["type"], o["id"]) not in reg]
    with ct_conn() as conn, conn.cursor() as cur:
        for o in unregistered:
            cur.execute(UPSERT_FINDING_SQL, {"type": o["type"], "id": o["id"], "namespace": o["namespace"]})
    if unregistered:
        log.warning("registry reconciler: %d unregistered components: %s",
                    len(unregistered), [(o["type"], o["id"]) for o in unregistered])
    return len(unregistered)
