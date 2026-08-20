"""Loads registry/*.yaml (git-versioned pipeline registry, plan §4.1) into
the `pipeline` table on startup and on a periodic refresh, so the registry
YAML files (not manual SQL) are the source of truth going forward."""
import glob
import json
import logging
import os

import yaml

from .db import ct_conn

log = logging.getLogger("control_tower.registry")

REGISTRY_DIR = os.environ.get("REGISTRY_DIR", "/registry")

UPSERT_SQL = """
INSERT INTO pipeline (pipeline_id, domain, tier, owner, owner_email, schedule,
                       sla_json, components_json, outputs_json, upstream_json)
VALUES (%(pipeline_id)s, %(domain)s, %(tier)s, %(owner)s, %(owner_email)s, %(schedule)s,
        %(sla_json)s, %(components_json)s, %(outputs_json)s, %(upstream_json)s)
ON CONFLICT (pipeline_id) DO UPDATE SET
    domain = EXCLUDED.domain, tier = EXCLUDED.tier, owner = EXCLUDED.owner,
    owner_email = EXCLUDED.owner_email, schedule = EXCLUDED.schedule,
    sla_json = EXCLUDED.sla_json, components_json = EXCLUDED.components_json,
    outputs_json = EXCLUDED.outputs_json, upstream_json = EXCLUDED.upstream_json,
    updated_at = now();
"""


def load_registry() -> list[dict]:
    entries = []
    for path in sorted(glob.glob(os.path.join(REGISTRY_DIR, "*.yaml"))):
        with open(path) as f:
            doc = yaml.safe_load(f)
        if not doc or "pipeline_id" not in doc:
            continue
        entries.append(doc)
    return entries


def sync_registry_to_db() -> int:
    entries = load_registry()
    with ct_conn() as conn, conn.cursor() as cur:
        for doc in entries:
            cur.execute(UPSERT_SQL, {
                "pipeline_id": doc["pipeline_id"],
                "domain": doc.get("domain", "unknown"),
                "tier": doc.get("tier", "P3"),
                "owner": doc.get("owner", "unowned"),
                "owner_email": doc.get("owner_email"),
                "schedule": doc.get("schedule"),
                "sla_json": json.dumps(doc.get("sla", {})),
                "components_json": json.dumps(doc.get("components", [])),
                "outputs_json": json.dumps(doc.get("outputs", [])),
                "upstream_json": json.dumps(doc.get("upstream", [])),
            })
    log.info("registry sync: %d pipelines loaded from %s", len(entries), REGISTRY_DIR)
    return len(entries)


def get_registry_components() -> dict[tuple[str, str], str]:
    """Returns {(type, external_id): pipeline_id} for reconciliation."""
    out = {}
    for doc in load_registry():
        for c in doc.get("components", []):
            out[(c["type"], c["id"])] = doc["pipeline_id"]
    return out
