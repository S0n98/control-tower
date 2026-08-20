import logging
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel

from . import metrics
from .db import ct_conn
from .scheduler import start as start_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("control_tower.main")

_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = start_scheduler()
    yield
    if _scheduler:
        _scheduler.shutdown(wait=False)


app = FastAPI(title="Data Observability Control Tower", lifespan=lifespan)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/metrics")
def metrics_endpoint():
    return PlainTextResponse(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)


class DQResult(BaseModel):
    dataset: str
    check_name: str
    dimension: Literal["freshness", "volume", "schema", "null_rate", "duplicate_rate", "custom"]
    run_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    status: Literal["pass", "warn", "fail"]
    observed: Optional[float] = None
    threshold: Optional[float] = None
    details: dict = {}


@app.post("/dq/ingest")
def dq_ingest(result: DQResult):
    import json
    with ct_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq_result (dataset, check_name, dimension, run_id, pipeline_id, status, observed, threshold, details)
            VALUES (%(dataset)s, %(check_name)s, %(dimension)s, %(run_id)s, %(pipeline_id)s, %(status)s, %(observed)s, %(threshold)s, %(details)s)
        """, {**result.model_dump(), "details": json.dumps(result.details)})

        if result.dimension == "freshness" and result.observed is not None:
            cur.execute("""
                INSERT INTO dataset_state (dataset, freshness_s, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (dataset) DO UPDATE SET freshness_s = EXCLUDED.freshness_s, updated_at = now()
            """, (result.dataset, result.observed))
        if result.dimension == "volume" and result.observed is not None:
            cur.execute("""
                INSERT INTO dataset_state (dataset, row_count, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (dataset) DO UPDATE SET row_count = EXCLUDED.row_count, updated_at = now()
            """, (result.dataset, int(result.observed)))
    return {"status": "recorded"}


class LineageEdge(BaseModel):
    src: str
    dst: str
    edge_type: str
    source_of_truth: Literal["auto", "declared"]


@app.post("/lineage/ingest")
def lineage_ingest(edges: list[LineageEdge]):
    with ct_conn() as conn, conn.cursor() as cur:
        for e in edges:
            cur.execute("""
                INSERT INTO lineage_edge (src, dst, edge_type, source_of_truth, last_seen)
                VALUES (%(src)s, %(dst)s, %(edge_type)s, %(source_of_truth)s, now())
                ON CONFLICT (src, dst, edge_type) DO UPDATE SET
                    source_of_truth = EXCLUDED.source_of_truth, last_seen = now()
            """, e.model_dump())
    return {"status": "recorded", "count": len(edges)}


@app.get("/registry/reconcile")
def registry_reconcile_findings():
    with ct_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT component_type, external_id, namespace, first_seen, last_seen
            FROM registry_reconcile_finding WHERE last_seen > now() - interval '10 minutes'
        """)
        return cur.fetchall()
