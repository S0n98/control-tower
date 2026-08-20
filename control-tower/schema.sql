-- Control Tower schema (plan §4.2).
-- Facts about pipeline runs, SLA evaluations, DQ results and lineage edges
-- live here, not in Prometheus - see plan §1 for why.

CREATE TABLE IF NOT EXISTS pipeline (
    pipeline_id     TEXT PRIMARY KEY,
    domain          TEXT NOT NULL,
    tier            TEXT NOT NULL CHECK (tier IN ('P1', 'P2', 'P3')),
    owner           TEXT NOT NULL,
    owner_email     TEXT,
    schedule        TEXT,
    sla_json        JSONB NOT NULL DEFAULT '{}',
    components_json JSONB NOT NULL DEFAULT '[]',
    outputs_json    JSONB NOT NULL DEFAULT '[]',
    upstream_json   JSONB NOT NULL DEFAULT '[]',
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id          TEXT PRIMARY KEY,
    pipeline_id     TEXT NOT NULL REFERENCES pipeline(pipeline_id),
    status          TEXT NOT NULL CHECK (status IN ('running','success','failed','unknown')),
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    duration_s      DOUBLE PRECISION,
    trigger         TEXT,
    error_summary   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline_started ON pipeline_run (pipeline_id, started_at DESC);

CREATE TABLE IF NOT EXISTS component_run (
    component_run_id TEXT PRIMARY KEY,
    run_id            TEXT REFERENCES pipeline_run(run_id),
    pipeline_id       TEXT NOT NULL REFERENCES pipeline(pipeline_id),
    type              TEXT NOT NULL CHECK (type IN ('nifi','airflow','spark','flink')),
    external_id       TEXT NOT NULL,
    status            TEXT NOT NULL,
    started_at        TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ,
    retry_count       INT NOT NULL DEFAULT 0,
    ui_url            TEXT,
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_component_run_pipeline ON component_run (pipeline_id, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS sla_evaluation (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_id     TEXT NOT NULL REFERENCES pipeline(pipeline_id),
    run_id          TEXT REFERENCES pipeline_run(run_id),
    sla_type        TEXT NOT NULL CHECK (sla_type IN
                        ('not_started','running_too_long','finished_late',
                         'freshness_breach','dq_failed','upstream_blocked')),
    expected_at     TIMESTAMPTZ,
    actual_at       TIMESTAMPTZ,
    breach_minutes  DOUBLE PRECISION,
    result          TEXT NOT NULL CHECK (result IN ('ok','at_risk','breached')),
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sla_eval_pipeline_time ON sla_evaluation (pipeline_id, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS dq_result (
    id              BIGSERIAL PRIMARY KEY,
    dataset         TEXT NOT NULL,
    check_name      TEXT NOT NULL,
    dimension       TEXT NOT NULL CHECK (dimension IN
                        ('freshness','volume','schema','null_rate','duplicate_rate','custom')),
    run_id          TEXT,
    pipeline_id     TEXT REFERENCES pipeline(pipeline_id),
    status          TEXT NOT NULL CHECK (status IN ('pass','warn','fail')),
    observed        DOUBLE PRECISION,
    threshold       DOUBLE PRECISION,
    details         JSONB NOT NULL DEFAULT '{}',
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_result_dataset_time ON dq_result (dataset, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS dataset_state (
    dataset             TEXT PRIMARY KEY,
    last_snapshot_ts    TIMESTAMPTZ,
    row_count           BIGINT,
    bytes               BIGINT,
    freshness_s         DOUBLE PRECISION,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lineage_edge (
    id              BIGSERIAL PRIMARY KEY,
    src             TEXT NOT NULL,
    dst             TEXT NOT NULL,
    edge_type       TEXT NOT NULL,
    source_of_truth TEXT NOT NULL CHECK (source_of_truth IN ('auto','declared')),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (src, dst, edge_type)
);

CREATE TABLE IF NOT EXISTS registry_reconcile_finding (
    id              BIGSERIAL PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('unregistered')),
    component_type  TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    namespace       TEXT,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (component_type, external_id, namespace)
);

-- sla_engine.py writes one row per (pipeline_id, sla_type) every poll cycle
-- (ok or breached, never silent) so "most recent row overall" is NOT a safe
-- way to read current status - different rule types race for "most recent"
-- within the same cycle. This view is the correct read: latest row per
-- (pipeline_id, sla_type), then worst-of across rule types per pipeline.
-- Every dashboard/query that wants "is this pipeline currently breaching"
-- should read this view, not sla_evaluation directly.
CREATE OR REPLACE VIEW pipeline_current_sla_status AS
WITH latest_per_rule AS (
    SELECT DISTINCT ON (pipeline_id, sla_type)
        pipeline_id, sla_type, result, breach_minutes, evaluated_at
    FROM sla_evaluation
    ORDER BY pipeline_id, sla_type, evaluated_at DESC
)
SELECT
    pipeline_id,
    CASE
        WHEN bool_or(result = 'breached') THEN 'breached'
        WHEN bool_or(result = 'at_risk') THEN 'at_risk'
        ELSE 'ok'
    END AS overall_result,
    (array_agg(sla_type ORDER BY (result = 'breached') DESC, (result = 'at_risk') DESC, evaluated_at DESC))[1] AS worst_sla_type,
    (array_agg(breach_minutes ORDER BY (result = 'breached') DESC, (result = 'at_risk') DESC, evaluated_at DESC))[1] AS worst_breach_minutes,
    max(evaluated_at) AS last_evaluated_at
FROM latest_per_rule
GROUP BY pipeline_id;
