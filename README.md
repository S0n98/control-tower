# Data Observability / Control Tower

Monitoring stack for a single-node RKE2 cluster: Prometheus, Grafana, Loki and Alertmanager,
plus a **Control Tower** service that tracks pipeline runs, SLAs, data quality and lineage
across NiFi, Spark, Flink and Airflow. Everything in this repo matches a real deployed
resource, not a template. For a fresh or air-gapped install run `scripts/offline-install.sh` (documented in `INSTALL.md`); for operational
notes and gotchas see `CLAUDE.md`.

## Why a Control Tower, not just more Prometheus

Prometheus is a time-series store for *current state of things that exist*. A pipeline
run is an *event with identity* — run_id, start, end, SLA deadline, DQ results — that
must be queryable 30+ days later. Modelling >100 runs/day of that in Prometheus causes
cardinality explosion and can't survive a 7-day retention window. So there's one new
brain-of-the-system component: **`control-tower/`**, a small FastAPI + APScheduler
service with its own Postgres, that polls Airflow's DB / NiFi's REST API / Spark &
Flink's Kubernetes CRs every 30s, evaluates SLA rules continuously, and exposes only
bounded-cardinality gauges to Prometheus (~5 series per pipeline). SLA/DQ/lineage facts
live in Postgres; Prometheus only sees the current-state summary.

```
                    Grafana (single pane of glass)
        L1 Control Tower  →  L2 per-system (NiFi/Spark/Flink/Airflow)  →  L3 Pipeline Detail
                    │                    │                        │
              Prometheus            Postgres                   Loki
             (control-tower    (control-tower-postgres:   (Fluent Bit ships
              /metrics +           pipeline registry,       pod logs here -
              blackbox +           run history, SLA          substituted for
              existing              evaluations, DQ           Elasticsearch,
              exporters)            results, lineage           see below)
                    │               edges)                        │
                    └──────────┬─────────────────────────────────┘
                          CONTROL TOWER (collector + SLA engine, polls every 30s)
                                │
        Airflow DB (read-only) · NiFi REST API · SparkApplication/FlinkDeployment
        CRs (K8s API) · Trino (DQ Tier-A checks against Iceberg)
                                │
                          Marquez (OpenLineage events + declared coarse edges)
```

## Scope

- **One cluster, one VM, 7 registered pipelines.** The architecture scales to more; only the
  fleet is small. `registry/*.yaml` is the pattern for adding real pipelines.
- **Logs:** Loki + Fluent Bit, queried through Grafana Explore.
- **Email:** MailHog (`alerting/mailhog.yaml`) catches every alert and digest email. For a real
  SMTP relay, change `smtp_smarthost` in `alerting/alertmanager-config.yaml`; nothing else changes.
- **Kafka:** no broker exists here. `kafka_to_iceberg_demo` is registered and permanently
  failing on purpose, so the SLA/alerting path has a real broken target. Do not remove it.
- **Lineage:** the Control Tower emits OpenLineage START/COMPLETE events for jobs it observes,
  plus declared coarse edges via `/lineage/ingest`, all landing in Marquez. Spark/Airflow
  OpenLineage auto-instrumentation is not installed (it would need custom images).
- **DQ Tier-A column rules** (not-null, primary key) are hardcoded per dataset in
  `dq/dq_tier_a_dag.py`; the registry has no column-schema field.
- **Log correlation:** the L3 dashboard correlates logs by namespace, since the sample
  workloads don't emit structured `run_id` logs.

## Layout

```
registry/                          # pipeline registry (git-versioned YAML)
  LABELLING-STANDARD.md            # pipeline_id/domain/tier/owner/run_id convention
  *.yaml                           # one file per registered pipeline
  dags/trigger_flink_job.py        # the sample Airflow DAG, tagged per the labelling standard

control-tower/                     # the new brain-of-the-system service
  app/                             # FastAPI + APScheduler source
    collectors/                   # airflow (DB) / nifi (REST) / spark & flink (K8s CRs) / registry_reconciler
    sla_engine.py                 # not_started / running_too_long / finished_late / freshness_breach / dq_failed / upstream_blocked
    metrics.py                    # bounded Prometheus gauges: pipeline_sla_status, pipeline_current_state, dataset_freshness_seconds, dq_score, pipeline_unregistered
    main.py                       # /metrics, /dq/ingest, /lineage/ingest, /registry/reconcile
  Dockerfile, requirements.txt, deploy.yaml, podmonitor.yaml
  postgres-values.yaml, schema.sql, seed_registry.sql
                                    # schema.sql also defines the pipeline_current_sla_status VIEW -
                                    # sla_engine.py writes one row per (pipeline_id, sla_type) every
                                    # poll cycle (ok or breached, never silent), so "most recent row
                                    # overall" is NOT a safe way to read current status once multiple
                                    # rule types are writing concurrently. Always read this view, not
                                    # sla_evaluation directly - every dashboard here does.
  blackbox-values.yaml            # HTTP health probes -> L1 infra tiles
  postgres-exporter.yaml          # monitors the metadata DBs (Control Tower's own + Airflow's)
  grafana-postgres-datasource.yaml

dq/
  dq_tier_a_dag.py                 # generic freshness/volume/schema/null/duplicate checks via Trino
  full_pipeline_demo_dag.py        # real orchestration across NiFi/Spark/Flink from one Airflow DAG -
                                    # checks NiFi's REST API, submits a fresh SparkApplication via the
                                    # K8s API, and restarts sample-statemachine via spec.restartNonce
  airflow-pipeline-rbac.yaml       # grants the airflow-scheduler ServiceAccount permission to
                                    # create/patch SparkApplication and FlinkDeployment CRs
  daily-digest-cronjob.yaml        # 07:00 email digest (CronJob) + a one-off test Job pattern

scripts/
  offline-install.sh               # automated offline install: prepare / load / install / verify
  offline.env.example              # settings for it (storage class, SMTP, NiFi, Airflow)

airflow/
  Dockerfile                       # Airflow image with trino/kubernetes/httpx baked in (offline installs)

lineage/
  marquez.yaml, marquez-web.yaml   # Marquez API (:30500) + Web UI (:30501)

alerting/
  control-tower-rules.yaml         # PipelineSLABreached / PipelineFailed / PipelineUnregistered / DomainDQScoreLow / ControlTowerDown / DeadMansSwitch
  alertmanager-config.yaml         # routes on `tier` label: P1->page(4h repeat), P2->ticket(24h), P3->digest-only(null receiver)
  mailhog.yaml                     # SMTP catcher (:30502)
  prometheus-rules.yaml            # per-engine rules (NiFi/Spark/Flink/Airflow), deployed as `pipeline-observability-rules`

grafana/dashboards/                # each file is provisioned into a Grafana folder via the
                                    # grafana_folder ConfigMap annotation (kiwigrid/k8s-sidecar's
                                    # FOLDER_ANNOTATION) - avoid "/" in folder names, the sidecar
                                    # reads it as a directory separator and silently mis-splits it.
  01-control-tower.json            # 01. Control Tower (L1): health strip (Platform/Pipelines/Data/K8s/
                                    #   Active Alerts, each clickable), 24h ops summary + trend charts,
                                    #   abnormal-pipelines-only table, active incidents table
  03-pipeline-overview.json        # 02. Pipeline Overview: ALL pipelines, filterable by domain/tier
  02-pipeline-detail.json          # 02. Pipeline Overview (L3): templated by $pipeline_id - status
                                    #   header, duration vs SLA, swimlane-style component/stage table,
                                    #   DQ checklist, logs, Marquez link
  04-pipeline-sla.json             # 02. Pipeline Overview: current SLA state per pipeline, 30d
                                    #   compliance trend, per-pipeline duration/miss-rate stats
  05-data-quality-overview.json    # 03. Data Quality: score, by-domain/dataset bars, failed checks
  06-data-freshness.json           # 03. Data Quality: expected-vs-actual freshness per dataset
  07-data-lineage.json             # 04. Data Lineage: recursive upstream/downstream chain + node
                                    #   graph per pipeline_id, from lineage_edge; links out to Marquez
  08-data-platform.json            # 06. Data Platform: Trino/MinIO/Nessie/Ranger health (blackbox
                                    #   only - see the dashboard's own scope note for why none of them
                                    #   expose native Prometheus metrics in this environment yet)
  nifi.json / spark.json / flink.json / airflow.json   # 05. Processing Engines (L2 per-system)

monitoring-cluster/                # live `helm get values` output for the 3 core charts (source of
                                    # truth - these are what's actually deployed, not hand-written)
  kube-prometheus-stack-values.yaml, loki-values.yaml, fluent-bit-values.yaml

workload-cluster/                  # PodMonitor/ServiceMonitor CRDs + sample job manifests feeding L2
  podmonitors/, servicemonitors/, spark-sample-app.yaml, flink-sample-app.yaml
```

## Access

| What | URL | Notes |
|---|---|---|
| Grafana (L1/L2/L3 dashboards) | `http://192.168.2.100:30300` | admin / PipelineObs2026! |
| Marquez lineage UI | `http://192.168.2.100:30501` | namespace `control-tower` has real OL events |
| MailHog (sent emails) | `http://192.168.2.100:30502` | every alert/digest email actually sent lands here |
| Control Tower API | `control-tower.monitoring.svc.cluster.local:8000` (in-cluster) | `/metrics`, `/dq/ingest`, `/lineage/ingest`, `/registry/reconcile` |

## Known limitations

- **Airflow KubernetesExecutor `up_for_reschedule` bug**: real scheduled/triggered DAG runs
  intermittently fail to locate their own DAG version when a fresh task pod re-parses a
  ConfigMap-mounted (non-git-sync) DAG file, and churn into `up_for_reschedule` instead of
  running. Workaround used throughout this build: `airflow exec deploy/airflow-scheduler -c
  scheduler -- airflow dags test <dag_id>`, which runs the DAG synchronously in-process,
  bypassing the executor/bundle-version lookup entirely. All 3 DAGs (`trigger_flink_job`,
  `dq_tier_a_checks`, `full_pipeline_demo`) are left **paused** for this reason - their cron
  schedules are documentation of intent, not actually driving real executor-based runs.
- **Airflow packages are installed at pod start** (`_PIP_ADDITIONAL_REQUIREMENTS`: trino,
  kubernetes, httpx), so this deployment needs PyPI access. Air-gapped installs use the
  prebuilt image from `airflow/Dockerfile` instead; see `INSTALL.md` §3.13.
- **NiFi's single-user auth tokens expire (~8-12h)** - the Control Tower's NiFi collector
  self-heals on a 401 by re-authenticating with mounted credentials (`nifi_collector.py`),
  but Prometheus's own direct PodMonitor scrape of NiFi's `/nifi-api/flow/metrics/prometheus`
  does not - if that token expires, `up{job="monitoring/nifi"}` will go stale until the
  Secret is manually rotated (`kubectl -n nifi exec nifi-0 -- curl ... /access/token`, update
  the `nifi-prometheus-token` Secret). A recurring CronJob to rotate it automatically is a
  reasonable follow-up, not built here.

## Adding a real pipeline

1. Add `registry/<pipeline_id>.yaml` (see existing files for the schema) and apply it to
   the `control-tower-registry` ConfigMap.
2. Add the four labels (`pipeline_id`/`domain`/`tier`/`owner`) to the actual K8s resource
   or Airflow DAG tags per `registry/LABELLING-STANDARD.md`.
3. The Control Tower picks it up on its next 30s poll cycle automatically - no restart
   needed (registry sync runs every cycle).
4. If it's not registered but is already running, `pipeline_unregistered` fires within
   5 minutes - that's the safety net for step 1 being skipped.
