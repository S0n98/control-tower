# INSTALL.md — Offline Installation Guide

This installs the **monitoring / Control Tower stack** described in `README.md` into an
air-gapped Kubernetes cluster. It does not install the data platform being observed
(NiFi, Airflow, Spark Operator, Flink Kubernetes Operator, Trino, Nessie, MinIO) — that's
a separate, product-specific concern; see "Data platform prerequisite" below for what
needs to exist first and how to check.

Every command here was run against this repo's own deployment (a single-node RKE2
cluster). The result is offline-safe: nothing fetches from the internet at runtime (§0.3).

## Automated install

`scripts/offline-install.sh` runs this whole guide as a script. Copy
`scripts/offline.env.example` to `offline.env` and edit it, then:

```bash
# builder (internet): charts, images, custom builds -> /root/monitor-offline.tar.gz
./scripts/offline-install.sh prepare

# every node (air-gapped), from the extracted bundle directory:
./repo/scripts/offline-install.sh load

# any host with kubectl:
./repo/scripts/offline-install.sh install            # all steps, in order
./repo/scripts/offline-install.sh install dq         # or re-run one step
./repo/scripts/offline-install.sh verify             # §4 checklist
```

The script finds chart images with `helm template` instead of a fixed list, sets Grafana's
folder provisioning (§3.4) at install time instead of patching it afterwards, and never
runs `helm upgrade`. The sections below document what each step does and are the
reference if a step fails.

## §0. Before you start

### 0.1 What you need on hand

| Item | Used for |
|---|---|
| A Kubernetes cluster (1.24+), reachable via `kubectl` | Everything. Built and tested on RKE2 single-node; any distro with a default `StorageClass` supporting `ReadWriteOnce` works. |
| A machine **with internet access** ("the builder") that can run `docker` and `helm` | Pulling images, fetching charts, building the two custom images (§1) |
| A way to move files from the builder to the air-gapped cluster's nodes (USB drive, one-way file transfer, internal artifact repository — whatever your environment allows) | Moving the image tarballs and chart archives across the gap |
| `helm` 3 CLI on the target (or at least on one machine with `kubectl` access to it) | Installing kube-prometheus-stack/Loki/Fluent Bit from local chart archives — **not** used for anything after initial install; see CLAUDE.md on why Helm upgrades are avoided here after day one |
| Shell access to every cluster node (or a DaemonSet-based image-loading job — not covered here, this guide assumes small node counts and manual `ctr import`) | Importing images into each node's containerd |

### 0.2 Data platform prerequisite

The Control Tower observes NiFi, Airflow (KubernetesExecutor), Spark Operator, and the
Flink Kubernetes Operator, and optionally runs DQ Tier-A checks against Trino+Nessie+MinIO.
**These must already be running** before §3.7 onward will have anything real to collect.
If they don't exist yet, this repo doesn't build them for you — this build's own copies
were installed via Helm charts (`spark-operator/spark-operator`, Apache's
`flink-kubernetes-operator` chart, `apache-airflow/airflow`, `trinodb/trino`,
`projectnessie/nessie`, `minio/operator` + a `Tenant` CR, `apache/nifi` as a plain
StatefulSet — see `workload-cluster/` for the PodMonitor/sample-job side of these, not
their own installation). Confirm what you're pointing the Control Tower at:

```bash
kubectl get pods -A | grep -E "nifi|airflow|spark-operator|flink"
```

### 0.3 No runtime internet access

Two things fetch from the internet at runtime in a connected setup. In an air-gapped install
both are replaced by prebuilt images:

1. **Airflow packages.** A connected deployment sets `_PIP_ADDITIONAL_REQUIREMENTS: "trino
   kubernetes httpx"` on the scheduler and dag-processor, which reinstalls from PyPI on every
   pod start. Offline, bake them into `airflow/Dockerfile` (built in §1.3, applied in §3.13).
2. **Daily digest CronJob.** It uses the already-built `control-tower:latest` image, which has
   `psycopg` baked in, rather than installing it at start.

If you add anything else that runs `pip install` in a `command:`, move it into a Dockerfile
the same way.

---

## §1. Prepare artifacts (on the builder, with internet)

### 1.1 Pull and save every image

```bash
mkdir -p offline-artifacts/images
cd offline-artifacts/images

IMAGES="
quay.io/prometheus-operator/prometheus-operator:v0.93.0
quay.io/prometheus-operator/prometheus-config-reloader:v0.93.0
quay.io/prometheus/prometheus:v3.13.2-distroless
quay.io/prometheus/alertmanager:v0.33.1
quay.io/prometheus/node-exporter:v1.12.1-distroless
registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1
docker.io/grafana/grafana:13.1.3
quay.io/kiwigrid/k8s-sidecar:2.10.1
docker.io/grafana/loki:3.6.11
docker.io/grafana/loki-canary:3.6.12
cr.fluentbit.io/fluent/fluent-bit:5.0.9
prom/blackbox-exporter:v0.25.0
quay.io/prometheuscommunity/postgres-exporter:v0.15.0
postgres:16-alpine
python:3.12-slim
mailhog/mailhog:v1.0.1
marquezproject/marquez:0.50.0
marquezproject/marquez-web:0.50.0
registry.k8s.io/sig-storage/nfs-subdir-external-provisioner:v4.0.2
"
for img in $IMAGES; do
  docker pull "$img"
  fname=$(echo "$img" | tr '/:' '__')
  docker save "$img" -o "${fname}.tar"
done
```

`python:3.12-slim` is only the base layer for the `control-tower` build (§1.3); it does not
need importing on the target.

### 1.2 Fetch Helm charts as local archives

```bash
mkdir -p offline-artifacts/charts
cd offline-artifacts/charts

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add fluent https://fluent.github.io/helm-charts
helm repo update

helm pull prometheus-community/kube-prometheus-stack --version 88.3.0
helm pull grafana/loki --version 7.3.0
helm pull fluent/fluent-bit --version 0.57.9
```

This produces three `.tgz` files. `helm install`/`upgrade` with a local `.tgz` path
doesn't need internet — only the images the chart references do, which are already
covered by §1.1.

### 1.3 Build the two custom images

```bash
cd /root/monitor/control-tower
docker build --provenance=false --sbom=false -t control-tower:latest .
docker save control-tower:latest -o ../offline-artifacts/images/control-tower.tar

cd /root/monitor/airflow
docker build --provenance=false --sbom=false -t airflow-control-tower:3.2.2 .
docker save airflow-control-tower:3.2.2 -o ../offline-artifacts/images/airflow-control-tower.tar
```

`--provenance=false --sbom=false` matters — see CLAUDE.md gotcha #2. Without it, `docker
buildx`'s default output is an OCI image *index* rather than a plain manifest, which
imports "successfully" via `ctr` but stays invisible to the CRI plugin, so kubelet reports
`ErrImageNeverPull` even though `ctr images ls` shows it present.

### 1.4 Package for transfer

```bash
cd offline-artifacts
tar czf monitor-offline-bundle.tar.gz images/ charts/
# copy /root/monitor/{registry,control-tower,dq,lineage,alerting,airflow,grafana,
#   monitoring-cluster,workload-cluster}/ alongside it - the whole repo, not just this bundle
```

Move `monitor-offline-bundle.tar.gz` and a copy of this repo across the gap.

---

## §2. Load images onto the target cluster

Repeat on **every node** (containerd's image store is per-node, not cluster-wide, unless
you're using a shared internal registry instead — see the note below).

```bash
tar xzf monitor-offline-bundle.tar.gz
cd images

# RKE2: use RKE2's own embedded ctr binary and socket, NOT the system /usr/bin/ctr if one
# exists - a version mismatch between them silently produces images that `ctr` can see but
# the CRI plugin (and therefore kubelet) can't. See CLAUDE.md gotcha #1. Adjust the path/
# socket below for your distro if not RKE2 (k3s: /var/lib/rancher/k3s/bin/ctr + the k3s
# containerd socket; vanilla containerd: plain `ctr`, no special binary needed).
CTR="/var/lib/rancher/rke2/bin/ctr --address /run/k3s/containerd/containerd.sock -n k8s.io"

for tarball in *.tar; do
  $CTR image import "$tarball"
done

# Verify the CRI plugin (not just ctr) can actually see them:
crictl images | grep -E "grafana|prometheus|control-tower|marquez|mailhog"
```

**If you have an internal registry mirror instead** (Harbor, etc.), it's simpler: `docker
load` each tarball on one machine, `docker tag`/`docker push` to the internal registry, and
skip per-node `ctr import` entirely — just point every manifest's `image:` field at the
mirror instead of the public registry names used throughout this repo, and use the
default `imagePullPolicy` instead of the `Never` used throughout (see §3 — every manifest
here uses `imagePullPolicy: Never` for the two custom images specifically because they
only exist as node-local imports, not because that's required in general).

---

## §3. Install, in order

Each step includes what to verify before moving to the next — don't skip the checks, a
failure two steps back is much harder to diagnose from three steps forward.

### 3.1 Namespace + storage

```bash
kubectl create namespace monitoring
kubectl get storageclass    # confirm a default StorageClass exists (nfs-client in this
                             # build's values files - substitute your own throughout §3
                             # wherever you see storageClassName: nfs-client)
```

### 3.2 kube-prometheus-stack (Prometheus, Grafana, Alertmanager, node-exporter, kube-state-metrics)

```bash
helm install kube-prometheus-stack offline-artifacts/charts/kube-prometheus-stack-88.3.0.tgz \
  -n monitoring -f monitoring-cluster/kube-prometheus-stack-values.yaml
kubectl -n monitoring rollout status deploy/kube-prometheus-stack-grafana --timeout=180s
```

Verify: `kubectl -n monitoring get pods` shows `kube-prometheus-stack-*` and
`prometheus-kube-prometheus-stack-prometheus-0` Running.

### 3.3 Loki + Fluent Bit

```bash
helm install loki offline-artifacts/charts/loki-7.3.0.tgz \
  -n monitoring -f monitoring-cluster/loki-values.yaml
helm install fluent-bit offline-artifacts/charts/fluent-bit-0.57.9.tgz \
  -n monitoring -f monitoring-cluster/fluent-bit-values.yaml
kubectl -n monitoring rollout status statefulset/loki --timeout=120s
```

Verify: a Loki datasource query for `{job="fluentbit"}` in Grafana Explore returns log
lines within a minute or two of Fluent Bit coming up.

### 3.4 Grafana folder + dashboard provisioning support

The dashboard sidecar needs `FOLDER_ANNOTATION` set and the provider config needs
`foldersFromFilesStructure: true` for the folder-per-dashboard layout in §3.9 to work —
neither is on by kube-prometheus-stack's defaults.

```bash
kubectl -n monitoring patch deployment kube-prometheus-stack-grafana --type=json -p '[
  {"op": "add", "path": "/spec/template/spec/containers/0/env/-",
   "value": {"name": "FOLDER_ANNOTATION", "value": "grafana_folder"}}
]'
kubectl -n monitoring create configmap kube-prometheus-stack-grafana-config-dashboards \
  --from-literal=provider.yaml="apiVersion: 1
providers:
  - name: 'sidecarProvider'
    orgId: 1
    folder: ''
    folderUid: ''
    type: file
    disableDeletion: false
    allowUiUpdates: false
    updateIntervalSeconds: 30
    options:
      foldersFromFilesStructure: true
      path: /tmp/dashboards" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n monitoring rollout restart deployment kube-prometheus-stack-grafana
```

**Never use `/` inside a folder title** you pass via `grafana_folder` annotations later —
see CLAUDE.md gotcha #4. Use `&` instead.

### 3.5 Control Tower Postgres + schema

```bash
kubectl apply -f control-tower/postgres-values.yaml
kubectl -n monitoring rollout status statefulset/control-tower-postgres --timeout=120s
kubectl -n monitoring cp control-tower/schema.sql control-tower-postgres-0:/tmp/schema.sql
kubectl -n monitoring exec control-tower-postgres-0 -- \
  psql -U control_tower -d control_tower -f /tmp/schema.sql
```

Verify: `psql ... -c "\dv"` lists `pipeline_current_sla_status`.

### 3.6 Pipeline registry

```bash
kubectl -n monitoring create configmap control-tower-registry \
  --from-file=registry/sparkpi_sample.yaml \
  --from-file=registry/nifi_ingest_sample.yaml \
  --from-file=registry/trigger_flink_job.yaml \
  --from-file=registry/sample_statemachine.yaml \
  --from-file=registry/kafka_to_iceberg_demo.yaml \
  --from-file=registry/dq_tier_a_checks.yaml \
  --from-file=registry/full_pipeline_demo.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

Replace/extend with `registry/<your_pipeline>.yaml` files per `registry/LABELLING-STANDARD.md`
for a real deployment — the ones above are this build's own demo pipelines.

### 3.7 NiFi credentials (skip if NiFi isn't part of your platform)

```bash
kubectl -n nifi get secret nifi-credentials -o json | \
  python3 -c "import json,sys; d=json.load(sys.stdin); d['metadata']={'name':'nifi-credentials','namespace':'monitoring'}; print(json.dumps(d))" | \
  kubectl apply -f -

# Seed an initial token (the collector self-refreshes on 401 after this - see CLAUDE.md gotcha re: token expiry)
TOKEN=$(kubectl -n nifi exec nifi-0 -- curl -sk -X POST "https://localhost:8443/nifi-api/access/token" \
  -d "username=admin&password=<your NiFi password>")
kubectl -n monitoring create secret generic nifi-prometheus-token --from-literal=token="$TOKEN"
```

### 3.8 Control Tower app

```bash
kubectl apply -f control-tower/deploy.yaml
kubectl apply -f control-tower/podmonitor.yaml
kubectl -n monitoring rollout status deployment/control-tower --timeout=90s
kubectl -n monitoring logs -l app=control-tower --tail=30
```

Verify the log tail shows `registry sync: N pipelines loaded`, each collector completing
without a traceback, and `Uvicorn running on http://0.0.0.0:8000`.

### 3.9 Health probes, Postgres exporters, Grafana datasource

```bash
kubectl apply -f control-tower/blackbox-values.yaml
kubectl apply -f control-tower/postgres-exporter.yaml
kubectl apply -f control-tower/grafana-postgres-datasource.yaml
```

Edit the target URL list inside `control-tower/blackbox-values.yaml`'s `Probe` resource to
match your actual platform service DNS names before applying — the ones there
(`trino.default.svc...`, `nessie-mgmt.default.svc...`, etc.) are this build's own.

Verify (after the datasource sidecar reloads, ~15s): Grafana → Connections → Data sources
→ "Control Tower (Postgres)" shows a green "Data source is working" test result. If the
"Database" field looks blank in the UI despite queries working, that's cosmetic (Grafana
13's UI reads `jsonData.database`, already set in this file) — not a real problem.

### 3.10 Marquez (lineage)

```bash
kubectl apply -f lineage/marquez.yaml
kubectl apply -f lineage/marquez-web.yaml
kubectl -n monitoring rollout status statefulset/marquez-postgres --timeout=120s
kubectl -n monitoring rollout status deployment/marquez --timeout=90s
```

Marquez's bundled config hardcodes `db.user=marquez` / `db.password=marquez` regardless
of env vars (CLAUDE.md gotcha #7) — `lineage/marquez.yaml` already matches this; don't
"fix" the password to something else without also patching Marquez's own config.

Verify: `curl http://<marquez-nodeport>/api/v1/namespaces` returns JSON. If `marquez-web`
logs `App listening on port undefined!`, its `WEB_PORT` env var is missing (gotcha #8) —
already set correctly in `lineage/marquez-web.yaml`.

### 3.11 Email: MailHog (or point at your real SMTP relay instead)

```bash
kubectl apply -f alerting/mailhog.yaml
```

**In a real deployment you almost certainly want your org's actual SMTP relay, not
MailHog** — MailHog exists in this build only because no relay was reachable from the
sandbox it was built in. To use a real relay: skip this file, and in §3.12's Alertmanager
config change `smtp_smarthost` from `mailhog.monitoring.svc.cluster.local:1025` to your
relay's address, and add `smtp_auth_username`/`smtp_auth_password` if it requires auth.
Nothing else changes — the routing/grouping/inhibition logic is relay-agnostic.

### 3.12 Alerting

```bash
kubectl apply -f alerting/control-tower-rules.yaml
kubectl apply -f alerting/prometheus-rules.yaml    # per-engine NiFi/Spark/Flink/Airflow rules
kubectl -n monitoring create secret generic alertmanager-kube-prometheus-stack-alertmanager \
  --from-file=alertmanager.yaml=alerting/alertmanager-config.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

Edit `alerting/alertmanager-config.yaml`'s `to:` addresses (currently
`{{ .CommonLabels.owner }}@company.internal`) to match your real domain before applying.

Verify: `kubectl -n monitoring logs alertmanager-kube-prometheus-stack-alertmanager-0 -c
alertmanager` shows `Completed loading of configuration file` with no error after this.

### 3.13 Data quality (Tier-A checks) — only if Trino/Nessie/MinIO are part of your platform

```bash
docker load -i offline-artifacts/images/airflow-control-tower.tar   # or ctr import, per §2
kubectl apply -f dq/airflow-pipeline-rbac.yaml
```

Then, on the Airflow scheduler and dag-processor Deployments: **remove**
`_PIP_ADDITIONAL_REQUIREMENTS` entirely and set `image: airflow-control-tower:3.2.2` (built
in §1.3) instead of the bare `apache/airflow:3.2.2`. Mount `dq/dq_tier_a_dag.py` and
`dq/full_pipeline_demo_dag.py` into `/opt/airflow/dags/` the same way
`registry/dags/trigger_flink_job.py` is (a ConfigMap + per-file `subPath` volumeMount on
scheduler, dag-processor, api-server, and triggerer — whole-directory ConfigMap mounts
don't pick up updates via `subPath`, see the pattern already in place for the sample DAG).

Edit `DATASET_CHECKS_CONFIG` at the top of `dq/dq_tier_a_dag.py` to point at your real
Iceberg tables instead of the demo `iceberg.demo.kafka_events`.

```bash
kubectl apply -f dq/daily-digest-cronjob.yaml
```

### 3.14 Dashboards

```bash
declare -A FOLDERS=(
  [01-control-tower.json]="01. Control Tower"
  [02-pipeline-detail.json]="02. Pipeline Overview"
  [03-pipeline-overview.json]="02. Pipeline Overview"
  [04-pipeline-sla.json]="02. Pipeline Overview"
  [05-data-quality-overview.json]="03. Data Quality"
  [06-data-freshness.json]="03. Data Quality"
  [07-data-lineage.json]="04. Data Lineage"
  [08-data-platform.json]="06. Data Platform"
  [nifi.json]="05. Processing Engines"
  [spark.json]="05. Processing Engines"
  [flink.json]="05. Processing Engines"
  [airflow.json]="05. Processing Engines"
)
for f in "${!FOLDERS[@]}"; do
  name="grafana-dashboard-${f%.json}"
  kubectl -n monitoring create configmap "$name" \
    --from-file="$f=grafana/dashboards/$f" \
    --dry-run=client -o yaml | kubectl label -f - --local -o yaml grafana_dashboard=1 \
    | kubectl annotate -f - --local -o yaml grafana_folder="${FOLDERS[$f]}" \
    | kubectl apply -f -
done
```

Optionally, annotate the dashboards bundled with kube-prometheus-stack (Kubernetes, Node
Exporter, etc.) with `grafana_folder="07. Kubernetes"` and `"08. VM & Infrastructure"` the same
way; otherwise they stay in the default folder.

---

## §4. Verification checklist

Work through this in order — each depends on the one before it:

1. `kubectl -n monitoring get pods` — everything `Running`/`Completed`, no `CrashLoopBackOff`.
2. Grafana reachable, logs in with the admin password set in
   `monitoring-cluster/kube-prometheus-stack-values.yaml`.
3. Folders `01.` through `08.` exist with the expected dashboards inside each (§3.4/§3.14).
4. "Control Tower (Postgres)" datasource test passes (§3.9).
5. L1 "Data Platform Control Tower" dashboard shows non-zero values in at least the
   PLATFORM/K8S tiles (these only need blackbox + kube-state-metrics, not the Control
   Tower's own collectors, so they're the fastest thing to go green).
6. `kubectl -n monitoring logs -l app=control-tower --tail=50` shows a full collector
   cycle with no tracebacks — this is your signal that NiFi/Spark/Flink/Airflow are all
   actually reachable from inside the cluster, not just that pods are up.
7. Pipeline Overview dashboard lists your registered pipelines with real `last_status`
   values (not all `unknown`).
8. Trigger one real alert (easiest: temporarily lower a `max_duration_minutes` in a
   registry YAML below a pipeline's real runtime) and confirm an email lands wherever
   §3.11 is pointed.
9. If DQ Tier-A is in scope: `kubectl -n airflow exec deploy/airflow-scheduler -c scheduler
   -- airflow dags test dq_tier_a_checks` completes and `dq_result` rows appear.

## §5. Troubleshooting

See the "Gotchas" section of `CLAUDE.md`: containerd/`ctr` mismatch, image index vs manifest,
Postgres `round()` typing, Grafana folder names containing `/`, the SLA-status view,
MinIO Operator reconciliation, Marquez credentials and the `marquez-web` port variable.
