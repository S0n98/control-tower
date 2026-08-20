# CLAUDE.md — Data Observability / Control Tower

This repo is infra-as-code for a **live, deployed** monitoring stack on the `default`/`rag`
single-node RKE2 cluster on this host — not a template. Every YAML/script here has a
matching real resource in the cluster. Read `README.md` first for architecture and
`INSTALL.md` for how to stand this up somewhere else (especially offline). This file is
operational notes for working on the code/config itself.

## Cluster access

```
export KUBECONFIG=/root/.kube/config
kubectl --context default -n monitoring get pods
```

Credentials: Grafana `admin`/`PipelineObs2026!`, NiFi `admin`/`PipelineObsNiFi2026!`,
Control Tower Postgres `control_tower`/`ControlTowerObs2026!`, Marquez Postgres
`marquez`/`marquez` (yes, matches the username — see gotcha below).

## Gotchas that will burn you again if you forget them

1. **Two different `ctr`/containerd builds exist on this host.** `/usr/bin/ctr` is a
   generic containerd v2.2 build; RKE2 runs its *own* embedded containerd v1.7 at
   `/var/lib/rancher/rke2/bin/ctr` / socket `/run/k3s/containerd/containerd.sock`. Importing
   an image with the wrong `ctr` writes metadata the CRI plugin silently can't see
   (`ctr images ls` shows it, `crictl images` doesn't, kubelet reports `ErrImageNeverPull`).
   **Always use `/var/lib/rancher/rke2/bin/ctr --address /run/k3s/containerd/containerd.sock
   -n k8s.io image import <tar>`** to load a locally-built image, never the bare `ctr`.

2. **`docker buildx` embeds provenance/attestation manifests by default**, turning a
   single-arch build into an OCI image *index* rather than a plain manifest — which then
   silently fails the same CRI-visibility way as #1, for a different reason. Build with
   `docker build --provenance=false --sbom=false ...` for anything you're going to
   `ctr image import` into this cluster.

3. **`round(double precision, integer)` doesn't exist in Postgres** — only
   `round(numeric, integer)`. Any SQL panel dividing two things and rounding needs an
   explicit `::numeric` cast (`round((freshness_s / 60.0)::numeric, 1)`), or Grafana shows a
   500 with `function round(...) does not exist`.

4. **Grafana dashboard folder names must not contain `/`.** The kiwigrid/k8s-sidecar
   writes dashboard JSON to `/tmp/dashboards/<grafana_folder annotation>/`, and with
   `foldersFromFilesStructure: true` a `/` in the annotation value is read as a real
   directory separator — `"08. VM / Infrastructure"` silently becomes nested/duplicate
   folders all titled `08. VM`. Use `&` instead: `"08. VM & Infrastructure"`.

5. **`pipeline_current_sla_status` (a Postgres VIEW in `schema.sql`) is the only safe way
   to read "is this pipeline currently breaching its SLA".** `sla_engine.py` writes one row
   per `(pipeline_id, sla_type)` on *every* poll cycle — ok or breached, never silent — so
   6 different rule types are all racing to be "the most recent row" for a pipeline at any
   given instant. Reading `sla_evaluation` directly with `ORDER BY evaluated_at DESC LIMIT 1`
   gives you whichever rule happened to evaluate last, not the worst active one. Every
   dashboard query and `metrics.py` reads the view; keep it that way.

6. **The MinIO Operator's Tenant CRD reconciles away manual StatefulSet edits.** Don't
   patch `myminio-pool-test` directly (e.g. to add `MINIO_PROMETHEUS_AUTH_TYPE=public`) —
   it gets silently reverted on the next operator reconcile. Edit the `Tenant` CRD itself
   (`kubectl -n default get tenant myminio`) if you need to change MinIO's env.

7. **Marquez's bundled `marquez.dev.yml` hardcodes `db.user=marquez` / `db.password=marquez`**
   — it does NOT read `POSTGRES_USER`/`POSTGRES_PASSWORD` env vars for the app's own DB
   connection (only `POSTGRES_HOST`/`POSTGRES_PORT` are templated). Don't fight this by
   trying to inject a different password; match it.

8. **`marquez-web` needs `WEB_PORT` set explicitly** (in addition to `MARQUEZ_HOST`/
   `MARQUEZ_PORT`) or it logs `App listening on port undefined!` and never binds.

9. **Kubectl port-forwards started via the `run_in_background` tool option are the
   reliable pattern in this environment** — bare `&`-backgrounded `kubectl port-forward`
   inside a single Bash call gets killed when that tool call's shell exits. If a
   port-forward you started earlier stops responding, just start a fresh one on a new
   local port rather than debugging the old one.

10. **`_PIP_ADDITIONAL_REQUIREMENTS` on Airflow re-installs from PyPI on every container
    start**, and it must be set on **both** `airflow-scheduler` and `airflow-dag-processor`
    — the dag-processor is the component that actually parses DAGs and reports import
    errors in Airflow 3.x, and it's easy to patch only the scheduler and then wonder why
    `airflow dags list-import-errors` still shows `ModuleNotFoundError` forever. This
    approach needs internet at pod-start time — see `INSTALL.md` for the offline
    alternative before deploying somewhere air-gapped.

## Common tasks

**Rebuild and redeploy the Control Tower after an `app/` change:**
```bash
cd /root/monitor/control-tower
docker build --provenance=false --sbom=false -t control-tower:latest .
docker save control-tower:latest -o /tmp/control-tower.tar
/var/lib/rancher/rke2/bin/ctr --address /run/k3s/containerd/containerd.sock -n k8s.io image rm docker.io/library/control-tower:latest
/var/lib/rancher/rke2/bin/ctr --address /run/k3s/containerd/containerd.sock -n k8s.io image import /tmp/control-tower.tar
kubectl --context default -n monitoring rollout restart deployment control-tower
```

**Add or update a dashboard** (must go through a ConfigMap, not the Grafana UI —
`allowUiUpdates: false` is set so provisioned dashboards don't drift from what's in git):
```bash
kubectl --context default -n monitoring create configmap grafana-dashboard-<name> \
  --from-file=<name>.json=grafana/dashboards/<name>.json \
  --dry-run=client -o yaml | kubectl --context default label -f - --local -o yaml grafana_dashboard=1 \
  | kubectl --context default annotate -f - --local -o yaml grafana_folder="<Folder Title>" \
  | kubectl --context default apply -f -
```

**Add a new pipeline to the registry** — see README's "Adding a real pipeline" section.

**Verify a Postgres-backed dashboard panel's query works** before trusting it renders:
```bash
curl -s -u admin:PipelineObs2026! -X POST http://localhost:3000/api/ds/query \
  -H "Content-Type: application/json" \
  -d '{"queries":[{"refId":"A","datasource":{"uid":"control-tower-postgres"},"rawSql":"<sql>","format":"table"}]}'
```
(requires `kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80` first)

## Don't

- Don't use `helm upgrade` on `kube-prometheus-stack` or `airflow` in this environment —
  concurrent Helm installs have crashed the apiserver here before (resource pressure on a
  single-node cluster). Patch the underlying Deployment/Secret/ConfigMap directly instead,
  the way every change in this repo was made.
- Don't delete `registry/kafka_to_iceberg_demo.yaml` or "fix" it by removing the pipeline —
  it's deliberately registered-and-permanently-failing (no Kafka broker exists in this
  environment) so the SLA/alerting path has a real broken target to validate against.
