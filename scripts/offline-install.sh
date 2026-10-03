#!/usr/bin/env bash
# Offline installer for the Data Observability / Control Tower stack.
# Automates INSTALL.md end to end. Four phases, run on different machines:
#
#   1. prepare   (builder, WITH internet)  pull charts + every image, build the two custom
#                images, write a self-contained bundle directory + .tar.gz
#   2. load      (EVERY cluster node)      import the bundle's images into containerd
#   3. install   (any host with kubectl)   install the stack in order, waiting at each step
#   4. verify    (any host with kubectl)   run the INSTALL.md §4 checklist
#
# Typical use:
#   builder$ ./scripts/offline-install.sh prepare            # -> /root/monitor-offline.tar.gz
#   ...copy the tarball across the air gap...
#   node$    tar xzf monitor-offline.tar.gz && cd monitor-offline
#   node$    ./repo/scripts/offline-install.sh load          # repeat on every node
#   admin$   cp repo/scripts/offline.env.example offline.env && vi offline.env
#   admin$   ./repo/scripts/offline-install.sh install
#   admin$   ./repo/scripts/offline-install.sh verify
#
# Single steps can be (re)run: ./offline-install.sh install <step> [<step> ...]
# Steps: preflight namespace kps loki ctdb registry nifi controltower probes monitors
#        marquez mail alerting dq dashboards
#
# Every step is idempotent (kubectl apply / dry-run|apply). Helm releases are installed
# once and skipped if they already exist - this script never runs `helm upgrade` (see
# CLAUDE.md: concurrent Helm upgrades have crashed the apiserver on small clusters).

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Configuration (override in offline.env next to the bundle, or via env vars)
# ---------------------------------------------------------------------------
CONFIG_FILE="${CONFIG_FILE:-}"
if [[ -z "$CONFIG_FILE" ]]; then
  for c in "$PWD/offline.env" "$REPO_ROOT/../offline.env" "$SCRIPT_DIR/offline.env"; do
    [[ -f "$c" ]] && { CONFIG_FILE="$c"; break; }
  done
fi
# shellcheck disable=SC1090
[[ -n "$CONFIG_FILE" && -f "$CONFIG_FILE" ]] && source "$CONFIG_FILE"

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${NAMESPACE:-monitoring}"
STORAGE_CLASS="${STORAGE_CLASS:-nfs-client}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-PipelineObs2026!}"

# Email: USE_MAILHOG=true deploys MailHog as the SMTP catcher. Set false and fill
# SMTP_SMARTHOST to use a real relay instead.
USE_MAILHOG="${USE_MAILHOG:-true}"
SMTP_SMARTHOST="${SMTP_SMARTHOST:-mailhog.monitoring.svc.cluster.local:1025}"
MAIL_DOMAIN="${MAIL_DOMAIN:-company.internal}"

# Optional parts: auto = enable if the platform namespace exists.
ENABLE_NIFI="${ENABLE_NIFI:-auto}"
NIFI_NAMESPACE="${NIFI_NAMESPACE:-nifi}"
NIFI_POD="${NIFI_POD:-nifi-0}"
NIFI_USER="${NIFI_USER:-admin}"
NIFI_PASSWORD="${NIFI_PASSWORD:-}"

ENABLE_DQ="${ENABLE_DQ:-auto}"
AIRFLOW_NAMESPACE="${AIRFLOW_NAMESPACE:-airflow}"
AIRFLOW_DAGS_DIR="${AIRFLOW_DAGS_DIR:-/opt/airflow/dags}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-docker.io/library/airflow-control-tower:3.2.2}"

ENABLE_ENGINE_MONITORS="${ENABLE_ENGINE_MONITORS:-true}"

# Builder-side settings
OUT_DIR="${OUT_DIR:-/root/monitor-offline}"
KPS_CHART_VERSION="${KPS_CHART_VERSION:-88.3.0}"
LOKI_CHART_VERSION="${LOKI_CHART_VERSION:-7.3.0}"
FLUENTBIT_CHART_VERSION="${FLUENTBIT_CHART_VERSION:-0.57.9}"

# Bundle location on the target: the directory containing images/ charts/ repo/.
BUNDLE_DIR="${BUNDLE_DIR:-$(cd "$REPO_ROOT/.." && pwd)}"
WORK_DIR="${WORK_DIR:-$BUNDLE_DIR/rendered}"

# Images referenced by the repo's own manifests (chart images are discovered via
# `helm template` in prepare, so they track the chart versions automatically).
STATIC_IMAGES=(
  prom/blackbox-exporter:v0.25.0
  quay.io/prometheuscommunity/postgres-exporter:v0.15.0
  postgres:16-alpine
  mailhog/mailhog:v1.0.1
  marquezproject/marquez:0.50.0
  marquezproject/marquez-web:0.50.0
)
CUSTOM_IMAGES=(
  control-tower:latest
  airflow-control-tower:3.2.2
)

# Folder for each dashboard (no "/" in folder names - CLAUDE.md gotcha #4).
declare -A DASHBOARD_FOLDERS=(
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

ALL_STEPS=(preflight namespace kps loki ctdb registry nifi controltower probes monitors
           marquez mail alerting dq dashboards)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✔\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m  ✘ %s\033[0m\n' "$*" >&2; exit 1; }
trap 'printf "\033[1;31m✘ failed at line %s: %s\033[0m\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

KCTX=(); HCTX=()
if [[ -n "$KUBE_CONTEXT" ]]; then KCTX=(--context "$KUBE_CONTEXT"); HCTX=(--kube-context "$KUBE_CONTEXT"); fi
k()  { kubectl "${KCTX[@]}" "$@"; }
kn() { kubectl "${KCTX[@]}" -n "$NAMESPACE" "$@"; }
h()  { helm "${HCTX[@]}" "$@"; }

need() { for b in "$@"; do command -v "$b" >/dev/null || die "required command not found: $b"; done; }
ns_exists() { k get namespace "$1" >/dev/null 2>&1; }
is_true() { [[ "$1" == true || "$1" == yes || "$1" == 1 ]]; }

# enabled <flag> <namespace>: true/false/auto(namespace exists)
enabled() {
  case "$1" in
    auto) ns_exists "$2" ;;
    *)    is_true "$1" ;;
  esac
}

rollout() { # rollout <kind/name> [timeout]
  log "waiting for $1"
  kn rollout status "$1" --timeout="${2:-300s}"
}

apply_rendered() { for f in "$@"; do k apply -f "$WORK_DIR/$f"; done; }

# containerd tooling: prefer the distro-embedded ctr (CLAUDE.md gotcha #1).
detect_ctr() {
  if [[ -x /var/lib/rancher/rke2/bin/ctr ]]; then
    CTR_BIN=/var/lib/rancher/rke2/bin/ctr; CTR_SOCK=/run/k3s/containerd/containerd.sock
    CRICTL_BIN=/var/lib/rancher/rke2/bin/crictl
  elif [[ -x /var/lib/rancher/k3s/data/current/bin/ctr ]] || command -v k3s >/dev/null 2>&1; then
    CTR_BIN="k3s ctr"; CTR_SOCK=/run/k3s/containerd/containerd.sock; CRICTL_BIN="k3s crictl"
  else
    CTR_BIN=ctr; CTR_SOCK=/run/containerd/containerd.sock; CRICTL_BIN=crictl
  fi
  CTR_BIN="${CTR_OVERRIDE:-$CTR_BIN}"; CTR_SOCK="${CTR_SOCK_OVERRIDE:-$CTR_SOCK}"
}

# ---------------------------------------------------------------------------
# Phase 1: prepare (builder with internet)
# ---------------------------------------------------------------------------
cmd_prepare() {
  need docker helm tar
  log "preparing bundle in $OUT_DIR"
  rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"/{images,charts,repo}

  log "fetching Helm charts"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
  helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
  helm repo add fluent https://fluent.github.io/helm-charts >/dev/null
  helm repo update >/dev/null
  helm pull prometheus-community/kube-prometheus-stack --version "$KPS_CHART_VERSION" -d "$OUT_DIR/charts"
  helm pull grafana/loki --version "$LOKI_CHART_VERSION" -d "$OUT_DIR/charts"
  helm pull fluent/fluent-bit --version "$FLUENTBIT_CHART_VERSION" -d "$OUT_DIR/charts"

  log "discovering chart images with helm template"
  local tmpl="$OUT_DIR/.templates.yaml"
  {
    helm template kube-prometheus-stack "$OUT_DIR/charts/kube-prometheus-stack-$KPS_CHART_VERSION.tgz" \
      -n "$NAMESPACE" -f "$REPO_ROOT/monitoring-cluster/kube-prometheus-stack-values.yaml"
    helm template loki "$OUT_DIR/charts/loki-$LOKI_CHART_VERSION.tgz" \
      -n "$NAMESPACE" -f "$REPO_ROOT/monitoring-cluster/loki-values.yaml"
    helm template fluent-bit "$OUT_DIR/charts/fluent-bit-$FLUENTBIT_CHART_VERSION.tgz" \
      -n "$NAMESPACE" -f "$REPO_ROOT/monitoring-cluster/fluent-bit-values.yaml"
  } > "$tmpl"
  {
    # plain image: fields, plus the config-reloader image the operator receives as a flag
    grep -oE '^\s*image:\s*"?[^"[:space:]]+' "$tmpl" | sed -E 's/^\s*image:\s*"?//'
    grep -oE -- '--prometheus-config-reloader=[^"[:space:]]+' "$tmpl" | cut -d= -f2
    printf '%s\n' "${STATIC_IMAGES[@]}"
  } | sort -u > "$OUT_DIR/images/IMAGES.txt"
  rm -f "$tmpl"
  log "$(wc -l < "$OUT_DIR/images/IMAGES.txt") public images to save:"
  sed 's/^/    /' "$OUT_DIR/images/IMAGES.txt"

  log "building custom images (--provenance=false --sbom=false: CLAUDE.md gotcha #2)"
  docker build --provenance=false --sbom=false -t control-tower:latest "$REPO_ROOT/control-tower"
  docker build --provenance=false --sbom=false -t airflow-control-tower:3.2.2 "$REPO_ROOT/airflow"

  log "pulling and saving images"
  local img fname
  while read -r img; do
    [[ -z "$img" ]] && continue
    fname="$(echo "$img" | tr '/:@' '___').tar"
    docker pull "$img" >/dev/null
    docker save "$img" -o "$OUT_DIR/images/$fname"
    ok "$img"
  done < "$OUT_DIR/images/IMAGES.txt"
  for img in "${CUSTOM_IMAGES[@]}"; do
    docker save "$img" -o "$OUT_DIR/images/$(echo "$img" | tr '/:' '__').tar"
    echo "docker.io/library/$img" >> "$OUT_DIR/images/IMAGES.txt"
    ok "$img (custom)"
  done

  log "copying repo (working tree, without .git)"
  tar -C "$REPO_ROOT" --exclude=.git --exclude=.claude -cf - . | tar -C "$OUT_DIR/repo" -xf -
  cp "$SCRIPT_DIR/offline.env.example" "$OUT_DIR/offline.env.example"

  log "packing $OUT_DIR.tar.gz"
  tar -C "$(dirname "$OUT_DIR")" -czf "$OUT_DIR.tar.gz" "$(basename "$OUT_DIR")"
  ok "bundle ready: $OUT_DIR.tar.gz ($(du -h "$OUT_DIR.tar.gz" | cut -f1))"
}

# ---------------------------------------------------------------------------
# Phase 2: load (every node)
# ---------------------------------------------------------------------------
cmd_load() {
  [[ -d "$BUNDLE_DIR/images" ]] || die "no images/ in $BUNDLE_DIR (set BUNDLE_DIR)"
  [[ $EUID -eq 0 ]] || die "load must run as root (containerd socket access)"
  detect_ctr
  log "importing images with: $CTR_BIN --address $CTR_SOCK -n k8s.io"
  local t
  for t in "$BUNDLE_DIR"/images/*.tar; do
    # shellcheck disable=SC2086
    $CTR_BIN --address "$CTR_SOCK" -n k8s.io image import "$t" >/dev/null
    ok "$(basename "$t")"
  done

  log "verifying the CRI plugin (not just ctr) can see every image"
  local missing=0 img
  while read -r img; do
    [[ -z "$img" ]] && continue
    # shellcheck disable=SC2086
    if ! $CRICTL_BIN --runtime-endpoint "unix://$CTR_SOCK" inspecti "$img" >/dev/null 2>&1; then
      warn "not visible to CRI: $img"; missing=$((missing + 1))
    fi
  done < "$BUNDLE_DIR/images/IMAGES.txt"
  (( missing == 0 )) || die "$missing image(s) invisible to kubelet - see CLAUDE.md gotchas #1/#2"
  ok "all images visible to kubelet on $(hostname)"
}

# ---------------------------------------------------------------------------
# Phase 3: install
# ---------------------------------------------------------------------------
render() {
  # Copy the repo into WORK_DIR and substitute environment-specific values, so the
  # bundle's repo/ stays pristine and re-rendering is always safe.
  rm -rf "$WORK_DIR"; mkdir -p "$WORK_DIR"
  tar -C "$REPO_ROOT" --exclude=.git -cf - . | tar -C "$WORK_DIR" -xf -
  find "$WORK_DIR" -name '*.yaml' -exec sed -i "s/nfs-client/$STORAGE_CLASS/g" {} +
  sed -i -e "s/company\.internal/$MAIL_DOMAIN/g" \
         -e "s|smtp_smarthost: .*|smtp_smarthost: \"$SMTP_SMARTHOST\"|" \
         "$WORK_DIR/alerting/alertmanager-config.yaml"
  # Grafana folder provisioning is set at install time instead of patched afterwards.
  local pw="${GRAFANA_ADMIN_PASSWORD//\'/\'\'}"
  cat > "$WORK_DIR/kps-overlay.yaml" <<EOF
grafana:
  adminPassword: '$pw'
  sidecar:
    dashboards:
      folderAnnotation: grafana_folder
      provider:
        foldersFromFilesStructure: true
        allowUiUpdates: false
EOF
}

helm_install_once() { # release chart values...
  local rel="$1" chart="$2"; shift 2
  if h status "$rel" -n "$NAMESPACE" >/dev/null 2>&1; then
    ok "helm release $rel already installed - skipping (no helm upgrade, see CLAUDE.md)"
    return
  fi
  local args=() v
  for v in "$@"; do args+=(-f "$v"); done
  h install "$rel" "$chart" -n "$NAMESPACE" "${args[@]}"
}

step_preflight() {
  need kubectl helm
  k version >/dev/null 2>&1 || die "cannot reach the cluster with kubectl ${KCTX[*]}"
  ok "cluster reachable"
  [[ -d "$BUNDLE_DIR/charts" ]] || die "no charts/ in $BUNDLE_DIR (set BUNDLE_DIR)"
  k get storageclass "$STORAGE_CLASS" >/dev/null 2>&1 \
    || die "StorageClass '$STORAGE_CLASS' not found - set STORAGE_CLASS (kubectl get sc)"
  ok "StorageClass $STORAGE_CLASS exists"
  if ! is_true "$USE_MAILHOG" && [[ "$SMTP_SMARTHOST" == mailhog.* ]]; then
    die "USE_MAILHOG=false but SMTP_SMARTHOST still points at MailHog"
  fi
  enabled "$ENABLE_NIFI" "$NIFI_NAMESPACE" && [[ -z "$NIFI_PASSWORD" ]] \
    && die "NiFi enabled but NIFI_PASSWORD is empty (or set ENABLE_NIFI=false)"
  ok "configuration looks consistent"
}

step_namespace() {
  k create namespace "$NAMESPACE" --dry-run=client -o yaml | k apply -f -
}

step_kps() {
  helm_install_once kube-prometheus-stack \
    "$BUNDLE_DIR/charts/kube-prometheus-stack-$KPS_CHART_VERSION.tgz" \
    "$WORK_DIR/monitoring-cluster/kube-prometheus-stack-values.yaml" "$WORK_DIR/kps-overlay.yaml"
  rollout deploy/kube-prometheus-stack-operator
  rollout deploy/kube-prometheus-stack-grafana
  log "waiting for Prometheus to start"
  kn wait --for=condition=Ready pod/prometheus-kube-prometheus-stack-prometheus-0 --timeout=300s
}

step_loki() {
  helm_install_once loki "$BUNDLE_DIR/charts/loki-$LOKI_CHART_VERSION.tgz" \
    "$WORK_DIR/monitoring-cluster/loki-values.yaml"
  helm_install_once fluent-bit "$BUNDLE_DIR/charts/fluent-bit-$FLUENTBIT_CHART_VERSION.tgz" \
    "$WORK_DIR/monitoring-cluster/fluent-bit-values.yaml"
  rollout statefulset/loki
  rollout daemonset/fluent-bit
}

step_ctdb() {
  apply_rendered control-tower/postgres-values.yaml
  rollout statefulset/control-tower-postgres
  log "waiting for Postgres to accept connections"
  local i
  for i in $(seq 1 30); do
    kn exec control-tower-postgres-0 -- pg_isready -U control_tower >/dev/null 2>&1 && break
    sleep 5
  done
  kn exec -i control-tower-postgres-0 -- \
    psql -U control_tower -d control_tower -v ON_ERROR_STOP=1 -q < "$WORK_DIR/control-tower/schema.sql"
  ok "schema applied (pipeline_current_sla_status view included)"
}

step_registry() {
  local args=() f
  for f in "$WORK_DIR"/registry/*.yaml; do args+=(--from-file="$f"); done
  kn create configmap control-tower-registry "${args[@]}" --dry-run=client -o yaml | k apply -f -
  ok "${#args[@]} pipelines registered"
}

step_nifi() {
  if ! enabled "$ENABLE_NIFI" "$NIFI_NAMESPACE"; then warn "NiFi disabled - skipping"; return; fi
  local data token
  data="$(k -n "$NIFI_NAMESPACE" get secret nifi-credentials -o jsonpath='{.data}')"
  printf '{"apiVersion":"v1","kind":"Secret","type":"Opaque","metadata":{"name":"nifi-credentials","namespace":"%s"},"data":%s}' \
    "$NAMESPACE" "$data" | k apply -f -
  token="$(k -n "$NIFI_NAMESPACE" exec "$NIFI_POD" -- curl -sk -X POST \
    https://localhost:8443/nifi-api/access/token \
    --data-urlencode "username=$NIFI_USER" --data-urlencode "password=$NIFI_PASSWORD")"
  [[ "$token" =~ ^[A-Za-z0-9._-]{20,}$ ]] || die "NiFi did not return a token (got: ${token:0:80})"
  kn create secret generic nifi-prometheus-token --from-literal=token="$token" \
    --dry-run=client -o yaml | k apply -f -
  ok "NiFi credentials + scrape token in place (token expires in ~8-12h, see README)"
}

step_controltower() {
  apply_rendered control-tower/deploy.yaml control-tower/podmonitor.yaml
  rollout deploy/control-tower
  sleep 40   # one 30s poll cycle
  if kn logs -l app=control-tower --tail=200 | grep -q Traceback; then
    warn "control-tower logs contain a Traceback - check: kubectl -n $NAMESPACE logs -l app=control-tower"
  else
    ok "control-tower running, no tracebacks in the first poll cycle"
  fi
}

step_probes() {
  apply_rendered control-tower/blackbox-values.yaml control-tower/postgres-exporter.yaml \
    control-tower/grafana-postgres-datasource.yaml
  rollout deploy/blackbox-exporter
  warn "blackbox probe targets are this repo's own service DNS names - edit the Probe in control-tower/blackbox-values.yaml if yours differ"
}

step_monitors() {
  if ! is_true "$ENABLE_ENGINE_MONITORS"; then warn "engine monitors disabled - skipping"; return; fi
  k apply -f "$WORK_DIR/workload-cluster/podmonitors/" -f "$WORK_DIR/workload-cluster/servicemonitors/"
}

step_marquez() {
  apply_rendered lineage/marquez.yaml lineage/marquez-web.yaml
  rollout statefulset/marquez-postgres
  rollout deploy/marquez
  rollout deploy/marquez-web
}

step_mail() {
  if ! is_true "$USE_MAILHOG"; then ok "using SMTP relay $SMTP_SMARTHOST - MailHog skipped"; return; fi
  apply_rendered alerting/mailhog.yaml
  rollout deploy/mailhog
}

step_alerting() {
  apply_rendered alerting/control-tower-rules.yaml alerting/prometheus-rules.yaml
  kn create secret generic alertmanager-kube-prometheus-stack-alertmanager \
    --from-file=alertmanager.yaml="$WORK_DIR/alerting/alertmanager-config.yaml" \
    --dry-run=client -o yaml | k apply -f -
  sleep 20
  if kn logs alertmanager-kube-prometheus-stack-alertmanager-0 -c alertmanager --tail=50 \
      | grep -q "Completed loading of configuration file"; then
    ok "Alertmanager loaded the config"
  else
    warn "no 'Completed loading' line yet - check the alertmanager pod logs"
  fi
}

# Patch one Airflow Deployment's main container: image, pip env, DAG mounts.
patch_airflow_deploy() { # deploy set_image(true/false)
  local d="$1" set_image="$2" ns="$AIRFLOW_NAMESPACE"
  k -n "$ns" get deploy "$d" >/dev/null 2>&1 || { warn "deploy/$d not found in $ns - skipping"; return; }
  local idx=0 name="" line
  while IFS='=' read -r n img; do
    if [[ "$img" == *airflow* ]]; then name="$n"; break; fi
    idx=$((idx + 1))
  done < <(k -n "$ns" get deploy "$d" \
             -o jsonpath='{range .spec.template.spec.containers[*]}{.name}={.image}{"\n"}{end}')
  [[ -n "$name" ]] || { warn "no airflow container in deploy/$d - skipping"; return; }

  if [[ "$set_image" == true ]]; then
    k -n "$ns" set image "deploy/$d" "$name=$AIRFLOW_IMAGE"
    k -n "$ns" set env "deploy/$d" -c "$name" _PIP_ADDITIONAL_REQUIREMENTS-
  fi
  line="$(k -n "$ns" get deploy "$d" -o jsonpath='{.spec.template.spec.volumes[*].name}')"
  if [[ " $line " != *" control-tower-dq-dags "* ]]; then
    k -n "$ns" patch deploy "$d" --type=json -p "[
      {\"op\":\"add\",\"path\":\"/spec/template/spec/volumes/-\",
       \"value\":{\"name\":\"control-tower-dq-dags\",\"configMap\":{\"name\":\"control-tower-dq-dags\"}}},
      {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/$idx/volumeMounts/-\",
       \"value\":{\"name\":\"control-tower-dq-dags\",\"mountPath\":\"$AIRFLOW_DAGS_DIR/dq_tier_a_dag.py\",\"subPath\":\"dq_tier_a_dag.py\"}},
      {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/$idx/volumeMounts/-\",
       \"value\":{\"name\":\"control-tower-dq-dags\",\"mountPath\":\"$AIRFLOW_DAGS_DIR/full_pipeline_demo_dag.py\",\"subPath\":\"full_pipeline_demo_dag.py\"}}
    ]"
  fi
  ok "deploy/$d patched (container $name)"
}

step_dq() {
  # The daily digest only needs the Control Tower DB, so it is installed regardless.
  # The manual-test Job in the same file is filtered out so install doesn't send an email.
  awk 'BEGIN{RS="\n---\n"; ORS="\n---\n"} !/\nkind: Job\n/ && !/^kind: Job\n/' \
    "$WORK_DIR/dq/daily-digest-cronjob.yaml" | k apply -f -

  if ! enabled "$ENABLE_DQ" "$AIRFLOW_NAMESPACE"; then warn "Airflow not found / DQ disabled - skipping DAGs"; return; fi
  k apply -f "$WORK_DIR/dq/airflow-pipeline-rbac.yaml" \
    || warn "RBAC apply failed - the flink/default namespaces it targets may not exist"
  k -n "$AIRFLOW_NAMESPACE" create configmap control-tower-dq-dags \
    --from-file="$WORK_DIR/dq/dq_tier_a_dag.py" --from-file="$WORK_DIR/dq/full_pipeline_demo_dag.py" \
    --dry-run=client -o yaml | k apply -f -
  # subPath mounts don't receive ConfigMap updates - pods restart on the patch below.
  patch_airflow_deploy airflow-scheduler true
  patch_airflow_deploy airflow-dag-processor true
  patch_airflow_deploy airflow-api-server false
  patch_airflow_deploy airflow-triggerer false
  local d
  for d in airflow-scheduler airflow-dag-processor; do
    k -n "$AIRFLOW_NAMESPACE" rollout status "deploy/$d" --timeout=600s || warn "deploy/$d not ready yet"
  done
  warn "edit DATASET_CHECKS_CONFIG in dq/dq_tier_a_dag.py to point at your real Iceberg tables"
}

step_dashboards() {
  local f name
  for f in "${!DASHBOARD_FOLDERS[@]}"; do
    name="grafana-dashboard-${f%.json}"
    kn create configmap "$name" --from-file="$f=$WORK_DIR/grafana/dashboards/$f" \
        --dry-run=client -o yaml \
      | k label -f - --local -o yaml grafana_dashboard=1 \
      | k annotate -f - --local -o yaml grafana_folder="${DASHBOARD_FOLDERS[$f]}" \
      | k apply -f -
  done
  ok "${#DASHBOARD_FOLDERS[@]} dashboards provisioned (sidecar picks them up within ~30s)"
}

cmd_install() {
  local steps=("$@")
  (( ${#steps[@]} )) || steps=("${ALL_STEPS[@]}")
  local s
  for s in "${steps[@]}"; do
    declare -F "step_$s" >/dev/null || die "unknown step: $s (valid: ${ALL_STEPS[*]})"
  done
  log "config: ${CONFIG_FILE:-<defaults>}  bundle: $BUNDLE_DIR  namespace: $NAMESPACE"
  render
  for s in "${steps[@]}"; do
    log "──── step: $s"
    "step_$s"
  done
  log "install finished - run: $0 verify"
}

# ---------------------------------------------------------------------------
# Phase 4: verify (INSTALL.md §4)
# ---------------------------------------------------------------------------
cmd_verify() {
  need kubectl curl
  local pass=0 fail=0
  check() { # description command...
    local d="$1"; shift
    if "$@" >/dev/null 2>&1; then ok "$d"; pass=$((pass + 1)); else warn "FAIL: $d"; fail=$((fail + 1)); fi
  }
  local node_ip
  node_ip="$(k get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"

  check "no crashing pods in $NAMESPACE" \
    bash -c "! kubectl ${KCTX[*]} -n $NAMESPACE get pods --no-headers | grep -E 'CrashLoopBackOff|Error|ErrImage|ImagePull'"
  check "Grafana healthy (:30300)" curl -sf "http://$node_ip:30300/api/health"
  check "Control Tower Postgres datasource works" \
    curl -sf -u "admin:$GRAFANA_ADMIN_PASSWORD" "http://$node_ip:30300/api/datasources/uid/control-tower-postgres/health"
  check "dashboards provisioned" \
    bash -c "curl -sf -u 'admin:$GRAFANA_ADMIN_PASSWORD' 'http://$node_ip:30300/api/search?type=dash-db' | grep -q 'Control Tower'"
  check "control-tower logs free of tracebacks" \
    bash -c "! kubectl ${KCTX[*]} -n $NAMESPACE logs -l app=control-tower --tail=300 | grep -q Traceback"
  check "pipelines loaded into the registry table" \
    bash -c "kubectl ${KCTX[*]} -n $NAMESPACE exec control-tower-postgres-0 -- psql -U control_tower -d control_tower -tAc 'select count(*) from pipeline' | grep -qv '^0$'"
  check "Marquez API (:30500)" curl -sf "http://$node_ip:30500/api/v1/namespaces"
  is_true "$USE_MAILHOG" && check "MailHog UI (:30502)" curl -sf "http://$node_ip:30502/"
  check "Alertmanager config loaded" \
    bash -c "kubectl ${KCTX[*]} -n $NAMESPACE logs alertmanager-kube-prometheus-stack-alertmanager-0 -c alertmanager | grep -q 'Completed loading of configuration file'"

  echo; log "verify: $pass passed, $fail failed"
  echo "  Manual checks left (INSTALL.md §4): trigger a test alert and confirm the email lands;"
  echo "  if DQ is in scope: kubectl -n $AIRFLOW_NAMESPACE exec deploy/airflow-scheduler -c scheduler -- airflow dags test dq_tier_a_checks"
  (( fail == 0 ))
}

usage() { sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; }

case "${1:-}" in
  prepare) shift; cmd_prepare "$@" ;;
  load)    shift; cmd_load "$@" ;;
  install) shift; cmd_install "$@" ;;
  verify)  shift; cmd_verify "$@" ;;
  *)       usage; exit 1 ;;
esac
