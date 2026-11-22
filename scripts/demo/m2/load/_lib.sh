#!/usr/bin/env bash
# Shared helpers for the load-test scripts (Kubernetes deployment).
# Source it; do not run it.

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "_lib.sh must be sourced" >&2
  exit 1
fi

LOAD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LOAD_DIR/../../../.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/load/${RUN_ID}}"
mkdir -p "$EVIDENCE_DIR"
NODE_IP="${NODE_IP:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')}"
API_URL="${API_URL:-http://${NODE_IP}:30080}"
TOKEN_URL="${TOKEN_URL:-http://${NODE_IP}:30083/oauth/token}"
K6="${K6:-k6}"

log() { echo "[load $(date -u +%H:%M:%SZ)] $*" | tee -a "$EVIDENCE_DIR/run.log"; }

# Settings for the duration of a test, restored by test_env_restore:
#  - client-api: per-client rate limit lifted (the test measures capacity,
#    not the per-client limit) and 1% trace sampling so tracing does not
#    distort the results;
#  - token-service: 15-minute access tokens so one token per client lasts
#    the whole run.
test_env_apply() {
  log "test settings: client-api RPS_LIMIT=1000000 BURST=100000, traces 1%, LOG_LEVEL=warn; token-service ACCESS_TOKEN_TTL=900"
  kubectl -n "$NS" set env deploy/client-api RPS_LIMIT=1000000 BURST=100000 \
    LOG_LEVEL=warn OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.01 >/dev/null
  kubectl -n "$NS" set env deploy/aggregator \
    LOG_LEVEL=warn OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.01 >/dev/null
  kubectl -n "$NS" set env deploy/token-service ACCESS_TOKEN_TTL=900 >/dev/null
  kubectl -n "$NS" rollout status deploy/client-api --timeout=180s >/dev/null
  kubectl -n "$NS" rollout status deploy/aggregator --timeout=180s >/dev/null
  kubectl -n "$NS" rollout status deploy/token-service --timeout=180s >/dev/null
}

test_env_restore() {
  log "restoring client-api, aggregator, and token-service settings"
  kubectl -n "$NS" set env deploy/client-api RPS_LIMIT- BURST- LOG_LEVEL- OTEL_TRACES_SAMPLER- OTEL_TRACES_SAMPLER_ARG- >/dev/null 2>&1 || true
  kubectl -n "$NS" set env deploy/aggregator LOG_LEVEL- OTEL_TRACES_SAMPLER- OTEL_TRACES_SAMPLER_ARG- >/dev/null 2>&1 || true
  kubectl -n "$NS" set env deploy/token-service ACCESS_TOKEN_TTL- >/dev/null 2>&1 || true
}

# kube ARGS...: kubectl with retries; an API server busy under load may
# time out once, and a missed scale step would invalidate the run.
kube() {
  local i
  for i in 1 2 3 4 5; do
    kubectl "$@" && return 0
    sleep 2
  done
  log "kubectl $* failed 5 times"
  return 1
}

# scale_aggregator N: set replicas and wait until all are ready.
scale_aggregator() {
  kube -n "$NS" scale deploy/aggregator --replicas="$1" >/dev/null
  kubectl -n "$NS" rollout status deploy/aggregator --timeout=300s >/dev/null
}

# record_replicas FILE: append "time,desired,ready,updated" every 2 s until killed.
record_replicas() {
  echo "time_utc,desired,ready,updated,pods" > "$1"
  while true; do
    local line
    line=$(kubectl -n "$NS" get deploy/aggregator -o jsonpath='{.spec.replicas},{.status.readyReplicas},{.status.updatedReplicas}' 2>/dev/null)
    local pods
    pods=$(kubectl -n "$NS" get pods -l app=aggregator --no-headers 2>/dev/null | awk '{printf "%s:%s ", $1, $3}')
    echo "$(date -u +%H:%M:%S),${line},${pods}" >> "$1"
    sleep 2
  done
}

# run_k6 SUMMARY_FILE [k6 -e args...]
run_k6() {
  local summary="$1"; shift
  "$K6" run --quiet --summary-export "$summary" \
    -e BASE_URL="$API_URL" -e TOKEN_URL="$TOKEN_URL" \
    -e FIELD_ID="${CLIENT_FIELD_TEAM_ID:-client-field-team}" -e FIELD_SECRET="${CLIENT_FIELD_TEAM_SECRET:-}" \
    -e MEDIA_ID="${CLIENT_MEDIA_ID:-client-media}" -e MEDIA_SECRET="${CLIENT_MEDIA_SECRET:-}" \
    "$@" tests/load/k6_m2.js
}

PY=""
for _c in python3 python; do
  if command -v "$_c" >/dev/null 2>&1 && "$_c" -c 'import json' >/dev/null 2>&1; then PY="$_c"; break; fi
done
