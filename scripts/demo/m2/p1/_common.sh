#!/usr/bin/env bash
# Shared helpers for P1 M2 demo & evidence scripts (compose and k8s modes).

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "ERROR: _common.sh must be sourced, not executed directly." >&2
  exit 1
fi

P1_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${P1_SCRIPT_DIR}/../../../.." && pwd)"
cd "$PROJECT_ROOT"

# Preserve explicit caller overrides before sourcing .env
_CALLER_MODE="${MODE:-}"
_CALLER_NAMESPACE="${NAMESPACE:-}"
_CALLER_AUDIT_TOKEN="${AUDIT_TOKEN:-}"
_CALLER_AGGREGATOR_URL="${AGGREGATOR_URL:-}"
_CALLER_BMKG_URL="${BMKG_URL:-}"
_CALLER_CLIENT_API_URL="${CLIENT_API_URL:-}"
_CALLER_RUN_ID="${RUN_ID:-}"
_CALLER_EVIDENCE_DIR="${EVIDENCE_DIR:-}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

[[ -n "$_CALLER_MODE" ]] && MODE="$_CALLER_MODE" || true
[[ -n "$_CALLER_NAMESPACE" ]] && NAMESPACE="$_CALLER_NAMESPACE" || true
[[ -n "$_CALLER_AUDIT_TOKEN" ]] && AUDIT_TOKEN="$_CALLER_AUDIT_TOKEN" || true
[[ -n "$_CALLER_AGGREGATOR_URL" ]] && AGGREGATOR_URL="$_CALLER_AGGREGATOR_URL" || true
[[ -n "$_CALLER_BMKG_URL" ]] && BMKG_URL="$_CALLER_BMKG_URL" || true
[[ -n "$_CALLER_CLIENT_API_URL" ]] && CLIENT_API_URL="$_CALLER_CLIENT_API_URL" || true
[[ -n "$_CALLER_RUN_ID" ]] && RUN_ID="$_CALLER_RUN_ID" || true
[[ -n "$_CALLER_EVIDENCE_DIR" ]] && EVIDENCE_DIR="$_CALLER_EVIDENCE_DIR" || true

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
AUDIT_TOKEN="${AUDIT_TOKEN:-${AGGREGATOR_AUDIT_TOKEN:-}}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/p1/${RUN_ID}}"
mkdir -p "$EVIDENCE_DIR"

# Resolve python3/python
PY=""
for _cand in python3 python; do
  if command -v "$_cand" >/dev/null 2>&1 && "$_cand" -c 'import json' >/dev/null 2>&1; then
    PY="$_cand"
    break
  fi
done
if [[ -z "$PY" ]]; then
  echo "ERROR: python3 (or python with json module) is required." >&2
  exit 1
fi

# Detect MODE if not explicitly set
if [[ -z "${MODE:-}" ]]; then
  if command -v kubectl >/dev/null 2>&1 && kubectl -n "$NAMESPACE" get statefulset canonical-store >/dev/null 2>&1; then
    MODE="k8s"
  else
    MODE="compose"
  fi
fi

if [[ "$MODE" != "compose" && "$MODE" != "k8s" ]]; then
  echo "ERROR: invalid MODE='$MODE' (expected 'compose' or 'k8s')." >&2
  exit 1
fi

COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
if [[ "$MODE" == "compose" ]] && command -v docker >/dev/null 2>&1; then
  _detected_name="$(docker compose config --format json 2>/dev/null \
    | "$PY" -c 'import sys,json;print(json.load(sys.stdin).get("name",""))' 2>/dev/null || true)"
  if [[ -n "$_detected_name" ]]; then
    COMPOSE_NAME="$_detected_name"
  fi
fi
DOCKER_INTERNAL_NET="${DOCKER_INTERNAL_NET:-${COMPOSE_NAME}_internal_api_net}"
DOCKER_STORE_NET="${DOCKER_STORE_NET:-${COMPOSE_NAME}_store_net}"
CURL_IMG="${CURL_IMG:-curlimages/curl:8.11.0}"
CRDB_IMG="${CRDB_IMG:-cockroachdb/cockroach:v24.1.6}"

_P1_HELPER_CURL=""
_P1_PF_PIDS=()
_P1_BG_PIDS=()
_P1_TMP_FILES=()

p1_log() {
  echo "[p1 $(date -u +%H:%M:%SZ)] $*"
}

p1_require_audit_token() {
  if [[ -z "${AUDIT_TOKEN:-}" ]]; then
    echo "ERROR: AUDIT_TOKEN (or AGGREGATOR_AUDIT_TOKEN) must be set in env or .env." >&2
    exit 1
  fi
}

p1_cleanup() {
  local pid
  for pid in "${_P1_BG_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${_P1_PF_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "${_P1_HELPER_CURL:-}" ]]; then
    docker rm -f "$_P1_HELPER_CURL" >/dev/null 2>&1 || true
  fi
  local f
  for f in "${_P1_TMP_FILES[@]:-}"; do
    if [[ -n "$f" ]]; then
      rm -f "$f" || true
    fi
  done
  return 0
}
trap p1_cleanup EXIT

p1_pick_port() {
  local start_port="$1"
  "$PY" - "$start_port" <<'PYEOF'
import socket, sys
p = int(sys.argv[1])
for cand in range(p, p + 50):
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", cand))
        s.close()
        print(cand)
        sys.exit(0)
    except OSError:
        s.close()
print(p)
PYEOF
}

AGG_BASE=""
AGG_VIA_HELPER=0

setup_aggregator_access() {
  if [[ -n "${AGGREGATOR_URL:-}" ]]; then
    AGG_BASE="${AGGREGATOR_URL%/}"
    AGG_VIA_HELPER=0
    return 0
  fi

  if [[ "$MODE" == "compose" ]]; then
    if curl -fsS --max-time 2 "http://127.0.0.1:8090/internal/health" >/dev/null 2>&1; then
      AGG_BASE="http://127.0.0.1:8090"
      AGG_VIA_HELPER=0
      return 0
    fi
    if ! docker network inspect "$DOCKER_INTERNAL_NET" >/dev/null 2>&1; then
      echo "ERROR: Docker network $DOCKER_INTERNAL_NET not found. Start compose stack first." >&2
      exit 1
    fi
    _P1_HELPER_CURL="p1-curl-$$"
    docker rm -f "$_P1_HELPER_CURL" >/dev/null 2>&1 || true
    local img=""
    for cand in "$CURL_IMG" "curlimages/curl:8.10.1" "curlimages/curl:8.5.0"; do
      if docker image inspect "$cand" >/dev/null 2>&1; then
        img="$cand"
        break
      fi
    done
    img="${img:-$CURL_IMG}"
    docker run -d --rm --name "$_P1_HELPER_CURL" --network "$DOCKER_INTERNAL_NET" "$img" sleep 3600 >/dev/null
    AGG_BASE="http://aggregator:8090"
    AGG_VIA_HELPER=1
    return 0
  fi

  # k8s mode: port-forward svc/aggregator
  local pf_port
  pf_port="$(p1_pick_port 18090)"
  kubectl -n "$NAMESPACE" port-forward svc/aggregator "${pf_port}:8090" >/dev/null 2>&1 &
  local pf_pid=$!
  _P1_PF_PIDS+=("$pf_pid")
  local i
  for i in $(seq 1 30); do
    if curl -fsS --max-time 2 "http://127.0.0.1:${pf_port}/internal/health" >/dev/null 2>&1; then
      AGG_BASE="http://127.0.0.1:${pf_port}"
      AGG_VIA_HELPER=0
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: kubectl port-forward svc/aggregator did not become ready." >&2
  exit 1
}

BMKG_BASE=""
setup_bmkg_access() {
  if [[ -n "${BMKG_URL:-}" ]]; then
    BMKG_BASE="${BMKG_URL%/}"
    return 0
  fi
  if [[ "$MODE" == "compose" ]]; then
    BMKG_BASE="http://127.0.0.1:${BMKG_PORT:-8081}"
    return 0
  fi
  local pf_port
  pf_port="$(p1_pick_port 18081)"
  kubectl -n "$NAMESPACE" port-forward svc/bmkg-mock "${pf_port}:8081" >/dev/null 2>&1 &
  local pf_pid=$!
  _P1_PF_PIDS+=("$pf_pid")
  local i
  for i in $(seq 1 30); do
    if curl -fsS --max-time 2 "http://127.0.0.1:${pf_port}/health" >/dev/null 2>&1; then
      BMKG_BASE="http://127.0.0.1:${pf_port}"
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: kubectl port-forward svc/bmkg-mock did not become ready." >&2
  exit 1
}

# agg_http_call METHOD PATH BODY_OUT_FILE [extra curl args...]
# Sets global HTTP_CODE and TIME_TOTAL_S. Never aborts on curl error/5xx.
agg_http_call() {
  local method="$1"
  local path="$2"
  local out_file="$3"
  shift 3
  local timeout="${HTTP_TIMEOUT:-10}"
  local raw_file meta
  raw_file="$(mktemp)"
  if [[ "$AGG_VIA_HELPER" -eq 1 ]]; then
    if ! docker exec "$_P1_HELPER_CURL" curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "${AGG_BASE}${path}" > "$raw_file" 2>/dev/null; then
      printf '\n000\t%s.000' "$timeout" > "$raw_file"
    fi
  else
    if ! curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "${AGG_BASE}${path}" > "$raw_file" 2>/dev/null; then
      printf '\n000\t%s.000' "$timeout" > "$raw_file"
    fi
  fi
  meta="$(tail -n 1 "$raw_file" | tr -d '\r')"
  sed '$d' "$raw_file" > "$out_file"
  rm -f "$raw_file"
  HTTP_CODE="${meta%%$'\t'*}"
  TIME_TOTAL_S="${meta#*$'\t'}"
  if [[ -z "$HTTP_CODE" ]]; then HTTP_CODE="000"; fi
  if [[ -z "$TIME_TOTAL_S" ]]; then TIME_TOTAL_S="0.000"; fi
  return 0
}

agg_get_json() {
  local path="$1"
  local out_file="$2"
  shift 2
  agg_http_call "GET" "$path" "$out_file" "$@"
  [[ "$HTTP_CODE" == "200" ]]
}

# List configured/present CRDB nodes (compose service names or k8s pod names)
crdb_list_nodes() {
  if [[ "$MODE" == "compose" ]]; then
    if [[ -n "${CRDB_NODES:-}" ]]; then
      echo "$CRDB_NODES"
    else
      echo "canonical-store-0 canonical-store-1 canonical-store-2 canonical-store-3"
    fi
  else
    kubectl -n "$NAMESPACE" get pods -l app=canonical-store -o jsonpath='{.items[*].metadata.name}'
  fi
}

# Return the first live CRDB service/pod that responds to SQL
crdb_alive_node() {
  local n
  for n in $(crdb_list_nodes); do
    if [[ "$MODE" == "compose" ]]; then
      if docker compose exec -T "$n" cockroach sql --insecure -d canonical -e "SELECT 1" >/dev/null 2>&1; then
        echo "$n"
        return 0
      fi
    else
      if kubectl -n "$NAMESPACE" exec "$n" -- cockroach sql --insecure -d canonical -e "SELECT 1" >/dev/null 2>&1; then
        echo "$n"
        return 0
      fi
    fi
  done
  return 1
}

crdb_exec() {
  local node="$1"
  shift
  if [[ "$MODE" == "compose" ]]; then
    docker compose exec -T "$node" "$@"
  else
    kubectl -n "$NAMESPACE" exec "$node" -- "$@"
  fi
}

crdb_sql() {
  local sql="$1"
  local format="${2:-table}"
  local node
  node="$(crdb_alive_node)"
  if [[ -z "$node" ]]; then
    echo "ERROR: no live CockroachDB node found." >&2
    return 1
  fi
  crdb_exec "$node" cockroach sql --insecure -d canonical --format="$format" -e "$sql"
}

crdb_node_status() {
  local format="${1:-tsv}"
  local extra="${2:-}"
  local node
  node="$(crdb_alive_node)"
  if [[ -z "$node" ]]; then
    echo "ERROR: no live CockroachDB node found." >&2
    return 1
  fi
  if [[ -n "$extra" ]]; then
    crdb_exec "$node" cockroach node status --insecure "$extra" --format="$format"
  else
    crdb_exec "$node" cockroach node status --insecure --format="$format"
  fi
}

crdb_vars() {
  local node="$1"
  crdb_exec "$node" curl -fsS --max-time 2 "http://localhost:8080/_status/vars" 2>/dev/null || true
}

# Print space-separated live CRDB node IDs from `cockroach node status`.
crdb_live_node_ids() {
  local st
  st="$(crdb_node_status tsv "--all" 2>/dev/null || true)"
  "$PY" - "$st" <<'PYEOF'
import csv, io, sys
raw = sys.argv[1].strip()
if not raw:
    sys.exit(0)
reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
print(" ".join(r["id"] for r in reader if r.get("is_live") == "true"))
PYEOF
}

# Print space-separated dead, non-decommissioned CRDB node IDs.
crdb_dead_node_ids() {
  local st
  st="$(crdb_node_status tsv "--all" 2>/dev/null || true)"
  "$PY" - "$st" <<'PYEOF'
import csv, io, sys
raw = sys.argv[1].strip()
if not raw:
    sys.exit(0)
reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
print(" ".join(
    r["id"] for r in reader
    if r.get("is_live") == "false"
    and r.get("membership") != "decommissioned"
    and r.get("is_decommissioning") == "false"
))
PYEOF
}

# Count ranges of canonical_events with fewer than 3 live replicas.
crdb_under_replicated_count() {
  local st rng
  st="$(crdb_node_status tsv "--all" 2>/dev/null || true)"
  rng="$(crdb_sql "SHOW RANGES FROM TABLE canonical_events WITH DETAILS;" tsv 2>/dev/null || true)"
  "$PY" - "$st" "$rng" <<'PYEOF'
import csv, io, sys
st_raw, rng_raw = sys.argv[1].strip(), sys.argv[2].strip()
if not st_raw or not rng_raw:
    print(999)
    sys.exit(0)
live = {r["id"] for r in csv.DictReader(io.StringIO(st_raw), delimiter="\t") if r.get("is_live") == "true"}
under = 0
for r in csv.DictReader(io.StringIO(rng_raw), delimiter="\t"):
    reps = [x.strip() for x in r.get("replicas", "").strip("{}").split(",") if x.strip()]
    if sum(1 for x in reps if x in live) < 3:
        under += 1
print(under)
PYEOF
}

# Sample CRDB per-node QPS/range metrics every INTERVAL_S for DURATION_S (or until killed)
sample_crdb_metrics_loop() {
  local out_csv="$1"
  local interval_s="${2:-2}"
  local max_duration_s="${3:-300}"
  echo "timestamp,node,store_id,node_id,sql_insert_count,sql_select_count,writes_per_sec,range_count,lease_count,logical_bytes" > "$out_csv"
  local end_ts=$(( $(date +%s) + max_duration_s ))
  while [[ $(date +%s) -lt $end_ts ]]; do
    local ts alive kv_rows nodes_list
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    alive="$(crdb_alive_node || true)"
    if [[ -n "$alive" ]]; then
      kv_rows="$(crdb_exec "$alive" cockroach sql --insecure -d canonical --format=csv -e "
        SELECT s.store_id,
               s.node_id,
               COALESCE(g.address, '') AS addr,
               COALESCE(g.locality, '') AS locality,
               round(s.writes_per_second, 2) AS wps,
               s.range_count,
               s.lease_count,
               s.logical_bytes
        FROM crdb_internal.kv_store_status s
        LEFT JOIN crdb_internal.gossip_nodes g ON g.node_id = s.node_id
        ORDER BY s.store_id;
      " 2>/dev/null | tail -n +2 | tr -d '\r' || true)"

      nodes_list="$(crdb_list_nodes)"
      local node
      for node in $nodes_list; do
        local vars ins sel match store_id node_id wps rc lc lb
        vars="$(crdb_vars "$node")"
        [[ -z "$vars" ]] && continue
        ins="$(echo "$vars" | awk '/^sql_insert_count\{/ {v=$2} END {print v+0}')"
        sel="$(echo "$vars" | awk '/^sql_select_count\{/ {v=$2} END {print v+0}')"
        ins="${ins:-0}"
        sel="${sel:-0}"
        match="$(echo "$kv_rows" | grep -E "(^|,)${node}([:.,]|,|$)" | head -n1 || true)"
        if [[ -z "$match" ]]; then
          match="$(echo "$kv_rows" | grep "node=${node}" | head -n1 || true)"
        fi
        if [[ -n "$match" ]]; then
          store_id="$(echo "$match" | cut -d',' -f1)"
          node_id="$(echo "$match" | cut -d',' -f2)"
          wps="$(echo "$match" | cut -d',' -f5)"
          rc="$(echo "$match" | cut -d',' -f6)"
          lc="$(echo "$match" | cut -d',' -f7)"
          lb="$(echo "$match" | cut -d',' -f8)"
        else
          store_id="0"; node_id="0"; wps="0"; rc="0"; lc="0"; lb="0"
        fi
        echo "${ts},${node},${store_id},${node_id},${ins},${sel},${wps},${rc},${lc},${lb}" >> "$out_csv"
      done
    fi
    sleep "$interval_s"
  done
}
