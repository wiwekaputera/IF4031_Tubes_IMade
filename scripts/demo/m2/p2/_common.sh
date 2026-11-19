#!/usr/bin/env bash
# Shared helpers for P2 M2 demo & evidence scripts (compose and k8s modes).

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "ERROR: _common.sh must be sourced, not executed directly." >&2
  exit 1
fi

P2_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${P2_SCRIPT_DIR}/../../../.." && pwd)"
cd "$PROJECT_ROOT"

# Preserve explicit caller overrides before sourcing .env
_CALLER_MODE="${MODE:-}"
_CALLER_NAMESPACE="${NAMESPACE:-}"
_CALLER_AUDIT_TOKEN="${AUDIT_TOKEN:-}"
_CALLER_AGGREGATOR_URL="${AGGREGATOR_URL:-}"
_CALLER_BMKG_URL="${BMKG_URL:-}"
_CALLER_PVMBG_URL="${PVMBG_URL:-}"
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
[[ -n "$_CALLER_PVMBG_URL" ]] && PVMBG_URL="$_CALLER_PVMBG_URL" || true
[[ -n "$_CALLER_RUN_ID" ]] && RUN_ID="$_CALLER_RUN_ID" || true
[[ -n "$_CALLER_EVIDENCE_DIR" ]] && EVIDENCE_DIR="$_CALLER_EVIDENCE_DIR" || true

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
AUDIT_TOKEN="${AUDIT_TOKEN:-${AGGREGATOR_AUDIT_TOKEN:-}}"
BMKG_API_KEY="${BMKG_API_KEY:-${AGGREGATOR_BMKG_API_KEY:-}}"
PVMBG_BEARER_TOKEN="${PVMBG_BEARER_TOKEN:-${AGGREGATOR_PVMBG_BEARER_TOKEN:-}}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/p2/${RUN_ID}}"
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
  if command -v kubectl >/dev/null 2>&1 && kubectl -n "$NAMESPACE" get deployment aggregator >/dev/null 2>&1; then
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
CURL_IMG="${CURL_IMG:-curlimages/curl:8.11.0}"

_P2_HELPER_CURL=""
_P2_PF_PIDS=()
_P2_BG_PIDS=()
_P2_TMP_FILES=()
_P2_K8S_CURL_POD=""

p2_log() {
  echo "[p2 $(date -u +%H:%M:%SZ)] $*"
}

p2_cleanup() {
  local pid
  for pid in "${_P2_BG_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${_P2_PF_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "${_P2_HELPER_CURL:-}" ]]; then
    docker rm -f "$_P2_HELPER_CURL" >/dev/null 2>&1 || true
  fi
  if [[ -n "${_P2_K8S_CURL_POD:-}" ]] && command -v kubectl >/dev/null 2>&1; then
    kubectl -n "$NAMESPACE" delete pod "$_P2_K8S_CURL_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  local f
  for f in "${_P2_TMP_FILES[@]:-}"; do
    if [[ -n "$f" ]]; then
      rm -f "$f" || true
    fi
  done
  return 0
}
trap p2_cleanup EXIT

p2_pick_port() {
  local start_port="$1"
  "$PY" - "$start_port" <<'PYEOF'
import socket, sys
p = int(sys.argv[1])
for cand in range(p, p + 80):
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

p2_pick_curl_img() {
  local cand
  for cand in "$CURL_IMG" "curlimages/curl:8.10.1" "curlimages/curl:8.5.0"; do
    if docker image inspect "$cand" >/dev/null 2>&1; then
      echo "$cand"
      return 0
    fi
  done
  echo "$CURL_IMG"
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
    if [[ -z "${_P2_HELPER_CURL:-}" ]]; then
      _P2_HELPER_CURL="p2-curl-$$"
      docker rm -f "$_P2_HELPER_CURL" >/dev/null 2>&1 || true
      local img
      img="$(p2_pick_curl_img)"
      docker run -d --rm --name "$_P2_HELPER_CURL" --network "$DOCKER_INTERNAL_NET" "$img" sleep 3600 >/dev/null
    fi
    AGG_BASE="http://aggregator:8090"
    AGG_VIA_HELPER=1
    return 0
  fi

  local pf_port
  pf_port="$(p2_pick_port 18090)"
  kubectl -n "$NAMESPACE" port-forward svc/aggregator "${pf_port}:8090" >/dev/null 2>&1 &
  local pf_pid=$!
  _P2_PF_PIDS+=("$pf_pid")
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
  pf_port="$(p2_pick_port 18081)"
  kubectl -n "$NAMESPACE" port-forward svc/bmkg-mock "${pf_port}:8081" >/dev/null 2>&1 &
  local pf_pid=$!
  _P2_PF_PIDS+=("$pf_pid")
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

PVMBG_BASE=""
setup_pvmbg_access() {
  if [[ -n "${PVMBG_URL:-}" ]]; then
    PVMBG_BASE="${PVMBG_URL%/}"
    return 0
  fi
  if [[ "$MODE" == "compose" ]]; then
    PVMBG_BASE="http://127.0.0.1:${PVMBG_PORT:-8082}"
    return 0
  fi
  local pf_port
  pf_port="$(p2_pick_port 18082)"
  kubectl -n "$NAMESPACE" port-forward svc/pvmbg-mock "${pf_port}:8082" >/dev/null 2>&1 &
  local pf_pid=$!
  _P2_PF_PIDS+=("$pf_pid")
  local i
  for i in $(seq 1 30); do
    if curl -fsS --max-time 2 "http://127.0.0.1:${pf_port}/health" >/dev/null 2>&1; then
      PVMBG_BASE="http://127.0.0.1:${pf_port}"
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: kubectl port-forward svc/pvmbg-mock did not become ready." >&2
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
    if ! docker exec "$_P2_HELPER_CURL" curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "${AGG_BASE}${path}" > "$raw_file" 2>/dev/null; then
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

# host_http_call METHOD FULL_URL OUT_FILE [extra curl args...]
# Always executes curl on the host (for host-exposed ports and kubectl port-forwards).
host_http_call() {
  local method="$1"
  local url="$2"
  local out_file="$3"
  shift 3
  local timeout="${HTTP_TIMEOUT:-10}"
  local raw_file meta
  raw_file="$(mktemp)"
  if ! curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "$url" > "$raw_file" 2>/dev/null; then
    printf '\n000\t%s.000' "$timeout" > "$raw_file"
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

# direct_http_call METHOD FULL_URL OUT_FILE [extra curl args...]
# Routes through helper container in compose when URL targets an internal container IP.
direct_http_call() {
  local method="$1"
  local url="$2"
  local out_file="$3"
  shift 3
  local timeout="${HTTP_TIMEOUT:-5}"
  local raw_file meta
  raw_file="$(mktemp)"
  if [[ "$MODE" == "compose" && "$AGG_VIA_HELPER" -eq 1 ]]; then
    if ! docker exec "$_P2_HELPER_CURL" curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "$url" > "$raw_file" 2>/dev/null; then
      printf '\n000\t%s.000' "$timeout" > "$raw_file"
    fi
  else
    if ! curl -sS -X "$method" -m "$timeout" -w '\n%{http_code}\t%{time_total}' "$@" "$url" > "$raw_file" 2>/dev/null; then
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

# CRDB helpers
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

# List running aggregator container names (compose) or pod names (k8s).
agg_list_instances() {
  if [[ "$MODE" == "compose" ]]; then
    local cids
    cids="$(docker compose ps -q aggregator 2>/dev/null || true)"
    if [[ -z "$cids" ]]; then
      return 0
    fi
    docker inspect --format '{{if .State.Running}}{{.Name}}{{end}}' $cids 2>/dev/null \
      | sed 's|^/||' | sed '/^$/d' | sort
  else
    kubectl -n "$NAMESPACE" get pods -l app=aggregator \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
      | sed '/^$/d' | sort
  fi
}

# Return the IP address of a compose container on DOCKER_INTERNAL_NET.
agg_compose_ip() {
  local container="$1"
  docker inspect -f "{{with index .NetworkSettings.Networks \"${DOCKER_INTERNAL_NET}\"}}{{.IPAddress}}{{end}}" "$container" 2>/dev/null || true
}

# Ensure at least N aggregator replicas are running and healthy/ready.
agg_ensure_replicas() {
  local want="${1:-3}"
  if [[ "$MODE" == "compose" ]]; then
    local running
    running="$(agg_list_instances | wc -l | tr -d ' ')"
    if [[ "$running" -lt "$want" ]]; then
      p2_log "Scaling compose aggregator to ${want} replicas..."
      docker compose up -d --scale "aggregator=${want}" --no-recreate >/dev/null
    fi
    local deadline=$(( $(date +%s) + 90 ))
    while [[ $(date +%s) -lt $deadline ]]; do
      local healthy_cnt=0 c st
      for c in $(agg_list_instances); do
        st="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' "$c" 2>/dev/null || true)"
        if [[ "$st" == "healthy" ]]; then
          healthy_cnt=$((healthy_cnt + 1))
        fi
      done
      if [[ "$healthy_cnt" -ge "$want" ]]; then
        return 0
      fi
      sleep 2
    done
    echo "ERROR: timed out waiting for ${want} healthy aggregator replicas." >&2
    return 1
  else
    local ready_cnt
    ready_cnt="$(kubectl -n "$NAMESPACE" get deployment aggregator -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
    ready_cnt="${ready_cnt:-0}"
    if [[ "$ready_cnt" -lt "$want" ]]; then
      p2_log "Waiting for deployment/aggregator to reach ${want} ready replicas..."
      kubectl -n "$NAMESPACE" scale deployment/aggregator --replicas="$want" >/dev/null 2>&1 || true
      kubectl -n "$NAMESPACE" rollout status deployment/aggregator --timeout=120s >/dev/null
    fi
  fi
}

# Query GET /internal/leader on a specific replica (container name in compose, pod name in k8s).
agg_instance_leader_json() {
  local target="$1"
  local out_file="$2"
  if [[ "$MODE" == "compose" ]]; then
    local ip
    ip="$(agg_compose_ip "$target")"
    if [[ -z "$ip" ]]; then
      echo "{}" > "$out_file"
      return 1
    fi
    direct_http_call "GET" "http://${ip}:8090/internal/leader" "$out_file"
    [[ "$HTTP_CODE" == "200" ]]
  else
    if kubectl -n "$NAMESPACE" get --raw "/api/v1/namespaces/${NAMESPACE}/pods/${target}:8090/proxy/internal/leader" > "$out_file" 2>/dev/null; then
      return 0
    fi
    local pf_port pf_pid i
    pf_port="$(p2_pick_port 18290)"
    kubectl -n "$NAMESPACE" port-forward "pod/${target}" "${pf_port}:8090" >/dev/null 2>&1 &
    pf_pid=$!
    for i in $(seq 1 20); do
      if curl -fsS --max-time 2 "http://127.0.0.1:${pf_port}/internal/leader" > "$out_file" 2>/dev/null; then
        kill "$pf_pid" 2>/dev/null || true
        wait "$pf_pid" 2>/dev/null || true
        return 0
      fi
      sleep 0.3
    done
    kill "$pf_pid" 2>/dev/null || true
    wait "$pf_pid" 2>/dev/null || true
    echo "{}" > "$out_file"
    return 1
  fi
}

# Query GET /internal/ready on a specific replica. Prints "HTTP_CODE BODY".
agg_instance_ready_probe() {
  local target="$1"
  local tmp
  tmp="$(mktemp)"
  _P2_TMP_FILES+=("$tmp")
  if [[ "$MODE" == "compose" ]]; then
    local ip
    ip="$(agg_compose_ip "$target")"
    if [[ -z "$ip" ]]; then
      echo "000 no_ip"
      return 0
    fi
    direct_http_call "GET" "http://${ip}:8090/internal/ready" "$tmp"
    echo "${HTTP_CODE} $(tr -d '\r\n' < "$tmp")"
  else
    local body
    if body="$(kubectl -n "$NAMESPACE" get --raw "/api/v1/namespaces/${NAMESPACE}/pods/${target}:8090/proxy/internal/ready" 2>/dev/null)"; then
      echo "200 $(echo "$body" | tr -d '\r\n')"
    else
      echo "503 unavailable"
    fi
  fi
}

# Probe all running replicas' /internal/leader endpoints and return a JSON summary file.
agg_probe_all_leaders() {
  local out_json="$1"
  local tmp_dir inst resp_file
  tmp_dir="$(mktemp -d)"
  for inst in $(agg_list_instances); do
    resp_file="${tmp_dir}/${inst}.json"
    agg_instance_leader_json "$inst" "$resp_file" >/dev/null 2>&1 || true
  done
  "$PY" - "$tmp_dir" "$out_json" <<'PYEOF'
import glob, json, os, sys
tmp_dir, out_json = sys.argv[1], sys.argv[2]
items = []
for p in sorted(glob.glob(os.path.join(tmp_dir, "*.json"))):
    target = os.path.basename(p)[:-5]
    try:
        data = json.load(open(p, encoding="utf-8"))
    except Exception:
        data = {}
    items.append({"target": target, "leader_response": data})
with open(out_json, "w", encoding="utf-8") as f:
    json.dump(items, f, indent=2)
PYEOF
  rm -rf "$tmp_dir"
}

# Find current leader across running replicas.
# Outputs tab-separated: TARGET<TAB>INSTANCE_ID<TAB>FENCING_TOKEN<TAB>EXPIRES_AT
agg_find_leader() {
  local tmp_json
  tmp_json="$(mktemp)"
  _P2_TMP_FILES+=("$tmp_json")
  agg_probe_all_leaders "$tmp_json"
  "$PY" - "$tmp_json" <<'PYEOF'
import json, sys
items = json.load(open(sys.argv[1], encoding="utf-8"))
for it in items:
    r = it.get("leader_response") or {}
    if r.get("is_leader") is True:
        print(f"{it['target']}\t{r.get('instance_id','')}\t{r.get('fencing_token',0)}\t{r.get('lease_expires_at','')}")
        sys.exit(0)
sys.exit(1)
PYEOF
}

# Wait up to TIMEOUT_S until a replica holds an active leader lease (optionally requiring holder != EXCLUDE_ID and token > MIN_TOKEN).
agg_wait_leader() {
  local timeout_s="${1:-45}"
  local exclude_id="${2:-}"
  local min_token="${3:-0}"
  local deadline=$(( $(date +%s) + timeout_s ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local tsv
    tsv="$(crdb_sql "
      SELECT holder, fencing_token, expires_at, (expires_at > now()) AS is_active
      FROM leader_leases
      WHERE name = 'ingest-poller';
    " tsv 2>/dev/null || true)"
    local parsed
    parsed="$("$PY" - "$tsv" "$exclude_id" "$min_token" <<'PYEOF' || true
import csv, io, sys
raw, excl, min_tok = sys.argv[1].strip(), sys.argv[2], int(sys.argv[3])
if not raw:
    sys.exit(1)
rows = list(csv.DictReader(io.StringIO(raw), delimiter="\t"))
if not rows:
    sys.exit(1)
r = rows[0]
holder = r.get("holder", "")
tok = int(r.get("fencing_token", "0") or 0)
active = r.get("is_active", "").strip().lower() in ("t", "true", "1")
if active and holder and holder != excl and tok > min_tok:
    print(f"{holder}\t{tok}\t{r.get('expires_at','')}")
    sys.exit(0)
sys.exit(1)
PYEOF
)"
    if [[ -n "$parsed" ]]; then
      echo "$parsed"
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# Collect logs from all aggregator instances (including recently stopped/exited containers in compose).
agg_collect_logs() {
  local out_file="$1"
  : > "$out_file"
  if [[ "$MODE" == "compose" ]]; then
    local cids c
    cids="$(docker ps -a -q --filter "label=com.docker.compose.project=${COMPOSE_NAME}" --filter "label=com.docker.compose.service=aggregator" 2>/dev/null || true)"
    for c in $cids; do
      local cname
      cname="$(docker inspect --format '{{.Name}}' "$c" 2>/dev/null | sed 's|^/||')"
      docker logs --timestamps "$c" 2>&1 | sed "s/^/[${cname}] /" >> "$out_file" || true
    done
  else
    kubectl -n "$NAMESPACE" logs -l app=aggregator --all-containers --prefix --tail=-1 > "$out_file" 2>&1 || true
  fi
}

# Query DB current time in RFC3339UTC format (microsecond precision).
crdb_now_utc() {
  crdb_sql "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS db_now;" tsv 2>/dev/null \
    | tail -n 1 | tr -d '\r'
}
