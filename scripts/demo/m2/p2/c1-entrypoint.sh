#!/usr/bin/env bash
# Criterion 1: >=3 Aggregator instances behind one entry point; unready/draining
# instance is removed from endpoints while continuous requests keep returning HTTP 200.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access
agg_ensure_replicas 3

REQUEST_LOOP_S="${REQUEST_LOOP_S:-18}"
REQ_INTERVAL_S="${REQ_INTERVAL_S:-0.25}"

p2_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"

# 1. Capture initial topology & endpoints
p2_log "Capturing initial >=3 aggregator topology and endpoints..."
if [[ "$MODE" == "compose" ]]; then
  {
    echo "=== docker compose ps aggregator ==="
    docker compose ps aggregator
    echo ""
    echo "=== aggregator container IPs & readiness ==="
    for c in $(agg_list_instances); do
      ip="$(agg_compose_ip "$c")"
      ready="$(agg_instance_ready_probe "$c")"
      echo "${c}  ip=${ip}  ready=${ready}"
    done
  } > "${EVIDENCE_DIR}/c1-topology-before.txt"

  {
    echo "=== DNS A records for single entrypoint 'aggregator' (before) ==="
    docker exec "$_P2_HELPER_CURL" sh -c 'nslookup aggregator 127.0.0.11 || getent ahosts aggregator' 2>&1 || true
  } > "${EVIDENCE_DIR}/c1-endpoints-before.txt"

  INIT_REPLICAS="$(agg_list_instances | wc -l | tr -d ' ')"
  INIT_ENDPOINTS="$(docker exec "$_P2_HELPER_CURL" getent ahosts aggregator 2>/dev/null | awk '{print $1}' | sort -u | wc -l | tr -d ' ')"
else
  {
    echo "=== kubectl get pods -o wide -l app=aggregator ==="
    kubectl -n "$NAMESPACE" get pods -o wide -l app=aggregator
    echo ""
    echo "=== distinct nodes hosting aggregator pods ==="
    kubectl -n "$NAMESPACE" get pods -l app=aggregator -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.nodeName}{"\n"}{end}'
  } > "${EVIDENCE_DIR}/c1-topology-before.txt"

  {
    echo "=== kubectl get endpoints aggregator (before) ==="
    kubectl -n "$NAMESPACE" get endpoints aggregator -o wide
    echo ""
    echo "=== kubectl get endpointslice (before) ==="
    kubectl -n "$NAMESPACE" get endpointslice -l kubernetes.io/service-name=aggregator -o wide || true
  } > "${EVIDENCE_DIR}/c1-endpoints-before.txt"

  INIT_REPLICAS="$(agg_list_instances | wc -l | tr -d ' ')"
  INIT_ENDPOINTS="$(kubectl -n "$NAMESPACE" get endpoints aggregator -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null | sed '/^$/d' | wc -l | tr -d ' ')"
fi

cat "${EVIDENCE_DIR}/c1-topology-before.txt"
cat "${EVIDENCE_DIR}/c1-endpoints-before.txt"

# Pick a victim (prefer non-leader if identifiable)
LEADER_TARGET="$(agg_find_leader 2>/dev/null | cut -f1 || true)"
VICTIM=""
for inst in $(agg_list_instances); do
  if [[ "$inst" != "$LEADER_TARGET" ]]; then
    VICTIM="$inst"
    break
  fi
done
VICTIM="${VICTIM:-$(agg_list_instances | head -n 1)}"
p2_log "Selected victim replica for unreadiness/drain test: ${VICTIM}"

# 2. Start continuous request loop against the single entrypoint
REQ_CSV="${EVIDENCE_DIR}/c1-requests.csv"
echo "timestamp,http_code,latency_s" > "$REQ_CSV"

if [[ "$MODE" == "k8s" ]]; then
  _P2_K8S_CURL_POD="p2-c1-curl-$$"
  p2_log "Starting in-cluster curl pod ${_P2_K8S_CURL_POD} to hit http://aggregator:8090..."
  if kubectl -n "$NAMESPACE" run "$_P2_K8S_CURL_POD" --image="$CURL_IMG" --restart=Never --command -- sleep 180 >/dev/null 2>&1 \
    && kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${_P2_K8S_CURL_POD}" --timeout=45s >/dev/null 2>&1; then
    K8S_USE_CURL_POD=1
  else
    p2_log "WARN: could not start in-cluster curl pod; falling back to port-forward svc/aggregator"
    kubectl -n "$NAMESPACE" delete pod "$_P2_K8S_CURL_POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    _P2_K8S_CURL_POD=""
    K8S_USE_CURL_POD=0
  fi
else
  K8S_USE_CURL_POD=0
fi

p2_log "Starting continuous request loop against single entrypoint for ${REQUEST_LOOP_S}s..."
(
  tmp_body="$(mktemp)"
  end_ts=$(( $(date +%s) + REQUEST_LOOP_S ))
  while [[ $(date +%s) -lt $end_ts ]]; do
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    if [[ "$MODE" == "k8s" && "$K8S_USE_CURL_POD" -eq 1 ]]; then
      out="$(kubectl -n "$NAMESPACE" exec "$_P2_K8S_CURL_POD" -- curl -sS -m 5 -o /dev/null -w '%{http_code}\t%{time_total}' "http://aggregator:8090/internal/events?limit=5" 2>/dev/null || echo -e "000\t5.000")"
      code="${out%%$'\t'*}"
      lat="${out#*$'\t'}"
      echo "${ts},${code:-000},${lat:-0.000}" >> "$REQ_CSV"
    else
      agg_http_call "GET" "/internal/events?limit=5" "$tmp_body"
      echo "${ts},${HTTP_CODE},${TIME_TOTAL_S}" >> "$REQ_CSV"
    fi
    sleep "$REQ_INTERVAL_S"
  done
  rm -f "$tmp_body"
) &
LOOP_PID=$!
_P2_BG_PIDS+=("$LOOP_PID")

sleep 2

# 3. Trigger unreadiness / drain on victim and capture shrinking endpoints
DURING_FILE="${EVIDENCE_DIR}/c1-endpoints-during.txt"
: > "$DURING_FILE"
MIN_ENDPOINTS="$INIT_ENDPOINTS"
VICTIM_UNREADY_SEEN=0

if [[ "$MODE" == "compose" ]]; then
  VICTIM_IP="$(agg_compose_ip "$VICTIM")"
  p2_log "Sending SIGTERM to ${VICTIM} (ip=${VICTIM_IP}) to trigger readiness drain (SetReady(false))..."
  docker kill -s TERM "$VICTIM" >/dev/null

  for i in $(seq 1 24); do
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    probe="$(agg_instance_ready_probe "$VICTIM" || true)"
    dns_ips="$(docker exec "$_P2_HELPER_CURL" getent ahosts aggregator 2>/dev/null | awk '{print $1}' | sort -u || true)"
    cnt="$(echo "$dns_ips" | sed '/^$/d' | wc -l | tr -d ' ')"
    {
      echo "--- sample ${i} @ ${ts} ---"
      echo "victim_ready_probe: ${probe}"
      echo "entrypoint_dns_count=${cnt}"
      echo "$dns_ips"
    } >> "$DURING_FILE"
    if [[ "$probe" == 503* || "$probe" == 000* ]]; then
      VICTIM_UNREADY_SEEN=1
    fi
    if [[ "$cnt" -lt "$MIN_ENDPOINTS" ]]; then
      MIN_ENDPOINTS="$cnt"
    fi
    if [[ "$cnt" -lt "$INIT_ENDPOINTS" && "$VICTIM_UNREADY_SEEN" -eq 1 && "$i" -ge 10 ]]; then
      break
    fi
    sleep 0.5
  done
else
  p2_log "Deleting pod ${VICTIM} (--wait=false) to trigger preStop + /internal/ready=503 drain..."
  kubectl -n "$NAMESPACE" delete pod "$VICTIM" --wait=false >/dev/null
  for i in $(seq 1 24); do
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    ep_addrs="$(kubectl -n "$NAMESPACE" get endpoints aggregator -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null | sed '/^$/d' || true)"
    not_ready="$(kubectl -n "$NAMESPACE" get endpoints aggregator -o jsonpath='{range .subsets[*].notReadyAddresses[*]}{.ip}{"\n"}{end}' 2>/dev/null | sed '/^$/d' || true)"
    cnt="$(echo "$ep_addrs" | sed '/^$/d' | wc -l | tr -d ' ')"
    {
      echo "--- sample ${i} @ ${ts} ---"
      echo "ready_endpoints_count=${cnt}"
      kubectl -n "$NAMESPACE" get endpoints aggregator -o wide 2>&1 || true
    } >> "$DURING_FILE"
    if [[ -n "$not_ready" || "$cnt" -lt "$INIT_ENDPOINTS" ]]; then
      VICTIM_UNREADY_SEEN=1
    fi
    if [[ "$cnt" -lt "$MIN_ENDPOINTS" ]]; then
      MIN_ENDPOINTS="$cnt"
    fi
    if [[ "$cnt" -lt "$INIT_ENDPOINTS" && "$i" -ge 8 ]]; then
      break
    fi
    sleep 0.5
  done
fi

wait "$LOOP_PID" || true

# 4. Restore 3 healthy replicas and capture post-recovery endpoints
p2_log "Restoring 3 healthy aggregator replicas..."
agg_ensure_replicas 3

if [[ "$MODE" == "compose" ]]; then
  {
    echo "=== docker compose ps aggregator (after restore) ==="
    docker compose ps aggregator
    echo ""
    echo "=== DNS A records for 'aggregator' (after restore) ==="
    docker exec "$_P2_HELPER_CURL" sh -c 'nslookup aggregator 127.0.0.11 || getent ahosts aggregator' 2>&1 || true
  } > "${EVIDENCE_DIR}/c1-endpoints-after.txt"
  AFTER_ENDPOINTS="$(docker exec "$_P2_HELPER_CURL" getent ahosts aggregator 2>/dev/null | awk '{print $1}' | sort -u | wc -l | tr -d ' ')"
else
  {
    echo "=== kubectl get pods -o wide -l app=aggregator (after restore) ==="
    kubectl -n "$NAMESPACE" get pods -o wide -l app=aggregator
    echo ""
    echo "=== kubectl get endpoints aggregator (after restore) ==="
    kubectl -n "$NAMESPACE" get endpoints aggregator -o wide
  } > "${EVIDENCE_DIR}/c1-endpoints-after.txt"
  AFTER_ENDPOINTS="$(kubectl -n "$NAMESPACE" get endpoints aggregator -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' 2>/dev/null | sed '/^$/d' | wc -l | tr -d ' ')"
fi

# 5. Verify results
"$PY" - "$REQ_CSV" "$INIT_REPLICAS" "$INIT_ENDPOINTS" "$MIN_ENDPOINTS" "$AFTER_ENDPOINTS" "$VICTIM_UNREADY_SEEN" \
  > "${EVIDENCE_DIR}/c1-verification.txt" <<'PYEOF'
import csv, statistics, sys

req_csv = sys.argv[1]
init_rep = int(sys.argv[2])
init_ep = int(sys.argv[3])
min_ep = int(sys.argv[4])
after_ep = int(sys.argv[5])
unready_seen = int(sys.argv[6])

rows = list(csv.DictReader(open(req_csv, encoding="utf-8")))
codes = [int(r["http_code"]) for r in rows]
lats = [float(r["latency_s"]) for r in rows]
non_200 = [c for c in codes if c != 200]

print("=== Criterion 1: Single Entrypoint & Unhealthy Instance Removal ===")
print(f"Initial replicas       : {init_rep}")
print(f"Initial endpoints      : {init_ep}")
print(f"Min endpoints during   : {min_ep}")
print(f"Restored endpoints     : {after_ep}")
print(f"Unready/503 observed   : {bool(unready_seen)}")
print(f"Total requests         : {len(rows)}")
print(f"HTTP 200 responses     : {sum(1 for c in codes if c == 200)}")
print(f"Non-200 responses      : {len(non_200)}")
if lats:
    print(f"Latency (s)            : min={min(lats):.4f} avg={statistics.mean(lats):.4f} max={max(lats):.4f}")

assert init_rep >= 3, f"expected >= 3 replicas, got {init_rep}"
assert init_ep >= 3, f"expected >= 3 initial endpoints, got {init_ep}"
assert unready_seen == 1 or min_ep < init_ep, "expected unready probe or endpoint shrink during drain"
assert min_ep < init_ep, f"expected endpoint count to shrink below {init_ep}, got min={min_ep}"
assert len(rows) > 0, "expected >0 entrypoint requests"
assert len(non_200) == 0, f"expected 0 non-200 responses during drain, got {len(non_200)}: {non_200[:10]}"
print("\nASSERTION OK: >=3 replicas behind single entrypoint; draining/unready instance dropped from endpoints with 0 failed requests.")
PYEOF

cat "${EVIDENCE_DIR}/c1-verification.txt"
p2_log "Criterion 1 complete. Evidence saved under ${EVIDENCE_DIR}/c1-*"
