#!/usr/bin/env bash
# Evidence for P3 (schema changes, dead-letter queue, replay), criteria 1-5:
#
#   1. Policy: PVMBG switches to its incompatible schema; v2 reports are parked.
#   2. Stored messages: dead_letters rows with reason and status.
#   3. Bad messages: the aggregator keeps running (restart counts before/after)
#      and the malformed-input tests pass.
#   4. Replay: the normalizer update (PVMBG_SCHEMA_V2_ADAPTER=true) is rolled
#      out, parked reports are replayed, and no canonical event is duplicated.
#   5. Only Internal Ops can inspect the DLQ (401 without or with a wrong token).
#
# Everything is written to $EVIDENCE_DIR; summary.txt has the numbers for the report.
# The script restores the normal schema and turns the adapter off at the end.
#
# Usage:
#   MODE=k8s     bash scripts/demo/m2/p3/p3.sh
#   MODE=compose bash scripts/demo/m2/p3/p3.sh
# Variables: ENV_FILE=.env, EVIDENCE_DIR, WAIT_PARKED_S=120, MIN_PARKED=3
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT" || exit 1

ENV_FILE="${ENV_FILE:-.env}"
# shellcheck disable=SC1090
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/p3/${RUN_ID}}"
WAIT_PARKED_S="${WAIT_PARKED_S:-120}"
MIN_PARKED="${MIN_PARKED:-3}"
mkdir -p "$EVIDENCE_DIR"

OPS_TOKEN="${INTERNAL_OPS_TOKEN:?INTERNAL_OPS_TOKEN must be set in $ENV_FILE}"
AUDIT_TOK="${AGGREGATOR_AUDIT_TOKEN:-${AUDIT_TOKEN:-}}"

if [[ -z "${MODE:-}" ]]; then
  if kubectl -n "$NS" get deploy aggregator >/dev/null 2>&1; then MODE=k8s; else MODE=compose; fi
fi

log() { echo "[p3 $(date -u +%H:%M:%SZ)] $*" | tee -a "$EVIDENCE_DIR/run.log"; }
summary() { echo "$*" | tee -a "$EVIDENCE_DIR/summary.txt"; }

# ── platform-specific access ─────────────────────────────────────────────────
PF_PID=""
cleanup() { [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null; }
trap cleanup EXIT

if [[ "$MODE" == "k8s" ]]; then
  NODE_IP="${NODE_IP:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')}"
  PVMBG_URL="${PVMBG_URL:-http://${NODE_IP}:30082}"
  AGG_LOCAL_PORT="${AGG_LOCAL_PORT:-18090}"
  kubectl -n "$NS" port-forward svc/aggregator "${AGG_LOCAL_PORT}:8090" >/dev/null 2>&1 &
  PF_PID=$!
  sleep 3
  agg() { curl -s "$@" --max-time 70 "http://127.0.0.1:${AGG_LOCAL_PORT}${AGG_PATH}"; }
  sql() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=table -e "$1"; }
  sqlv() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  restarts() { kubectl -n "$NS" get pods -l app=aggregator -o custom-columns=POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount,NODE:.spec.nodeName --no-headers; }
  set_adapter() {
    kubectl -n "$NS" set env deploy/aggregator PVMBG_SCHEMA_V2_ADAPTER="$1" >/dev/null
    kubectl -n "$NS" rollout status deploy/aggregator --timeout=240s >/dev/null
    # the port-forward pointed at a pod that may be gone after the rollout
    [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null
    kubectl -n "$NS" port-forward svc/aggregator "${AGG_LOCAL_PORT}:8090" >/dev/null 2>&1 &
    PF_PID=$!
    sleep 3
  }
else
  COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
  PVMBG_URL="${PVMBG_URL:-http://localhost:${PVMBG_PORT:-8082}}"
  agg() { docker run --rm --network "${COMPOSE_NAME}_internal_api_net" curlimages/curl:8.11.0 -s "$@" --max-time 70 "http://aggregator:8090${AGG_PATH}"; }
  sql() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=table -e "$1"; }
  sqlv() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  restarts() { for c in $(docker compose ps -q aggregator); do docker inspect -f '{{.Name}} restarts={{.RestartCount}} started={{.State.StartedAt}}' "$c"; done; }
  set_adapter() {
    AGGREGATOR_PVMBG_SCHEMA_V2_ADAPTER="$1" docker compose up -d --no-deps aggregator >/dev/null 2>&1
    for _ in $(seq 1 60); do
      [[ "$(docker compose ps aggregator --format '{{.Health}}' | sort -u)" == "healthy" ]] && break
      sleep 2
    done
  }
fi

pvmbg_incompatible() { # prints true/false after toggling once
  curl -s -X POST "${PVMBG_URL}/admin/schema-version?mode=incompatible" |
    python3 -c 'import sys,json; print(str(json.load(sys.stdin).get("incompatible")).lower())'
}
set_incompatible() { # $1 = true|false
  local state
  state="$(pvmbg_incompatible)"
  [[ "$state" != "$1" ]] && state="$(pvmbg_incompatible)"
  echo "$state"
}
parked_pvmbg() { sqlv "SELECT count(*) FROM dead_letters WHERE source='PVMBG' AND status='parked'"; }

log "mode=$MODE evidence=$EVIDENCE_DIR"
: > "$EVIDENCE_DIR/summary.txt"
summary "P3 evidence run $RUN_ID (mode $MODE)"

# ── 3 (before) ───────────────────────────────────────────────────────────────
restarts > "$EVIDENCE_DIR/aggregator-restarts-before.txt" 2>&1

# ── 1 + 2. Incompatible schema → parked ──────────────────────────────────────
before="$(parked_pvmbg)"; before="${before:-0}"
log "parked PVMBG messages before: $before"
log "switching PVMBG to the incompatible schema"
state="$(set_incompatible true)"
log "PVMBG incompatible mode: $state"
curl -s -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN:-}" "${PVMBG_URL}/volcanic-reports?limit=2" > "$EVIDENCE_DIR/pvmbg-v2-sample.json"

deadline=$((SECONDS + WAIT_PARKED_S)); now="$before"
while (( SECONDS < deadline )); do
  now="$(parked_pvmbg)"; now="${now:-0}"
  (( now - before >= MIN_PARKED )) && break
  sleep 5
done
log "parked PVMBG messages after the schema change: $now"
summary "1-2. v2 reports parked: $((now - before)) new (total parked PVMBG: $now)"

AGG_PATH="/internal/dlq?status=parked&limit=10" agg -H "Authorization: Bearer ${OPS_TOKEN}" > "$EVIDENCE_DIR/dlq-parked.json"
sql "SELECT status, reason, count(*) AS n FROM dead_letters WHERE source='PVMBG' GROUP BY status, reason ORDER BY status, n DESC" > "$EVIDENCE_DIR/dead-letters-by-reason.txt" 2>&1
sql "SELECT id, source_ref_id, reason, status, retryable, attempts, received_at, length(raw_payload) AS raw_bytes FROM dead_letters WHERE source='PVMBG' ORDER BY id DESC LIMIT 10" > "$EVIDENCE_DIR/dead-letters-latest.txt" 2>&1
log "dead letters by status and reason:"; cat "$EVIDENCE_DIR/dead-letters-by-reason.txt" | tee -a "$EVIDENCE_DIR/run.log"

# ── 5. Access control ────────────────────────────────────────────────────────
c_none="$(AGG_PATH=/internal/dlq agg -o /dev/null -w '%{http_code}')"
c_wrong="$(AGG_PATH=/internal/dlq agg -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer wrong-token')"
c_ops="$(AGG_PATH=/internal/dlq agg -o /dev/null -w '%{http_code}' -H "Authorization: Bearer ${OPS_TOKEN}")"
c_audit="n/a"
[[ -n "$AUDIT_TOK" ]] && c_audit="$(AGG_PATH=/internal/audit/unprocessed agg -o /dev/null -w '%{http_code}' -H "Authorization: Bearer ${AUDIT_TOK}")"
printf 'GET /internal/dlq without token:        %s\nGET /internal/dlq with a wrong token:   %s\nGET /internal/dlq with Internal Ops:    %s\nGET /internal/audit/unprocessed (audit): %s\n' \
  "$c_none" "$c_wrong" "$c_ops" "$c_audit" | tee "$EVIDENCE_DIR/access.txt" | tee -a "$EVIDENCE_DIR/run.log"
summary "5. DLQ access: no token $c_none, wrong token $c_wrong, Internal Ops $c_ops, audit unprocessed $c_audit"

# ── 4. Normalizer update + replay ────────────────────────────────────────────
log "rolling out the normalizer update (PVMBG_SCHEMA_V2_ADAPTER=true)"
set_adapter true
log "replaying parked messages"
AGG_PATH="/internal/dlq/replay?limit=1000" agg -X POST -H "Authorization: Bearer ${OPS_TOKEN}" | tee "$EVIDENCE_DIR/replay-result.json" | tee -a "$EVIDENCE_DIR/run.log"; echo
sql "SELECT status, count(*) AS n FROM dead_letters WHERE source='PVMBG' GROUP BY status ORDER BY status" > "$EVIDENCE_DIR/dead-letters-after-replay.txt" 2>&1
cat "$EVIDENCE_DIR/dead-letters-after-replay.txt" | tee -a "$EVIDENCE_DIR/run.log"
dups="$(sqlv "SELECT count(*) FROM (SELECT source, source_ref_id FROM canonical_events GROUP BY source, source_ref_id HAVING count(*) > 1) d")"
sql "SELECT source_ref_id, occurred_at, attributes->>'reported_at_source' AS reported_at_source, version FROM canonical_events WHERE source='PVMBG' AND attributes->>'reported_at_source' = 'received_at' ORDER BY occurred_at DESC LIMIT 10" > "$EVIDENCE_DIR/replayed-events.txt" 2>&1
resolved="$(sqlv "SELECT count(*) FROM dead_letters WHERE source='PVMBG' AND status='resolved'")"
still="$(parked_pvmbg)"
summary "4. Replay: $(tr -d '\n' < "$EVIDENCE_DIR/replay-result.json"); resolved PVMBG rows now ${resolved:-?}, still parked ${still:-?}; duplicate (source, source_ref_id) groups in canonical_events: ${dups:-?}"

# ── 3 (after) + tests ────────────────────────────────────────────────────────
restarts > "$EVIDENCE_DIR/aggregator-restarts-after.txt" 2>&1
summary "3. Aggregator restarts before: $(tr '\n' ';' < "$EVIDENCE_DIR/aggregator-restarts-before.txt")"
summary "   Aggregator restarts after:  $(tr '\n' ';' < "$EVIDENCE_DIR/aggregator-restarts-after.txt") (the rollout for the adapter replaces pods; restart counts stay 0)"
if command -v go >/dev/null 2>&1; then
  (cd services/aggregator && go test -count=1 -v ./internal/schema/ ./internal/dlq/ ./internal/poller/ 2>&1) > "$EVIDENCE_DIR/malformed-input-tests.txt"
  summary "   Malformed-input tests: $(grep -c '^--- PASS' "$EVIDENCE_DIR/malformed-input-tests.txt") passed, $(grep -c '^--- FAIL' "$EVIDENCE_DIR/malformed-input-tests.txt") failed"
fi

# ── restore ──────────────────────────────────────────────────────────────────
log "restoring: normal PVMBG schema, adapter off"
set_incompatible false >/dev/null
set_adapter false
log "done. Summary:"
cat "$EVIDENCE_DIR/summary.txt"
