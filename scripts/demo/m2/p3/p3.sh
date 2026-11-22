#!/usr/bin/env bash
# Evidence for P3 (schema changes, dead-letter queue, replay), criteria 1-5:
#
#   1. Policy: PVMBG first adds a field (confidence_level), which is accepted
#      and stored as an attribute; then it switches to its incompatible schema
#      and the v2 reports are parked.
#   2. Stored messages: dead_letters rows with reason and status.
#   3. Bad messages: broken messages are published straight to the ingest
#      stream on the running system and end up parked; the aggregator keeps
#      running (restart counts before/after) and the malformed-input tests pass.
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
# Strip Windows line endings so a .env saved on Windows still loads.
if [[ -f "$ENV_FILE" ]]; then set -a; source <(tr -d '\r' < "$ENV_FILE"); set +a; fi

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
  # Stop early when kubectl is not pointed at a cluster running the stack.
  if ! kubectl get ns "$NS" >/dev/null 2>&1; then
    echo "namespace $NS not found in kubectl context '$(kubectl config current-context 2>/dev/null)'." >&2
    echo "Deploy the stack first (kubectl apply -k deploy/k8s/overlays/multi-node) or run with MODE=compose." >&2
    exit 1
  fi
  if ! kubectl -n "$NS" get deploy aggregator >/dev/null 2>&1; then
    echo "deployment aggregator not found in namespace $NS." >&2
    exit 1
  fi
fi

if [[ "$MODE" == "k8s" ]]; then
  NODE_IP="${NODE_IP:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')}"
  PVMBG_URL="${PVMBG_URL:-http://${NODE_IP}:30082}"
  AGG_LOCAL_PORT="${AGG_LOCAL_PORT:-18090}"
  # Forward to a single aggregator pod that is ready and not shutting down.
  # A forward to the Service binds to one pod when it starts, so after a
  # rollout it can point at a pod that is about to disappear.
  reconnect_agg() {
    [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null
    local pod=""
    for _ in $(seq 1 30); do
      pod="$(kubectl -n "$NS" get pods -l app=aggregator --field-selector=status.phase=Running \
        -o jsonpath='{range .items[*]}{.metadata.name},{.metadata.deletionTimestamp},{.status.containerStatuses[0].ready}{"\n"}{end}' |
        awk -F, '$2=="" && $3=="true" {print $1; exit}')"
      [[ -n "$pod" ]] && break
      sleep 2
    done
    kubectl -n "$NS" port-forward "pod/${pod}" "${AGG_LOCAL_PORT}:8090" >/dev/null 2>&1 &
    PF_PID=$!
    for _ in $(seq 1 15); do
      [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:${AGG_LOCAL_PORT}/internal/health")" == "200" ]] && return 0
      sleep 1
    done
    echo "aggregator forward to ${pod:-<none>} did not answer" >&2
  }
  reconnect_agg
  publish_raw() { # publishes each argument as one message on the PVMBG ingest subject
    local cmds="" m
    for m in "$@"; do cmds+="nats -s nats://nats-jetstream:4222 pub hazard.ingest.PVMBG $(printf '%q' "$m") >/dev/null && "; done
    kubectl -n "$NS" run "p3-nats-${RANDOM}" --rm -i --quiet --restart=Never --image=natsio/nats-box:latest \
      --labels=app=p3-probe -- sh -c "${cmds}true"
  }
  agg() { curl -s "$@" --max-time 70 "http://127.0.0.1:${AGG_LOCAL_PORT}${AGG_PATH}"; }
  sql() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=table -e "$1"; }
  sqlv() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  restarts() { kubectl -n "$NS" get pods -l app=aggregator -o custom-columns=POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount,NODE:.spec.nodeName --no-headers; }
  set_adapter() {
    kubectl -n "$NS" set env deploy/aggregator PVMBG_SCHEMA_V2_ADAPTER="$1" >/dev/null
    kubectl -n "$NS" rollout status deploy/aggregator --timeout=240s >/dev/null
    # Old pods may still be terminating once the rollout reports success.
    for _ in $(seq 1 60); do
      [[ "$(kubectl -n "$NS" get pods -l app=aggregator -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"\n"}{end}' | grep -c .)" == "0" ]] && break
      sleep 2
    done
    reconnect_agg
  }
else
  COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
  PVMBG_URL="${PVMBG_URL:-http://localhost:${PVMBG_PORT:-8082}}"
  agg() { docker run --rm --network "${COMPOSE_NAME}_internal_api_net" curlimages/curl:8.11.0 -s "$@" --max-time 70 "http://aggregator:8090${AGG_PATH}"; }
  sql() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=table -e "$1"; }
  sqlv() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  restarts() { for c in $(docker compose ps -q aggregator); do docker inspect -f '{{.Name}} restarts={{.RestartCount}} started={{.State.StartedAt}}' "$c"; done; }
  reconnect_agg() { :; }
  publish_raw() { # publishes each argument as one message on the PVMBG ingest subject
    local m
    for m in "$@"; do
      docker run --rm --network "container:${COMPOSE_NAME}-nats-jetstream-1" natsio/nats-box:latest \
        nats -s nats://127.0.0.1:4222 pub hazard.ingest.PVMBG "$m" >/dev/null
    done
  }
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
set_additive() { # $1 = true|false; the plain toggle adds or removes confidence_level
  local state
  state="$(curl -s -X POST "${PVMBG_URL}/admin/schema-version" | python3 -c 'import sys,json; print(str(json.load(sys.stdin).get("confidence_level")).lower())')"
  [[ "$state" != "$1" ]] && state="$(curl -s -X POST "${PVMBG_URL}/admin/schema-version" | python3 -c 'import sys,json; print(str(json.load(sys.stdin).get("confidence_level")).lower())')"
  echo "$state"
}

log "mode=$MODE evidence=$EVIDENCE_DIR"
: > "$EVIDENCE_DIR/summary.txt"
summary "P3 evidence run $RUN_ID (mode $MODE)"

# ── 3 (before) ───────────────────────────────────────────────────────────────
restarts > "$EVIDENCE_DIR/aggregator-restarts-before.txt" 2>&1

# ── 1a. Additive change: a new field is accepted ─────────────────────────────
with_field() { sqlv "SELECT count(*) FROM canonical_events WHERE source='PVMBG' AND attributes ? 'confidence_level'"; }
dl_all() { sqlv "SELECT count(*) FROM dead_letters WHERE source='PVMBG'"; }
f_before="$(with_field)"; f_before="${f_before:-0}"; d_before="$(dl_all)"; d_before="${d_before:-0}"
log "additive change: PVMBG adds confidence_level (mode: $(set_additive true))"
curl -s -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN:-}" "${PVMBG_URL}/volcanic-reports?limit=2" > "$EVIDENCE_DIR/pvmbg-additive-sample.json"
f_now="$f_before"
for _ in $(seq 1 24); do
  f_now="$(with_field)"; f_now="${f_now:-0}"
  (( f_now > f_before )) && break
  sleep 5
done
d_now="$(dl_all)"; d_now="${d_now:-0}"
sql "SELECT source_ref_id, attributes->>'confidence_level' AS confidence_level, occurred_at FROM canonical_events WHERE source='PVMBG' AND attributes ? 'confidence_level' ORDER BY occurred_at DESC LIMIT 5" > "$EVIDENCE_DIR/additive-accepted.txt" 2>&1
set_additive false >/dev/null
summary "1a. Additive field confidence_level: $((f_now - f_before)) PVMBG events stored with it as an attribute, $((d_now - d_before)) new dead letters"

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
replay_parked_before="$(parked_pvmbg)"
replay_resolved_before="$(sqlv "SELECT count(*) FROM dead_letters WHERE source='PVMBG' AND status='resolved'")"
: > "$EVIDENCE_DIR/replay-result.json"
for attempt in 1 2 3; do
  out="$(AGG_PATH="/internal/dlq/replay?limit=1000" agg -X POST -H "Authorization: Bearer ${OPS_TOKEN}" -w $'\n%{http_code}')"
  code="${out##*$'\n'}"
  if [[ "$code" == "200" ]]; then
    printf '%s\n' "${out%$'\n'*}" > "$EVIDENCE_DIR/replay-result.json"
    break
  fi
  log "replay attempt $attempt answered HTTP ${code:-none}; reconnecting"
  reconnect_agg
done
tee -a "$EVIDENCE_DIR/run.log" < "$EVIDENCE_DIR/replay-result.json"
sql "SELECT status, count(*) AS n FROM dead_letters WHERE source='PVMBG' GROUP BY status ORDER BY status" > "$EVIDENCE_DIR/dead-letters-after-replay.txt" 2>&1
cat "$EVIDENCE_DIR/dead-letters-after-replay.txt" | tee -a "$EVIDENCE_DIR/run.log"
dups="$(sqlv "SELECT count(*) FROM (SELECT source, source_ref_id FROM canonical_events GROUP BY source, source_ref_id HAVING count(*) > 1) d")"
sql "SELECT source_ref_id, occurred_at, attributes->>'reported_at_source' AS reported_at_source, version FROM canonical_events WHERE source='PVMBG' AND attributes->>'reported_at_source' = 'received_at' ORDER BY occurred_at DESC LIMIT 10" > "$EVIDENCE_DIR/replayed-events.txt" 2>&1
resolved="$(sqlv "SELECT count(*) FROM dead_letters WHERE source='PVMBG' AND status='resolved'")"
still="$(parked_pvmbg)"
# The counts come from the table, because an answer can be lost when the
# forwarded connection drops even though the replay itself finished.
summary "4. Replay: PVMBG parked before ${replay_parked_before:-?}, resolved by the replay $(( ${resolved:-0} - ${replay_resolved_before:-0} )), still parked ${still:-?}; duplicate (source, source_ref_id) groups in canonical_events: ${dups:-?} (API answer: $(tr -d '\n' < "$EVIDENCE_DIR/replay-result.json"))"

# ── 3b. Broken messages published live to the ingest stream ─────────────────
tag="p3-${RUN_ID}"
since="$(sqlv "SELECT now()::STRING")"
bad_where="received_at >= '${since}' AND (reason = 'invalid json' OR reason LIKE 'validation failed%')"
bad_count() { sqlv "SELECT count(*) FROM dead_letters WHERE ${bad_where}"; }
b_before="$(bad_count)"; b_before="${b_before:-0}"
restarts > "$EVIDENCE_DIR/aggregator-restarts-before-malformed.txt" 2>&1
log "publishing broken messages to hazard.ingest.PVMBG"
publish_raw "not json ${tag}" \
  "{\"op\":\"upsert\",\"source\":\"PVMBG\",\"source_ref_id\":\"${tag}-truncated\"" \
  "{\"op\":\"upsert\",\"source\":\"PVMBG\",\"source_ref_id\":\"${tag}-missing-fields\"}" \
  >> "$EVIDENCE_DIR/run.log" 2>&1
b_now="$b_before"
for _ in $(seq 1 12); do
  b_now="$(bad_count)"; b_now="${b_now:-0}"
  (( b_now - b_before >= 3 )) && break
  sleep 5
done
sql "SELECT source, reason, status, received_at, length(raw_payload) AS raw_bytes FROM dead_letters WHERE ${bad_where} ORDER BY id" > "$EVIDENCE_DIR/malformed-live.txt" 2>&1
restarts > "$EVIDENCE_DIR/aggregator-restarts-after-malformed.txt" 2>&1
summary "3b. Broken messages published to the ingest stream: 3, parked: $((b_now - b_before)); aggregator restarts just before: $(tr '\n' ';' < "$EVIDENCE_DIR/aggregator-restarts-before-malformed.txt") just after: $(tr '\n' ';' < "$EVIDENCE_DIR/aggregator-restarts-after-malformed.txt")"

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
