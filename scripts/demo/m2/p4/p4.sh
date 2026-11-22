#!/usr/bin/env bash
# Evidence for P4 (satellite fire feed), criteria 1-5 and 7-8:
#
#   1. WILDFIRE: example fires in satellite-store and the matching HazardEvents
#      in the canonical store.
#   2. Event time and lateness: late / duplicate / low-confidence totals.
#   3. Update and retract: a fire whose source_ref_id stayed the same over many
#      versions, delete messages after merges, retracted detections.
#   4. 60x feed: latency p50/p95/p99 (read -> committed) and lag sampled over
#      SAMPLE_S seconds, until the ingestor is caught up.
#   7. Live feed: the feed's own session (latest offset, virtual time).
#   8. Disconnect/restart: the ingestor pod is killed; its log shows the resume
#      offset, and the time until lag is back to 0 is measured.
#
# Criterion 5 (kill mid-run, identical result) is the replay test in
# services/satellite-ingestor/internal/e2e. Criterion 6 needs the assistants'
# reference results.
#
# Usage:
#   MODE=k8s     bash scripts/demo/m2/p4/p4.sh
#   MODE=compose bash scripts/demo/m2/p4/p4.sh
# Variables: ENV_FILE=.env, EVIDENCE_DIR, SAMPLE_S=60, CATCHUP_TIMEOUT_S=300, SKIP_KILL=0
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT" || exit 1

ENV_FILE="${ENV_FILE:-.env}"
# shellcheck disable=SC1090
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/p4/${RUN_ID}}"
SAMPLE_S="${SAMPLE_S:-60}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-300}"
SKIP_KILL="${SKIP_KILL:-0}"
SAT_USER="${SATELLITE_STORE_POSTGRES_USER:-satellite}"
SAT_DB="${SATELLITE_STORE_POSTGRES_DB:-satellite}"
FEED_URL="${SATELLITE_FEED_URL:-https://satellite.naufarrel.dev}"
mkdir -p "$EVIDENCE_DIR"

if [[ -z "${MODE:-}" ]]; then
  if kubectl -n "$NS" get deploy satellite-ingestor >/dev/null 2>&1; then MODE=k8s; else MODE=compose; fi
fi

log() { echo "[p4 $(date -u +%H:%M:%SZ)] $*" | tee -a "$EVIDENCE_DIR/run.log"; }
summary() { echo "$*" | tee -a "$EVIDENCE_DIR/summary.txt"; }

PF_PID=""
cleanup() { [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null; }
trap cleanup EXIT

if [[ "$MODE" == "k8s" ]]; then
  METRICS_LOCAL_PORT="${METRICS_LOCAL_PORT:-19091}"
  forward_metrics() {
    [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null
    kubectl -n "$NS" port-forward deploy/satellite-ingestor "${METRICS_LOCAL_PORT}:9090" >/dev/null 2>&1 &
    PF_PID=$!
    sleep 3
  }
  forward_metrics
  METRICS_URL="http://127.0.0.1:${METRICS_LOCAL_PORT}/metrics"
  satsql() { kubectl -n "$NS" exec deploy/satellite-store -- psql -U "$SAT_USER" -d "$SAT_DB" -c "$1"; }
  satval() { kubectl -n "$NS" exec deploy/satellite-store -- psql -U "$SAT_USER" -d "$SAT_DB" -tAc "$1"; }
  crdb() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=table -e "$1"; }
  kill_ingestor() {
    kubectl -n "$NS" delete pod -l app=satellite-ingestor --wait=false >/dev/null
    sleep 2
    kubectl -n "$NS" rollout status deploy/satellite-ingestor --timeout=180s >/dev/null
    forward_metrics
  }
  ingestor_logs() { kubectl -n "$NS" logs deploy/satellite-ingestor --since=5m; }
else
  METRICS_URL="${METRICS_URL:-http://localhost:${SATELLITE_METRICS_PORT:-9091}/metrics}"
  satsql() { docker compose exec -T satellite-store psql -U "$SAT_USER" -d "$SAT_DB" -c "$1"; }
  satval() { docker compose exec -T satellite-store psql -U "$SAT_USER" -d "$SAT_DB" -tAc "$1"; }
  crdb() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=table -e "$1"; }
  kill_ingestor() {
    docker compose kill satellite-ingestor >/dev/null 2>&1
    docker compose start satellite-ingestor >/dev/null 2>&1
    sleep 3
  }
  ingestor_logs() { docker compose logs --since 5m satellite-ingestor; }
fi

metric() { # $1 = metric name with optional labels; prints the value
  curl -s --max-time 5 "$METRICS_URL" | awk -v m="$1" '$1 == m {print $2; exit}'
}

log "mode=$MODE evidence=$EVIDENCE_DIR"
: > "$EVIDENCE_DIR/summary.txt"
summary "P4 evidence run $RUN_ID (mode $MODE)"

# ── 7. Live feed session ─────────────────────────────────────────────────────
if [[ -n "${SATELLITE_FEED_TOKEN:-}" ]]; then
  curl -s --max-time 10 -H "Authorization: Bearer ${SATELLITE_FEED_TOKEN}" "${FEED_URL}/v1/session" > "$EVIDENCE_DIR/feed-session.json"
  summary "7. Feed session: $(cat "$EVIDENCE_DIR/feed-session.json")"
fi

# ── 2. Totals ────────────────────────────────────────────────────────────────
satsql "SELECT next_offset, watermark, late, low_conf, duplicate, unknown, invalid FROM satellite_state" > "$EVIDENCE_DIR/satellite-state.txt" 2>&1
cat "$EVIDENCE_DIR/satellite-state.txt" | tee -a "$EVIDENCE_DIR/run.log"
summary "2. $(satval "SELECT format('next_offset=%s late=%s low_confidence=%s duplicate=%s unknown_key=%s invalid=%s', next_offset, late, low_conf, duplicate, unknown, invalid) FROM satellite_state")"
summary "   dead letters: $(satval "SELECT count(*) FROM satellite_dead_letters")"

# ── 1. WILDFIRE ──────────────────────────────────────────────────────────────
satsql "SELECT source_ref_id, severity, detection_count, round(total_frp_mw::numeric, 1) AS frp_mw, satellites, occurred_at, last_seen_at FROM satellite_fires ORDER BY detection_count DESC LIMIT 10" > "$EVIDENCE_DIR/satellite-fires-top.txt" 2>&1
satsql "SELECT severity, count(*) FROM satellite_fires GROUP BY severity ORDER BY count(*) DESC" > "$EVIDENCE_DIR/satellite-fires-by-severity.txt" 2>&1
crdb "SELECT source_ref_id, severity, area_name, occurred_at, source_version, (attributes->>'detection_count')::INT AS detections, deleted_at IS NOT NULL AS deleted FROM canonical_events WHERE source = 'SATELIT' ORDER BY (attributes->>'detection_count')::INT DESC NULLS LAST LIMIT 10" > "$EVIDENCE_DIR/canonical-wildfire-top.txt" 2>&1
crdb "SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS live, count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted FROM canonical_events WHERE source = 'SATELIT'" > "$EVIDENCE_DIR/canonical-wildfire-counts.txt" 2>&1
summary "1. fires in satellite-store: $(satval "SELECT count(*) FROM satellite_fires"); canonical WILDFIRE events: $(tr -s ' \n' ' ' < "$EVIDENCE_DIR/canonical-wildfire-counts.txt")"

# ── 3. Stable IDs across updates, deletes after merges, retracts ─────────────
top="$(satval "SELECT payload->>'source_ref_id' FROM satellite_outbox WHERE payload->>'op' = 'upsert' GROUP BY 1 ORDER BY count(*) DESC LIMIT 1")"
if [[ -n "$top" ]]; then
  satsql "SELECT payload->>'op' AS op, (payload->>'source_version')::bigint AS version, payload->'attributes'->>'detection_count' AS detections, payload->>'severity' AS severity FROM satellite_outbox WHERE payload->>'source_ref_id' = '$top' ORDER BY id" > "$EVIDENCE_DIR/stable-id-history.txt" 2>&1
  crdb "SELECT source_ref_id, source_version, version, severity, (attributes->>'detection_count')::INT AS detections FROM canonical_events WHERE source = 'SATELIT' AND source_ref_id = '$top'" > "$EVIDENCE_DIR/stable-id-canonical.txt" 2>&1
  summary "3. fire $top kept its source_ref_id over $(satval "SELECT count(*) FROM satellite_outbox WHERE payload->>'source_ref_id' = '$top'") versions"
fi
summary "   delete messages (fires merged away or emptied): $(satval "SELECT count(*) FROM satellite_outbox WHERE payload->>'op' = 'delete'"); retracted detections: $(satval "SELECT count(*) FROM satellite_detections WHERE retracted")"

# ── 4. Latency and lag ───────────────────────────────────────────────────────
python3 scripts/satellite/report.py "$METRICS_URL" > "$EVIDENCE_DIR/report-start.txt" 2>&1
log "sampling lag every 5 s for ${SAMPLE_S} s"
echo "time_utc,lag_events,lag_feed_seconds,next_offset" > "$EVIDENCE_DIR/lag-samples.csv"
end=$((SECONDS + SAMPLE_S))
while (( SECONDS < end )); do
  echo "$(date -u +%H:%M:%S),$(metric satellite_feed_lag_offsets),$(metric satellite_event_time_lag_seconds),$(metric satellite_next_offset)" >> "$EVIDENCE_DIR/lag-samples.csv"
  sleep 5
done
python3 scripts/satellite/report.py "$METRICS_URL" > "$EVIDENCE_DIR/report-end.txt" 2>&1
cat "$EVIDENCE_DIR/report-end.txt" | tee -a "$EVIDENCE_DIR/run.log"
summary "4. $(grep -E 'latency p(50|95|99)' "$EVIDENCE_DIR/report-end.txt" | tr -s ' ' | tr '\n' ';') max lag in window: $(awk -F, 'NR>1 && $2!="" {if ($2>m) m=$2} END {print m+0}' "$EVIDENCE_DIR/lag-samples.csv") events"

# ── 8. Kill and resume ───────────────────────────────────────────────────────
if [[ "$SKIP_KILL" != "1" ]]; then
  before_offset="$(satval "SELECT next_offset FROM satellite_state")"
  log "killing the ingestor at committed offset $before_offset"
  t0=$SECONDS
  kill_ingestor
  sleep 5
  ingestor_logs | grep -E 'resume_offset|starting feed' | tail -3 > "$EVIDENCE_DIR/resume-log.txt"
  cat "$EVIDENCE_DIR/resume-log.txt" | tee -a "$EVIDENCE_DIR/run.log"
  caught=""
  while (( SECONDS - t0 < CATCHUP_TIMEOUT_S )); do
    lag="$(metric satellite_feed_lag_offsets)"
    if [[ -n "$lag" && "${lag%.*}" -le 0 ]]; then caught=$((SECONDS - t0)); break; fi
    sleep 2
  done
  summary "8. killed at committed offset $before_offset; restart log: $(tr '\n' ' ' < "$EVIDENCE_DIR/resume-log.txt")"
  summary "   lag back to 0 after: ${caught:-not within ${CATCHUP_TIMEOUT_S}} s"
fi

log "done. Summary:"
cat "$EVIDENCE_DIR/summary.txt"
