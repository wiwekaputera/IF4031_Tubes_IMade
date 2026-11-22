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
#   4b. End to end: time from the ingestor's commit to the HazardEvent being
#       stored in canonical_events, matched per fire and version.
#   7. Live feed: the feed's own session (latest offset, virtual time).
#   8. Disconnect/restart: the ingestor pod is killed; the offset committed while
#      it is down is compared with the offset it resumes from, and the time
#      until lag is back to its pre-kill level is measured.
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
# Strip Windows line endings so a .env saved on Windows still loads.
if [[ -f "$ENV_FILE" ]]; then set -a; source <(tr -d '\r' < "$ENV_FILE"); set +a; fi

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
  # Stop early when kubectl is not pointed at a cluster running the stack.
  if ! kubectl get ns "$NS" >/dev/null 2>&1; then
    echo "namespace $NS not found in kubectl context '$(kubectl config current-context 2>/dev/null)'." >&2
    echo "Deploy the stack first (kubectl apply -k deploy/k8s/overlays/multi-node) or run with MODE=compose." >&2
    exit 1
  fi
  if ! kubectl -n "$NS" get deploy satellite-ingestor >/dev/null 2>&1; then
    echo "deployment satellite-ingestor not found in namespace $NS." >&2
    exit 1
  fi
fi

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
  satsql() { kubectl -n "$NS" exec satellite-store-0 -- psql -U "$SAT_USER" -d "$SAT_DB" -c "$1"; }
  satval() { kubectl -n "$NS" exec satellite-store-0 -- psql -U "$SAT_USER" -d "$SAT_DB" -tAc "$1"; }
  crdb() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=table -e "$1"; }
  crdbv() { kubectl -n "$NS" exec canonical-store-0 -- cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  stop_ingestor() { kubectl -n "$NS" delete pod -l app=satellite-ingestor --grace-period=0 --force >/dev/null 2>&1; }
  start_ingestor() {
    sleep 2
    kubectl -n "$NS" rollout status deploy/satellite-ingestor --timeout=180s >/dev/null
    forward_metrics
  }
  ingestor_logs() { kubectl -n "$NS" logs deploy/satellite-ingestor --since-time="$1"; }
else
  METRICS_URL="${METRICS_URL:-http://localhost:${SATELLITE_METRICS_PORT:-9091}/metrics}"
  satsql() { docker compose exec -T satellite-store psql -U "$SAT_USER" -d "$SAT_DB" -c "$1"; }
  satval() { docker compose exec -T satellite-store psql -U "$SAT_USER" -d "$SAT_DB" -tAc "$1"; }
  crdb() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=table -e "$1"; }
  crdbv() { docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical --format=csv -e "$1" | tail -n +2; }
  stop_ingestor() { docker compose kill satellite-ingestor >/dev/null 2>&1; }
  start_ingestor() { docker compose start satellite-ingestor >/dev/null 2>&1; sleep 3; }
  ingestor_logs() { docker compose logs --no-log-prefix --since "$1" satellite-ingestor; }
fi

metric() { # $1 = metric name with optional labels; prints the value as an integer
  # Prometheus writes large values as 2.8e+06, so convert before bash compares them.
  curl -s --max-time 5 "$METRICS_URL" | awk -v m="$1" '$1 == m {printf "%.0f\n", $2; exit}'
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
wf="$(crdbv "SELECT count(*) FILTER (WHERE deleted_at IS NULL), count(*) FILTER (WHERE deleted_at IS NOT NULL) FROM canonical_events WHERE source = 'SATELIT'")"
summary "1. fires in satellite-store: $(satval "SELECT count(*) FROM satellite_fires"); canonical WILDFIRE events: live ${wf%%,*}, deleted ${wf##*,}"

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
# From the first and last sample: how fast the ingestor reads, how fast the
# feed grows, and how long the remaining backlog takes at that pace.
summary "   $(awk -F, 'NR>1 && $2!="" {split($1,t,":"); s=t[1]*3600+t[2]*60+t[3]; if (!n++) {s0=s; l0=$2; o0=$4} s1=s; l1=$2; o1=$4}
  END {dt=s1-s0; if (dt<=0) {print "rates: not enough samples"; exit}
       ing=(o1-o0)/dt; drain=(l0-l1)/dt; feed=ing-drain;
       printf "rates: ingest %.1f events/s, feed %.1f events/s, backlog shrinking %.1f events/s", ing, feed, drain;
       if (drain>0) printf "; backlog of %d events clears in about %.1f h", l1, l1/drain/3600;
       else printf "; backlog not shrinking"}' "$EVIDENCE_DIR/lag-samples.csv")"

# ── 4b. End to end: committed by the ingestor -> stored in canonical_events ─
# Matches each canonical WILDFIRE row changed in the last few minutes with the
# outbox row of the same fire and version, and compares the two timestamps
# (outbox created_at is the ingestor's commit; updated_at is the aggregator's).
crdbv "SELECT source_ref_id, source_version, extract(epoch FROM updated_at) FROM canonical_events WHERE source = 'SATELIT' AND updated_at > now() - INTERVAL '3 minutes'" > "$EVIDENCE_DIR/e2e-canonical.csv" 2>/dev/null
satval "SELECT payload->>'source_ref_id', payload->>'source_version', extract(epoch FROM created_at) FROM satellite_outbox WHERE created_at > now() - INTERVAL '10 minutes'" > "$EVIDENCE_DIR/e2e-outbox.txt" 2>/dev/null
e2e="$(python3 - "$EVIDENCE_DIR/e2e-outbox.txt" "$EVIDENCE_DIR/e2e-canonical.csv" <<'PY'
import sys
out = {}
for line in open(sys.argv[1]):
    p = line.strip().split("|")
    if len(p) == 3 and p[2]:
        out[(p[0], p[1])] = float(p[2])
lat = []
for line in open(sys.argv[2]):
    p = line.strip().split(",")
    if len(p) == 3 and (p[0], p[1]) in out:
        lat.append((float(p[2]) - out[(p[0], p[1])]) * 1000)
if not lat:
    print("no matching rows in the last 3 minutes")
else:
    lat.sort()
    q = lambda f: lat[min(len(lat) - 1, int(f * len(lat)))]
    print(f"n={len(lat)} p50 {q(.5):.0f} ms, p95 {q(.95):.0f} ms, max {lat[-1]:.0f} ms")
PY
)"
summary "4b. End to end, ingestor commit to HazardEvent stored: $e2e"

# ── 8. Kill and resume ───────────────────────────────────────────────────────
# The offset is read while the ingestor is down, so it must equal the offset
# the new process resumes from. Catch-up is measured as the time until lag is
# back to where it was before the kill; while a backlog from earlier downtime
# remains, lag never reaches 0 within the window.
if [[ "$SKIP_KILL" != "1" ]]; then
  lag_before="$(metric satellite_feed_lag_offsets)"
  log "killing the ingestor (lag before: ${lag_before:-?})"
  t0=$SECONDS
  killed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stop_ingestor
  sleep 2
  down_offset="$(satval "SELECT next_offset FROM satellite_state")"
  start_ingestor
  # The new process logs its resume offset once its stores are ready; wait for it.
  : > "$EVIDENCE_DIR/resume-log.txt"
  for _ in $(seq 1 45); do
    ingestor_logs "$killed_at" 2>/dev/null | grep '"resume_offset"' | tail -1 > "$EVIDENCE_DIR/resume-log.txt"
    [[ -s "$EVIDENCE_DIR/resume-log.txt" ]] && break
    sleep 2
  done
  cat "$EVIDENCE_DIR/resume-log.txt" | tee -a "$EVIDENCE_DIR/run.log"
  resume_offset="$(grep -oE '"resume_offset":[0-9]+' "$EVIDENCE_DIR/resume-log.txt" | tail -1 | cut -d: -f2)"
  match="no"; [[ -n "$resume_offset" && "$resume_offset" == "$down_offset" ]] && match="yes"
  caught=""
  while (( SECONDS - t0 < CATCHUP_TIMEOUT_S )); do
    # A new process reports lag 0 until it has read from the feed, so only
    # count samples taken after it has committed past the restart offset.
    lag="$(metric satellite_feed_lag_offsets)"
    next="$(metric satellite_next_offset)"
    if [[ -n "$lag" && -n "$lag_before" && -n "$next" && -n "$down_offset" ]] \
       && (( next > down_offset && lag <= lag_before )); then
      caught=$((SECONDS - t0)); break
    fi
    sleep 2
  done
  summary "8. committed offset while down: ${down_offset:-?}; resumed from: ${resume_offset:-?}; same: $match"
  summary "   back to the pre-kill lag (${lag_before:-?} events) after: ${caught:-not within ${CATCHUP_TIMEOUT_S}} s (includes restart time)"
fi

log "done. Summary:"
cat "$EVIDENCE_DIR/summary.txt"
