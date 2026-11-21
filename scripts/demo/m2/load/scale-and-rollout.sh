#!/usr/bin/env bash
# Sustained load with scaling and a rolling update of the aggregator.
#
#   VUS=10000 bash scripts/demo/m2/load/scale-and-rollout.sh
#
# Timeline (seconds after the ramp, while VUS stay constant):
#     0  3 aggregator replicas
#    20  scale to 5            (SCALE_UP_AT)
#    60  scale to 2            (SCALE_DOWN_AT)
#    95  rolling update        (ROLL_AT): new image if NEW_IMAGE is set
#                              (e.g. bnpb/aggregator:v2), otherwise a new
#                              pod template revision of the same image
#   150  end of hold           (HOLD)
#
# k6 runs on this machine, outside the cluster. Errors are counted per phase;
# the run passes when unexpected errors (not 200/429/503) stay below 1% overall
# and are 0 during the rolling update.
#
# Evidence: evidence/load/<run>/ (k6 summary, replica timeline, verdict).
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

VUS="${VUS:-10000}"
RAMP="${RAMP:-30s}"
HOLD="${HOLD:-150s}"
SCALE_UP_AT="${SCALE_UP_AT:-20}"
SCALE_DOWN_AT="${SCALE_DOWN_AT:-60}"
ROLL_AT="${ROLL_AT:-95}"
RAMP_S="${RAMP%s}"

trap 'kill "${REC_PID:-}" "${K6_PID:-}" 2>/dev/null; test_env_restore' EXIT

log "run=$RUN_ID vus=$VUS ramp=$RAMP hold=$HOLD api=$API_URL evidence=$EVIDENCE_DIR"
test_env_apply
log "aggregator -> 3 replicas"
scale_aggregator 3

record_replicas "$EVIDENCE_DIR/replicas.csv" &
REC_PID=$!

run_k6 "$EVIDENCE_DIR/k6-summary.json" -e SCENARIO=load -e VUS="$VUS" -e RAMP="$RAMP" -e HOLD="$HOLD" \
  -e SCALE_UP_AT="$SCALE_UP_AT" -e SCALE_DOWN_AT="$SCALE_DOWN_AT" -e ROLL_AT="$ROLL_AT" \
  > "$EVIDENCE_DIR/k6-output.txt" 2>&1 &
K6_PID=$!

# The hold starts after setup and the ramp.
sleep $((RAMP_S + 3))
HOLD_T0=$(date +%s)
at() { local target=$1; local now; now=$(( $(date +%s) - HOLD_T0 )); (( target > now )) && sleep $(( target - now )); }

at "$SCALE_UP_AT";   log "aggregator -> 5 replicas"; kube -n "$NS" scale deploy/aggregator --replicas=5 >/dev/null
at "$SCALE_DOWN_AT"; log "aggregator -> 2 replicas"; kube -n "$NS" scale deploy/aggregator --replicas=2 >/dev/null
at "$ROLL_AT"
if [[ -n "${NEW_IMAGE:-}" ]]; then
  log "rolling update: aggregator image -> $NEW_IMAGE"
  kube -n "$NS" set image deploy/aggregator aggregator="$NEW_IMAGE" >/dev/null
else
  log "rolling update: new pod template revision"
  kube -n "$NS" patch deploy/aggregator -p \
    "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"bnpb/rollout\":\"$RUN_ID\"}}}}}" >/dev/null
fi
kubectl -n "$NS" rollout status deploy/aggregator --timeout=300s 2>&1 | tail -1 | tee -a "$EVIDENCE_DIR/run.log"

wait "$K6_PID"; K6_RC=$?
kill "$REC_PID" 2>/dev/null
log "k6 finished (exit $K6_RC)"

"$PY" - "$EVIDENCE_DIR/k6-summary.json" <<'PYEOF' | tee "$EVIDENCE_DIR/verdict.txt"
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
def count(name):
    return int(m.get(name, {}).get("count", 0))
reqs = m.get("http_reqs", {})
print(f"requests        : {int(reqs.get('count', 0))}  ({reqs.get('rate', 0):.0f}/s)")
print(f"served (200)    : {count('served_total')}")
print(f"controlled 429/503: {count('controlled_refusals_total')}")
print(f"unexpected errors: {count('unexpected_errors_total')}  rate {m.get('unexpected_error_rate', {}).get('value', 0):.4%}")
lat = m.get("served_latency", {})
print(f"latency (200)   : p95 {lat.get('p(95)', 0):.0f} ms, p99 {lat.get('p(99)', 0):.0f} ms")
print("per phase (served / unexpected errors):")
for p in ["ramp", "replicas_3", "replicas_5", "replicas_2", "rolling_update", "tail"]:
    s = int(m.get(f"served_total{{phase:{p}}}", {}).get("count", 0))
    e = int(m.get(f"unexpected_errors_total{{phase:{p}}}", {}).get("count", 0))
    print(f"  {p:15s} {s:>9} / {e}")
ok = m.get("unexpected_error_rate", {}).get("value", 1) < 0.01 and \
     int(m.get("unexpected_errors_total{phase:rolling_update}", {}).get("count", 0)) == 0
print("RESULT:", "PASS" if ok else "FAIL")
PYEOF
log "replica timeline: $EVIDENCE_DIR/replicas.csv"
