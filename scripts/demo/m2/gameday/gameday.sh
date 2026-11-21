#!/usr/bin/env bash
# Game day: a combination of failures on the Kubernetes deployment, then a
# recovery window with no intervention at all, and a before/after snapshot of
# the alert-path SLO and the source states.
#
#   bash scripts/demo/m2/gameday/gameday.sh
#
# Timeline (seconds from start; every fault is injected and lifted by this
# script on schedule, nothing is fixed by hand):
#     0  background load: media + field-team clients (load shedding active)
#    30  PVMBG outage on                              (mock admin endpoint)
#    60  coastal sensor pod deleted (restarts from its beginning)
#    90  network partition: nothing may reach the coastal sensor
#   150  partition lifted
#   180  PVMBG outage off
#   180..RECOVERY_END  recovery window: no action of any kind
#
# Optional: TESTER_SCENARIO=<name> also runs a Tester Agent practice scenario
# at t=120 (e.g. one that stops the worker node). Freezing a VM or pulling a
# network cable is done by hand at the time shown in the printed timeline.
#
# Variables: MEDIA_VUS (default 800), FIELD_VUS (20), RECOVERY_S (180)
# Evidence: evidence/gameday/<run>/ (timeline, snapshots, k6 summary)
set -uo pipefail
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/gameday/$(date -u +%Y%m%dT%H%M%SZ)}"
source "$(dirname "${BASH_SOURCE[0]}")/../load/_lib.sh"

MEDIA_VUS="${MEDIA_VUS:-800}"
FIELD_VUS="${FIELD_VUS:-20}"
RECOVERY_S="${RECOVERY_S:-180}"
PVMBG_URL="${PVMBG_URL:-http://${NODE_IP}:30082}"
PROM_URL="${PROM_URL:-http://${NODE_IP}:30090}"
TOTAL_S=$((180 + RECOVERY_S))

T0=$(date +%s)
mark() { echo "$(date -u +%H:%M:%S) t+$(( $(date +%s) - T0 ))s $*" | tee -a "$EVIDENCE_DIR/timeline.txt"; }
at() { local now=$(( $(date +%s) - T0 )); (( $1 > now )) && sleep $(( $1 - now )); }

partition_on() {
  kubectl -n "$NS" apply -f - >/dev/null <<'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: gameday-isolate-sensor
spec:
  podSelector:
    matchLabels:
      app: sensor-pantai
  policyTypes: [Ingress]
  ingress: []
EOF
}
partition_off() { kubectl -n "$NS" delete networkpolicy gameday-isolate-sensor --ignore-not-found >/dev/null; }

pvmbg_outage() { # pvmbg_outage true|false: toggle until the state matches
  local want="$1" state
  state=$(curl -s -m 5 -X POST "$PVMBG_URL/admin/outage" | sed -n 's/.*"outage":\([a-z]*\).*/\1/p')
  [[ "$state" != "$want" ]] && curl -s -m 5 -X POST "$PVMBG_URL/admin/outage" >/dev/null
}

snapshot() { # snapshot NAME: key metrics from Prometheus
  "$PY" - "$PROM_URL" > "$EVIDENCE_DIR/snapshot-$1.txt" <<'PYEOF'
import json, sys, urllib.parse, urllib.request
prom = sys.argv[1]
queries = {
    "alert SLI (1h, delivered<=5s / published)":
        'sum(increase(notification_delivery_latency_seconds_bucket{path="priority",le="5"}[1h])) / clamp_min(sum(increase(sensor_pantai_critical_published_total[1h])),1)',
    "critical published (1h)": 'sum(increase(sensor_pantai_critical_published_total[1h]))',
    "critical delivered (1h)": 'sum(increase(notification_delivery_latency_seconds_count{path="priority"}[1h]))',
    "priority delivery p95 (s, 1h)": 'histogram_quantile(0.95, sum by (le)(rate(notification_delivery_latency_seconds_bucket{path="priority"}[1h])))',
    "sensor breaker (0 closed,1 open,2 half)": 'max(sensor_pantai_breaker_state)',
    "sensor requests sent (5m)": 'sum(increase(sensor_pantai_requests_total{outcome!="short_circuited"}[5m]))',
    "sensor requests short-circuited (5m)": 'sum(increase(sensor_pantai_requests_total{outcome="short_circuited"}[5m]))',
    "sensor restarts seen": 'sum(sensor_pantai_sensor_resets_total)',
    "rejected observations": 'sum(sensor_pantai_rejected_total)',
    "PVMBG/BMKG outage (leader view)": 'max by (source) (aggregator_source_status{status="OUTAGE"} * on (instance) group_left aggregator_leader)',
    "client-api shed by scope (5m)": 'sum by (scope)(increase(client_api_shed_total[5m]))',
    "client-api 5xx other than 503 (5m)": 'sum(increase(http_requests_total{service="client-api",code=~"5..",code!="503"}[5m]))',
}
for name, q in queries.items():
    try:
        r = json.load(urllib.request.urlopen(prom + "/api/v1/query?" + urllib.parse.urlencode({"query": q}), timeout=10))
        vals = [(x["metric"], x["value"][1]) for x in r["data"]["result"]]
        out = ", ".join((("/".join(v for v in m.values()) + "=") if m else "") + f"{float(val):.3f}" for m, val in vals) or "-"
    except Exception as e:  # keep going: the snapshot is evidence, not a gate
        out = f"error: {e}"
    print(f"{name:45s} {out}")
PYEOF
}

cleanup() {
  partition_off
  pvmbg_outage false
  kill "${K6_PID:-}" 2>/dev/null
  test_env_restore
}
trap cleanup EXIT

log "game day run=$RUN_ID evidence=$EVIDENCE_DIR"
test_env_apply
snapshot before
cat "$EVIDENCE_DIR/snapshot-before.txt"

T0=$(date +%s)
mark "start: background load media=$MEDIA_VUS field=$FIELD_VUS for ${TOTAL_S}s"
run_k6 "$EVIDENCE_DIR/k6-summary.json" -e SCENARIO=shed -e MEDIA_VUS="$MEDIA_VUS" -e FIELD_VUS="$FIELD_VUS" \
  -e DURATION="${TOTAL_S}s" -e SLEEP_S=0.2 > "$EVIDENCE_DIR/k6-output.txt" 2>&1 &
K6_PID=$!

at 30;  mark "fault: PVMBG outage ON";                 pvmbg_outage true
at 60;  mark "fault: coastal sensor pod deleted";      kubectl -n "$NS" delete pod -l app=sensor-pantai --wait=false >/dev/null
at 90;  mark "fault: network partition (sensor unreachable)"; partition_on
if [[ -n "${TESTER_SCENARIO:-}" ]]; then
  at 120; mark "fault: Tester Agent practice scenario '$TESTER_SCENARIO'"
  kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$TESTER_SCENARIO" >> "$EVIDENCE_DIR/tester-agent.txt" 2>&1 &
fi
at 150; mark "fault lifted: network partition removed"; partition_off
at 180; mark "fault lifted: PVMBG outage OFF";         pvmbg_outage false
mark "recovery window: no intervention for ${RECOVERY_S}s"
snapshot during
wait "$K6_PID"
mark "end of recovery window"
snapshot after

echo
echo "== timeline";        cat "$EVIDENCE_DIR/timeline.txt"
echo "== after recovery";  cat "$EVIDENCE_DIR/snapshot-after.txt"
"$PY" - "$EVIDENCE_DIR/k6-summary.json" <<'PYEOF' | tee "$EVIDENCE_DIR/verdict.txt"
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
print(f"field team served during the game day: {m.get('field_team_served_rate', {}).get('value', 0):.2%}")
print(f"media refused (controlled)           : {m.get('media_refused_rate', {}).get('value', 0):.2%}")
print(f"unexpected errors                    : {int(m.get('unexpected_errors_total', {}).get('count', 0))}")
PYEOF
echo "Write the post-mortem from: $EVIDENCE_DIR (timeline, snapshots, Grafana 'BNPB — Coastal sensor and alert SLO')."
