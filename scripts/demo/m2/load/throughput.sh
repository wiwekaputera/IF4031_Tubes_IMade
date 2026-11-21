#!/usr/bin/env bash
# Throughput with 1 and with 3 aggregator replicas (target: 3 replicas >= 2x).
#
#   VUS=600 DURATION=60s bash scripts/demo/m2/load/throughput.sh
#
# Same closed-loop load (VUS clients, SLEEP_S between requests) for both runs;
# throughput = served (HTTP 200) requests per second. client-api is scaled to
# CLIENT_API_REPLICAS (default 4) for both runs so the gateway is not the
# limit, and restored afterwards.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

VUS="${VUS:-600}"
DURATION="${DURATION:-60s}"
SLEEP_S="${SLEEP_S:-0.05}"
CLIENT_API_REPLICAS="${CLIENT_API_REPLICAS:-4}"
ORIG_API=$(kubectl -n "$NS" get deploy/client-api -o jsonpath='{.spec.replicas}')
ORIG_AGG=$(kubectl -n "$NS" get deploy/aggregator -o jsonpath='{.spec.replicas}')

trap 'kubectl -n "$NS" scale deploy/client-api --replicas="$ORIG_API" >/dev/null; kubectl -n "$NS" scale deploy/aggregator --replicas="$ORIG_AGG" >/dev/null; test_env_restore' EXIT

log "run=$RUN_ID vus=$VUS duration=$DURATION client-api replicas=$CLIENT_API_REPLICAS"
test_env_apply
kubectl -n "$NS" scale deploy/client-api --replicas="$CLIENT_API_REPLICAS" >/dev/null
kubectl -n "$NS" rollout status deploy/client-api --timeout=180s >/dev/null

for r in 1 3; do
  log "aggregator -> $r replica(s)"
  scale_aggregator "$r"
  sleep 15   # leader election settles, connections warm up
  run_k6 "$EVIDENCE_DIR/k6-replicas-$r.json" -e SCENARIO=throughput -e VUS="$VUS" -e DURATION="$DURATION" \
    -e SLEEP_S="$SLEEP_S" > "$EVIDENCE_DIR/k6-replicas-$r.txt" 2>&1
done

"$PY" - "$EVIDENCE_DIR" <<'PYEOF' | tee "$EVIDENCE_DIR/verdict.txt"
import json, sys
res = {}
for r in (1, 3):
    m = json.load(open(f"{sys.argv[1]}/k6-replicas-{r}.json"))["metrics"]
    served = m.get("served_total", {})
    lat = m.get("served_latency", {})
    res[r] = served.get("rate", 0)
    print(f"{r} replica(s): {served.get('rate', 0):8.1f} served/s, p95 {lat.get('p(95)', 0):.0f} ms, "
          f"unexpected errors {int(m.get('unexpected_errors_total', {}).get('count', 0))}")
ratio = res[3] / res[1] if res[1] else 0
print(f"ratio 3 vs 1: {ratio:.2f}x  ->", "PASS (>= 2.0x)" if ratio >= 2 else "below 2.0x")
PYEOF
