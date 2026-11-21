#!/usr/bin/env bash
# Prioritised load shedding: many media clients and a few field-team clients
# at the same time, beyond client-api's capacity.
#
#   MEDIA_VUS=3000 FIELD_VUS=50 DURATION=60s bash scripts/demo/m2/load/shedding.sh
#
# Expected: field team served (>= 99% HTTP 200); media refused in a controlled
# way (503 + Retry-After), never timeouts or 500s.
# MAX_INFLIGHT (default: the deployed value) can be lowered for a small cluster.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

MEDIA_VUS="${MEDIA_VUS:-3000}"
FIELD_VUS="${FIELD_VUS:-50}"
DURATION="${DURATION:-60s}"
SLEEP_S="${SLEEP_S:-0.1}"

restore() {
  [[ -n "${MAX_INFLIGHT:-}" ]] && kubectl -n "$NS" set env deploy/client-api MAX_INFLIGHT- >/dev/null 2>&1
  test_env_restore
}
trap restore EXIT

log "run=$RUN_ID media=$MEDIA_VUS field=$FIELD_VUS duration=$DURATION"
test_env_apply
if [[ -n "${MAX_INFLIGHT:-}" ]]; then
  log "client-api MAX_INFLIGHT=$MAX_INFLIGHT"
  kubectl -n "$NS" set env deploy/client-api MAX_INFLIGHT="$MAX_INFLIGHT" >/dev/null
  kubectl -n "$NS" rollout status deploy/client-api --timeout=180s >/dev/null
fi

run_k6 "$EVIDENCE_DIR/k6-shed.json" -e SCENARIO=shed -e MEDIA_VUS="$MEDIA_VUS" -e FIELD_VUS="$FIELD_VUS" \
  -e DURATION="$DURATION" -e SLEEP_S="$SLEEP_S" > "$EVIDENCE_DIR/k6-shed.txt" 2>&1

"$PY" - "$EVIDENCE_DIR/k6-shed.json" <<'PYEOF' | tee "$EVIDENCE_DIR/verdict.txt"
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
fs = m.get("field_team_served_rate", {}).get("value", 0)
mr = m.get("media_refused_rate", {}).get("value", 0)
fl = m.get("field_team_latency", {})
ue = int(m.get("unexpected_errors_total", {}).get("count", 0))
print(f"field team served (200): {fs:.2%}   p95 {fl.get('p(95)', 0):.0f} ms")
print(f"media refused (503/429): {mr:.2%}")
print(f"unexpected errors       : {ue}")
print("RESULT:", "PASS" if fs >= 0.99 and ue == 0 else "FAIL")
PYEOF
