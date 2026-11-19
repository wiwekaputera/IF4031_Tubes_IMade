#!/usr/bin/env bash
# Criterion 7: Analytics dashboard verification, Audit API consistency check,
# partial-availability API test, and screenshot instructions.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access

TEST_PARTIAL="${TEST_PARTIAL:-1}"
PAUSE_MOCKS="${PAUSE_MOCKS:-1}"
MOCKS_PAUSED=0
unpause_mocks() {
  if [[ "$MOCKS_PAUSED" -eq 1 && "$MODE" == "compose" ]]; then
    p1_log "Unpausing bmkg-mock and pvmbg-mock..."
    docker compose unpause bmkg-mock pvmbg-mock >/dev/null 2>&1 || true
    MOCKS_PAUSED=0
  fi
}
c7_cleanup() {
  unpause_mocks
  p1_cleanup
}
trap c7_cleanup EXIT

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"

if [[ "$MODE" == "compose" && "$PAUSE_MOCKS" == "1" ]]; then
  p1_log "Pausing bmkg-mock & pvmbg-mock briefly so dashboard summary and audit scan see the same snapshot..."
  docker compose pause bmkg-mock pvmbg-mock >/dev/null
  MOCKS_PAUSED=1
  sleep 4
fi

# 1. Verify /analytics/ static assets & zero CDN links
TMP_HTML="$(mktemp)"
TMP_JS="$(mktemp)"
TMP_APP="$(mktemp)"
_P1_TMP_FILES+=("$TMP_HTML" "$TMP_JS" "$TMP_APP")

agg_http_call "GET" "/analytics/" "$TMP_HTML"
[[ "$HTTP_CODE" == "200" ]] || { echo "ERROR: GET /analytics/ -> HTTP ${HTTP_CODE}" >&2; exit 1; }
agg_http_call "GET" "/analytics/chart.umd.min.js" "$TMP_JS"
[[ "$HTTP_CODE" == "200" ]] || { echo "ERROR: GET /analytics/chart.umd.min.js -> HTTP ${HTTP_CODE}" >&2; exit 1; }
agg_http_call "GET" "/analytics/app.js" "$TMP_APP"
[[ "$HTTP_CODE" == "200" ]] || { echo "ERROR: GET /analytics/app.js -> HTTP ${HTTP_CODE}" >&2; exit 1; }

if grep -qiE 'https?://(cdn|unpkg|jsdelivr)' "$TMP_HTML" "$TMP_APP"; then
  echo "ERROR: External CDN reference detected in /analytics/ assets" >&2
  exit 1
fi
p1_log "Verified /analytics/ (HTML + vendored Chart.js + app.js) returns HTTP 200 with zero CDN links."

# 2. Fetch analytics summaries
agg_get_json "/internal/analytics/summary?range=all" "${EVIDENCE_DIR}/c7-analytics-summary-all.json"
agg_get_json "/internal/analytics/summary?range=7d" "${EVIDENCE_DIR}/c7-analytics-summary-7d.json"

# 3. Run scripts/p1/dashboard-vs-audit.sh (dashboard == Audit API)
p1_log "Running scripts/p1/dashboard-vs-audit.sh (RANGE=all and RANGE=7d)..."
{
  echo "=== RANGE=all ==="
  AGGREGATOR_URL="${AGG_BASE}" AUDIT_TOKEN="${AUDIT_TOKEN}" AUDIT_LIMIT="${AUDIT_LIMIT:-250}" \
    CURL_HELPER_CONTAINER="${_P1_HELPER_CURL}" RANGE=all bash scripts/p1/dashboard-vs-audit.sh
  echo ""
  echo "=== RANGE=7d ==="
  AGGREGATOR_URL="${AGG_BASE}" AUDIT_TOKEN="${AUDIT_TOKEN}" AUDIT_LIMIT="${AUDIT_LIMIT:-250}" \
    CURL_HELPER_CONTAINER="${_P1_HELPER_CURL}" RANGE=7d bash scripts/p1/dashboard-vs-audit.sh
} | tee "${EVIDENCE_DIR}/c7-dashboard-vs-audit.txt"

unpause_mocks

# 4. Verify partial-banner API flag when Raft quorum is lost (briefly stop 3 of 4 CRDB nodes so RF=3 quorum is lost on all shards regardless of replica placement)
if [[ "$MODE" == "compose" && "$TEST_PARTIAL" == "1" ]]; then
  p1_log "Testing partial-availability banner state (briefly stopping canonical-store-1, canonical-store-2, canonical-store-3)..."
  docker compose stop -t 2 canonical-store-1 canonical-store-2 canonical-store-3 >/dev/null
  sleep 2
  agg_get_json "/internal/analytics/summary?range=all" "${EVIDENCE_DIR}/c7-analytics-partial.json" || true

  p1_log "Restarting canonical-store-1, canonical-store-2, and canonical-store-3..."
  docker compose start canonical-store-1 canonical-store-2 canonical-store-3 >/dev/null
  for n in canonical-store-1 canonical-store-2 canonical-store-3; do
    for _ in $(seq 1 30); do
      if docker compose exec -T "$n" curl -fsS "http://localhost:8080/health?ready=1" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
  done
  sleep 3
  agg_get_json "/internal/analytics/summary?range=all" "${EVIDENCE_DIR}/c7-analytics-recovered.json"

  "$PY" - "${EVIDENCE_DIR}/c7-analytics-partial.json" "${EVIDENCE_DIR}/c7-analytics-recovered.json" \
    > "${EVIDENCE_DIR}/c7-partial-check.txt" <<'PYEOF'
import json, sys
part = json.load(open(sys.argv[1], encoding="utf-8"))
rec = json.load(open(sys.argv[2], encoding="utf-8"))
print(f"During storage quorum outage : partial={part.get('partial')} unavailable_units={part.get('unavailable_units')} total={part.get('total')}")
print(f"After recovery               : partial={rec.get('partial')} unavailable_units={rec.get('unavailable_units')} total={rec.get('total')}")
assert part.get("partial") is True, f"expected partial=true during quorum outage, got {part.get('partial')}"
assert len(part.get("unavailable_units") or []) > 0, "expected non-empty unavailable_units during quorum outage"
assert rec.get("partial") is False, f"expected partial=false after recovery, got {rec.get('partial')}"
print("ASSERTION OK: partial banner flag triggered during outage and cleared after recovery.")
PYEOF
  cat "${EVIDENCE_DIR}/c7-partial-check.txt"
fi

# 5. Print screenshot instructions for demo / report
cat <<'EOF'

========================================================================
  Analytics Dashboard Browser & Screenshot Instructions
========================================================================
1. Expose the aggregator UI locally:
   - Docker Compose (if port 8090 is not bound on host):
       socat / temporary port forward, or add "8090:8090" to aggregator ports,
       or run: kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/aggregator 8090:8090
   - Open: http://localhost:8090/analytics/
2. Screenshot #1 (Healthy state):
   - Select range "All" (or "7d"), confirm line chart (5 hazard categories),
     severity bar chart, and top areas table render, and total matches Audit API.
3. Screenshot #2 (Partial banner during storage outage / c4):
   - During a node/quorum outage (e.g. 2 CRDB pods/containers stopped, or
     during brief lease transfer in c4), enable 10s auto-refresh (or click Refresh).
   - Capture the yellow/red partial warning banner:
       "Data belum lengkap — unit tidak terjangkau: shard-..."
     and the "≥" prefix on displayed counts.
========================================================================
EOF

p1_log "Criterion 7 complete. Evidence saved under ${EVIDENCE_DIR}/c7-*"
