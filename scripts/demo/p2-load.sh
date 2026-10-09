#!/usr/bin/env bash
# p2-load.sh — Problem 2 criteria #1 and #2 in one run.
#
#   1. Recreate ONLY pvmbg-mock with a fixed 3 s delay (PVMBG_DELAY_MIN_MS =
#      PVMBG_DELAY_MAX_MS = 3000). No other container is touched.
#   2. Run tests/load/k6_p2.js against client-api: SEISMIC-only and VOLCANIC
#      requests in parallel, 50 VUs, 60 s, p50/p95/p99, 429 count.
#   3. Restore pvmbg-mock to the delay configured in .env (also on Ctrl+C).
#
# K6_MODE (default: docker)
#   docker : run k6 from the grafana/k6 image attached to edge_net, hitting
#            http://client-api:8080 directly. Measures the gateway itself and
#            needs no local k6 install. Recommended on Windows/macOS, where
#            Docker Desktop's host port forwarding adds hundreds of ms under load.
#   local  : run the locally installed k6 against http://localhost:8080.
#
# Usage:
#   bash scripts/demo/p2-load.sh
#   K6_MODE=local bash scripts/demo/p2-load.sh
#   PVMBG_TEST_DELAY_MS=3000 VUS=50 DURATION=60s bash scripts/demo/p2-load.sh
#   bash scripts/demo/p2-load.sh --quiet          # extra args are passed to k6 run
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

log(){ echo "[p2-load] $*"; }

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

K6_MODE="${K6_MODE:-docker}"
K6_IMG="${K6_IMG:-grafana/k6:latest}"

if [[ "$K6_MODE" == "local" ]] && ! command -v k6 >/dev/null 2>&1; then
  echo "ERROR: k6 not found. Install it, or use the default K6_MODE=docker." >&2
  exit 1
fi
if [[ -z "$(docker compose ps -q client-api 2>/dev/null || true)" ]]; then
  echo "ERROR: stack is not running. Start it first: bash scripts/demo/up.sh" >&2
  exit 1
fi

PVMBG_PORT="${PVMBG_PORT:-8082}"
TEST_DELAY_MS="${PVMBG_TEST_DELAY_MS:-3000}"
ORIG_MIN="${PVMBG_DELAY_MIN_MS:-500}"
ORIG_MAX="${PVMBG_DELAY_MAX_MS:-3000}"
CLIENT_ID="${CLIENT_FIELD_TEAM_ID:-client-field-team}"
CLIENT_SECRET="${CLIENT_FIELD_TEAM_SECRET:-change-me-field-team-secret}"

restore(){
  log "Restoring pvmbg-mock delay to ${ORIG_MIN}-${ORIG_MAX} ms (from .env)..."
  PVMBG_DELAY_MIN_MS="$ORIG_MIN" PVMBG_DELAY_MAX_MS="$ORIG_MAX" \
    docker compose up -d --no-deps --wait pvmbg-mock >/dev/null 2>&1 || true
}
trap restore EXIT

log "Recreating ONLY pvmbg-mock with a fixed ${TEST_DELAY_MS} ms delay..."
PVMBG_DELAY_MIN_MS="$TEST_DELAY_MS" PVMBG_DELAY_MAX_MS="$TEST_DELAY_MS" \
  docker compose up -d --no-deps --wait pvmbg-mock >/dev/null 2>&1

pvmbg_time="$(curl -s -o /dev/null -w '%{time_total}' -m 10 \
  -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN:-}" \
  "http://localhost:${PVMBG_PORT}/volcanic-reports?limit=1" || echo "?")"
log "Direct PVMBG response time now: ${pvmbg_time}s (expected ~$((TEST_DELAY_MS / 1000))s)"

k6_rc=0
if [[ "$K6_MODE" == "docker" ]]; then
  COMPOSE_NAME="$(docker compose config --format json 2>/dev/null \
    | grep -o '"name": *"[^"]*"' | head -n1 | sed 's/.*"\([^"]*\)"$/\1/' || true)"
  EDGE_NET="${COMPOSE_NAME:-if4031-tubes-imade}_edge_net"
  log "Running k6 ($K6_IMG on ${EDGE_NET} -> http://client-api:8080): SEISMIC-only + VOLCANIC in parallel..."
  # Script is fed via stdin ("-") so no volume mount / path conversion is needed.
  docker run --rm -i --network "$EDGE_NET" "$K6_IMG" run \
    -e BASE_URL=http://client-api:8080 \
    -e TOKEN_URL=http://token-service:8083/oauth/token \
    -e CLIENT_ID="$CLIENT_ID" -e CLIENT_SECRET="$CLIENT_SECRET" \
    -e VUS="${VUS:-50}" -e DURATION="${DURATION:-60s}" \
    "$@" - < tests/load/k6_p2.js || k6_rc=$?
else
  log "Running local k6 -> http://localhost:${CLIENT_API_PORT:-8080}: SEISMIC-only + VOLCANIC in parallel..."
  k6 run \
    -e BASE_URL="http://localhost:${CLIENT_API_PORT:-8080}" \
    -e TOKEN_URL="http://localhost:${TOKEN_SERVICE_PORT:-8083}/oauth/token" \
    -e CLIENT_ID="$CLIENT_ID" -e CLIENT_SECRET="$CLIENT_SECRET" \
    -e VUS="${VUS:-50}" -e DURATION="${DURATION:-60s}" \
    "$@" tests/load/k6_p2.js || k6_rc=$?
fi

echo
log "Read the summary above for the report:"
log "  throughput        -> http_reqs (req/s)"
log "  BMKG-only latency -> bmkg_only_latency med (p50) / p(95) / p(99)   [P2 #1: p95 < 300 ms]"
log "  error rate        -> error_rate (excludes 429)                     [P2 #2: < 1%]"
log "  controlled 429s   -> rate_limited_total"
exit "$k6_rc"
