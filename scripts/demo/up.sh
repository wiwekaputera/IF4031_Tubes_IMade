#!/usr/bin/env bash
# up.sh — Build and start the Person A stack (bmkg-mock, pvmbg-mock,
# canonical-store, aggregator) and wait until every service reports
# Docker "healthy". nats-jetstream is pulled in automatically as an
# aggregator dependency.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Person A services the demo exercises (canonical-store is the DB the
# aggregator writes to, so it is part of the readiness set).
SERVICES=(bmkg-mock pvmbg-mock canonical-store aggregator)
BUILD_TIMEOUT="${UP_BUILD_TIMEOUT:-300}"
HEALTH_TIMEOUT="${UP_HEALTH_TIMEOUT:-240}"
POLL_INTERVAL=3

log(){ echo "[up] $*"; }

health_of() {
  local svc="$1" st
  st="$(docker compose ps --format '{{.Health}}' "$svc" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ -z "$st" || "$st" == "<no value>" ]]; then
    local cid
    cid="$(docker compose ps -q "$svc" 2>/dev/null || true)"
    if [[ -n "$cid" ]]; then
      st="$(docker inspect --format '{{.State.Health.Status}}' "$cid" 2>/dev/null | tr -d '[:space:]' || true)"
    fi
  fi
  printf '%s' "${st:-starting}"
}

wait_healthy() {
  local svc="$1" elapsed=0 st
  while [[ $elapsed -lt $HEALTH_TIMEOUT ]]; do
    st="$(health_of "$svc")"
    case "$st" in
      healthy)
        log "$svc is healthy"
        return 0
        ;;
      unhealthy)
        log "ERROR: $svc is unhealthy"
        return 1
        ;;
    esac
    log "$svc: ${st:-starting} ..."
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
  done
  log "ERROR: $svc did not become healthy within ${HEALTH_TIMEOUT}s"
  return 1
}

log "Starting Person A stack (build may take a while on first run)..."
docker compose up -d --build --wait "${SERVICES[@]}" 2>/dev/null || \
  docker compose up -d --build "${SERVICES[@]}"

for svc in "${SERVICES[@]}"; do
  wait_healthy "$svc"
done

log ""
log "==================================================="
log "  Person A stack is READY (all services healthy)"
log "  bmkg-mock       http://localhost:8081   (port 8081)"
log "  pvmbg-mock      http://localhost:8082   (port 8082)"
log "  aggregator      http://aggregator:8090  (internal_api_net only)"
log "  canonical-store postgresql store_net    (no host port)"
log "==================================================="
log "Next: bash scripts/demo/smoke.sh"
