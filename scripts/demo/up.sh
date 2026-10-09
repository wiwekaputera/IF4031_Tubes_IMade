#!/usr/bin/env bash
# up.sh — Prepare .env (first run), then build and start the WHOLE stack
# (upstream mocks, aggregator + stores, NATS + consumers, token-service,
# client-api) and wait until every service reports Docker "healthy".
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Every service in docker-compose.yml, so all problem demos (P1–P5) work.
SERVICES=(
  bmkg-mock pvmbg-mock
  canonical-store nats-jetstream aggregator
  stub-sink notification-consumer audit-consumer
  token-db token-service client-api
)
BUILD_TIMEOUT="${UP_BUILD_TIMEOUT:-300}"
HEALTH_TIMEOUT="${UP_HEALTH_TIMEOUT:-240}"
POLL_INTERVAL=3

log(){ echo "[up] $*"; }

# ---- first-run preparation of .env ----------------------------------------
# .env is never committed. Create it from the template and generate a local
# ES256 signing key, otherwise token-service refuses to start.
if [[ ! -f .env ]]; then
  log ".env not found -> copying .env.example to .env"
  cp .env.example .env
fi

current_key="$(grep -E '^ES256_PRIVATE_KEY_B64=' .env | head -n1 | cut -d= -f2- || true)"
if [[ -z "$current_key" || "$current_key" == change-me* ]]; then
  log "ES256_PRIVATE_KEY_B64 is empty/placeholder -> generating a local key (scripts/keys/gen-es256.sh)"
  new_line="$(bash scripts/keys/gen-es256.sh | grep -E '^ES256_PRIVATE_KEY_B64=')"
  if grep -qE '^ES256_PRIVATE_KEY_B64=' .env; then
    # base64 never contains '|', so it is a safe sed delimiter
    sed -i "s|^ES256_PRIVATE_KEY_B64=.*|${new_line}|" .env
  else
    printf '\n%s\n' "$new_line" >> .env
  fi
  log "ES256 key written to .env"
fi

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

log "Starting full stack (build may take a while on first run)..."
docker compose up -d --build --wait "${SERVICES[@]}" 2>/dev/null || \
  docker compose up -d --build "${SERVICES[@]}"

for svc in "${SERVICES[@]}"; do
  wait_healthy "$svc"
done

log ""
log "==================================================="
log "  All services are READY (all services healthy)"
log "  bmkg-mock       http://localhost:8081   (port 8081)"
log "  pvmbg-mock      http://localhost:8082   (port 8082)"
log "  aggregator      http://aggregator:8090  (internal_api_net only)"
log "  canonical-store postgresql store_net    (no host port)"
log "  nats-jetstream  nats://nats-jetstream:4222 (bus_net only)"
log "  consumers       notification-consumer, audit-consumer, stub-sink (bus_net only)"
log "  token-service   http://localhost:8083   (port 8083)"
log "  client-api      http://localhost:8080   (port 8080)"
log "==================================================="
log "Next: bash scripts/demo/smoke.sh     # P1/P2 ingest pipeline"
log "      bash scripts/demo/p3.sh        # P3 auth & projection"
log "      bash scripts/demo/p2-load.sh   # P2 load test (k6)"
log "      bash scripts/demo/p5.sh        # P5 fan-out + new consumer"
