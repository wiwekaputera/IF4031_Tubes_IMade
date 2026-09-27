#!/usr/bin/env bash
# down.sh — Tear down the entire compose project and remove its
# named/anonymous volumes so the next `up.sh` starts from a clean state.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

log(){ echo "[down] $*"; }

# Remove the demo helper curl container if it is still around (smoke.sh
# should clean up on exit, but this guards against a crashed run).
log "Removing any leftover demo-curl helper container..."
docker rm -f demo-smoke-curl >/dev/null 2>&1 || true

log "Tearing down compose project (down -v)..."
# --rmi=local removes built local images; -v removes declared volumes.
docker compose down -v --rmi local

log ""
log "==================================================="
log "  Tear-down complete."
log "  - All compose services stopped and removed."
log "  - Named volumes (canonical_store_data, nats_data, ...)"
log "    removed -> next up.sh starts from a clean DB."
log "==================================================="
