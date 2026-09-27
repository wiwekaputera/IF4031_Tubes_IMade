#!/usr/bin/env bash
# smoke.sh — Exercise the Person A stack against a live `docker compose up`
# stack and assert the key endpoints respond correctly.
#
# The aggregator is built on a distroless image (no curl) and publishes no
# host port — it lives only on the internal_api_net network. We therefore run
# a single `curlimages/curl` helper container joined to that network and
# `docker exec` curl into it for every aggregator request.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# ---- credentials (sourced from .env if present) -------------------------
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
BMKG_API_KEY="${BMKG_API_KEY:-test-bmkg-key}"
PVMBG_BEARER_TOKEN="${PVMBG_BEARER_TOKEN:-test-pvmbg-token}"

# ---- counters / output helpers -------------------------------------------
PASS=0
FAIL=0
ok(){   echo "  [PASS] $*"; PASS=$((PASS+1)); }
bad(){  echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }

# ---- preconditions -------------------------------------------------------
if [[ -z "$(docker compose ps -q aggregator 2>/dev/null || true)" ]]; then
  echo "ERROR: aggregator is not running. Start the stack first:" >&2
  echo "       bash scripts/demo/up.sh" >&2
  exit 1
fi

# Resolve the internal network the aggregator is attached to. The aggregator
# publishes no host port, so we reach http://aggregator:8090 from a container
# on the same bridge network (internal_api_net).
COMPOSE_NAME="$(docker compose config --format json 2>/dev/null \
  | python3 -c 'import sys,json;print(json.load(sys.stdin).get("name",""))' 2>/dev/null || true)"
COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
NET="${COMPOSE_NAME}_internal_api_net"

CURL_IMG="${CURL_IMG:-curlimages/curl:8.11.0}"

# ---- shared temp file + helper container --------------------------------
RESP_FILE="$(mktemp)"
HELPER_NAME="demo-smoke-curl"
cleanup(){
  rm -f "$RESP_FILE"
  docker rm -f "$HELPER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log(){ echo "[smoke] $*"; }

# host_get URL [curl args...]  -> body in $RESP_FILE, code in $HTTP_CODE
# Body is captured from stdout (NOT -o inside a container) so the host temp
# file always reflects the curl that just ran, whether local or via exec.
host_get(){
  local url="$1"; shift
  local out
  out="$(curl -sS -m 15 -w '\n%{http_code}' "$@" "$url" 2>/dev/null || true)"
  HTTP_CODE="$(printf '%s' "$out" | tail -n1)"
  BODY="$(printf '%s' "$out" | sed '$d')"
  printf '%s' "$BODY" > "$RESP_FILE"
}

# agg_get URL [curl args...]  -> body in $RESP_FILE, code in $HTTP_CODE
# (executed from the helper container on internal_api_net)
agg_get(){
  local url="$1"; shift
  local out
  out="$(docker exec "$HELPER_NAME" curl -sS -m 15 -w '\n%{http_code}' "$@" "$url" 2>/dev/null || true)"
  HTTP_CODE="$(printf '%s' "$out" | tail -n1)"
  BODY="$(printf '%s' "$out" | sed '$d')"
  printf '%s' "$BODY" > "$RESP_FILE"
}

# py_check 'python using d=json.load(open(sys.argv[1]))'  -> exit 0/1
py_check(){
  python3 - "$RESP_FILE" <<PYEOF
import sys,json
try:
    f=open(sys.argv[1])
    d=json.load(f)
except Exception:
    sys.exit(2)
$1
PYEOF
}

# numeric body count (for arrays) via python
body_len(){
  python3 - "$RESP_FILE" <<'PYEOF'
import sys,json
try:
    d=json.load(open(sys.argv[1]))
    print(len(d) if isinstance(d,list) else 0)
except Exception:
    print(0)
PYEOF
}

# ---- spin up the helper curl container ----------------------------------
log "Ensuring curl helper image ($CURL_IMG)..."
if ! docker pull "$CURL_IMG" >/dev/null 2>&1; then
  log "WARN: could not pull $CURL_IMG (offline?); relying on locally cached image"
fi

log "Starting network-scoped helper on ${NET}..."
docker rm -f "$HELPER_NAME" >/dev/null 2>&1 || true
if ! docker run -d --rm --name "$HELPER_NAME" --network "$NET" "$CURL_IMG" sleep 3600 >/dev/null 2>&1; then
  echo "ERROR: could not start helper container ($CURL_IMG) on ${NET}." >&2
  echo "       The image may be missing; run: docker pull $CURL_IMG" >&2
  exit 1
fi

# =========================================================================
# 1) BMKG mock — health
# =========================================================================
echo
echo "=== BMKG mock (:8081) ==="
host_get "http://localhost:8081/health"
if [[ "$HTTP_CODE" == "200" ]] && py_check 'assert d.get("status")=="UP"'; then
  ok "GET /health -> 200 {\"status\":\"UP\"}"
else
  bad "GET /health -> HTTP ${HTTP_CODE:-000} (expected 200, status=UP)"
fi

# 2) BMKG mock — seismic events (>=22)
host_get "http://localhost:8081/seismic-events" -H "X-BMKG-Key: ${BMKG_API_KEY}"
bmkg_n="$(body_len)"
if [[ "$HTTP_CODE" == "200" ]] && py_check "assert isinstance(d,list) and len(d)>=22"; then
  ok "GET /seismic-events -> 200, count=$bmkg_n (>=22)"
else
  bad "GET /seismic-events -> HTTP ${HTTP_CODE:-000}, count=$bmkg_n (expected >=22)"
fi

# =========================================================================
# 3) PVMBG mock — health
# =========================================================================
echo
echo "=== PVMBG mock (:8082) ==="
host_get "http://localhost:8082/health"
if [[ "$HTTP_CODE" == "200" ]] && py_check 'assert d.get("status")=="UP"'; then
  ok "GET /health -> 200 {\"status\":\"UP\"}"
else
  bad "GET /health -> HTTP ${HTTP_CODE:-000} (expected 200, status=UP)"
fi

# 4) PVMBG mock — volcanic reports (>=22, volcano IDs GNG-001..GNG-005)
host_get "http://localhost:8082/volcanic-reports" -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN}"
pvmbg_n="$(body_len)"
if [[ "$HTTP_CODE" == "200" ]] && py_check '
ids={r.get("volcano_id") for r in d}
need={"GNG-001","GNG-002","GNG-003","GNG-004","GNG-005"}
assert isinstance(d,list) and len(d)>=22 and need.issubset(ids)
'; then
  ok "GET /volcanic-reports -> 200, count=$pvmbg_n (>=22), volcano IDs GNG-001..GNG-005 present"
else
  bad "GET /volcanic-reports -> HTTP ${HTTP_CODE:-000}, count=$pvmbg_n (expected >=22 with GNG-001..005)"
fi

# =========================================================================
# 5) Aggregator — internal health
# =========================================================================
echo
echo "=== Aggregator (:8090, via ${NET}) ==="
agg_get "http://aggregator:8090/internal/health"
if [[ "$HTTP_CODE" == "200" ]] && py_check 'assert d.get("status")=="UP"'; then
  ok "GET /internal/health -> 200 {\"status\":\"UP\"}"
else
  bad "GET /internal/health -> HTTP ${HTTP_CODE:-000} (expected 200, status=UP)"
fi

# =========================================================================
# 6) Aggregator — /internal/events sources + count
#    The aggregator only serves /internal/events (no host port); source
#    health (bmkg/pvmbg UP + last_synced_at) is reported in meta.sources.
#    We wait for the poller's first cycle to ingest the seeded events.
# =========================================================================
log "Waiting for aggregator poller to ingest events..."
EVENT_COUNT=0
for _ in {1..90}; do
  agg_get "http://aggregator:8090/internal/events?limit=100"
  if [[ "$HTTP_CODE" == "200" ]]; then
    EVENT_COUNT="$(python3 - "$RESP_FILE" <<'PYEOF'
import sys,json
try:
    d=json.load(open(sys.argv[1]))
    print(d.get("meta",{}).get("count",0))
except Exception:
    print(0)
PYEOF
)"
    case "$EVENT_COUNT" in
      ''|*[!0-9]*) EVENT_COUNT=0 ;;
    esac
  fi
  if [[ "$EVENT_COUNT" -ge 22 ]]; then
    break
  fi
  sleep 1
done

if [[ "$HTTP_CODE" == "200" ]] && py_check '
m=d.get("meta",{})
assert m.get("count",0)>=22, "count<%d"%m.get("count",0)
src=m.get("sources",{})
b=src.get("bmkg",{})
p=src.get("pvmbg",{})
assert b.get("status")=="UP" and "last_synced_at" in b, "bmkg not ready"
assert p.get("status")=="UP" and "last_synced_at" in p, "pvmbg not ready"
'; then
  ok "GET /internal/events?limit=100 -> 200, meta.count=$EVENT_COUNT (>=22), sources bmkg/pvmbg UP with last_synced_at"
else
  bad "GET /internal/events -> HTTP ${HTTP_CODE:-000}, meta.count=$EVENT_COUNT (expected >=22, sources UP)"
fi

# =========================================================================
# 7) Canonical store — tables + volcano_reference count via psql
# =========================================================================
echo
echo "=== Canonical store (psql) ==="
tables="$(docker compose exec -T canonical-store psql -U aggregator -d canonical -c '\dt' 2>&1 || true)"
if printf '%s' "$tables" | grep -qE 'canonical_events|outbox|volcano_reference'; then
  ok "psql \\dt lists canonical_events, outbox, volcano_reference"
else
  bad "psql \\dt did not list expected tables"
fi

volcano_count="$(docker compose exec -T canonical-store psql -U aggregator -d canonical -t -A -c 'SELECT count(*) FROM volcano_reference;' 2>&1 | tr -d '[:space:]' || true)"
if [[ "$volcano_count" == "5" ]]; then
  ok "SELECT count(*) FROM volcano_reference -> 5"
else
  bad "volcano_reference count != 5 (got: ${volcano_count:-<empty>})"
fi

# =========================================================================
# Summary
# =========================================================================
echo
echo "==================================================="
echo "  Smoke test summary: ${PASS} passed, ${FAIL} failed"
echo "==================================================="
if [[ "$FAIL" -gt 0 ]]; then
  echo "  RESULT: FAIL"
  exit 1
fi
echo "  RESULT: ALL CHECKS PASSED"
exit 0
