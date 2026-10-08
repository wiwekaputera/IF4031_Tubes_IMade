#!/usr/bin/env bash
# p3.sh — End-to-end integration and demo script for Problem 3 (P3)
#
# Tests and verifies:
#   1. Token issuance for 3 clients (client_credentials); only field_team gets refresh_token
#   2. GET /v1/events without token -> 401 Unauthorized
#   3. Token with tampered signature -> 401 Unauthorized
#   4. Media GET /v1/events -> 200 OK, summary fields only (no lat/lon/attributes)
#   5. Media GET /v1/events?view=full -> 403 Forbidden (insufficient_scope)
#   6. Field team GET /v1/events?view=full -> 200 OK, full fields
#   7. Wait for access token expiry -> 401 Unauthorized
#   8. Refresh token for field team -> 200 OK, new access & rotated refresh token
#   9. Reuse detection: presenting OLD refresh token -> 400 invalid_grant
#  10. Burst request > BURST -> 429 Too Many Requests + Retry-After header
#  11. Upstream credential isolation (cross-credentials) -> 403 on both BMKG & PVMBG
#  12. Correlation ID propagation: X-Correlation-ID matches in client-api & aggregator logs
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# ---- credentials and ports (sourced from .env if present) ----------------
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TOKEN_SERVICE_PORT="${TOKEN_SERVICE_PORT:-8083}"
CLIENT_API_PORT="${CLIENT_API_PORT:-8080}"
BMKG_PORT="${BMKG_PORT:-8081}"
PVMBG_PORT="${PVMBG_PORT:-8082}"

BMKG_API_KEY="${BMKG_API_KEY:-change-me-bmkg-api-key}"
PVMBG_BEARER_TOKEN="${PVMBG_BEARER_TOKEN:-change-me-pvmbg-bearer-token}"

CLIENT_MEDIA_ID="${CLIENT_MEDIA_ID:-client-media}"
CLIENT_MEDIA_SECRET="${CLIENT_MEDIA_SECRET:-change-me-media-secret}"

CLIENT_FIELD_TEAM_ID="${CLIENT_FIELD_TEAM_ID:-client-field-team}"
CLIENT_FIELD_TEAM_SECRET="${CLIENT_FIELD_TEAM_SECRET:-change-me-field-team-secret}"

CLIENT_INTERNAL_OPS_ID="${CLIENT_INTERNAL_OPS_ID:-client-internal-ops}"
CLIENT_INTERNAL_OPS_SECRET="${CLIENT_INTERNAL_OPS_SECRET:-change-me-internal-ops-secret}"

CLIENT_API_BURST="${CLIENT_API_BURST:-20}"

TOKEN_URL="http://localhost:${TOKEN_SERVICE_PORT}/oauth/token"
CLIENT_API_URL="http://localhost:${CLIENT_API_PORT}"

# ---- counters / output helpers -------------------------------------------
PASS=0
FAIL=0
ok(){   echo "  [PASS] $*"; PASS=$((PASS+1)); }
bad(){  echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
log(){  echo "[p3-demo] $*"; }

# ---- preconditions -------------------------------------------------------
if [[ -z "$(docker compose ps -q client-api 2>/dev/null || true)" ]] || \
   [[ -z "$(docker compose ps -q token-service 2>/dev/null || true)" ]]; then
  echo "ERROR: client-api or token-service is not running. Start the stack first:" >&2
  echo "       bash scripts/demo/up.sh" >&2
  echo "   or: docker compose up -d" >&2
  exit 1
fi

# ---- shared temp files ---------------------------------------------------
RESP_FILE="$(mktemp)"
HDR_FILE="$(mktemp)"

cleanup(){
  rm -f "$RESP_FILE" "$HDR_FILE"
}
trap cleanup EXIT

# ---- HTTP helpers --------------------------------------------------------
host_req(){
  local method="$1"; shift
  local url="$1"; shift
  rm -f "$HDR_FILE"
  local out
  out="$(curl -sS -m 15 -X "$method" -D "$HDR_FILE" -w '\n%{http_code}' "$@" "$url" 2>/dev/null || true)"
  HTTP_CODE="$(printf '%s' "$out" | tail -n1)"
  BODY="$(printf '%s' "$out" | sed '$d')"
  printf '%s' "$BODY" > "$RESP_FILE"
}

host_get(){
  host_req "GET" "$@"
}

host_post_form(){
  local url="$1"; shift
  rm -f "$HDR_FILE"
  local out
  out="$(curl -sS -m 15 -X POST -D "$HDR_FILE" -H "Content-Type: application/x-www-form-urlencoded" -w '\n%{http_code}' "$@" "$url" 2>/dev/null || true)"
  HTTP_CODE="$(printf '%s' "$out" | tail -n1)"
  BODY="$(printf '%s' "$out" | sed '$d')"
  printf '%s' "$BODY" > "$RESP_FILE"
}

hdr_get(){
  local name="$1"
  grep -i "^${name}:" "$HDR_FILE" 2>/dev/null | head -n1 | cut -d: -f2- | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]\r]*$//' || true
}

# py_check 'assertions using d=json.load(open(sys.argv[1]))' -> exit 0/1
py_check(){
  python3 - "$RESP_FILE" <<PYEOF
import sys, json
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
except Exception:
    sys.exit(2)
$1
PYEOF
}

# py_val key -> prints value or empty string
py_val(){
  python3 - "$RESP_FILE" "$1" <<'PYEOF'
import sys, json
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    v = d.get(sys.argv[2], "")
    print(v if v is not None else "")
except Exception:
    print("")
PYEOF
}

# =========================================================================
# Skenario 1: Ambil token untuk 3 client (client_credentials)
# Ekspektasi: 200 OK, hanya field_team yang mendapatkan refresh_token
# =========================================================================
echo
echo "=== Skenario 1: Ambil token untuk 3 client (client_credentials) ==="

# 1.1 Media Client
host_post_form "$TOKEN_URL" \
  -d "grant_type=client_credentials" \
  -d "client_id=${CLIENT_MEDIA_ID}" \
  -d "client_secret=${CLIENT_MEDIA_SECRET}"

if [[ "$HTTP_CODE" == "200" ]] && py_check '
assert bool(d.get("access_token")), "missing access_token"
assert d.get("scope") == "media", "scope is not media"
assert d.get("token_type") == "Bearer", "token_type not Bearer"
assert not d.get("refresh_token"), "media should NOT have refresh_token"
'; then
  MEDIA_TOKEN="$(py_val access_token)"
  ok "Client media: 200, scope=media, refresh_token=null"
else
  bad "Client media: expected 200 with scope=media and no refresh_token (HTTP ${HTTP_CODE:-000})"
  MEDIA_TOKEN=""
fi

# 1.2 Field Team Client
host_post_form "$TOKEN_URL" \
  -d "grant_type=client_credentials" \
  -d "client_id=${CLIENT_FIELD_TEAM_ID}" \
  -d "client_secret=${CLIENT_FIELD_TEAM_SECRET}"

if [[ "$HTTP_CODE" == "200" ]] && py_check '
assert bool(d.get("access_token")), "missing access_token"
assert d.get("scope") == "field_team", "scope is not field_team"
assert bool(d.get("refresh_token")), "field_team MUST have refresh_token"
'; then
  FIELD_TOKEN="$(py_val access_token)"
  FIELD_REFRESH_TOKEN="$(py_val refresh_token)"
  FIELD_EXPIRES_IN="$(py_val expires_in)"
  ok "Client field_team: 200, scope=field_team, refresh_token present"
else
  bad "Client field_team: expected 200 with refresh_token present (HTTP ${HTTP_CODE:-000})"
  FIELD_TOKEN=""
  FIELD_REFRESH_TOKEN=""
  FIELD_EXPIRES_IN=60
fi

# 1.3 Internal Ops Client
host_post_form "$TOKEN_URL" \
  -d "grant_type=client_credentials" \
  -d "client_id=${CLIENT_INTERNAL_OPS_ID}" \
  -d "client_secret=${CLIENT_INTERNAL_OPS_SECRET}"

if [[ "$HTTP_CODE" == "200" ]] && py_check '
assert bool(d.get("access_token")), "missing access_token"
assert d.get("scope") == "internal_ops", "scope is not internal_ops"
assert not d.get("refresh_token"), "internal_ops should NOT have refresh_token"
'; then
  OPS_TOKEN="$(py_val access_token)"
  ok "Client internal_ops: 200, scope=internal_ops, refresh_token=null"
else
  bad "Client internal_ops: expected 200 with scope=internal_ops and no refresh_token (HTTP ${HTTP_CODE:-000})"
  OPS_TOKEN=""
fi

# =========================================================================
# Skenario 2: GET /v1/events tanpa token
# Ekspektasi: 401 Unauthorized + WWW-Authenticate header
# =========================================================================
echo
echo "=== Skenario 2: GET /v1/events tanpa token ==="
host_get "${CLIENT_API_URL}/v1/events"
www_auth="$(hdr_get "WWW-Authenticate")"
if [[ "$HTTP_CODE" == "401" && "$www_auth" == *'error="invalid_token"'* ]]; then
  ok "GET /v1/events tanpa token -> 401 + WWW-Authenticate: Bearer error=\"invalid_token\""
else
  bad "GET /v1/events tanpa token -> HTTP ${HTTP_CODE:-000} (expected 401, header: $www_auth)"
fi

# =========================================================================
# Skenario 3: Token dengan signature diubah
# Ekspektasi: 401 Unauthorized
# =========================================================================
echo
echo "=== Skenario 3: Token dengan signature diubah ==="
ACTIVE_TOKEN="${MEDIA_TOKEN:-${FIELD_TOKEN}}"
if [[ -n "$ACTIVE_TOKEN" ]]; then
  TAMPERED_TOKEN="$(python3 -c "
import sys
parts = sys.argv[1].split('.')
if len(parts) == 3:
    sig = parts[2]
    alt = sig[:-4] + ('xxxx' if not sig.endswith('xxxx') else 'yyyy')
    print(parts[0] + '.' + parts[1] + '.' + alt)
else:
    print(sys.argv[1] + 'tampered')
" "$ACTIVE_TOKEN")"

  host_get "${CLIENT_API_URL}/v1/events" -H "Authorization: Bearer ${TAMPERED_TOKEN}"
  if [[ "$HTTP_CODE" == "401" ]]; then
    ok "GET /v1/events dengan signature diubah -> 401 Unauthorized"
  else
    bad "GET /v1/events dengan signature diubah -> HTTP ${HTTP_CODE:-000} (expected 401)"
  fi
else
  bad "Lewati skenario 3: tidak ada token valid untuk di-tamper"
fi

# =========================================================================
# Skenario 4: Media GET /v1/events (hanya field ringkasan)
# Ekspektasi: 200 OK, tanpa latitude/longitude/attributes
# =========================================================================
echo
echo "=== Skenario 4: Media GET /v1/events ==="
if [[ -n "$MEDIA_TOKEN" ]]; then
  host_get "${CLIENT_API_URL}/v1/events" -H "Authorization: Bearer ${MEDIA_TOKEN}"
  if [[ "$HTTP_CODE" == "200" ]] && py_check '
data = d.get("data", [])
assert isinstance(data, list), "data must be a list"
allowed = {"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"}
forbidden = {"latitude", "longitude", "attributes", "source_ref_id"}
for item in data:
    keys = set(item.keys())
    assert allowed.issubset(keys), f"missing allowed summary keys: {allowed - keys}"
    assert not (forbidden & keys), f"forbidden raw keys present in media scope: {forbidden & keys}"
'; then
    ok "Media GET /v1/events -> 200, field ringkasan saja (tanpa latitude/longitude/attributes)"
  else
    bad "Media GET /v1/events -> HTTP ${HTTP_CODE:-000} (expected 200 with summary projection)"
  fi
else
  bad "Lewati skenario 4: MEDIA_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 5: Media GET /v1/events?view=full
# Ekspektasi: 403 Forbidden dengan error="insufficient_scope"
# =========================================================================
echo
echo "=== Skenario 5: Media GET /v1/events?view=full ==="
if [[ -n "$MEDIA_TOKEN" ]]; then
  host_get "${CLIENT_API_URL}/v1/events?view=full" -H "Authorization: Bearer ${MEDIA_TOKEN}"
  www_auth="$(hdr_get "WWW-Authenticate")"
  if [[ "$HTTP_CODE" == "403" ]] && py_check 'assert d.get("error") == "insufficient_scope"'; then
    ok "Media GET /v1/events?view=full -> 403 Forbidden (error=\"insufficient_scope\")"
  else
    bad "Media GET /v1/events?view=full -> HTTP ${HTTP_CODE:-000} (expected 403 insufficient_scope)"
  fi
else
  bad "Lewati skenario 5: MEDIA_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 6: Field team GET /v1/events?view=full
# Ekspektasi: 200 OK, seluruh field lengkap (termasuk latitude/longitude/attributes)
# =========================================================================
echo
echo "=== Skenario 6: Field team GET /v1/events?view=full ==="
if [[ -n "$FIELD_TOKEN" ]]; then
  host_get "${CLIENT_API_URL}/v1/events?view=full" -H "Authorization: Bearer ${FIELD_TOKEN}"
  if [[ "$HTTP_CODE" == "200" ]] && py_check '
data = d.get("data", [])
assert isinstance(data, list), "data must be a list"
if len(data) > 0:
    for item in data:
        assert "latitude" in item and "longitude" in item, "missing coordinates"
        assert "attributes" in item, "missing attributes"
'; then
    ok "Field team GET /v1/events?view=full -> 200, field lengkap (latitude, longitude, attributes ada)"
  else
    bad "Field team GET /v1/events?view=full -> HTTP ${HTTP_CODE:-000} (expected 200 with full fields)"
  fi
else
  bad "Lewati skenario 6: FIELD_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 7: Tunggu token expired -> request
# Ekspektasi: 401 Unauthorized
# =========================================================================
echo
echo "=== Skenario 7: Tunggu token expired -> request ==="
if [[ -n "$FIELD_TOKEN" ]]; then
  WAIT_S="${FIELD_EXPIRES_IN:-60}"
  case "$WAIT_S" in
    ''|*[!0-9]*) WAIT_S=60 ;;
  esac
  # Add small buffer beyond expiration
  WAIT_S=$((WAIT_S + 2))
  log "Menunggu token field team kedaluwarsa (${WAIT_S} detik)..."
  sleep "$WAIT_S"

  host_get "${CLIENT_API_URL}/v1/events" -H "Authorization: Bearer ${FIELD_TOKEN}"
  www_auth="$(hdr_get "WWW-Authenticate")"
  if [[ "$HTTP_CODE" == "401" && "$www_auth" == *'error="invalid_token"'* ]]; then
    ok "Token expired -> 401 Unauthorized + WWW-Authenticate: Bearer error=\"invalid_token\""
  else
    bad "Token expired -> HTTP ${HTTP_CODE:-000} (expected 401, header: $www_auth)"
  fi
else
  bad "Lewati skenario 7: FIELD_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 8: Refresh token field team
# Ekspektasi: 200 OK, access_token & refresh_token baru (rotasi single-use)
# =========================================================================
echo
echo "=== Skenario 8: Refresh token field team ==="
NEW_FIELD_TOKEN=""
NEW_FIELD_REFRESH_TOKEN=""
if [[ -n "$FIELD_REFRESH_TOKEN" ]]; then
  host_post_form "$TOKEN_URL" \
    -d "grant_type=refresh_token" \
    -d "client_id=${CLIENT_FIELD_TEAM_ID}" \
    -d "client_secret=${CLIENT_FIELD_TEAM_SECRET}" \
    -d "refresh_token=${FIELD_REFRESH_TOKEN}"

  if [[ "$HTTP_CODE" == "200" ]] && py_check "
new_acc = d.get('access_token')
new_ref = d.get('refresh_token')
old_ref = '$FIELD_REFRESH_TOKEN'
assert bool(new_acc), 'missing new access_token'
assert bool(new_ref), 'missing new refresh_token'
assert new_ref != old_ref, 'refresh_token was not rotated'
"; then
    NEW_FIELD_TOKEN="$(py_val access_token)"
    NEW_FIELD_REFRESH_TOKEN="$(py_val refresh_token)"
    ok "Refresh token field team -> 200, access token & refresh token baru (rotasi single-use)"
  else
    bad "Refresh token field team -> HTTP ${HTTP_CODE:-000} (expected 200 with rotated tokens)"
  fi
else
  bad "Lewati skenario 8: FIELD_REFRESH_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 9: Pakai refresh token LAMA lagi
# Ekspektasi: 400 Bad Request + error="invalid_grant" (reuse detection)
# =========================================================================
echo
echo "=== Skenario 9: Pakai refresh token LAMA lagi (reuse detection) ==="
if [[ -n "$FIELD_REFRESH_TOKEN" ]]; then
  host_post_form "$TOKEN_URL" \
    -d "grant_type=refresh_token" \
    -d "client_id=${CLIENT_FIELD_TEAM_ID}" \
    -d "client_secret=${CLIENT_FIELD_TEAM_SECRET}" \
    -d "refresh_token=${FIELD_REFRESH_TOKEN}"

  if [[ "$HTTP_CODE" == "400" ]] && py_check 'assert d.get("error") == "invalid_grant"'; then
    ok "Pakai refresh token lama lagi -> 400 invalid_grant (reuse detection berhasil)"
  else
    bad "Pakai refresh token lama lagi -> HTTP ${HTTP_CODE:-000} (expected 400 invalid_grant)"
  fi
else
  bad "Lewati skenario 9: FIELD_REFRESH_TOKEN tidak tersedia"
fi

# =========================================================================
# Skenario 10: Burst request > BURST
# Ekspektasi: Terjadi 429 Too Many Requests + header Retry-After
# =========================================================================
echo
echo "=== Skenario 10: Burst request > BURST ==="
BURST_TOKEN="${OPS_TOKEN:-${NEW_FIELD_TOKEN}}"
if [[ -z "$BURST_TOKEN" ]]; then
  # Fallback ambil token baru jika sebelumnya kosong
  host_post_form "$TOKEN_URL" \
    -d "grant_type=client_credentials" \
    -d "client_id=${CLIENT_INTERNAL_OPS_ID}" \
    -d "client_secret=${CLIENT_INTERNAL_OPS_SECRET}"
  BURST_TOKEN="$(py_val access_token)"
fi

BURST_NUM=$((CLIENT_API_BURST + 15))
GOT_429=0
RETRY_AFTER_VAL=""

log "Mengirim $BURST_NUM request berturut-turut untuk menguji burst rate limiter..."
for _ in $(seq 1 "$BURST_NUM"); do
  host_get "${CLIENT_API_URL}/v1/events" -H "Authorization: Bearer ${BURST_TOKEN}"
  if [[ "$HTTP_CODE" == "429" ]]; then
    GOT_429=1
    RETRY_AFTER_VAL="$(hdr_get "Retry-After")"
    break
  fi
done

if [[ "$GOT_429" -eq 1 && -n "$RETRY_AFTER_VAL" ]]; then
  ok "Burst request terlampaui -> 429 Too Many Requests + Retry-After: ${RETRY_AFTER_VAL}s"
else
  bad "Burst request -> expected 429 with Retry-After (got 429=$GOT_429, Retry-After=\"$RETRY_AFTER_VAL\")"
fi

# =========================================================================
# Skenario 11: Isolasi kredensial upstream (cross-credentials)
# Ekspektasi: 403 Forbidden pada kedua endpoint BMKG & PVMBG
# =========================================================================
echo
echo "=== Skenario 11: Isolasi kredensial upstream (cross-credentials) ==="

# 11.1 Kirim Bearer PVMBG ke BMKG (:8081)
host_get "http://localhost:${BMKG_PORT}/seismic-events" -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN}"
if [[ "$HTTP_CODE" == "403" ]]; then
  ok "Kirim Bearer PVMBG ke BMKG (:8081) -> 403 Forbidden"
else
  bad "Kirim Bearer PVMBG ke BMKG (:8081) -> HTTP ${HTTP_CODE:-000} (expected 403)"
fi

# 11.2 Kirim X-BMKG-Key ke PVMBG (:8082)
host_get "http://localhost:${PVMBG_PORT}/volcanic-reports" -H "X-BMKG-Key: ${BMKG_API_KEY}"
if [[ "$HTTP_CODE" == "403" ]]; then
  ok "Kirim X-BMKG-Key ke PVMBG (:8082) -> 403 Forbidden"
else
  bad "Kirim X-BMKG-Key ke PVMBG (:8082) -> HTTP ${HTTP_CODE:-000} (expected 403)"
fi

# =========================================================================
# Skenario 12: Propagasi X-Correlation-ID di log client-api & aggregator
# Ekspektasi: X-Correlation-ID sama tercatat di log client-api & aggregator
# =========================================================================
echo
echo "=== Skenario 12: Propagasi X-Correlation-ID di log client-api & aggregator ==="
DEMO_CORR_ID="demo-p3-corr-$(date +%s)-$RANDOM"
TRACE_TOKEN="${OPS_TOKEN:-${NEW_FIELD_TOKEN}}"
if [[ -z "$TRACE_TOKEN" ]]; then
  TRACE_TOKEN="${MEDIA_TOKEN}"
fi

host_get "${CLIENT_API_URL}/v1/events" \
  -H "Authorization: Bearer ${TRACE_TOKEN}" \
  -H "X-Correlation-ID: ${DEMO_CORR_ID}"

CLIENT_RESP_CORR="$(hdr_get "X-Correlation-ID")"

# Berikan jeda sejenak untuk flush log docker
sleep 1

CLIENT_LOGS="$(docker compose logs --tail=150 client-api 2>&1 || true)"
AGG_LOGS="$(docker compose logs --tail=150 aggregator 2>&1 || true)"

CLIENT_MATCH=0
AGG_MATCH=0

if printf '%s' "$CLIENT_LOGS" | grep -q "$DEMO_CORR_ID"; then
  CLIENT_MATCH=1
fi
if printf '%s' "$AGG_LOGS" | grep -q "$DEMO_CORR_ID"; then
  AGG_MATCH=1
fi

if [[ "$HTTP_CODE" == "200" && "$CLIENT_RESP_CORR" == "$DEMO_CORR_ID" && "$CLIENT_MATCH" -eq 1 && "$AGG_MATCH" -eq 1 ]]; then
  ok "X-Correlation-ID ($DEMO_CORR_ID) berhasil dipropagasi dan tercatat di log client-api & aggregator"
else
  bad "X-Correlation-ID ($DEMO_CORR_ID) gagal dipropagasi: HTTP=$HTTP_CODE, resp_corr=$CLIENT_RESP_CORR, client_log=$CLIENT_MATCH, agg_log=$AGG_MATCH"
fi

# =========================================================================
# Summary
# =========================================================================
echo
echo "==================================================="
echo "  P3 Demo Summary: ${PASS} passed, ${FAIL} failed"
echo "==================================================="
if [[ "$FAIL" -gt 0 ]]; then
  echo "  RESULT: FAIL"
  exit 1
fi
echo "  RESULT: ALL CHECKS PASSED"
exit 0
