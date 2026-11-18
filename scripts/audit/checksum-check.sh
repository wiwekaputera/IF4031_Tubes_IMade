#!/usr/bin/env bash
set -euo pipefail

SOURCE="${1:-}"
CRDB_SERVICE="${CRDB_SERVICE:-canonical-store-0}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
TOKEN="${AUDIT_TOKEN:-${AGGREGATOR_AUDIT_TOKEN:-}}"

if [[ -n "$SOURCE" ]]; then
  case "$SOURCE" in
    BMKG|PVMBG|SATELIT|SENSOR_PANTAI) ;;
    *)
      echo "Invalid source: $SOURCE (expected BMKG, PVMBG, SATELIT, or SENSOR_PANTAI)" >&2
      exit 1
      ;;
  esac
  SQL="SELECT source_ref_id FROM canonical_events WHERE source='${SOURCE}' AND deleted_at IS NULL"
  QUERY="?source=${SOURCE}"
else
  SQL="SELECT source_ref_id FROM canonical_events WHERE deleted_at IS NULL"
  QUERY=""
fi

sha256_cmd() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

# Fetch source_ref_id list from CockroachDB, skip CSV header, strip CR, sort in byte order.
IDS=$(docker compose exec -T "$CRDB_SERVICE" cockroach sql --insecure -d canonical --format=csv -e "$SQL" \
  | tail -n +2 \
  | tr -d '\r' \
  | sed '/^$/d' \
  | LC_ALL=C sort)

if [[ -z "$IDS" ]]; then
  COUNT=0
  INDEPENDENT_SUM=$(printf '' | sha256_cmd)
else
  COUNT=$(printf '%s\n' "$IDS" | wc -l | tr -d ' ')
  INDEPENDENT_SUM=$(printf '%s' "$IDS" | sha256_cmd)
fi

echo "source=${SOURCE:-ALL} count=${COUNT} independent_sha256=${INDEPENDENT_SUM}"

if [[ -n "$TOKEN" ]]; then
  API_JSON=$(curl -fsS -H "Authorization: Bearer ${TOKEN}" "${AGGREGATOR_URL}/internal/audit/checksum${QUERY}")
  API_SUM=$(printf '%s' "$API_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["checksum"])')
  API_COUNT=$(printf '%s' "$API_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')
  echo "api_count=${API_COUNT} api_sha256=${API_SUM}"
  if [[ "$INDEPENDENT_SUM" != "$API_SUM" || "$COUNT" != "$API_COUNT" ]]; then
    echo "[FAIL] Checksum mismatch!" >&2
    exit 1
  fi
  echo "[PASS] Checksum matches."
fi
