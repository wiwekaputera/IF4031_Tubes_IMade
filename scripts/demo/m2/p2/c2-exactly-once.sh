#!/usr/bin/env bash
# Criterion 2: Exactly-once upstream polling with >=3 aggregator replicas.
# Resets BMKG and PVMBG mock request recorders, lets 3 replicas run for ~60s,
# verifies duplicates == [] and single active instance in both mocks, and
# confirms zero duplicate (source, source_ref_id) rows in canonical_events.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access
setup_bmkg_access
setup_pvmbg_access
agg_ensure_replicas 3

DURATION_S="${DURATION_S:-60}"
WINDOW_S="${WINDOW_S:-$((DURATION_S + 30))}"

p2_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} DURATION_S=${DURATION_S}"

# 1. Record replica leader views before observation window
agg_probe_all_leaders "${EVIDENCE_DIR}/c2-leaders-before.json"

# 2. Reset both mock request recorders
p2_log "Resetting mock request recorders on bmkg-mock and pvmbg-mock..."
RESET_TMP="$(mktemp)"
_P2_TMP_FILES+=("$RESET_TMP")

host_http_call "POST" "${BMKG_BASE}/admin/requests/reset" "$RESET_TMP"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: POST ${BMKG_BASE}/admin/requests/reset failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi
host_http_call "POST" "${PVMBG_BASE}/admin/requests/reset" "$RESET_TMP"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: POST ${PVMBG_BASE}/admin/requests/reset failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi

# 3. Wait observation window while 3 replicas are running
p2_log "Observing 3 aggregator replicas for ${DURATION_S}s..."
sleep "$DURATION_S"

# 4. Capture mock request logs, leader lease, watermarks, and DB duplicate check
BMKG_JSON="${EVIDENCE_DIR}/c2-bmkg-requests.json"
PVMBG_JSON="${EVIDENCE_DIR}/c2-pvmbg-requests.json"

host_http_call "GET" "${BMKG_BASE}/admin/requests?window_s=${WINDOW_S}" "$BMKG_JSON"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: GET ${BMKG_BASE}/admin/requests failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi

host_http_call "GET" "${PVMBG_BASE}/admin/requests?window_s=${WINDOW_S}" "$PVMBG_JSON"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: GET ${PVMBG_BASE}/admin/requests failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi

agg_probe_all_leaders "${EVIDENCE_DIR}/c2-leaders-after.json"

{
  echo "=== leader_leases ==="
  crdb_sql "
    SELECT name, holder, fencing_token, expires_at, now() AS db_now, (expires_at > now()) AS is_active
    FROM leader_leases
    WHERE name = 'ingest-poller';
  "
  echo ""
  echo "=== poller_watermarks ==="
  crdb_sql "
    SELECT source, watermark, fencing_token, updated_at
    FROM poller_watermarks
    ORDER BY source;
  "
} > "${EVIDENCE_DIR}/c2-lease-and-watermarks.txt"

DUP_TSV="$(crdb_sql "
  SELECT source, source_ref_id, count(*) AS cnt
  FROM canonical_events
  GROUP BY source, source_ref_id
  HAVING count(*) > 1;
" tsv)"

{
  echo "=== Duplicate (source, source_ref_id) rows in canonical_events ==="
  echo "$DUP_TSV"
  echo ""
  echo "=== Per-source total vs distinct source_ref_id ==="
  crdb_sql "
    SELECT source,
           count(*) AS total_rows,
           count(DISTINCT source_ref_id) AS distinct_source_ref_ids
    FROM canonical_events
    WHERE deleted_at IS NULL
    GROUP BY source
    ORDER BY source;
  "
} > "${EVIDENCE_DIR}/c2-db-duplicates.txt"

REPLICA_COUNT="$(agg_list_instances | wc -l | tr -d ' ')"

# 5. Verify assertions
"$PY" - "$BMKG_JSON" "$PVMBG_JSON" "$DUP_TSV" "$REPLICA_COUNT" \
  > "${EVIDENCE_DIR}/c2-verification.txt" <<'PYEOF'
import csv, io, json, sys

bmkg = json.load(open(sys.argv[1], encoding="utf-8"))
pvmbg = json.load(open(sys.argv[2], encoding="utf-8"))
dup_tsv = sys.argv[3].strip()
replica_count = int(sys.argv[4])

dup_rows = list(csv.DictReader(io.StringIO(dup_tsv), delimiter="\t")) if dup_tsv else []

bmkg_insts = sorted((bmkg.get("by_instance") or {}).keys())
pvmbg_insts = sorted((pvmbg.get("by_instance") or {}).keys())
bmkg_dups = bmkg.get("duplicates") or []
pvmbg_dups = pvmbg.get("duplicates") or []

print("=== Criterion 2: Exactly-Once Upstream Polling & Zero DB Duplicates ===")
print(f"Running aggregator replicas : {replica_count}")
print(f"BMKG total requests         : {bmkg.get('total', 0)}")
print(f"BMKG by_instance            : {bmkg.get('by_instance', {})}")
print(f"BMKG duplicates             : {bmkg_dups}")
print(f"PVMBG total requests        : {pvmbg.get('total', 0)}")
print(f"PVMBG by_instance           : {pvmbg.get('by_instance', {})}")
print(f"PVMBG duplicates            : {pvmbg_dups}")
print(f"DB duplicate rows           : {len(dup_rows)}")

assert replica_count >= 3, f"expected >= 3 replicas, got {replica_count}"
assert bmkg.get("total", 0) > 0, "expected > 0 BMKG requests"
assert pvmbg.get("total", 0) > 0, "expected > 0 PVMBG requests"
assert len(bmkg_dups) == 0, f"BMKG reported duplicate upstream requests: {bmkg_dups}"
assert len(pvmbg_dups) == 0, f"PVMBG reported duplicate upstream requests: {pvmbg_dups}"
assert len(bmkg_insts) == 1, f"expected 1 active BMKG poller instance, got {bmkg_insts}"
assert len(pvmbg_insts) == 1, f"expected 1 active PVMBG poller instance, got {pvmbg_insts}"
assert bmkg_insts == pvmbg_insts, f"expected same active leader for BMKG ({bmkg_insts}) and PVMBG ({pvmbg_insts})"
assert len(dup_rows) == 0, f"expected 0 duplicate (source, source_ref_id) rows in DB, got {dup_rows}"
print("\nASSERTION OK: 3 replicas active, single leader polled each upstream (duplicates=[]), and 0 DB duplicates.")
PYEOF

cat "${EVIDENCE_DIR}/c2-verification.txt"
p2_log "Criterion 2 complete. Evidence saved under ${EVIDENCE_DIR}/c2-*"
