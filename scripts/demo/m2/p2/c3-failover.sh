#!/usr/bin/env bash
# Criterion 3: Abrupt leader failure & failover <= 60s without gaps or duplicates.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access
setup_bmkg_access
setup_pvmbg_access
agg_ensure_replicas 3

MAX_FAILOVER_S="${MAX_FAILOVER_S:-60}"
POST_TAKEOVER_POLL_WAIT_S="${POST_TAKEOVER_POLL_WAIT_S:-8}"
KILL_MODE="${KILL_MODE:-pod}"

p2_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} KILL_MODE=${KILL_MODE}"

# 1. Identify current leader
LEADER_LINE=""
for _ in $(seq 1 20); do
  if LEADER_LINE="$(agg_find_leader 2>/dev/null || true)" && [[ -n "$LEADER_LINE" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "$LEADER_LINE" ]]; then
  echo "ERROR: could not identify an active leader via /internal/leader." >&2
  exit 1
fi

OLD_TARGET="$(echo "$LEADER_LINE" | cut -f1)"
OLD_INSTANCE_ID="$(echo "$LEADER_LINE" | cut -f2)"
OLD_TOKEN="$(echo "$LEADER_LINE" | cut -f3)"
OLD_EXPIRES="$(echo "$LEADER_LINE" | cut -f4)"

p2_log "Current leader: target=${OLD_TARGET} instance_id=${OLD_INSTANCE_ID} fencing_token=${OLD_TOKEN} expires_at=${OLD_EXPIRES}"

agg_probe_all_leaders "${EVIDENCE_DIR}/c3-leaders-before.json"
{
  echo "leader_target=${OLD_TARGET}"
  echo "leader_instance_id=${OLD_INSTANCE_ID}"
  echo "fencing_token=${OLD_TOKEN}"
  echo "lease_expires_at=${OLD_EXPIRES}"
  echo ""
  echo "=== leader_leases (before) ==="
  crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  echo ""
  echo "=== poller_watermarks (before) ==="
  crdb_sql "SELECT source, watermark, fencing_token, updated_at FROM poller_watermarks ORDER BY source;"
} > "${EVIDENCE_DIR}/c3-state-before.txt"

# 2. Abruptly kill the leader and record T0
T0_JSON="$("$PY" -c 'import json, time; from datetime import datetime, timezone; t=time.time(); print(json.dumps({"ms": int(t*1000), "utc": datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")}))')"
T0_MS="$("$PY" -c 'import json,sys; print(json.loads(sys.argv[1])["ms"])' "$T0_JSON")"
T0_UTC="$("$PY" -c 'import json,sys; print(json.loads(sys.argv[1])["utc"])' "$T0_JSON")"

if [[ "$MODE" == "compose" ]]; then
  p2_log "Sending SIGKILL to leader container ${OLD_TARGET} at ${T0_UTC}..."
  docker kill -s KILL "$OLD_TARGET" >/dev/null
else
  if [[ "$KILL_MODE" == "node" ]]; then
    K8S_NODE="$(kubectl -n "$NAMESPACE" get pod "$OLD_TARGET" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    p2_log "KILL_MODE=node: leader pod ${OLD_TARGET} is on node '${K8S_NODE}'."
    if [[ -n "${KIND_NODE:-}" ]]; then
      docker stop "$KIND_NODE" >/dev/null
    elif [[ -n "${TESTER_SCENARIO:-}" ]] && kubectl -n tester-agent get ds tester-agent >/dev/null 2>&1; then
      kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$TESTER_SCENARIO"
    else
      echo "Power off / stop node '${K8S_NODE}' hosting ${OLD_TARGET}, then press ENTER..."
      read -r
    fi
  else
    p2_log "Force-deleting leader pod ${OLD_TARGET} (--grace-period=0 --force) at ${T0_UTC}..."
    kubectl -n "$NAMESPACE" delete pod "$OLD_TARGET" --grace-period=0 --force >/dev/null 2>&1 || true
  fi
fi

# 3. Wait for another instance to acquire the lease
p2_log "Waiting up to ${MAX_FAILOVER_S}s for a surviving replica to acquire leadership (token > ${OLD_TOKEN})..."
NEW_LEASE="$(agg_wait_leader "$MAX_FAILOVER_S" "$OLD_INSTANCE_ID" "$OLD_TOKEN" || true)"
T1_DB_MS="$("$PY" -c 'import time; print(int(time.time()*1000))')"

if [[ -z "$NEW_LEASE" ]]; then
  echo "ERROR: no new leader acquired within ${MAX_FAILOVER_S}s after killing ${OLD_TARGET}." >&2
  agg_collect_logs "${EVIDENCE_DIR}/c3-aggregator-logs-raw.log" || true
  exit 1
fi

NEW_HOLDER="$(echo "$NEW_LEASE" | cut -f1)"
NEW_TOKEN="$(echo "$NEW_LEASE" | cut -f2)"
NEW_EXPIRES="$(echo "$NEW_LEASE" | cut -f3)"
DB_TAKEOVER_S="$("$PY" -c "import sys; print(f'{(int(sys.argv[1]) - int(sys.argv[2])) / 1000.0:.3f}')" "$T1_DB_MS" "$T0_MS")"
p2_log "New leader observed in DB: holder=${NEW_HOLDER} fencing_token=${NEW_TOKEN} (db_observed=${DB_TAKEOVER_S}s)"

# 4. Let new leader run poll cycles, then check mocks vs DB for zero gaps & zero duplicates
p2_log "Waiting ${POST_TAKEOVER_POLL_WAIT_S}s for new leader to complete poll cycles..."
sleep "$POST_TAKEOVER_POLL_WAIT_S"

BMKG_EVENTS_TMP="$(mktemp)"
PVMBG_EVENTS_TMP="$(mktemp)"
_P2_TMP_FILES+=("$BMKG_EVENTS_TMP" "$PVMBG_EVENTS_TMP")

host_http_call "GET" "${BMKG_BASE}/seismic-events?limit=250" "$BMKG_EVENTS_TMP" -H "X-BMKG-Key: ${BMKG_API_KEY}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: GET ${BMKG_BASE}/seismic-events failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi
host_http_call "GET" "${PVMBG_BASE}/volcanic-reports?limit=250" "$PVMBG_EVENTS_TMP" -H "Authorization: Bearer ${PVMBG_BEARER_TOKEN}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: GET ${PVMBG_BASE}/volcanic-reports failed (HTTP ${HTTP_CODE})" >&2
  exit 1
fi

# Give the poller one more interval so every event seen in the snapshot has been persisted
sleep 5

# Collect logs and DB state
agg_collect_logs "${EVIDENCE_DIR}/c3-aggregator-logs-raw.log"
grep -E "leader_acquired|leader_lost|lease_released|fencing|write rejected|lease acquire/renew failed" \
  "${EVIDENCE_DIR}/c3-aggregator-logs-raw.log" > "${EVIDENCE_DIR}/c3-leader-fencing.log" || true

agg_probe_all_leaders "${EVIDENCE_DIR}/c3-leaders-after.json"

DB_REFS_CSV="${EVIDENCE_DIR}/c3-db-source-refs.csv"
crdb_sql "
  SELECT source, source_ref_id, written_by, fencing_token
  FROM canonical_events
  WHERE deleted_at IS NULL
  ORDER BY source, source_ref_id;
" csv > "$DB_REFS_CSV"

DUP_TSV="$(crdb_sql "
  SELECT source, source_ref_id, count(*) AS cnt
  FROM canonical_events
  GROUP BY source, source_ref_id
  HAVING count(*) > 1;
" tsv)"

{
  echo "=== leader_leases (after) ==="
  crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  echo ""
  echo "=== poller_watermarks (after) ==="
  crdb_sql "SELECT source, watermark, fencing_token, updated_at FROM poller_watermarks ORDER BY source;"
  echo ""
  echo "=== Duplicate check ==="
  echo "$DUP_TSV"
} > "${EVIDENCE_DIR}/c3-state-after.txt"

# 5. Verify takeover <= 60s, zero gaps, zero duplicates
"$PY" - "$T0_UTC" "$T0_MS" "$T1_DB_MS" "$OLD_INSTANCE_ID" "$OLD_TOKEN" "$NEW_HOLDER" "$NEW_TOKEN" \
  "$MAX_FAILOVER_S" "${EVIDENCE_DIR}/c3-leader-fencing.log" "$BMKG_EVENTS_TMP" "$PVMBG_EVENTS_TMP" \
  "$DB_REFS_CSV" "$DUP_TSV" \
  > "${EVIDENCE_DIR}/c3-verification.txt" <<'PYEOF'
import csv, io, json, re, sys
from datetime import datetime

(
    t0_utc, t0_ms, t1_db_ms, old_inst, old_tok, new_holder, new_tok,
    max_failover_s, log_path, bmkg_path, pvmbg_path, db_csv_path, dup_tsv
) = sys.argv[1:14]

t0_ms = int(t0_ms)
t1_db_ms = int(t1_db_ms)
old_tok = int(old_tok)
new_tok = int(new_tok)
max_failover_s = float(max_failover_s)

def parse_iso(s):
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

t0_dt = parse_iso(t0_utc)
log_takeover_s = None
log_acq_time = None

with open(log_path, encoding="utf-8", errors="replace") as f:
    for line in f:
        if "leader_acquired" not in line:
            continue
        m = re.search(r"(\{.*\})\s*$", line)
        if not m:
            continue
        try:
            obj = json.loads(m.group(1))
        except Exception:
            continue
        if int(obj.get("fencing_token", 0)) == new_tok and obj.get("instance_id") == new_holder:
            ts = obj.get("time")
            if ts:
                acq_dt = parse_iso(ts)
                diff = (acq_dt - t0_dt).total_seconds()
                if diff >= 0:
                    log_takeover_s = diff
                    log_acq_time = ts
                    break

db_takeover_s = (t1_db_ms - t0_ms) / 1000.0
effective_takeover_s = log_takeover_s if log_takeover_s is not None else db_takeover_s

bmkg_events = json.load(open(bmkg_path, encoding="utf-8"))
pvmbg_events = json.load(open(pvmbg_path, encoding="utf-8"))
bmkg_ids = {e["event_id"] for e in bmkg_events if e.get("event_id")}
pvmbg_ids = {e["report_id"] for e in pvmbg_events if e.get("report_id")}

db_bmkg = set()
db_pvmbg = set()
with open(db_csv_path, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["source"] == "BMKG":
            db_bmkg.add(r["source_ref_id"])
        elif r["source"] == "PVMBG":
            db_pvmbg.add(r["source_ref_id"])

missing_bmkg = sorted(bmkg_ids - db_bmkg)
missing_pvmbg = sorted(pvmbg_ids - db_pvmbg)
dup_rows = list(csv.DictReader(io.StringIO(dup_tsv.strip()), delimiter="\t")) if dup_tsv.strip() else []

print("=== Criterion 3: Leader Failover <= 60s, Zero Gaps, Zero Duplicates ===")
print(f"Old leader             : {old_inst} (fencing_token={old_tok})")
print(f"Kill timestamp (UTC)   : {t0_utc}")
print(f"New leader             : {new_holder} (fencing_token={new_tok})")
print(f"Log leader_acquired    : {log_acq_time or 'not matched in log'}")
if log_takeover_s is not None:
    print(f"Takeover time (log)    : {log_takeover_s:.3f} s")
print(f"Takeover time (DB obs) : {db_takeover_s:.3f} s")
print(f"Max allowed            : {max_failover_s:.1f} s")
print(f"BMKG mock events       : {len(bmkg_ids)} (missing in DB: {len(missing_bmkg)})")
print(f"PVMBG mock events      : {len(pvmbg_ids)} (missing in DB: {len(missing_pvmbg)})")
print(f"DB duplicate rows      : {len(dup_rows)}")

assert new_holder != old_inst, "new leader must differ from killed leader"
assert new_tok > old_tok, f"expected fencing_token > {old_tok}, got {new_tok}"
assert effective_takeover_s <= max_failover_s, f"failover took {effective_takeover_s:.3f}s > {max_failover_s}s"
assert len(missing_bmkg) == 0, f"missing BMKG events in DB after failover: {missing_bmkg[:10]}"
assert len(missing_pvmbg) == 0, f"missing PVMBG events in DB after failover: {missing_pvmbg[:10]}"
assert len(dup_rows) == 0, f"found duplicate rows in DB: {dup_rows}"
print(f"\nASSERTION OK: failover completed in {effective_takeover_s:.3f}s (<= {max_failover_s:.0f}s) with 0 gaps and 0 duplicates.")
PYEOF

cat "${EVIDENCE_DIR}/c3-verification.txt"

# Restore 3 replicas for subsequent scenarios
p2_log "Restoring 3 healthy aggregator replicas..."
agg_ensure_replicas 3
p2_log "Criterion 3 complete. Evidence saved under ${EVIDENCE_DIR}/c3-*"
