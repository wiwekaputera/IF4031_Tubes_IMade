#!/usr/bin/env bash
# Criterion 6: After c4 + c5, decommission dead CRDB nodes, verify every range
# has 3 live replicas, and survive a subsequent node loss with zero data loss
# and zero HTTP 5xx.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access

NEXT_VICTIM="${NEXT_VICTIM:-canonical-store-2}"
PAUSE_MOCKS="${PAUSE_MOCKS:-1}"
HAMMER_DURATION_S="${HAMMER_DURATION_S:-15}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-5}"

MOCKS_PAUSED=0
unpause_mocks() {
  if [[ "$MOCKS_PAUSED" -eq 1 && "$MODE" == "compose" ]]; then
    p1_log "Unpausing bmkg-mock and pvmbg-mock..."
    docker compose unpause bmkg-mock pvmbg-mock >/dev/null 2>&1 || true
    MOCKS_PAUSED=0
  fi
}
c6_cleanup() {
  unpause_mocks
  p1_cleanup
}
trap c6_cleanup EXIT

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} NEXT_VICTIM=${NEXT_VICTIM}"

crdb_node_status tsv "--all" > "${EVIDENCE_DIR}/c6-node-status-before.txt"

# Decommission any dead, non-decommissioned CRDB nodes from earlier c4 wipe
DEAD_IDS="$(crdb_dead_node_ids)"

ALIVE_SQL_NODE="$(crdb_alive_node)"
: > "${EVIDENCE_DIR}/c6-decommission.log"
if [[ -n "$DEAD_IDS" ]]; then
  for did in $DEAD_IDS; do
    if [[ -z "$did" ]]; then continue; fi
    p1_log "Decommissioning dead CRDB node_id=${did}..."
    crdb_exec "$ALIVE_SQL_NODE" cockroach node decommission "$did" --insecure --wait=all \
      >> "${EVIDENCE_DIR}/c6-decommission.log" 2>&1 || \
      crdb_exec "$ALIVE_SQL_NODE" cockroach node decommission "$did" --insecure --wait=none \
      >> "${EVIDENCE_DIR}/c6-decommission.log" 2>&1 || true
  done
else
  echo "No dead non-decommissioned nodes found." > "${EVIDENCE_DIR}/c6-decommission.log"
fi

# Confirm every range of canonical_events has 3 live replicas (wait briefly if rebalancing)
p1_log "Verifying every range of canonical_events has 3 live replicas..."
UNDER_REP="999"
for _ in $(seq 1 12); do
  UNDER_REP="$(crdb_under_replicated_count)"
  if [[ "$UNDER_REP" == "0" ]]; then
    break
  fi
  sleep 5
done

{
  echo "under_replicated_ranges=${UNDER_REP}"
  echo ""
  crdb_sql "SHOW RANGES FROM TABLE canonical_events WITH DETAILS;"
} > "${EVIDENCE_DIR}/c6-ranges-healthy.txt"

if [[ "$UNDER_REP" != "0" ]]; then
  echo "ERROR: ${UNDER_REP} ranges still have < 3 live replicas!" >&2
  exit 1
fi
p1_log "Confirmed: 0 under-replicated ranges on canonical_events."

if [[ "$MODE" == "compose" && "$PAUSE_MOCKS" == "1" ]]; then
  p1_log "Pausing bmkg-mock & pvmbg-mock for clean checksum comparison..."
  docker compose pause bmkg-mock pvmbg-mock >/dev/null
  MOCKS_PAUSED=1
  sleep 5
fi

# Repeat short node-loss drill on NEXT_VICTIM
agg_get_json "/internal/audit/checksum" "${EVIDENCE_DIR}/c6-checksum-before.json" -H "Authorization: Bearer ${AUDIT_TOKEN}"

if [[ "$MODE" == "compose" ]]; then
  p1_log "Stopping ${NEXT_VICTIM} to verify survival of subsequent node loss..."
  docker compose kill "$NEXT_VICTIM" >/dev/null
else
  p1_log "Draining/stopping ${NEXT_VICTIM} in k8s..."
  if [[ -n "${KIND_NEXT_VICTIM_NODE:-}" ]]; then
    docker stop "$KIND_NEXT_VICTIM_NODE" >/dev/null
  else
    kubectl -n "$NAMESPACE" delete pod "$NEXT_VICTIM" --grace-period=0 --force >/dev/null 2>&1 || true
  fi
fi

HAMMER_CSV="${EVIDENCE_DIR}/c6-hammer.csv"
echo "timestamp,endpoint,http_code,latency_s,partial" > "$HAMMER_CSV"
TMP_F="$(mktemp)"
_P1_TMP_FILES+=("$TMP_F")
END_TS=$(( $(date +%s) + HAMMER_DURATION_S ))
while [[ $(date +%s) -lt $END_TS ]]; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  agg_http_call "GET" "/internal/events?limit=20" "$TMP_F"
  P1="$("$PY" -c 'import json,sys;print(str(bool(json.load(open(sys.argv[1])).get("meta",{}).get("partial",False))).lower())' "$TMP_F" 2>/dev/null || echo "parse_err")"
  echo "${TS},/internal/events,${HTTP_CODE},${TIME_TOTAL_S},${P1}" >> "$HAMMER_CSV"

  agg_http_call "GET" "/internal/audit/checksum" "$TMP_F" -H "Authorization: Bearer ${AUDIT_TOKEN}"
  P2="$("$PY" -c 'import json,sys;print(str(bool(json.load(open(sys.argv[1])).get("partial",False))).lower())' "$TMP_F" 2>/dev/null || echo "parse_err")"
  echo "${TS},/internal/audit/checksum,${HTTP_CODE},${TIME_TOTAL_S},${P2}" >> "$HAMMER_CSV"
  sleep 0.5
done

agg_get_json "/internal/audit/checksum" "${EVIDENCE_DIR}/c6-checksum-after.json" -H "Authorization: Bearer ${AUDIT_TOKEN}"

if [[ "$MODE" == "compose" ]]; then
  p1_log "Restoring ${NEXT_VICTIM} so cluster returns to 4 healthy nodes..."
  docker compose start "$NEXT_VICTIM" >/dev/null
  for _ in $(seq 1 30); do
    if docker compose exec -T "$NEXT_VICTIM" curl -fsS "http://localhost:8080/health?ready=1" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  sleep 3
elif [[ -n "${KIND_NEXT_VICTIM_NODE:-}" ]]; then
  docker start "$KIND_NEXT_VICTIM_NODE" >/dev/null
fi

unpause_mocks
crdb_node_status tsv "--all" > "${EVIDENCE_DIR}/c6-node-status-after.txt" || true

"$PY" - "${EVIDENCE_DIR}/c6-checksum-before.json" "${EVIDENCE_DIR}/c6-checksum-after.json" "$HAMMER_CSV" "$UNDER_REP" \
  > "${EVIDENCE_DIR}/c6-verification.txt" <<'PYEOF'
import csv, json, statistics, sys

b = json.load(open(sys.argv[1], encoding="utf-8"))
a = json.load(open(sys.argv[2], encoding="utf-8"))
rows = list(csv.DictReader(open(sys.argv[3], encoding="utf-8")))
under_rep = sys.argv[4]

codes = [int(r["http_code"]) for r in rows]
lats = [float(r["latency_s"]) for r in rows]
five_xx = [c for c in codes if c >= 500 or c == 0]
match = (b["count"] == a["count"] and b["checksum"] == a["checksum"] and not a.get("partial", False))

print("=== Criterion 6: Survive Subsequent Node Loss ===")
print(f"Pre-drill under-replicated ranges : {under_rep}")
print(f"Hammer requests total             : {len(rows)}")
print(f"HTTP 5xx / timeouts               : {len(five_xx)}")
if lats:
    print(f"Latency (s)                       : min={min(lats):.4f} avg={statistics.mean(lats):.4f} max={max(lats):.4f}")
print(f"Checksum before                   : count={b['count']} sha256={b['checksum']}")
print(f"Checksum after                    : count={a['count']} sha256={a['checksum']} partial={a.get('partial')}")
print(f"Checksum match                    : {'MATCH' if match else 'MISMATCH'}")

assert len(five_xx) == 0, f"got {len(five_xx)} 5xx/timeout errors"
assert match, "checksum mismatch on second node loss"
print("ASSERTION OK: cluster survived subsequent node loss with 0 5xx and identical checksum.")
PYEOF
cat "${EVIDENCE_DIR}/c6-verification.txt"

p1_log "Criterion 6 complete. Evidence saved under ${EVIDENCE_DIR}/c6-*"
