#!/usr/bin/env bash
# Criterion 4: Worker/store node loss WITH data wipe.
# Records audit checksums before, triggers node loss + data wipe, hammers
# /internal/events and /internal/audit/checksum every 500ms during recovery
# (verifying zero HTTP 5xx and no hangs), waits for up-replication, and
# verifies checksums match before vs after.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access

VICTIM_NODE="${VICTIM_NODE:-canonical-store-3}"
PAUSE_MOCKS="${PAUSE_MOCKS:-1}"
HAMMER_DURATION_S="${HAMMER_DURATION_S:-25}"
WAIT_RECOVERY_S="${WAIT_RECOVERY_S:-150}"
LEAVE_VICTIM_STOPPED="${LEAVE_VICTIM_STOPPED:-1}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-5}"

MOCKS_PAUSED=0
unpause_mocks() {
  if [[ "$MOCKS_PAUSED" -eq 1 && "$MODE" == "compose" ]]; then
    p1_log "Unpausing bmkg-mock and pvmbg-mock..."
    docker compose unpause bmkg-mock pvmbg-mock >/dev/null 2>&1 || true
    MOCKS_PAUSED=0
  fi
}
c4_cleanup() {
  unpause_mocks
  p1_cleanup
}
trap c4_cleanup EXIT

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} VICTIM_NODE=${VICTIM_NODE}"

if [[ "$MODE" == "compose" && "$PAUSE_MOCKS" == "1" ]]; then
  p1_log "Pausing bmkg-mock & pvmbg-mock so live events do not change checksum during node loss..."
  docker compose pause bmkg-mock pvmbg-mock >/dev/null
  MOCKS_PAUSED=1
  sleep 5
fi

capture_checksums() {
  local out_json="$1"
  local tmp_all tmp_bmkg tmp_pvmbg
  tmp_all="$(mktemp)"; tmp_bmkg="$(mktemp)"; tmp_pvmbg="$(mktemp)"
  _P1_TMP_FILES+=("$tmp_all" "$tmp_bmkg" "$tmp_pvmbg")
  agg_get_json "/internal/audit/checksum" "$tmp_all" -H "Authorization: Bearer ${AUDIT_TOKEN}" || { echo "ERROR: checksum failed" >&2; return 1; }
  agg_get_json "/internal/audit/checksum?source=BMKG" "$tmp_bmkg" -H "Authorization: Bearer ${AUDIT_TOKEN}" || { echo "ERROR: checksum?source=BMKG failed" >&2; return 1; }
  agg_get_json "/internal/audit/checksum?source=PVMBG" "$tmp_pvmbg" -H "Authorization: Bearer ${AUDIT_TOKEN}" || { echo "ERROR: checksum?source=PVMBG failed" >&2; return 1; }
  "$PY" - "$tmp_all" "$tmp_bmkg" "$tmp_pvmbg" "$out_json" <<'PYEOF'
import json, sys
out = {
    "all": json.load(open(sys.argv[1], encoding="utf-8")),
    "BMKG": json.load(open(sys.argv[2], encoding="utf-8")),
    "PVMBG": json.load(open(sys.argv[3], encoding="utf-8")),
}
with open(sys.argv[4], "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
PYEOF
}

p1_log "Recording pre-loss audit checksums, distribution, and CRDB node status..."
capture_checksums "${EVIDENCE_DIR}/c4-checksum-before.json"
agg_get_json "/internal/admin/distribution" "${EVIDENCE_DIR}/c4-distribution-before.json" -H "Authorization: Bearer ${AUDIT_TOKEN}" || true
crdb_node_status tsv "--all" > "${EVIDENCE_DIR}/c4-node-status-before.txt" || true

# Trigger node loss + data wipe
if [[ "$MODE" == "compose" ]]; then
  VOL_NAME="${COMPOSE_NAME}_${VICTIM_NODE//-/_}_data"
  p1_log "Killing ${VICTIM_NODE} and wiping volume ${VOL_NAME}..."
  docker compose kill "$VICTIM_NODE" >/dev/null
  docker run --rm --entrypoint /bin/bash -v "${VOL_NAME}:/cockroach/cockroach-data" "$CRDB_IMG" \
    -c "rm -rf /cockroach/cockroach-data/* /cockroach/cockroach-data/.[!.]* 2>/dev/null || true"
  p1_log "${VICTIM_NODE} killed and data wiped."
else
  if kubectl -n tester-agent get ds tester-agent >/dev/null 2>&1; then
    p1_log "Listing Tester Agent practice scenarios..."
    kubectl exec -n tester-agent daemonset/tester-agent -- /app list > "${EVIDENCE_DIR}/c4-tester-agent-list.txt" 2>&1 || true
    cat "${EVIDENCE_DIR}/c4-tester-agent-list.txt"
  fi
  K8S_HOST_NODE="$(kubectl -n "$NAMESPACE" get pod "$VICTIM_NODE" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  p1_log "Target pod ${VICTIM_NODE} is on Kubernetes node '${K8S_HOST_NODE:-unknown}'."
  if [[ -n "${KIND_VICTIM_NODE:-}" ]]; then
    p1_log "Stopping kind node ${KIND_VICTIM_NODE} and wiping hostPath /var/lib/bnpb/canonical-store..."
    docker exec "$KIND_VICTIM_NODE" rm -rf /var/lib/bnpb/canonical-store/"${VICTIM_NODE}" || true
    docker stop "$KIND_VICTIM_NODE" >/dev/null
  elif [[ -n "${TESTER_SCENARIO:-}" ]]; then
    p1_log "Running Tester Agent practice scenario: ${TESTER_SCENARIO}"
    kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$TESTER_SCENARIO"
  else
    echo ""
    echo ">>> MANUAL ACTION REQUIRED (k8s mode) <<<"
    echo "Drain and power off node '${K8S_HOST_NODE:-<worker-node>}' AND wipe /var/lib/bnpb/canonical-store on it:"
    echo "  kubectl drain ${K8S_HOST_NODE:-<worker-node>} --ignore-daemonsets --delete-emptydir-data --force"
    echo "  # Power off VM, then rm -rf /var/lib/bnpb/canonical-store"
    echo "Press ENTER once the node is powered off and its disk wiped..."
    read -r
  fi
fi

# Hammer /internal/events and /internal/audit/checksum every 500ms while recovering
HAMMER_CSV="${EVIDENCE_DIR}/c4-hammer.csv"
echo "timestamp,endpoint,http_code,latency_s,partial,unavailable_units" > "$HAMMER_CSV"
p1_log "Hammering /internal/events and /internal/audit/checksum every 500ms for ${HAMMER_DURATION_S}s..."

HAMMER_TMP="$(mktemp)"
_P1_TMP_FILES+=("$HAMMER_TMP")
END_TS=$(( $(date +%s) + HAMMER_DURATION_S ))
while [[ $(date +%s) -lt $END_TS ]]; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  agg_http_call "GET" "/internal/events?limit=20" "$HAMMER_TMP"
  META_INFO="$("$PY" - "$HAMMER_TMP" <<'PYEOF'
import json, sys
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
    m = d.get("meta", {})
    print(f"{str(bool(m.get('partial', False))).lower()}\t{';'.join(m.get('unavailable_units') or [])}")
except Exception:
    print("parse_err\t")
PYEOF
)"
  echo "${TS},/internal/events,${HTTP_CODE},${TIME_TOTAL_S},${META_INFO%%$'\t'*},${META_INFO#*$'\t'}" >> "$HAMMER_CSV"

  agg_http_call "GET" "/internal/audit/checksum" "$HAMMER_TMP" -H "Authorization: Bearer ${AUDIT_TOKEN}"
  CS_INFO="$("$PY" - "$HAMMER_TMP" <<'PYEOF'
import json, sys
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
    print(f"{str(bool(d.get('partial', False))).lower()}\t{';'.join(d.get('unavailable_units') or [])}")
except Exception:
    print("parse_err\t")
PYEOF
)"
  echo "${TS},/internal/audit/checksum,${HTTP_CODE},${TIME_TOTAL_S},${CS_INFO%%$'\t'*},${CS_INFO#*$'\t'}" >> "$HAMMER_CSV"

  sleep 0.5
done

# Wait for surviving nodes to finish up-replicating so every range has 3 live replicas
p1_log "Waiting up to ${WAIT_RECOVERY_S}s for CockroachDB up-replication to 3 live replicas on surviving nodes..."
RECOVERY_LOG="${EVIDENCE_DIR}/c4-recovery.log"
: > "$RECOVERY_LOG"
REC_END=$(( $(date +%s) + WAIT_RECOVERY_S ))
UNDER_REP="999"
while [[ $(date +%s) -lt $REC_END ]]; do
  LIVE_IDS="$(crdb_live_node_ids | tr ' ' ',')"
  UNDER_REP="$(crdb_under_replicated_count)"

  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) live_nodes=[${LIVE_IDS}] under_replicated_ranges=${UNDER_REP}" | tee -a "$RECOVERY_LOG"
  if [[ "$UNDER_REP" == "0" ]]; then
    break
  fi
  sleep 5
done

if [[ "$MODE" == "compose" && "$LEAVE_VICTIM_STOPPED" == "0" ]]; then
  p1_log "Restarting ${VICTIM_NODE} (LEAVE_VICTIM_STOPPED=0)..."
  docker compose up -d "$VICTIM_NODE" >/dev/null
  sleep 10
fi

p1_log "Recording post-recovery checksums, distribution, and CRDB node status..."
capture_checksums "${EVIDENCE_DIR}/c4-checksum-after.json"
agg_get_json "/internal/admin/distribution" "${EVIDENCE_DIR}/c4-distribution-after.json" -H "Authorization: Bearer ${AUDIT_TOKEN}" || true
crdb_node_status tsv "--all" > "${EVIDENCE_DIR}/c4-node-status-after.txt" || true

"$PY" - "${EVIDENCE_DIR}/c4-checksum-before.json" "${EVIDENCE_DIR}/c4-checksum-after.json" "$HAMMER_CSV" "$UNDER_REP" \
  > "${EVIDENCE_DIR}/c4-verification.txt" <<'PYEOF'
import csv, json, statistics, sys

before = json.load(open(sys.argv[1], encoding="utf-8"))
after = json.load(open(sys.argv[2], encoding="utf-8"))
hammer_csv = sys.argv[3]
under_rep = sys.argv[4]

rows = list(csv.DictReader(open(hammer_csv, encoding="utf-8")))
codes = [int(r["http_code"]) for r in rows]
lats = [float(r["latency_s"]) for r in rows]
five_xx = [c for c in codes if c >= 500 or c == 0]
partial_rows = [r for r in rows if r["partial"] == "true"]

print("=== Criterion 4: Node Loss + Data Wipe Verification ===")
print(f"Hammer requests total : {len(rows)}")
print(f"HTTP 200 responses    : {sum(1 for c in codes if c == 200)}")
print(f"HTTP 5xx / timeout    : {len(five_xx)}")
print(f"Partial=true responses: {len(partial_rows)}")
if lats:
    print(f"Latency (s)           : min={min(lats):.4f} avg={statistics.mean(lats):.4f} max={max(lats):.4f}")
print(f"Under-replicated rngs : {under_rep}")
print("")
print("Checksum Comparison (Before vs After):")
all_match = True
for key in ("all", "BMKG", "PVMBG"):
    b = before[key]
    a = after[key]
    match = (b["count"] == a["count"] and b["checksum"] == a["checksum"] and not a.get("partial", False))
    if not match:
        all_match = False
    print(f"  [{key:<5}] before=(count={b['count']}, sha256={b['checksum'][:16]}...) "
          f"after=(count={a['count']}, sha256={a['checksum'][:16]}..., partial={a.get('partial')}) -> {'MATCH' if match else 'MISMATCH'}")

assert len(rows) > 0, "no hammer requests recorded"
assert len(five_xx) == 0, f"detected {len(five_xx)} HTTP 5xx/timeout errors during node loss"
assert all_match, "checksum mismatch before vs after node loss"
print("\nASSERTION OK: zero 5xx, no hangs, and 100% identical checksums before/after node loss + wipe.")
PYEOF
cat "${EVIDENCE_DIR}/c4-verification.txt"

p1_log "Criterion 4 complete. Evidence saved under ${EVIDENCE_DIR}/c4-*"
