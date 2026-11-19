#!/usr/bin/env bash
# Criterion 5: Frozen/suspended leader (longer than LEASE_TTL_S=15s) loses lease
# to a surviving replica and writes ZERO rows after waking/unpausing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access
agg_ensure_replicas 3

FREEZE_DURATION_S="${FREEZE_DURATION_S:-23}"
POST_UNPAUSE_WAIT_S="${POST_UNPAUSE_WAIT_S:-10}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"

p2_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} FREEZE_DURATION_S=${FREEZE_DURATION_S}"

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

agg_probe_all_leaders "${EVIDENCE_DIR}/c5-leaders-before.json"
FREEZE_DB_TIME="$(crdb_now_utc)"
{
  echo "freeze_db_time=${FREEZE_DB_TIME}"
  echo "leader_target=${OLD_TARGET}"
  echo "leader_instance_id=${OLD_INSTANCE_ID}"
  echo "fencing_token=${OLD_TOKEN}"
  echo "lease_expires_at=${OLD_EXPIRES}"
  echo ""
  echo "=== leader_leases (before freeze) ==="
  crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  echo ""
  echo "=== poller_watermarks (before freeze) ==="
  crdb_sql "SELECT source, watermark, fencing_token, updated_at FROM poller_watermarks ORDER BY source;"
} > "${EVIDENCE_DIR}/c5-state-before.txt"

# Ensure container/node is unpaused if interrupted
UNPAUSED_ON_EXIT=0
c5_cleanup() {
  if [[ "$UNPAUSED_ON_EXIT" == "1" ]]; then
    if [[ "$MODE" == "compose" ]]; then
      docker unpause "$OLD_TARGET" >/dev/null 2>&1 || true
    elif [[ -n "${KIND_NODE:-}" ]]; then
      docker unpause "$KIND_NODE" >/dev/null 2>&1 || true
    fi
  fi
  p2_cleanup
}
trap c5_cleanup EXIT

# 2. Freeze leader > LEASE_TTL_S, observe takeover, then unfreeze
if [[ "$MODE" == "compose" ]]; then
  p2_log "Pausing leader container ${OLD_TARGET} at db_now=${FREEZE_DB_TIME} for ${FREEZE_DURATION_S}s (> LEASE_TTL_S=15s)..."
  docker pause "$OLD_TARGET" >/dev/null
  UNPAUSED_ON_EXIT=1

  sleep "$FREEZE_DURATION_S"

  p2_log "Checking for surviving replica takeover..."
  NEW_LEASE="$(agg_wait_leader 25 "$OLD_INSTANCE_ID" "$OLD_TOKEN" || true)"
  if [[ -z "$NEW_LEASE" ]]; then
    echo "ERROR: no surviving replica took over leadership while ${OLD_TARGET} was paused." >&2
    docker unpause "$OLD_TARGET" >/dev/null 2>&1 || true
    UNPAUSED_ON_EXIT=0
    exit 1
  fi
  TAKEOVER_DB_TIME="$(crdb_now_utc)"
  NEW_HOLDER="$(echo "$NEW_LEASE" | cut -f1)"
  NEW_TOKEN="$(echo "$NEW_LEASE" | cut -f2)"
  p2_log "Takeover confirmed while leader frozen: new_holder=${NEW_HOLDER} new_token=${NEW_TOKEN} takeover_db_time=${TAKEOVER_DB_TIME}"

  {
    echo "takeover_db_time=${TAKEOVER_DB_TIME}"
    echo "=== leader_leases (while leader frozen, after takeover) ==="
    crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  } > "${EVIDENCE_DIR}/c5-state-during-freeze.txt"

  UNPAUSE_DB_TIME="$(crdb_now_utc)"
  p2_log "Unpausing frozen ex-leader ${OLD_TARGET} at db_now=${UNPAUSE_DB_TIME}..."
  docker unpause "$OLD_TARGET" >/dev/null
  UNPAUSED_ON_EXIT=0
else
  K8S_NODE="$(kubectl -n "$NAMESPACE" get pod "$OLD_TARGET" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  p2_log "Leader pod ${OLD_TARGET} is on node '${K8S_NODE:-unknown}'."

  if [[ -n "${KIND_NODE:-}" ]]; then
    p2_log "Pausing kind node ${KIND_NODE} for ${FREEZE_DURATION_S}s..."
    docker pause "$KIND_NODE" >/dev/null
    UNPAUSED_ON_EXIT=1
    sleep "$FREEZE_DURATION_S"
  else
    cat <<EOF

>>> MANUAL ACTION REQUIRED (MODE=k8s) — STEP 1: SUSPEND VM <<<
Leader pod '${OLD_TARGET}' is running on VM/node '${K8S_NODE:-<leader-vm>}'.
Suspend/pause that VM for at least ${FREEZE_DURATION_S}s (> LEASE_TTL_S=15s):
  # VirtualBox:
  VBoxManage controlvm "${K8S_NODE:-<vm-name>}" pause
  # libvirt/KVM:
  virsh suspend "${K8S_NODE:-<vm-name>}"
  # UTM (macOS): click Pause in UTM UI for '${K8S_NODE:-<vm-name>}'

EOF
    if [[ "$NON_INTERACTIVE" != "1" ]]; then
      echo "Press ENTER once the VM has been paused for > 20s..."
      read -r
    else
      p2_log "NON_INTERACTIVE=1: waiting ${FREEZE_DURATION_S}s..."
      sleep "$FREEZE_DURATION_S"
    fi
  fi

  NEW_LEASE="$(agg_wait_leader 45 "$OLD_INSTANCE_ID" "$OLD_TOKEN" || true)"
  if [[ -z "$NEW_LEASE" ]]; then
    echo "ERROR: no surviving replica took over leadership during VM freeze." >&2
    if [[ -n "${KIND_NODE:-}" ]]; then
      docker unpause "$KIND_NODE" >/dev/null 2>&1 || true
      UNPAUSED_ON_EXIT=0
    fi
    exit 1
  fi
  TAKEOVER_DB_TIME="$(crdb_now_utc)"
  NEW_HOLDER="$(echo "$NEW_LEASE" | cut -f1)"
  NEW_TOKEN="$(echo "$NEW_LEASE" | cut -f2)"
  p2_log "Takeover confirmed: new_holder=${NEW_HOLDER} new_token=${NEW_TOKEN} takeover_db_time=${TAKEOVER_DB_TIME}"

  {
    echo "takeover_db_time=${TAKEOVER_DB_TIME}"
    echo "=== leader_leases (during freeze, after takeover) ==="
    crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  } > "${EVIDENCE_DIR}/c5-state-during-freeze.txt"

  UNPAUSE_DB_TIME="$(crdb_now_utc)"
  if [[ -n "${KIND_NODE:-}" ]]; then
    p2_log "Unpausing kind node ${KIND_NODE} at db_now=${UNPAUSE_DB_TIME}..."
    docker unpause "$KIND_NODE" >/dev/null
    UNPAUSED_ON_EXIT=0
  else
    cat <<EOF

>>> MANUAL ACTION REQUIRED (MODE=k8s) — STEP 2: RESUME VM <<<
Resume VM '${K8S_NODE:-<leader-vm>}' now:
  # VirtualBox:
  VBoxManage controlvm "${K8S_NODE:-<vm-name>}" resume
  # libvirt/KVM:
  virsh resume "${K8S_NODE:-<vm-name>}"
  # UTM (macOS): click Resume in UTM UI

EOF
    if [[ "$NON_INTERACTIVE" != "1" ]]; then
      echo "Press ENTER once the VM has resumed..."
      read -r
    fi
  fi
fi

# 3. Wait for woken ex-leader to run renew/poll cycle and log demotion / fenced rejection
p2_log "Waiting ${POST_UNPAUSE_WAIT_S}s for woken ex-leader (${OLD_INSTANCE_ID}) to run renew/poll..."
sleep "$POST_UNPAUSE_WAIT_S"

# 4. Collect logs and DB proof
agg_collect_logs "${EVIDENCE_DIR}/c5-aggregator-logs-raw.log"
grep -E "leader_acquired|leader_lost|lease_released|fencing|write rejected|lease acquire/renew failed" \
  "${EVIDENCE_DIR}/c5-aggregator-logs-raw.log" > "${EVIDENCE_DIR}/c5-leader-fencing.log" || true
grep "$OLD_INSTANCE_ID" "${EVIDENCE_DIR}/c5-leader-fencing.log" > "${EVIDENCE_DIR}/c5-woken-leader.log" || true

agg_probe_all_leaders "${EVIDENCE_DIR}/c5-leaders-after.json"

STALE_AFTER_FREEZE="$(crdb_sql "
  SELECT count(*) AS cnt
  FROM canonical_events
  WHERE fencing_token = ${OLD_TOKEN}
    AND updated_at > '${FREEZE_DB_TIME}'::TIMESTAMPTZ;
" tsv | tail -n 1 | tr -d '\r')"

STALE_AFTER_TAKEOVER="$(crdb_sql "
  SELECT count(*) AS cnt
  FROM canonical_events
  WHERE fencing_token <= ${OLD_TOKEN}
    AND updated_at >= '${TAKEOVER_DB_TIME}'::TIMESTAMPTZ;
" tsv | tail -n 1 | tr -d '\r')"

STALE_AFTER_UNPAUSE="$(crdb_sql "
  SELECT count(*) AS cnt
  FROM canonical_events
  WHERE fencing_token <= ${OLD_TOKEN}
    AND updated_at >= '${UNPAUSE_DB_TIME}'::TIMESTAMPTZ;
" tsv | tail -n 1 | tr -d '\r')"

DUP_TSV="$(crdb_sql "
  SELECT source, source_ref_id, count(*) AS cnt
  FROM canonical_events
  GROUP BY source, source_ref_id
  HAVING count(*) > 1;
" tsv)"

{
  echo "freeze_db_time=${FREEZE_DB_TIME}"
  echo "takeover_db_time=${TAKEOVER_DB_TIME}"
  echo "unpause_db_time=${UNPAUSE_DB_TIME}"
  echo "old_token=${OLD_TOKEN} (${OLD_INSTANCE_ID})"
  echo "new_token=${NEW_TOKEN} (${NEW_HOLDER})"
  echo ""
  echo "=== Stale writes with old token (${OLD_TOKEN}) after freeze_db_time ==="
  echo "count=${STALE_AFTER_FREEZE}"
  echo ""
  echo "=== Stale writes with token <= ${OLD_TOKEN} after takeover_db_time ==="
  echo "count=${STALE_AFTER_TAKEOVER}"
  echo ""
  echo "=== Stale writes with token <= ${OLD_TOKEN} after unpause_db_time ==="
  echo "count=${STALE_AFTER_UNPAUSE}"
  echo ""
  echo "=== Per-token write summary ==="
  crdb_sql "
    SELECT fencing_token,
           written_by,
           count(*) AS event_count,
           min(updated_at) AS min_updated_at,
           max(updated_at) AS max_updated_at
    FROM canonical_events
    WHERE fencing_token IS NOT NULL
    GROUP BY fencing_token, written_by
    ORDER BY fencing_token;
  "
  echo ""
  echo "=== leader_leases (after unpause) ==="
  crdb_sql "SELECT name, holder, fencing_token, expires_at, now() AS db_now FROM leader_leases WHERE name='ingest-poller';"
  echo ""
  echo "=== poller_watermarks (after unpause) ==="
  crdb_sql "SELECT source, watermark, fencing_token, updated_at FROM poller_watermarks ORDER BY source;"
} > "${EVIDENCE_DIR}/c5-state-after.txt"

# 5. Verify assertions
"$PY" - "$OLD_INSTANCE_ID" "$OLD_TOKEN" "$NEW_HOLDER" "$NEW_TOKEN" \
  "$STALE_AFTER_FREEZE" "$STALE_AFTER_TAKEOVER" "$STALE_AFTER_UNPAUSE" \
  "$DUP_TSV" "${EVIDENCE_DIR}/c5-woken-leader.log" "${EVIDENCE_DIR}/c5-leaders-after.json" \
  > "${EVIDENCE_DIR}/c5-verification.txt" <<'PYEOF'
import csv, io, json, sys

(
    old_inst, old_tok, new_holder, new_tok,
    stale_freeze, stale_takeover, stale_unpause,
    dup_tsv, woken_log_path, leaders_after_path
) = sys.argv[1:11]

old_tok = int(old_tok)
new_tok = int(new_tok)
stale_freeze = int(stale_freeze)
stale_takeover = int(stale_takeover)
stale_unpause = int(stale_unpause)

woken_lines = [l.strip() for l in open(woken_log_path, encoding="utf-8", errors="replace") if l.strip()]
leader_lost_seen = any("leader_lost" in l for l in woken_lines)
fenced_reject_seen = any("write rejected: stale fencing token" in l for l in woken_lines)

leaders_after = json.load(open(leaders_after_path, encoding="utf-8"))
active_after = [it["target"] for it in leaders_after if (it.get("leader_response") or {}).get("is_leader") is True]
old_still_leader = any(
    (it.get("leader_response") or {}).get("instance_id") == old_inst
    and (it.get("leader_response") or {}).get("is_leader") is True
    for it in leaders_after
)

dup_rows = list(csv.DictReader(io.StringIO(dup_tsv.strip()), delimiter="\t")) if dup_tsv.strip() else []

print("=== Criterion 5: Frozen Leader Safety & Zero Post-Wake Stale Writes ===")
print(f"Frozen leader                 : {old_inst} (old_token={old_tok})")
print(f"New leader                    : {new_holder} (new_token={new_tok})")
print(f"Stale writes after freeze     : {stale_freeze}")
print(f"Stale writes after takeover   : {stale_takeover}")
print(f"Stale writes after unpause    : {stale_unpause}")
print(f"Woken leader logged leader_lost        : {leader_lost_seen}")
print(f"Woken leader logged stale write reject : {fenced_reject_seen}")
print(f"Active leaders after unpause           : {len(active_after)} ({active_after})")
print(f"Frozen leader still thinks it's leader : {old_still_leader}")
print(f"DB duplicate rows                      : {len(dup_rows)}")

assert new_holder != old_inst, "expected a different replica to take over while old leader was frozen"
assert new_tok > old_tok, f"expected new fencing_token > {old_tok}, got {new_tok}"
assert stale_takeover == 0, f"expected 0 writes with token <= {old_tok} after takeover, got {stale_takeover}"
assert stale_unpause == 0, f"expected 0 writes with token <= {old_tok} after unpause, got {stale_unpause}"
assert not old_still_leader, "woken ex-leader must not report is_leader=true"
assert len(active_after) == 1, f"expected exactly 1 active leader after unpause, got {active_after}"
assert leader_lost_seen or fenced_reject_seen, "expected woken ex-leader to log leader_lost or write rejected: stale fencing token"
assert len(dup_rows) == 0, f"expected 0 DB duplicates, got {dup_rows}"
print("\nASSERTION OK: frozen leader lost lease, surviving replica incremented fencing_token, and woken ex-leader wrote 0 rows.")
PYEOF

cat "${EVIDENCE_DIR}/c5-verification.txt"
p2_log "Criterion 5 complete. Evidence saved under ${EVIDENCE_DIR}/c5-*"
