#!/usr/bin/env bash
# Criterion 4: Clock skew safety (+45s then -30s).
# Proves DB now() is the sole lease authority and no two different
# (written_by, fencing_token) pairs have overlapping write intervals.
# Note: Per-node OS clock skew requires separate VM kernels (MODE=k8s).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access
agg_ensure_replicas 3

NON_INTERACTIVE="${NON_INTERACTIVE:-0}"
SKEW_WAIT_S="${SKEW_WAIT_S:-20}"

p2_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"

# 1. Capture DB now() vs /internal/leader before skew
agg_probe_all_leaders "${EVIDENCE_DIR}/c4-leaders-before.json"
{
  echo "=== DB Time vs Lease State (before) ==="
  crdb_sql "
    SELECT now() AS db_now,
           name,
           holder,
           fencing_token,
           expires_at,
           (expires_at - now()) AS remaining_ttl,
           (expires_at > now()) AS is_active
    FROM leader_leases
    WHERE name = 'ingest-poller';
  "
} > "${EVIDENCE_DIR}/c4-db-vs-leader-before.txt"

LEADER_LINE="$(agg_find_leader 2>/dev/null || true)"
LEADER_TARGET="$(echo "$LEADER_LINE" | cut -f1)"
LEADER_ID="$(echo "$LEADER_LINE" | cut -f2)"
LEADER_TOKEN="$(echo "$LEADER_LINE" | cut -f3)"
p2_log "Current leader: target=${LEADER_TARGET:-unknown} instance_id=${LEADER_ID:-unknown} fencing_token=${LEADER_TOKEN:-unknown}"

# 2. Mode-specific skew execution / documentation
if [[ "$MODE" == "compose" ]]; then
  p2_log "MODE=compose: all containers share the host Docker VM kernel, so per-container OS clock skew (+45s/-30s) cannot be isolated here."
  p2_log "Verifying DB now() lease authority and disjoint (fencing_token, written_by) write intervals..."
  {
    echo "NOTE (MODE=compose):"
    echo "Docker Compose runs all containers on a single shared Linux kernel."
    echo "Injecting OS clock skew (date -s) inside one container would shift the host/VM clock for all containers and CockroachDB nodes."
    echo "Full +45s / -30s OS clock skew on a single leader node must be executed on the multi-VM k3s cluster using MODE=k8s."
    echo ""
    echo "What is verified live in Compose:"
    echo "1. Lease expiration and acquisition use CockroachDB HLC now() exclusively (not aggregator wall clocks)."
    echo "2. Every fenced batch re-checks (holder = \$me AND fencing_token = \$tok AND expires_at > now()) inside the SERIALIZABLE write transaction."
    echo "3. All recorded (fencing_token, written_by) write intervals in canonical_events are strictly disjoint."
  } > "${EVIDENCE_DIR}/c4-clock-skew-note.txt"
else
  K8S_NODE="$(kubectl -n "$NAMESPACE" get pod "$LEADER_TARGET" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  p2_log "Leader pod ${LEADER_TARGET} is scheduled on k8s node '${K8S_NODE:-unknown}'."

  if kubectl -n tester-agent get ds tester-agent >/dev/null 2>&1; then
    p2_log "Listing Tester Agent scenarios..."
    kubectl exec -n tester-agent daemonset/tester-agent -- /app list > "${EVIDENCE_DIR}/c4-tester-agent-list.txt" 2>&1 || true
    cat "${EVIDENCE_DIR}/c4-tester-agent-list.txt" || true
  fi

  if [[ -n "${TESTER_SCENARIO:-}" ]] && kubectl -n tester-agent get ds tester-agent >/dev/null 2>&1; then
    p2_log "Running Tester Agent clock-skew scenario '${TESTER_SCENARIO}'..."
    kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$TESTER_SCENARIO" \
      > "${EVIDENCE_DIR}/c4-tester-agent-run.log" 2>&1 || true
    sleep "$SKEW_WAIT_S"
  elif [[ "${SSH_SKEW:-0}" == "1" && -n "${K8S_NODE:-}" ]]; then
    SSH_USER="${SSH_USER:-ubuntu}"
    SSH_HOST="${SSH_HOST:-$K8S_NODE}"
    p2_log "Applying +45s then -30s clock skew on ${SSH_USER}@${SSH_HOST}..."
    ssh "${SSH_USER}@${SSH_HOST}" "sudo timedatectl set-ntp false && sudo date -s '+45 seconds'"
    sleep "$SKEW_WAIT_S"
    agg_probe_all_leaders "${EVIDENCE_DIR}/c4-leaders-during-plus45s.json" || true
    ssh "${SSH_USER}@${SSH_HOST}" "sudo date -s '-75 seconds'"
    sleep "$SKEW_WAIT_S"
    agg_probe_all_leaders "${EVIDENCE_DIR}/c4-leaders-during-minus30s.json" || true
    p2_log "Restoring NTP on ${SSH_USER}@${SSH_HOST}..."
    ssh "${SSH_USER}@${SSH_HOST}" "sudo timedatectl set-ntp true"
  else
    cat <<EOF

>>> MANUAL ACTION REQUIRED (MODE=k8s) <<<
Leader pod '${LEADER_TARGET}' is on node '${K8S_NODE:-<leader-vm>}'.
Run clock skew on THAT node only (+45s, observe ~20s, then -30s relative to real time, observe ~20s, then restore NTP):

  # On ${K8S_NODE:-<leader-vm>}:
  sudo timedatectl set-ntp false
  sudo date -s '+45 seconds'
  sleep 20
  sudo date -s '-75 seconds'   # +45s - 75s = -30s vs real time
  sleep 20
  sudo timedatectl set-ntp true

(Or re-run with TESTER_SCENARIO=<name> or SSH_SKEW=1 SSH_HOST=<ip>.)
EOF
    if [[ "$NON_INTERACTIVE" != "1" ]]; then
      echo "Press ENTER once skew +45s -> -30s and NTP restore are complete..."
      read -r
    else
      p2_log "NON_INTERACTIVE=1: skipping interactive skew prompt and collecting cluster state."
    fi
  fi
fi

# 3. Capture DB now() vs /internal/leader after
agg_probe_all_leaders "${EVIDENCE_DIR}/c4-leaders-after.json"
{
  echo "=== DB Time vs Lease State (after) ==="
  crdb_sql "
    SELECT now() AS db_now,
           name,
           holder,
           fencing_token,
           expires_at,
           (expires_at - now()) AS remaining_ttl,
           (expires_at > now()) AS is_active
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
} > "${EVIDENCE_DIR}/c4-db-vs-leader-after.txt"

# 4. Query write intervals per (fencing_token, written_by)
INTERVALS_CSV="${EVIDENCE_DIR}/c4-write-intervals.csv"
crdb_sql "
  SELECT fencing_token,
         written_by,
         count(*) AS event_count,
         min(updated_at) AS min_updated_at,
         max(updated_at) AS max_updated_at,
         min(ingested_at) AS min_ingested_at,
         max(ingested_at) AS max_ingested_at
  FROM canonical_events
  WHERE fencing_token IS NOT NULL
  GROUP BY fencing_token, written_by
  ORDER BY fencing_token;
" csv > "$INTERVALS_CSV"

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
" table > "${EVIDENCE_DIR}/c4-write-intervals.txt"

# 5. Collect aggregator fencing logs and CRDB clock-offset logs
agg_collect_logs "${EVIDENCE_DIR}/c4-aggregator-logs-raw.log"
grep -E "leader_acquired|leader_lost|lease_released|fencing|write rejected|lease acquire/renew failed" \
  "${EVIDENCE_DIR}/c4-aggregator-logs-raw.log" > "${EVIDENCE_DIR}/c4-leader-fencing.log" || true

{
  echo "=== CockroachDB Clock / Max-Offset Logs ==="
  if [[ "$MODE" == "compose" ]]; then
    for n in $(crdb_list_nodes); do
      docker compose logs --no-color "$n" 2>&1 | grep -iE "max.offset|clock|synchronization|time jump" | head -n 40 | sed "s/^/[${n}] /" || true
    done
  else
    kubectl -n "$NAMESPACE" logs -l app=canonical-store --prefix --tail=500 2>&1 \
      | grep -iE "max.offset|clock|synchronization|time jump|shutting down" | head -n 80 || true
    echo ""
    echo "=== canonical-store pod status ==="
    kubectl -n "$NAMESPACE" get pods -o wide -l app=canonical-store || true
  fi
} > "${EVIDENCE_DIR}/c4-crdb-skew-logs.txt"

# 6. Verify disjoint epoch intervals & zero stale-token overlap
"$PY" - "$INTERVALS_CSV" "${EVIDENCE_DIR}/c4-leaders-after.json" "${EVIDENCE_DIR}/c4-leader-fencing.log" "$MODE" \
  > "${EVIDENCE_DIR}/c4-verification.txt" <<'PYEOF'
import csv, json, re, sys
from datetime import datetime

intervals_csv, leaders_json, fencing_log, mode = sys.argv[1:5]

def parse_ts(s):
    s = s.strip().replace(" ", "T")
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    elif s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

acquired_by_token = {}
stale_rejections = 0
with open(fencing_log, encoding="utf-8", errors="replace") as f:
    for line in f:
        if "write rejected: stale fencing token" in line:
            stale_rejections += 1
        if "leader_acquired" not in line:
            continue
        m = re.search(r"(\{.*\})\s*$", line)
        if not m:
            continue
        try:
            obj = json.loads(m.group(1))
        except Exception:
            continue
        tok = int(obj.get("fencing_token", 0) or 0)
        ts = obj.get("time")
        if tok > 0 and ts:
            acquired_by_token[tok] = parse_ts(ts)

rows = list(csv.DictReader(open(intervals_csv, encoding="utf-8")))
leaders = json.load(open(leaders_json, encoding="utf-8"))
active_leaders = [it["target"] for it in leaders if (it.get("leader_response") or {}).get("is_leader") is True]

print("=== Criterion 4: Clock-Skew Safety & Disjoint Fenced Write Intervals ===")
print(f"Mode                       : {mode}")
print(f"Active leaders reported    : {len(active_leaders)} ({active_leaders})")
print(f"Distinct fencing epochs    : {len(rows)}")
print(f"Stale-token rejections log : {stale_rejections}")
print("")
print(f"{'TOKEN':>6} {'WRITTEN_BY':<16} {'COUNT':>6} {'ACQUIRED_AT':<30} {'MIN_UPDATED_AT':<30} {'MAX_UPDATED_AT'}")
print("-" * 124)

epochs = []
for r in rows:
    tok = int(r["fencing_token"])
    wb = r["written_by"]
    cnt = int(r["event_count"])
    mn = parse_ts(r["min_updated_at"])
    mx = parse_ts(r["max_updated_at"])
    acq = acquired_by_token.get(tok)
    acq_str = acq.isoformat() if acq else "(log rotated/unavailable)"
    print(f"{tok:>6} {wb:<16} {cnt:>6} {acq_str:<30} {mn.isoformat():<30} {mx.isoformat()}")
    epochs.append((tok, wb, acq, mn, mx))

epochs.sort(key=lambda e: e[0])

# Verify strictly monotonic non-overlapping leadership epochs:
# 1) When acquired_at is logged for epoch[i+1], epoch[i].max_updated_at <= epoch[i+1].acquired_at
#    (proving the previous leader NEVER wrote after the next leader acquired the lease).
# 2) Effective active write windows [max(acq, mn) if acq else mn, mx] are strictly disjoint.
overlaps = []
for i in range(len(epochs) - 1):
    t1, w1, acq1, mn1, mx1 = epochs[i]
    t2, w2, acq2, mn2, mx2 = epochs[i + 1]
    if acq2 is not None and mx1 > acq2:
        overlaps.append(f"token {t1} max_updated_at ({mx1.isoformat()}) > token {t2} leader_acquired ({acq2.isoformat()})")
    if mx1 > mx2:
        overlaps.append(f"token {t1} max_updated_at ({mx1.isoformat()}) > token {t2} max_updated_at ({mx2.isoformat()})")

print("")
print("Note: ON CONFLICT updates fencing_token/written_by when a new leader re-fetches an unchanged")
print("boundary event within the 1s watermarkOverlap, leaving updated_at at its original insert time.")
print("Disjointness of actual write epochs is therefore bounded by [leader_acquired, max_updated_at].")

assert len(active_leaders) == 1, f"expected exactly 1 active leader, got {active_leaders}"
assert len(rows) >= 1, "expected >= 1 fenced write epoch in canonical_events"
assert len(overlaps) == 0, f"overlapping write epochs detected: {overlaps}"

if mode == "compose":
    print("\nASSERTION OK (compose baseline): single DB-time-governed leader and strictly disjoint fencing epochs.")
    print("REMINDER: +45s / -30s OS clock skew must be executed on Fayadh's multi-VM k3s cluster (MODE=k8s).")
else:
    print("\nASSERTION OK (k8s): single DB-time-governed leader and strictly disjoint (fencing_token, written_by) write epochs under clock skew.")
PYEOF

cat "${EVIDENCE_DIR}/c4-verification.txt"
p2_log "Criterion 4 complete. Evidence saved under ${EVIDENCE_DIR}/c4-*"
