#!/usr/bin/env bash
# Criterion 5: Join a new/empty node while read+write load is running, showing
# ranges and leases rebalancing onto the new node with zero failed requests.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access
setup_bmkg_access

JOIN_NODE="${JOIN_NODE:-canonical-store-3}"
SNAPSHOT_COUNT="${SNAPSHOT_COUNT:-5}"
SNAPSHOT_INTERVAL_S="${SNAPSHOT_INTERVAL_S:-15}"
BURST_COUNT="${BURST_COUNT:-200}"
BURST_DURATION_S="${BURST_DURATION_S:-45}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-5}"

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR} JOIN_NODE=${JOIN_NODE}"

# Start continuous read load in background
LOAD_CSV="${EVIDENCE_DIR}/c5-load-requests.csv"
echo "timestamp,endpoint,http_code,latency_s,partial" > "$LOAD_CSV"

run_read_load() {
  local tmp_f
  tmp_f="$(mktemp)"
  while true; do
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    agg_http_call "GET" "/internal/events?limit=20" "$tmp_f"
    local p1
    p1="$("$PY" -c 'import json,sys;print(str(bool(json.load(open(sys.argv[1])).get("meta",{}).get("partial",False))).lower())' "$tmp_f" 2>/dev/null || echo "false")"
    echo "${ts},/internal/events,${HTTP_CODE},${TIME_TOTAL_S},${p1}" >> "$LOAD_CSV"

    agg_http_call "GET" "/internal/audit/checksum" "$tmp_f" -H "Authorization: Bearer ${AUDIT_TOKEN}"
    local p2
    p2="$("$PY" -c 'import json,sys;print(str(bool(json.load(open(sys.argv[1])).get("partial",False))).lower())' "$tmp_f" 2>/dev/null || echo "false")"
    echo "${ts},/internal/audit/checksum,${HTTP_CODE},${TIME_TOTAL_S},${p2}" >> "$LOAD_CSV"
    sleep 1
  done
  rm -f "$tmp_f"
}

p1_log "Starting continuous read load + triggering sustained BMKG write burst (${BURST_COUNT} events over ${BURST_DURATION_S}s)..."
run_read_load &
READ_PID=$!
_P1_BG_PIDS+=("$READ_PID")

BURST_PAYLOAD="$("$PY" -c 'import json,sys; print(json.dumps({"region_name":"Jawa Barat","count":int(sys.argv[1]),"duration_s":int(sys.argv[2]),"lat":-6.9,"lon":107.6}))' "$BURST_COUNT" "$BURST_DURATION_S")"
curl -fsS -m 10 -X POST -H "Content-Type: application/json" -d "$BURST_PAYLOAD" \
  "${BMKG_BASE}/admin/burst" > "${EVIDENCE_DIR}/c5-burst-response.json" || true

# Join empty/new node
if [[ "$MODE" == "compose" ]]; then
  if [[ -n "$(docker compose ps -q "$JOIN_NODE" 2>/dev/null || true)" ]] && \
     [[ "$(docker inspect -f '{{.State.Running}}' "$(docker compose ps -q "$JOIN_NODE")" 2>/dev/null || echo false)" == "true" ]]; then
    p1_log "${JOIN_NODE} is already running; stopping and wiping its disk so it joins as an empty node..."
    VOL_NAME="${COMPOSE_NAME}_${JOIN_NODE//-/_}_data"
    docker compose kill "$JOIN_NODE" >/dev/null
    docker run --rm --entrypoint /bin/bash -v "${VOL_NAME}:/cockroach/cockroach-data" "$CRDB_IMG" \
      -c "rm -rf /cockroach/cockroach-data/* /cockroach/cockroach-data/.[!.]* 2>/dev/null || true"
  fi
  p1_log "Starting ${JOIN_NODE} with empty disk..."
  docker compose up -d "$JOIN_NODE" >/dev/null
  for _ in $(seq 1 30); do
    if docker compose exec -T "$JOIN_NODE" curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
else
  echo ""
  echo ">>> K3S / K8S NODE JOIN COMMAND (for report & demo) <<<"
  echo "  # On the new/wiped worker VM:"
  echo "  curl -sfL https://get.k3s.io | K3S_URL=https://<CONTROL_PLANE_IP>:6443 K3S_TOKEN=<NODE_TOKEN> sh -"
  echo "  # Or if re-enabling a wiped existing worker node:"
  echo "  kubectl uncordon <worker-node>"
  if [[ -n "${KIND_JOIN_NODE:-}" ]]; then
    p1_log "Starting kind node ${KIND_JOIN_NODE}..."
    docker start "$KIND_JOIN_NODE" >/dev/null
  else
    echo "Press ENTER once the new/empty node has joined and its canonical-store pod is Running..."
    read -r
  fi
fi

# Capture distribution snapshots every 15s
SNAP_TXT="${EVIDENCE_DIR}/c5-rebalance-snapshots.txt"
SNAP_JSONL="${EVIDENCE_DIR}/c5-rebalance-snapshots.jsonl"
: > "$SNAP_TXT"
: > "$SNAP_JSONL"
TMP_DIST="$(mktemp)"
_P1_TMP_FILES+=("$TMP_DIST")

for i in $(seq 1 "$SNAPSHOT_COUNT"); do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  p1_log "Capturing rebalance snapshot ${i}/${SNAPSHOT_COUNT}..."
  if agg_get_json "/internal/admin/distribution" "$TMP_DIST" -H "Authorization: Bearer ${AUDIT_TOKEN}"; then
    "$PY" - "$TMP_DIST" "$TS" "$i" "$SNAP_JSONL" >> "$SNAP_TXT" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
ts, idx, jsonl_path = sys.argv[2], int(sys.argv[3]), sys.argv[4]
with open(jsonl_path, "a", encoding="utf-8") as jf:
    jf.write(json.dumps({"snapshot": idx, "timestamp": ts, "distribution": d}) + "\n")
print(f"--- Snapshot #{idx} @ {ts} (total_rows={d.get('total_rows')}, total_ranges={d.get('total_ranges')}) ---")
for n in d.get("nodes", []):
    print(f"  store={n['store_id']:>2} node={n['node']:<20} replicas={n['replica_count']:>3} leases={n['leaseholder_count']:>3} range_pct={n['percent_of_ranges_on_node']:>6.2f}% logical_bytes={n['store_logical_bytes']}")
PYEOF
  fi
  if [[ "$i" -lt "$SNAPSHOT_COUNT" ]]; then
    sleep "$SNAPSHOT_INTERVAL_S"
  fi
done

kill "$READ_PID" 2>/dev/null || true
wait "$READ_PID" 2>/dev/null || true
crdb_node_status tsv "--all" > "${EVIDENCE_DIR}/c5-node-status-after.txt" || true

"$PY" - "$LOAD_CSV" "$SNAP_JSONL" "$JOIN_NODE" > "${EVIDENCE_DIR}/c5-verification.txt" <<'PYEOF'
import csv, json, sys

load_csv, snap_jsonl, join_node = sys.argv[1], sys.argv[2], sys.argv[3]
rows = list(csv.DictReader(open(load_csv, encoding="utf-8")))
bad = [r for r in rows if r["http_code"] != "200"]
snaps = [json.loads(line) for line in open(snap_jsonl, encoding="utf-8") if line.strip()]
last_nodes = snaps[-1]["distribution"]["nodes"] if snaps else []

# In compose/k8s, newest store_id is the newly joined empty node (or matches join_node).
newest = max(last_nodes, key=lambda n: n["store_id"]) if last_nodes else {}
print("=== Criterion 5: Add / Rejoin Empty Node Under Load ===")
print(f"Total read requests during join : {len(rows)}")
print(f"Failed requests (non-200)       : {len(bad)}")
print(f"Snapshots captured              : {len(snaps)}")
if newest:
    print(f"Newest store                    : store_id={newest['store_id']} node={newest['node']} replicas={newest['replica_count']} leases={newest['leaseholder_count']} range_pct={newest['percent_of_ranges_on_node']}%")

assert len(rows) > 0, "expected read requests recorded"
assert len(bad) == 0, f"expected 0 failed requests, got {len(bad)}"
assert newest and newest.get("replica_count", 0) > 0, f"expected newest node to hold >0 replicas, got {newest}"
print("ASSERTION OK: zero failed requests and ranges rebalanced onto the new/empty node.")
PYEOF
cat "${EVIDENCE_DIR}/c5-verification.txt"

p1_log "Criterion 5 complete. Evidence saved under ${EVIDENCE_DIR}/c5-*"
