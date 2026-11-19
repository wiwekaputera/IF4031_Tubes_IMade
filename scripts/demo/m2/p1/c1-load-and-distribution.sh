#!/usr/bin/env bash
# Criterion 1: Load historical earthquake catalog and report per-shard / per-node data distribution.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access

CATALOG_ROWS="${CATALOG_ROWS:-50000}"
CATALOG_FILE="${CATALOG_FILE:-/tmp/p1-sample-catalog-${CATALOG_ROWS}.csv}"
LOADER_CONCURRENCY="${LOADER_CONCURRENCY:-4}"
LOADER_BATCH_SIZE="${LOADER_BATCH_SIZE:-500}"
VERIFY_IDEMPOTENT="${VERIFY_IDEMPOTENT:-1}"

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"

if [[ ! -f "$CATALOG_FILE" ]]; then
  p1_log "Generating skewed ${CATALOG_ROWS}-row historical catalog -> ${CATALOG_FILE}"
  "$PY" scripts/p1/gen-sample-catalog.py --rows "$CATALOG_ROWS" --out "$CATALOG_FILE"
fi
ABS_CATALOG="$(cd "$(dirname "$CATALOG_FILE")" && pwd)/$(basename "$CATALOG_FILE")"

if [[ "$MODE" == "compose" ]]; then
  p1_log "Running /loader (pass 1) in compose..."
  docker compose run --rm --no-deps --entrypoint /loader \
    -v "${ABS_CATALOG}:/data/catalog.csv:ro" \
    aggregator \
    --file /data/catalog.csv \
    --concurrency "$LOADER_CONCURRENCY" \
    --batch-size "$LOADER_BATCH_SIZE" \
    2>&1 | tee "${EVIDENCE_DIR}/c1-loader.log"

  if [[ "$VERIFY_IDEMPOTENT" == "1" ]]; then
    p1_log "Running /loader (pass 2, idempotency check)..."
    docker compose run --rm --no-deps --entrypoint /loader \
      -v "${ABS_CATALOG}:/data/catalog.csv:ro" \
      aggregator \
      --file /data/catalog.csv \
      --concurrency "$LOADER_CONCURRENCY" \
      --batch-size "$LOADER_BATCH_SIZE" \
      2>&1 | tee "${EVIDENCE_DIR}/c1-loader-idempotent.log"
  fi
else
  p1_log "Running Kubernetes loader job in namespace ${NAMESPACE}..."
  p1_log "Ensure ${CATALOG_FILE} is available at /var/lib/bnpb/catalog/catalog.csv on the scheduled node (or customize deploy/k8s/jobs/aggregator-loader.yaml)."
  kubectl -n "$NAMESPACE" delete job aggregator-loader --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -f deploy/k8s/jobs/aggregator-loader.yaml
  kubectl -n "$NAMESPACE" wait --for=condition=complete job/aggregator-loader --timeout=300s
  kubectl -n "$NAMESPACE" logs job/aggregator-loader | tee "${EVIDENCE_DIR}/c1-loader.log"
fi

p1_log "Fetching GET /internal/admin/distribution..."
DIST_JSON="${EVIDENCE_DIR}/c1-distribution.json"
if ! agg_get_json "/internal/admin/distribution" "$DIST_JSON" -H "Authorization: Bearer ${AUDIT_TOKEN}"; then
  echo "ERROR: GET /internal/admin/distribution failed with HTTP ${HTTP_CODE}" >&2
  exit 1
fi

"$PY" - "$DIST_JSON" > "${EVIDENCE_DIR}/c1-distribution.txt" <<'PYEOF'
import json, sys

with open(sys.argv[1], encoding="utf-8") as f:
    data = json.load(f)

shards = data.get("shards", [])
nodes = data.get("nodes", [])
print(f"Total Rows: {data.get('total_rows', 0)}  |  Total Ranges: {data.get('total_ranges', 0)}\n")
print("Per-Shard Distribution:")
print(f"{'UNIT':<10} {'ROWS':>10} {'LEASEHOLDER':<22} {'REPLICAS'}")
print("-" * 78)
for s in shards:
    reps = ", ".join(s.get("replicas") or [])
    print(f"{s['unit']:<10} {s['row_count']:>10} {s.get('leaseholder', ''):<22} {reps}")

print("\nPer-Node / Store Summary:")
print(f"{'NODE':<22} {'STORE':>5} {'REPLICAS':>8} {'LEASES':>7} {'RANGE_%':>8} {'APPROX_BYTES':>14} {'LOGICAL_BYTES':>14}")
print("-" * 84)
for n in nodes:
    print(
        f"{n['node']:<22} {n['store_id']:>5} {n['replica_count']:>8} "
        f"{n['leaseholder_count']:>7} {n['percent_of_ranges_on_node']:>7.2f}% "
        f"{n['approximate_bytes']:>14} {n['store_logical_bytes']:>14}"
    )

assert data.get("total_rows", 0) > 0, "expected total_rows > 0"
non_empty_shards = sum(1 for s in shards if s.get("row_count", 0) > 0)
assert non_empty_shards == 8, f"expected all 8 shards populated, got {non_empty_shards}"
if len(nodes) >= 4:
    max_pct = max(float(n.get("percent_of_ranges_on_node", 0.0)) for n in nodes)
    assert max_pct < 100.0, f"expected no node holding 100% of ranges, got {max_pct}%"
    print(f"\nASSERTION OK: all 8 shards populated; max node range share = {max_pct:.2f}% (< 100%)")
PYEOF
cat "${EVIDENCE_DIR}/c1-distribution.txt"

p1_log "Capturing raw CockroachDB SHOW RANGES and kv_store_status..."
{
  echo "=== Per-Shard Row Counts ==="
  crdb_sql "
    SELECT shard, count(*) AS row_count
    FROM canonical_events
    WHERE deleted_at IS NULL
    GROUP BY shard
    ORDER BY shard;
  "
  echo ""
  echo "=== Store Status ==="
  crdb_sql "
    SELECT s.store_id, s.node_id, COALESCE(g.locality, '') AS locality,
           s.range_count, s.lease_count, s.logical_bytes, s.writes_per_second
    FROM crdb_internal.kv_store_status s
    LEFT JOIN crdb_internal.gossip_nodes g ON g.node_id = s.node_id
    ORDER BY s.store_id;
  "
  echo ""
  echo "=== SHOW RANGES FROM TABLE canonical_events WITH DETAILS ==="
  crdb_sql "SHOW RANGES FROM TABLE canonical_events WITH DETAILS;"
} > "${EVIDENCE_DIR}/c1-show-ranges.txt"

p1_log "Criterion 1 complete. Evidence saved under ${EVIDENCE_DIR}/c1-*"
