#!/usr/bin/env bash
set -euo pipefail

# Load .env if present for AGGREGATOR_AUDIT_TOKEN
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
TOKEN="${AUDIT_TOKEN:-${AGGREGATOR_AUDIT_TOKEN:-}}"
CRDB_SERVICE="${CRDB_SERVICE:-canonical-store-0}"
DOCKER_NET="${DOCKER_NET:-if4031-tubes-imade_internal_api_net}"

fetch_distribution_json() {
  if [[ -z "$TOKEN" ]]; then
    echo "WARN: AUDIT_TOKEN / AGGREGATOR_AUDIT_TOKEN is empty; skipping HTTP endpoint call." >&2
    return 1
  fi
  if curl -fsS --max-time 5 -H "Authorization: Bearer ${TOKEN}" "${AGGREGATOR_URL}/internal/admin/distribution" 2>/dev/null; then
    return 0
  fi
  # Fallback for Docker Compose where aggregator:8090 is only on internal_api_net.
  if docker network inspect "${DOCKER_NET}" >/dev/null 2>&1; then
    docker run --rm --network "${DOCKER_NET}" curlimages/curl:8.10.1 \
      -fsS --max-time 5 -H "Authorization: Bearer ${TOKEN}" "http://aggregator:8090/internal/admin/distribution"
    return $?
  fi
  return 1
}

echo "=== Aggregator Distribution Endpoint (GET /internal/admin/distribution) ==="
if JSON_OUT=$(fetch_distribution_json); then
  python3 - "$JSON_OUT" <<'PY'
import json, sys

data = json.loads(sys.argv[1])
print(f"Total Rows: {data['total_rows']}  |  Total Ranges: {data['total_ranges']}\n")

print("Per-Shard Distribution:")
print(f"{'UNIT':<10} {'ROWS':>10} {'LEASEHOLDER':<20} {'REPLICAS'}")
print("-" * 72)
for s in data.get("shards", []):
    reps = ", ".join(s.get("replicas", []))
    print(f"{s['unit']:<10} {s['row_count']:>10} {s.get('leaseholder', ''):<20} {reps}")

print("\nPer-Node / Store Summary:")
print(f"{'NODE':<20} {'STORE':>5} {'REPLICAS':>8} {'LEASES':>7} {'RANGE_%':>8} {'APPROX_BYTES':>14} {'LOGICAL_BYTES':>14}")
print("-" * 82)
for n in data.get("nodes", []):
    print(
        f"{n['node']:<20} {n['store_id']:>5} {n['replica_count']:>8} "
        f"{n['leaseholder_count']:>7} {n['percent_of_ranges_on_node']:>7.2f}% "
        f"{n['approximate_bytes']:>14} {n['store_logical_bytes']:>14}"
    )
PY
else
  echo "Could not reach /internal/admin/distribution (set AGGREGATOR_URL and AUDIT_TOKEN, or ensure aggregator is running)."
fi

echo ""
echo "=== Equivalent Raw SQL (CockroachDB: ${CRDB_SERVICE}) ==="
docker compose exec -T "${CRDB_SERVICE}" cockroach sql --insecure -d canonical -e "
SELECT shard, count(*) AS row_count
FROM canonical_events
WHERE deleted_at IS NULL
GROUP BY shard
ORDER BY shard;

SELECT s.store_id, s.node_id, COALESCE(g.locality, '') AS locality,
       s.range_count, s.lease_count, s.logical_bytes, s.writes_per_second
FROM crdb_internal.kv_store_status s
LEFT JOIN crdb_internal.gossip_nodes g ON g.node_id = s.node_id
ORDER BY s.store_id;
"
