#!/usr/bin/env bash
set -euo pipefail

# Sample per-node CockroachDB write/read QPS and range metrics every 2s into a CSV.
# Usage: bash scripts/p1/load-metrics.sh [duration_s] [output_csv]

DURATION_S="${1:-30}"
OUT_CSV="${2:-data/p1-load-metrics.csv}"
INTERVAL_S="${INTERVAL_S:-2}"
CRDB_SERVICE="${CRDB_SERVICE:-canonical-store-0}"
NODES=(canonical-store-0 canonical-store-1 canonical-store-2 canonical-store-3)

mkdir -p "$(dirname "$OUT_CSV")"
echo "timestamp,node,sql_insert_count,sql_select_count,writes_per_sec,range_count,lease_count,logical_bytes" > "$OUT_CSV"

echo "Sampling CRDB per-node metrics every ${INTERVAL_S}s for ${DURATION_S}s -> ${OUT_CSV}"

END_TS=$(( $(date +%s) + DURATION_S ))
while [[ $(date +%s) -lt $END_TS ]]; do
  TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Query kv_store_status once per tick for writes_per_second, range_count, lease_count, logical_bytes.
  KV_ROWS=$(docker compose exec -T "$CRDB_SERVICE" cockroach sql --insecure -d canonical --format=csv -e "
    SELECT COALESCE(g.locality, 'store-' || s.store_id::STRING) AS node,
           round(s.writes_per_second, 2),
           s.range_count,
           s.lease_count,
           s.logical_bytes
    FROM crdb_internal.kv_store_status s
    LEFT JOIN crdb_internal.gossip_nodes g ON g.node_id = s.node_id
    ORDER BY s.store_id;
  " | tail -n +2 | tr -d '\r')

  for node in "${NODES[@]}"; do
    VARS=$(docker compose exec -T "$node" curl -fsS http://localhost:8080/_status/vars 2>/dev/null || true)
    INS=$(echo "$VARS" | awk '/^sql_insert_count\{/ {v=$2} END {print v+0}')
    SEL=$(echo "$VARS" | awk '/^sql_select_count\{/ {v=$2} END {print v+0}')
    INS="${INS:-0}"
    SEL="${SEL:-0}"

    KV_LINE=$(echo "$KV_ROWS" | grep "node=${node}," | head -n 1 || true)
    if [[ -n "$KV_LINE" ]]; then
      WPS=$(echo "$KV_LINE" | cut -d',' -f2)
      RC=$(echo "$KV_LINE" | cut -d',' -f3)
      LC=$(echo "$KV_LINE" | cut -d',' -f4)
      LB=$(echo "$KV_LINE" | cut -d',' -f5)
    else
      WPS="0"
      RC="0"
      LC="0"
      LB="0"
    fi

    echo "${TS},${node},${INS},${SEL},${WPS},${RC},${LC},${LB}" >> "$OUT_CSV"
  done

  sleep "$INTERVAL_S"
done

echo "Saved metrics to ${OUT_CSV}"
