#!/usr/bin/env bash
# Criterion 2: Measure per-node load during skewed catalog load + regional BMKG burst,
# and compare against hypothetical region-keyed partitioning.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

p1_require_audit_token
setup_aggregator_access
setup_bmkg_access

C2_LOAD_ROWS="${C2_LOAD_ROWS:-5000}"
BURST_REGION="${BURST_REGION:-Sulawesi Tengah}"
BURST_COUNT="${BURST_COUNT:-300}"
BURST_DURATION_S="${BURST_DURATION_S:-10}"
BURST_LAT="${BURST_LAT:--0.9}"
BURST_LON="${BURST_LON:-119.87}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-2}"

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"

# Generate a skewed CSV with run-unique IDs so phase (a) executes real inserts across shards.
C2_CSV="/tmp/p1-c2-skew-${RUN_ID}.csv"
_P1_TMP_FILES+=("$C2_CSV")
if [[ "$MODE" == "k8s" && "$C2_LOAD_ROWS" -gt 3000 ]]; then
  C2_LOAD_ROWS=3000
fi
"$PY" scripts/p1/gen-sample-catalog.py --rows "$C2_LOAD_ROWS" --seed 99 --out "$C2_CSV"
"$PY" - "$C2_CSV" "$RUN_ID" <<'PYEOF'
import csv, sys
path, run_id = sys.argv[1], sys.argv[2]
rows = list(csv.reader(open(path, encoding="utf-8")))
for r in rows[1:]:
    r[6] = f"skew-{run_id}-{r[6]}"
with open(path, "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(rows)
PYEOF

LOAD_CSV="${EVIDENCE_DIR}/c2-load-qps.csv"
p1_log "Phase (a): Sampling CRDB metrics every ${SAMPLE_INTERVAL_S}s during ${C2_LOAD_ROWS}-row skewed load..."
sample_crdb_metrics_loop "$LOAD_CSV" "$SAMPLE_INTERVAL_S" 120 &
SAMPLER_PID=$!
_P1_BG_PIDS+=("$SAMPLER_PID")
sleep "$SAMPLE_INTERVAL_S"

if [[ "$MODE" == "compose" ]]; then
  docker compose run --rm --no-deps --entrypoint /loader \
    -v "${C2_CSV}:/data/c2-skew.csv:ro" \
    aggregator \
    --file /data/c2-skew.csv \
    --concurrency "${LOADER_CONCURRENCY:-4}" \
    --batch-size "${LOADER_BATCH_SIZE:-500}" \
    > "${EVIDENCE_DIR}/c2-loader.log" 2>&1
else
  CM_NAME="p1-c2-catalog-${RUN_ID,,}"
  CM_NAME="${CM_NAME//[^a-z0-9-]/-}"
  JOB_NAME="p1-c2-loader-${RUN_ID,,}"
  JOB_NAME="${JOB_NAME//[^a-z0-9-]/-}"
  kubectl -n "$NAMESPACE" delete configmap "$CM_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" create configmap "$CM_NAME" --from-file=catalog.csv="$C2_CSV"
  cat <<YAML | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 1
  template:
    metadata:
      labels:
        app: aggregator
        role: aggregator
    spec:
      restartPolicy: Never
      containers:
        - name: loader
          image: bnpb/aggregator:latest
          imagePullPolicy: IfNotPresent
          command: ["/loader"]
          args: ["--file", "/data/catalog.csv", "--concurrency", "4", "--batch-size", "500"]
          env:
            - name: DB_DSN
              valueFrom:
                secretKeyRef:
                  name: aggregator-secrets
                  key: DB_DSN
          volumeMounts:
            - name: cat
              mountPath: /data
              readOnly: true
      volumes:
        - name: cat
          configMap:
            name: ${CM_NAME}
YAML
  kubectl -n "$NAMESPACE" wait --for=condition=complete "job/${JOB_NAME}" --timeout=180s
  kubectl -n "$NAMESPACE" logs "job/${JOB_NAME}" > "${EVIDENCE_DIR}/c2-loader.log" 2>&1
  kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete configmap "$CM_NAME" --ignore-not-found >/dev/null 2>&1 || true
fi

sleep "$SAMPLE_INTERVAL_S"
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true

BURST_CSV="${EVIDENCE_DIR}/c2-burst-qps.csv"
BURST_WAIT=$((BURST_DURATION_S + 10))
p1_log "Phase (b): Sampling CRDB metrics during regional burst (${BURST_REGION}, count=${BURST_COUNT}, duration=${BURST_DURATION_S}s)..."
sample_crdb_metrics_loop "$BURST_CSV" "$SAMPLE_INTERVAL_S" $((BURST_WAIT + 5)) &
BURST_PID=$!
_P1_BG_PIDS+=("$BURST_PID")
sleep "$SAMPLE_INTERVAL_S"

BURST_PAYLOAD="$("$PY" -c 'import json,sys; print(json.dumps({"region_name":sys.argv[1],"count":int(sys.argv[2]),"duration_s":int(sys.argv[3]),"lat":float(sys.argv[4]),"lon":float(sys.argv[5])}))' \
  "$BURST_REGION" "$BURST_COUNT" "$BURST_DURATION_S" "$BURST_LAT" "$BURST_LON")"

BURST_CODE="$(curl -sS -m 10 -X POST \
  -H "Content-Type: application/json" \
  -d "$BURST_PAYLOAD" \
  -o "${EVIDENCE_DIR}/c2-burst-response.json" \
  -w '%{http_code}' \
  "${BMKG_BASE}/admin/burst" || echo "000")"
p1_log "POST /admin/burst -> HTTP ${BURST_CODE}"
if [[ "$BURST_CODE" != "202" && "$BURST_CODE" != "200" ]]; then
  echo "ERROR: POST ${BMKG_BASE}/admin/burst failed with HTTP ${BURST_CODE}" >&2
  cat "${EVIDENCE_DIR}/c2-burst-response.json" >&2 || true
  exit 1
fi

sleep "$BURST_WAIT"
kill "$BURST_PID" 2>/dev/null || true
wait "$BURST_PID" 2>/dev/null || true

p1_log "Querying per-area and per-shard counts + /internal/admin/distribution..."
crdb_sql "
  SELECT area_name, count(*) AS row_count
  FROM canonical_events
  WHERE deleted_at IS NULL
  GROUP BY area_name
  ORDER BY row_count DESC;
" csv | tr -d '\r' > "${EVIDENCE_DIR}/c2-area-counts.csv"

crdb_sql "
  SELECT shard, count(*) AS row_count
  FROM canonical_events
  WHERE deleted_at IS NULL
  GROUP BY shard
  ORDER BY shard;
" csv | tr -d '\r' > "${EVIDENCE_DIR}/c2-shard-counts.csv"

DIST_JSON="${EVIDENCE_DIR}/c2-distribution.json"
agg_get_json "/internal/admin/distribution" "$DIST_JSON" -H "Authorization: Bearer ${AUDIT_TOKEN}" || true

"$PY" - "$LOAD_CSV" "$BURST_CSV" "${EVIDENCE_DIR}/c2-area-counts.csv" "${EVIDENCE_DIR}/c2-shard-counts.csv" \
  "${EVIDENCE_DIR}/c2-skew-summary.txt" "${EVIDENCE_DIR}/c2-skew-summary.json" <<'PYEOF'
import csv, hashlib, json, statistics, sys

load_csv, burst_csv, area_csv, shard_csv, out_txt, out_json = sys.argv[1:]

def analyze_qps(path):
    by_node = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n = row["node"]
            ins = int(float(row["sql_insert_count"] or 0))
            sel = int(float(row["sql_select_count"] or 0))
            wps = float(row["writes_per_sec"] or 0)
            rc = int(float(row["range_count"] or 0))
            lc = int(float(row["lease_count"] or 0))
            st = by_node.setdefault(n, {"ins": [], "sel": [], "wps": [], "rc": [], "lc": []})
            st["ins"].append(ins)
            st["sel"].append(sel)
            st["wps"].append(wps)
            st["rc"].append(rc)
            st["lc"].append(lc)
    summary = {}
    for n, st in sorted(by_node.items()):
        summary[n] = {
            "insert_delta": max(st["ins"]) - min(st["ins"]) if st["ins"] else 0,
            "select_delta": max(st["sel"]) - min(st["sel"]) if st["sel"] else 0,
            "peak_writes_per_sec": max(st["wps"]) if st["wps"] else 0.0,
            "avg_writes_per_sec": round(statistics.mean(st["wps"]), 2) if st["wps"] else 0.0,
            "end_range_count": st["rc"][-1] if st["rc"] else 0,
            "end_lease_count": st["lc"][-1] if st["lc"] else 0,
        }
    wps_vals = [v["peak_writes_per_sec"] for v in summary.values() if v["peak_writes_per_sec"] > 0]
    ratio = round(max(wps_vals) / min(wps_vals), 2) if len(wps_vals) >= 2 else None
    return summary, ratio

load_stats, load_wps_ratio = analyze_qps(load_csv)
burst_stats, burst_wps_ratio = analyze_qps(burst_csv)

shards = []
with open(shard_csv, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        shards.append((int(r["shard"]), int(r["row_count"])))
shard_counts = [c for _, c in shards]
shard_ratio = round(max(shard_counts) / min(shard_counts), 3) if shard_counts and min(shard_counts) > 0 else None
shard_cv = round(statistics.stdev(shard_counts) / statistics.mean(shard_counts) * 100, 2) if len(shard_counts) > 1 else 0.0

areas = []
with open(area_csv, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        areas.append((r["area_name"], int(r["row_count"])))
total_rows = sum(c for _, c in areas)

node_names = sorted(load_stats.keys()) or [f"node-{i}" for i in range(4)]
num_nodes = len(node_names)
hypo_nodes = {n: 0 for n in node_names}
for area, cnt in areas:
    idx = int.from_bytes(hashlib.sha256(area.encode("utf-8")).digest()[:4], "big") % num_nodes
    hypo_nodes[node_names[idx]] += cnt
hypo_counts = list(hypo_nodes.values())
hypo_nonzero = [c for c in hypo_counts if c > 0]
hypo_ratio = round(max(hypo_counts) / min(hypo_nonzero), 2) if len(hypo_nonzero) >= 2 else float("inf")

report = {
    "total_rows": total_rows,
    "actual_shards": {
        "counts": {f"shard-{s}": c for s, c in shards},
        "max_min_ratio": shard_ratio,
        "cv_percent": shard_cv,
    },
    "phase_a_load_nodes": load_stats,
    "phase_a_peak_wps_max_min_ratio": load_wps_ratio,
    "phase_b_burst_nodes": burst_stats,
    "phase_b_peak_wps_max_min_ratio": burst_wps_ratio,
    "hypothetical_region_partition": {
        "nodes": {n: {"rows": c, "pct": round(c * 100.0 / total_rows, 2) if total_rows else 0.0} for n, c in hypo_nodes.items()},
        "max_min_ratio": hypo_ratio,
    },
    "top_areas": [{"area_name": a, "rows": c, "pct": round(c * 100.0 / total_rows, 2) if total_rows else 0.0} for a, c in areas[:10]],
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

lines = []
lines.append("=== Criterion 2: Skew Handling & Load Distribution Summary ===")
lines.append(f"Total active rows: {total_rows}")
lines.append("")
lines.append("1. Actual Hash-Partitioned Shard Distribution (shard-0..7):")
for s, c in shards:
    pct = (c * 100.0 / total_rows) if total_rows else 0.0
    lines.append(f"   shard-{s}: {c:>8} rows ({pct:5.2f}%)")
lines.append(f"   Shard max/min ratio: {shard_ratio}  |  CV: {shard_cv}%")
lines.append("")
lines.append("2. Phase (a) Skewed Catalog Load - Per-Node CRDB Metrics:")
for n, st in load_stats.items():
    lines.append(f"   {n:<22} insert_delta={st['insert_delta']:<7} peak_wps={st['peak_writes_per_sec']:<8} avg_wps={st['avg_writes_per_sec']:<8} ranges={st['end_range_count']} leases={st['end_lease_count']}")
lines.append(f"   Peak writes/s max/min ratio (load): {load_wps_ratio}")
lines.append("")
lines.append("3. Phase (b) Regional BMKG Burst - Per-Node CRDB Metrics:")
for n, st in burst_stats.items():
    lines.append(f"   {n:<22} insert_delta={st['insert_delta']:<7} peak_wps={st['peak_writes_per_sec']:<8} avg_wps={st['avg_writes_per_sec']:<8} ranges={st['end_range_count']} leases={st['end_lease_count']}")
lines.append(f"   Peak writes/s max/min ratio (burst): {burst_wps_ratio}")
lines.append("")
lines.append("4. Hypothetical Region-Keyed Partitioning (hash(area_name) % N):")
for n, info in report["hypothetical_region_partition"]["nodes"].items():
    lines.append(f"   {n:<22} {info['rows']:>8} rows ({info['pct']:5.2f}%)")
lines.append(f"   Hypothetical max/min ratio: {hypo_ratio}")
lines.append("")
lines.append("5. Top Regions in Dataset:")
for item in report["top_areas"]:
    lines.append(f"   {item['area_name']:<24} {item['rows']:>8} rows ({item['pct']:5.2f}%)")

txt = "\n".join(lines) + "\n"
with open(out_txt, "w", encoding="utf-8") as f:
    f.write(txt)
print(txt)
PYEOF

p1_log "Criterion 2 complete. Evidence saved under ${EVIDENCE_DIR}/c2-*"
