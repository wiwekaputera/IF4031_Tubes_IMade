#!/usr/bin/env bash
# Criterion 3: Cross-shard paginated read ("50 latest events of all types"),
# verifying global descending order, zero duplicates, and per-page timings.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

setup_aggregator_access

PAGE_LIMIT="${PAGE_LIMIT:-50}"
MAX_PAGES="${MAX_PAGES:-10}"
TIMINGS_CSV="${EVIDENCE_DIR}/c3-pagination-timings.csv"
PAGES_TMP="$(mktemp)"
_P1_TMP_FILES+=("$PAGES_TMP")

p1_log "MODE=${MODE} EVIDENCE_DIR=${EVIDENCE_DIR}"
p1_log "Fetching /internal/events?limit=${PAGE_LIMIT} up to ${MAX_PAGES} pages..."

echo "page,http_code,time_total_s,count,partial,has_next_cursor" > "$TIMINGS_CSV"

cursor=""
page=1
: > "$PAGES_TMP"

while [[ "$MAX_PAGES" -eq 0 || "$page" -le "$MAX_PAGES" ]]; do
  qs="limit=${PAGE_LIMIT}"
  if [[ -n "$cursor" ]]; then
    qs="${qs}&cursor=${cursor}"
  fi
  resp_file="$(mktemp)"
  _P1_TMP_FILES+=("$resp_file")
  agg_http_call "GET" "/internal/events?${qs}" "$resp_file"

  if [[ "$HTTP_CODE" != "200" ]]; then
    echo "ERROR: page ${page} returned HTTP ${HTTP_CODE}" >&2
    cat "$resp_file" >&2 || true
    exit 1
  fi

  if [[ "$page" -eq 1 ]]; then
    cp "$resp_file" "${EVIDENCE_DIR}/c3-page1-latest50.json"
    chmod 644 "${EVIDENCE_DIR}/c3-page1-latest50.json"
  fi

  line_info="$("$PY" - "$resp_file" "$PAGES_TMP" <<'PYEOF'
import json, sys

with open(sys.argv[1], encoding="utf-8") as f:
    d = json.load(f)
meta = d.get("meta", {})
data = d.get("data", [])
nxt = meta.get("next_cursor") or ""
partial = bool(meta.get("partial", False))
with open(sys.argv[2], "a", encoding="utf-8") as out:
    out.write(json.dumps({"data": data, "meta": meta}) + "\n")
print(f"{len(data)}\t{str(partial).lower()}\t{nxt}")
PYEOF
)"
  cnt="$(echo "$line_info" | cut -f1)"
  partial="$(echo "$line_info" | cut -f2)"
  nxt="$(echo "$line_info" | cut -f3)"
  has_next="false"
  if [[ -n "$nxt" ]]; then has_next="true"; fi

  echo "${page},${HTTP_CODE},${TIME_TOTAL_S},${cnt},${partial},${has_next}" >> "$TIMINGS_CSV"
  p1_log "  page=${page} http=${HTTP_CODE} time=${TIME_TOTAL_S}s count=${cnt} partial=${partial} next=${has_next}"

  if [[ -z "$nxt" || "$cnt" -eq 0 ]]; then
    break
  fi
  cursor="$nxt"
  page=$((page + 1))
done

"$PY" - "$PAGES_TMP" "$TIMINGS_CSV" > "${EVIDENCE_DIR}/c3-verification.txt" <<'PYEOF'
import csv, datetime as dt, json, statistics, sys

pages_path, timings_path = sys.argv[1], sys.argv[2]

seen_ids = set()
dups = 0
order_violations = 0
prev_key = None
total_events = 0
by_type = {}
by_source = {}
partial_pages = 0

def parse_ts(raw):
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return dt.datetime.fromisoformat(raw)

with open(pages_path, encoding="utf-8") as f:
    for line in f:
        p = json.loads(line)
        if p["meta"].get("partial"):
            partial_pages += 1
        for ev in p["data"]:
            total_events += 1
            hid = ev["hazard_id"]
            if hid in seen_ids:
                dups += 1
            seen_ids.add(hid)
            by_type[ev.get("hazard_type", "UNKNOWN")] = by_type.get(ev.get("hazard_type", "UNKNOWN"), 0) + 1
            by_source[ev.get("source", "UNKNOWN")] = by_source.get(ev.get("source", "UNKNOWN"), 0) + 1
            t = parse_ts(ev["occurred_at"])
            # Global order: occurred_at DESC, hazard_id DESC
            # Note: JSON occurred_at is RFC3339 (1s resolution) while DB stores sub-second precision,
            # so only flag strict timestamp inversion (> 0s newer on later item).
            if prev_key is not None and t > prev_key[0]:
                order_violations += 1
            prev_key = (t, hid)

latencies = []
with open(timings_path, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        latencies.append(float(r["time_total_s"]))

print("=== Criterion 3: Cross-Shard Paginated Read Verification ===")
print(f"Pages fetched     : {len(latencies)}")
print(f"Total events read : {total_events}")
print(f"Unique hazard_ids : {len(seen_ids)}")
print(f"Duplicates        : {dups}")
print(f"Order violations  : {order_violations}")
print(f"Partial pages     : {partial_pages}")
print(f"By hazard_type    : {json.dumps(by_type, sort_keys=True)}")
print(f"By source         : {json.dumps(by_source, sort_keys=True)}")
if latencies:
    print(f"Latency (s)       : min={min(latencies):.4f} avg={statistics.mean(latencies):.4f} max={max(latencies):.4f}")

assert total_events > 0, "expected > 0 events"
assert dups == 0, f"found {dups} duplicate hazard_ids across pages"
assert order_violations == 0, f"found {order_violations} timestamp ordering violations"
print("ASSERTION OK: global descending order verified with 0 duplicates.")
PYEOF
cat "${EVIDENCE_DIR}/c3-verification.txt"

# Optional client-api edge check if available
CLIENT_API_BASE="${CLIENT_API_URL:-http://127.0.0.1:${CLIENT_API_PORT:-8080}}"
TOKEN_SVC_BASE="${TOKEN_SERVICE_URL:-http://127.0.0.1:${TOKEN_SERVICE_PORT:-8083}}"
if curl -fsS --max-time 2 "${CLIENT_API_BASE}/health" >/dev/null 2>&1 && [[ -n "${CLIENT_INTERNAL_OPS_ID:-}" && -n "${CLIENT_INTERNAL_OPS_SECRET:-}" ]]; then
  p1_log "client-api detected at ${CLIENT_API_BASE}; verifying /v1/events?limit=${PAGE_LIMIT}..."
  TOK_JSON="$(curl -fsS -m 5 -X POST "${TOKEN_SVC_BASE}/oauth/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=${CLIENT_INTERNAL_OPS_ID}&client_secret=${CLIENT_INTERNAL_OPS_SECRET}" || true)"
  ACCESS_TOK="$("$PY" -c 'import sys,json;print(json.loads(sys.argv[1]).get("access_token",""))' "${TOK_JSON:-{}}" 2>/dev/null || true)"
  if [[ -n "$ACCESS_TOK" ]]; then
    CAPI_META="$(curl -sS -m 10 -H "Authorization: Bearer ${ACCESS_TOK}" \
      -o "${EVIDENCE_DIR}/c3-client-api-latest50.json" \
      -w '%{http_code}\t%{time_total}' \
      "${CLIENT_API_BASE}/v1/events?limit=${PAGE_LIMIT}" || echo "000\t0")"
    echo "client-api GET /v1/events?limit=${PAGE_LIMIT} -> http_code=$(echo "$CAPI_META" | cut -f1) time_total_s=$(echo "$CAPI_META" | cut -f2)" \
      | tee -a "${EVIDENCE_DIR}/c3-verification.txt"
  fi
fi

p1_log "Criterion 3 complete. Evidence saved under ${EVIDENCE_DIR}/c3-*"
