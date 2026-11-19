#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
TOKEN="${AUDIT_TOKEN:-${AGGREGATOR_AUDIT_TOKEN:-}}"
DOCKER_NET="${DOCKER_NET:-if4031-tubes-imade_internal_api_net}"
RANGE="${RANGE:-7d}"
AUDIT_LIMIT="${AUDIT_LIMIT:-500}"
CURL_HELPER_CONTAINER="${CURL_HELPER_CONTAINER:-}"

if [[ -z "$TOKEN" ]]; then
  echo "ERROR: AUDIT_TOKEN / AGGREGATOR_AUDIT_TOKEN is required to query /internal/audit/events." >&2
  exit 1
fi

http_get() {
  local path="$1"
  local auth="${2:-}"
  local curl_args=(-fsS --max-time 10)
  if [[ -n "$auth" ]]; then
    curl_args+=(-H "Authorization: Bearer ${auth}")
  fi

  if curl "${curl_args[@]}" "${AGGREGATOR_URL}${path}" 2>/dev/null; then
    return 0
  fi
  if [[ -n "${CURL_HELPER_CONTAINER}" ]]; then
    docker exec "${CURL_HELPER_CONTAINER}" curl "${curl_args[@]}" "http://aggregator:8090${path}"
    return $?
  fi
  if docker network inspect "${DOCKER_NET}" >/dev/null 2>&1; then
    docker run --rm --network "${DOCKER_NET}" curlimages/curl:8.10.1 \
      "${curl_args[@]}" "http://aggregator:8090${path}"
    return $?
  fi
  return 1
}

SUMMARY_JSON=$(http_get "/internal/analytics/summary?range=${RANGE}")

python3 - "$SUMMARY_JSON" "$AGGREGATOR_URL" "$TOKEN" "$DOCKER_NET" "$AUDIT_LIMIT" "$CURL_HELPER_CONTAINER" <<'PY'
import datetime
import json
import subprocess
import sys
import urllib.parse
import urllib.request

summary = json.loads(sys.argv[1])
base_url = sys.argv[2].rstrip("/")
token = sys.argv[3]
docker_net = sys.argv[4]
audit_limit = sys.argv[5]
curl_helper = sys.argv[6]

if summary.get("partial"):
    print(f"WARN: analytics summary is partial; unavailable_units={summary.get('unavailable_units', [])}", file=sys.stderr)

def parse_ts(raw):
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(raw)

win_from = parse_ts(summary.get("from", ""))
win_to = parse_ts(summary.get("to", ""))

def fetch_audit_page(cursor):
    qs = {"limit": str(audit_limit)}
    if cursor:
        qs["cursor"] = cursor
    path = "/internal/audit/events?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(
        base_url + path,
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        if curl_helper:
            cmd = [
                "docker", "exec", curl_helper, "curl", "-fsS", "--max-time", "10",
                "-H", f"Authorization: Bearer {token}",
                f"http://aggregator:8090{path}",
            ]
        else:
            cmd = [
                "docker", "run", "--rm", "--network", docker_net,
                "curlimages/curl:8.10.1", "-fsS", "--max-time", "10",
                "-H", f"Authorization: Bearer {token}",
                f"http://aggregator:8090{path}",
            ]
        out = subprocess.check_output(cmd, text=True)
        return json.loads(out)

cats = ["SEISMIC", "TSUNAMI", "VOLCANIC", "WILDFIRE", "COASTAL"]
audit_counts = {c: 0 for c in cats}
audit_total = 0

cursor = None
while True:
    page = fetch_audit_page(cursor)
    if page.get("partial"):
        print(f"WARN: audit page is partial; unavailable_units={page.get('unavailable_units', [])}", file=sys.stderr)
    for ev in (page.get("items") or page.get("events") or []):
        occ = parse_ts(ev.get("occurred_at"))
        if occ is not None:
            if win_from is not None and occ < win_from:
                continue
            if win_to is not None and occ > win_to:
                continue
        ht = ev.get("hazard_type", "")
        attrs = ev.get("attributes") or {}
        if ht == "SEISMIC" and attrs.get("tsunami_warning") is not None:
            cat = "TSUNAMI"
        else:
            cat = ht
        audit_counts[cat] = audit_counts.get(cat, 0) + 1
        audit_total += 1
    cursor = page.get("next_cursor")
    if not cursor:
        break

dash_cats = summary.get("by_category") or {}
dash_total = int(summary.get("total", 0))

print(f"Window: {summary.get('from') or 'ALL'} -> {summary.get('to')}")
print(f"{'CATEGORY':<12} {'DASHBOARD':>12} {'AUDIT':>12} {'STATUS'}")
print("-" * 48)
mismatch = False
for c in cats:
    d_val = int(dash_cats.get(c, 0))
    a_val = int(audit_counts.get(c, 0))
    ok = d_val == a_val
    if not ok:
        mismatch = True
    print(f"{c:<12} {d_val:>12} {a_val:>12} {'OK' if ok else 'DIFF'}")

tot_ok = dash_total == audit_total
if not tot_ok:
    mismatch = True
print("-" * 48)
print(f"{'TOTAL':<12} {dash_total:>12} {audit_total:>12} {'OK' if tot_ok else 'DIFF'}")

if mismatch:
    print("MISMATCH")
    sys.exit(1)
print("MATCH")
PY
