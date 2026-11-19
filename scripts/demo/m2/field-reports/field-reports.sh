#!/usr/bin/env bash
# End-to-end check of the field report path:
#
#   A. Downstream dead: field-report-consumer is stopped, yet POST /field-reports
#      still answers 202 quickly (only the JetStream ack is awaited).
#   B. Crashes: N reports are sent while client-api and field-report-consumer
#      are killed (SIGKILL) mid-run. The client retries a report with the same
#      Idempotency-Key until it gets 202, and re-sends every accepted report
#      once more, as a phone does when the 202 is lost.
#   C. Result: every report answered 202 reaches the receiver exactly once.
#
# Usage:
#   MODE=k8s     bash scripts/demo/m2/field-reports/field-reports.sh
#   MODE=compose bash scripts/demo/m2/field-reports/field-reports.sh
# Variables: N=200, API_URL, TOKEN_URL, ENV_FILE=.env, EVIDENCE_DIR
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
N="${N:-200}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/field-reports/${RUN_ID}}"
mkdir -p "$EVIDENCE_DIR"
PREFIX="fr-${RUN_ID}"

PY=""
for cand in python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import json' >/dev/null 2>&1; then PY="$cand"; break; fi
done
[[ -n "$PY" ]] || { echo "python3 (or python) is required" >&2; exit 1; }

if [[ -z "${MODE:-}" ]]; then
  if kubectl -n "$NS" get deploy client-api >/dev/null 2>&1; then MODE=k8s; else MODE=compose; fi
fi

if [[ "$MODE" == "k8s" ]]; then
  NODE_IP="${NODE_IP:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')}"
  API_URL="${API_URL:-http://${NODE_IP}:30080}"
  TOKEN_URL="${TOKEN_URL:-http://${NODE_IP}:30083}"
else
  API_URL="${API_URL:-http://localhost:${CLIENT_API_PORT:-8080}}"
  TOKEN_URL="${TOKEN_URL:-http://localhost:${TOKEN_SERVICE_PORT:-8083}}"
  COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
fi

log() { echo "[field-reports $(date -u +%H:%M:%SZ)] $*" | tee -a "$EVIDENCE_DIR/run.log"; }

# ── platform-specific actions ────────────────────────────────────────────────
consumer_stop() {
  if [[ "$MODE" == "k8s" ]]; then
    kubectl -n "$NS" scale deploy/field-report-consumer --replicas=0 >/dev/null
    kubectl -n "$NS" wait --for=delete pod -l app=field-report-consumer --timeout=60s >/dev/null 2>&1
  else
    docker compose stop field-report-consumer >/dev/null 2>&1
  fi
}
consumer_start() {
  if [[ "$MODE" == "k8s" ]]; then
    kubectl -n "$NS" scale deploy/field-report-consumer --replicas=1 >/dev/null
  else
    docker compose start field-report-consumer >/dev/null 2>&1
  fi
}
kill_client_api() {
  if [[ "$MODE" == "k8s" ]]; then
    kubectl -n "$NS" delete pod -l app=client-api --grace-period=0 --force --wait=false >/dev/null 2>&1
  else
    docker compose kill -s SIGKILL client-api >/dev/null 2>&1; sleep 2; docker compose start client-api >/dev/null 2>&1
  fi
}
kill_consumer() {
  if [[ "$MODE" == "k8s" ]]; then
    kubectl -n "$NS" delete pod -l app=field-report-consumer --grace-period=0 --force --wait=false >/dev/null 2>&1
  else
    docker compose kill -s SIGKILL field-report-consumer >/dev/null 2>&1; sleep 2; docker compose start field-report-consumer >/dev/null 2>&1
  fi
}
# Prints the receiver's GET /field-reports JSON.
sink_reports() {
  if [[ "$MODE" == "k8s" ]]; then
    kubectl -n "$NS" run "sink-read-$RANDOM" --rm -i --restart=Never --quiet --image=curlimages/curl:8.11.0 \
      --labels=app=sink-read,role=other \
      --overrides='{"spec":{"containers":[{"name":"c","image":"curlimages/curl:8.11.0","command":["curl","-s","http://stub-sink:8085/field-reports"],"resources":{"requests":{"memory":"16Mi"},"limits":{"memory":"64Mi"}}}]}}' 2>/dev/null
  else
    docker run --rm --network "${COMPOSE_NAME}_bus_net" curlimages/curl:8.11.0 -s http://stub-sink:8085/field-reports
  fi
}

# ── client side ──────────────────────────────────────────────────────────────
TOKEN=""; TOKEN_AT=0
token() { # access tokens live 60 s: refresh every 45 s
  local now; now=$(date +%s)
  if [[ -z "$TOKEN" || $((now - TOKEN_AT)) -ge 45 ]]; then
    TOKEN=$(curl -s -m 5 -X POST "$TOKEN_URL/oauth/token" -H "Content-Type: application/x-www-form-urlencoded" \
      -d "grant_type=client_credentials&client_id=${CLIENT_FIELD_TEAM_ID}&client_secret=${CLIENT_FIELD_TEAM_SECRET}" \
      | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
    TOKEN_AT=$now
  fi
  echo "$TOKEN"
}
body() {
  printf '{"category":"DAMAGE","severity":"HIGH","area_name":"Palu, Sulawesi Tengah","latitude":-0.9,"longitude":119.87,"description":"Uji laporan %s","reported_at":"%s"}' \
    "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
# post KEY -> prints "<http_code> <time_total> <body>"
post() {
  curl -s -m 5 -o /tmp/fr-body.$$ -w '%{http_code} %{time_total}' -X POST "$API_URL/field-reports" \
    -H "Authorization: Bearer $(token)" -H "Content-Type: application/json" \
    -H "Idempotency-Key: $1" -H "X-Correlation-ID: $1" -d "$(body "$1")" 2>/dev/null || echo "000 0"
  echo " $(cat /tmp/fr-body.$$ 2>/dev/null)"
}
# send_until_accepted KEY -> retries like a field device until 202 (max 60 tries)
send_until_accepted() {
  local key="$1" try out
  for try in $(seq 1 60); do
    out=$(post "$key")
    echo "$key try=$try $out" >> "$EVIDENCE_DIR/requests.log"
    if [[ "${out%% *}" == "202" ]]; then return 0; fi
    TOKEN=""   # a fresh token after a failure
    sleep 0.5
  done
  return 1
}

[[ -n "$(token)" ]] || { echo "no token for ${CLIENT_FIELD_TEAM_ID:-?} from $TOKEN_URL" >&2; exit 1; }
log "mode=$MODE api=$API_URL n=$N evidence=$EVIDENCE_DIR"

# ── A. downstream dead ───────────────────────────────────────────────────────
log "A. stopping field-report-consumer, sending 20 reports"
consumer_stop
: > "$EVIDENCE_DIR/phase-a.txt"
for i in $(seq 1 20); do
  out=$(post "${PREFIX}-a-$i")
  echo "${PREFIX}-a-$i $out" >> "$EVIDENCE_DIR/phase-a.txt"
done
"$PY" - "$EVIDENCE_DIR/phase-a.txt" <<'PYEOF' | tee -a "$EVIDENCE_DIR/run.log"
import sys
rows=[l.split(" ",3) for l in open(sys.argv[1]) if l.strip()]
codes=[r[1] for r in rows]; times=sorted(float(r[2]) for r in rows)
p95=times[max(0,int(len(times)*0.95)-1)]
print(f"   consumer down: {codes.count('202')}/{len(rows)} answered 202, max {times[-1]*1000:.0f} ms, p95 {p95*1000:.0f} ms")
PYEOF
consumer_start

# ── B. crashes while sending ─────────────────────────────────────────────────
log "B. sending $N reports; client-api killed at $((N/3)), consumer killed at $((2*N/3))"
: > "$EVIDENCE_DIR/accepted.txt"; : > "$EVIDENCE_DIR/failed.txt"; : > "$EVIDENCE_DIR/resend.txt"
for i in $(seq 1 "$N"); do
  key="${PREFIX}-b-$i"
  [[ "$i" -eq $((N/3)) ]] && { log "   SIGKILL client-api pods"; kill_client_api; }
  [[ "$i" -eq $((2*N/3)) ]] && { log "   SIGKILL field-report-consumer"; kill_consumer; }
  if send_until_accepted "$key"; then
    echo "$key" >> "$EVIDENCE_DIR/accepted.txt"
    # The 202 might have been lost on the way: send the same report again.
    out=$(post "$key"); echo "$key $out" >> "$EVIDENCE_DIR/resend.txt"
  else
    echo "$key" >> "$EVIDENCE_DIR/failed.txt"
  fi
done
accepted_a=20
accepted_b=$(wc -l < "$EVIDENCE_DIR/accepted.txt")
log "   accepted (202): A=$accepted_a B=$accepted_b, never accepted: $(wc -l < "$EVIDENCE_DIR/failed.txt")"

# ── C. compare with what the receiver got ────────────────────────────────────
expected=$((accepted_a + accepted_b))
log "C. waiting up to 180 s for the receiver to hold $expected reports of this run"
for _ in $(seq 1 36); do
  sink_reports > "$EVIDENCE_DIR/sink.json"
  got=$("$PY" -c 'import sys,json;d=json.load(open(sys.argv[1]));print(sum(1 for x in d["data"] if x["report"]["report_id"].startswith(sys.argv[2])))' \
    "$EVIDENCE_DIR/sink.json" "$PREFIX" 2>/dev/null || echo 0)
  [[ "$got" -ge "$expected" ]] && break
  sleep 5
done

"$PY" - "$EVIDENCE_DIR" "$PREFIX" <<'PYEOF' | tee -a "$EVIDENCE_DIR/run.log"
import sys, json, collections
ev, prefix = sys.argv[1], sys.argv[2]
accepted = {f"{prefix}-a-{i}" for i in range(1, 21)}
accepted |= {l.strip() for l in open(f"{ev}/accepted.txt") if l.strip()}
sink = json.load(open(f"{ev}/sink.json"))
ids = [x["report"]["report_id"] for x in sink["data"] if x["report"]["report_id"].startswith(prefix)]
counts = collections.Counter(ids)
missing = sorted(accepted - set(ids))
stored_twice = [k for k, c in counts.items() if c > 1]
unexpected = sorted(set(ids) - accepted)
resend_dup = sum(1 for l in open(f"{ev}/resend.txt") if '"duplicate":true' in l)
print(f"   accepted with 202 : {len(accepted)}")
print(f"   at the receiver   : {len(set(ids))}")
print(f"   missing           : {len(missing)} {missing[:5]}")
print(f"   processed twice   : {len(stored_twice)}")
print(f"   stored without a 202 seen by the client: {len(unexpected)}")
print(f"   re-sent reports answered duplicate=true: {resend_dup}/{len(accepted)-20}")
print(f"   receiver ignored repeated deliveries (all runs): {sink.get('duplicates_ignored')}")
ok = not missing and not stored_twice
print("   RESULT:", "PASS - no 202 report lost, none processed twice" if ok else "FAIL")
sys.exit(0 if ok else 1)
PYEOF
status=${PIPESTATUS[0]}
rm -f /tmp/fr-body.$$
exit "$status"
