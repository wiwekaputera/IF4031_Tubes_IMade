#!/usr/bin/env bash
# p5.sh
#
# Tests and verifies:
#   1. Producer (aggregator outbox relay) publishes HazardEvents to stream
#      BNPB_EVENTS, and >= 2 independent durable consumers are subscribed
#   2. Stop notification-consumer: producer keeps publishing and
#      audit-consumer keeps consuming; messages for the stopped consumer
#      accumulate in JetStream (num_pending > 0)
#   3. Start notification-consumer again: the missed messages are caught up
#      (num_pending back to 0) — nothing is lost (durable consumer, 24h retention)
#   4. Add a NEW consumer on the existing stream/subject WITHOUT changing any
#      producer code (nats-box CLI), and fetch events from it
#
# A nats-box helper container is attached to bus_net (the broker publishes no
# host port), the same way smoke.sh reaches the aggregator.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

STREAM="BNPB_EVENTS"
SUBJECT_FILTER="bnpb.events.>"
NEW_CONSUMER="${P5_NEW_CONSUMER:-p5-demo-dashboard}"
STOP_SECONDS="${P5_STOP_SECONDS:-25}"
NATS_BOX_IMG="${NATS_BOX_IMG:-natsio/nats-box:latest}"
HELPER_NAME="demo-p5-natsbox"

PASS=0
FAIL=0
ok(){   echo "  [PASS] $*"; PASS=$((PASS+1)); }
bad(){  echo "  [FAIL] $*"; FAIL=$((FAIL+1)); }
log(){  echo "[p5-demo] $*"; }

# ---- python interpreter (python3 on Linux/macOS, python on Windows Git Bash)
PY=""
for cand in python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import json' >/dev/null 2>&1; then
    PY="$cand"
    break
  fi
done
if [[ -z "$PY" ]]; then
  echo "ERROR: Python 3 is required (python3 or python not found on PATH)." >&2
  exit 1
fi

# ---- preconditions -------------------------------------------------------
for svc in nats-jetstream aggregator notification-consumer audit-consumer stub-sink; do
  if [[ -z "$(docker compose ps -q "$svc" 2>/dev/null || true)" ]]; then
    echo "ERROR: $svc is not running. Start the full stack first: bash scripts/demo/up.sh" >&2
    exit 1
  fi
done

COMPOSE_NAME="$(docker compose config --format json 2>/dev/null \
  | "$PY" -c 'import sys,json;print(json.load(sys.stdin).get("name",""))' 2>/dev/null || true)"
COMPOSE_NAME="${COMPOSE_NAME:-if4031-tubes-imade}"
NET="${COMPOSE_NAME}_bus_net"

cleanup(){
  docker rm -f "$HELPER_NAME" >/dev/null 2>&1 || true
  # never leave the demo consumer stopped
  docker compose start notification-consumer >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "Starting nats-box helper on ${NET}..."
docker rm -f "$HELPER_NAME" >/dev/null 2>&1 || true
if ! docker run -d --rm --name "$HELPER_NAME" --network "$NET" "$NATS_BOX_IMG" sleep 3600 >/dev/null; then
  echo "ERROR: could not start $NATS_BOX_IMG on $NET (try: docker pull $NATS_BOX_IMG)" >&2
  exit 1
fi

nats(){
  docker exec "$HELPER_NAME" nats -s nats://nats-jetstream:4222 "$@"
}

# consumer_field CONSUMER FIELD -> num_pending | stream_seq | ack_floor
consumer_field(){
  nats consumer info "$STREAM" "$1" --json 2>/dev/null | "$PY" -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(-1); sys.exit(0)
f = sys.argv[1]
if f == "num_pending":
    print(d.get("num_pending", 0) + d.get("num_ack_pending", 0))
elif f == "stream_seq":
    print(d.get("delivered", {}).get("stream_seq", 0))
elif f == "ack_floor":
    print(d.get("ack_floor", {}).get("stream_seq", 0))
' "$2"
}

stream_last_seq(){
  nats stream info "$STREAM" --json 2>/dev/null | "$PY" -c '
import sys, json
try:
    print(json.load(sys.stdin).get("state", {}).get("last_seq", 0))
except Exception:
    print(-1)
'
}

# =========================================================================
# Skenario 1: Stream + >= 2 consumer independen
# =========================================================================
echo
echo "=== Skenario 1: Stream ${STREAM} dan consumer yang berlangganan ==="
CONSUMERS="$(nats consumer ls "$STREAM" --names 2>/dev/null | tr -d '\r' || true)"
echo "$CONSUMERS" | sed 's/^/    consumer: /'
if echo "$CONSUMERS" | grep -qx "notification-consumer" && echo "$CONSUMERS" | grep -qx "audit-consumer"; then
  ok "Stream ${STREAM} punya >= 2 durable consumer independen (notification-consumer, audit-consumer)"
else
  bad "notification-consumer / audit-consumer tidak ditemukan di stream ${STREAM}"
fi

SEQ0="$(stream_last_seq)"
log "Menunggu event baru dipublikasikan aggregator (maks 30 dtk)..."
for _ in $(seq 1 30); do
  sleep 1
  if [[ "$(stream_last_seq)" -gt "$SEQ0" ]]; then break; fi
done
SEQ1="$(stream_last_seq)"
if [[ "$SEQ1" -gt "$SEQ0" ]]; then
  ok "Producer mempublikasikan event baru ke ${STREAM} (last_seq ${SEQ0} -> ${SEQ1})"
else
  bad "Tidak ada event baru di ${STREAM} dalam 30 dtk (last_seq=${SEQ1})"
fi

# =========================================================================
# Skenario 2: Stop notification-consumer -> producer & audit tetap jalan
# =========================================================================
echo
echo "=== Skenario 2: Stop notification-consumer selama ${STOP_SECONDS} dtk ==="
docker compose stop notification-consumer >/dev/null 2>&1
SEQ_STOP="$(stream_last_seq)"
AUDIT_BEFORE="$(consumer_field audit-consumer stream_seq)"
log "notification-consumer STOPPED (stream last_seq=${SEQ_STOP}); menunggu ${STOP_SECONDS} dtk..."
sleep "$STOP_SECONDS"

SEQ_AFTER="$(stream_last_seq)"
AUDIT_AFTER="$(consumer_field audit-consumer stream_seq)"
NOTIF_PENDING="$(consumer_field notification-consumer num_pending)"

if [[ "$SEQ_AFTER" -gt "$SEQ_STOP" ]]; then
  ok "Producer tetap publish tanpa tertahan (last_seq ${SEQ_STOP} -> ${SEQ_AFTER})"
else
  bad "Producer tidak publish selama consumer mati (last_seq tetap ${SEQ_AFTER})"
fi
if [[ "$AUDIT_AFTER" -gt "$AUDIT_BEFORE" ]]; then
  ok "audit-consumer tetap menerima event (delivered stream_seq ${AUDIT_BEFORE} -> ${AUDIT_AFTER})"
else
  bad "audit-consumer tidak menerima event baru (stream_seq ${AUDIT_AFTER})"
fi
if [[ "$NOTIF_PENDING" -gt 0 ]]; then
  ok "Event untuk notification-consumer tertahan di JetStream: num_pending=${NOTIF_PENDING} (tidak hilang)"
else
  bad "num_pending notification-consumer = ${NOTIF_PENDING} (expected > 0)"
fi

# =========================================================================
# Skenario 3: Start lagi -> event yang terlewat tersusul
# =========================================================================
echo
echo "=== Skenario 3: Start notification-consumer -> event tersusul ==="
docker compose start notification-consumer >/dev/null 2>&1
CAUGHT_UP=0
for _ in $(seq 1 45); do
  sleep 1
  if [[ "$(consumer_field notification-consumer num_pending)" -eq 0 ]]; then
    CAUGHT_UP=1
    break
  fi
done
NOTIF_ACK="$(consumer_field notification-consumer ack_floor)"
if [[ "$CAUGHT_UP" -eq 1 && "$NOTIF_ACK" -ge "$SEQ_AFTER" ]]; then
  ok "notification-consumer menyusul semua event yang terlewat (num_pending=0, ack_floor=${NOTIF_ACK} >= ${SEQ_AFTER})"
else
  bad "notification-consumer belum menyusul (num_pending=$(consumer_field notification-consumer num_pending), ack_floor=${NOTIF_ACK})"
fi
log "Log notification-consumer setelah start (event yang tersusul):"
docker compose logs --since 60s notification-consumer 2>/dev/null | grep -o '"event_key":"[^"]*"' | tail -n 5 | sed 's/^/    /' || true

# =========================================================================
# Skenario 4: Tambah consumer BARU tanpa mengubah kode producer
# =========================================================================
echo
echo "=== Skenario 4: Tambah consumer baru '${NEW_CONSUMER}' pada stream yang sama ==="
nats consumer rm "$STREAM" "$NEW_CONSUMER" -f >/dev/null 2>&1 || true
if nats consumer add "$STREAM" "$NEW_CONSUMER" \
     --pull --filter "$SUBJECT_FILTER" --deliver all --ack explicit --defaults >/dev/null 2>&1; then
  ok "Consumer durable baru '${NEW_CONSUMER}' dibuat lewat CLI (aggregator tidak diubah / di-restart)"
else
  bad "Gagal membuat consumer '${NEW_CONSUMER}'"
fi

FETCHED="$(nats consumer next "$STREAM" "$NEW_CONSUMER" --count 3 --timeout 10s 2>&1 || true)"
N_FETCHED="$(printf '%s\n' "$FETCHED" | grep -c 'bnpb.events.hazard' || true)"
printf '%s\n' "$FETCHED" | grep -E 'bnpb.events.hazard|"hazard_type"' | cut -c1-160 | head -n 6 | sed 's/^/    /' || true
if [[ "$N_FETCHED" -ge 1 ]]; then
  ok "Consumer baru menerima ${N_FETCHED} HazardEvent dari subject yang sudah ada"
else
  bad "Consumer baru tidak menerima event"
fi
log "Daftar consumer sekarang:"
nats consumer ls "$STREAM" --names 2>/dev/null | tr -d '\r' | sed 's/^/    consumer: /'

if [[ "${P5_KEEP_CONSUMER:-0}" != "1" ]]; then
  nats consumer rm "$STREAM" "$NEW_CONSUMER" -f >/dev/null 2>&1 || true
  log "Consumer demo '${NEW_CONSUMER}' dihapus lagi (P5_KEEP_CONSUMER=1 untuk mempertahankan)"
fi

# =========================================================================
# Summary
# =========================================================================
echo
echo "==================================================="
echo "  P5 Demo Summary: ${PASS} passed, ${FAIL} failed"
echo "==================================================="
if [[ "$FAIL" -gt 0 ]]; then
  echo "  RESULT: FAIL"
  exit 1
fi
echo "  RESULT: ALL CHECKS PASSED"
exit 0
