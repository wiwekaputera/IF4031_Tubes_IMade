# IF4031 Milestone 2 — BNPB Disaster Coordination System

> **Course:** IF4031 — Arsitektur Aplikasi Terdistribusi  
> **Milestone:** 2 — Distributed Data, High Availability and Stream Processing  
> **Team:** IMade — Made, Theo, Fayadh  
> The Milestone 1 README is kept as [README_M1.md](README_M1.md).

---

## What Changed from Milestone 1

| Area | Milestone 1 | Milestone 2 | Owner | Status |
| :--- | :--- | :--- | :--- | :---: |
| Platform | Docker Compose on one machine | Kubernetes (k3s), two nodes with separate kernels | Fayadh | 🚧 In progress |
| Canonical store (P1) | One PostgreSQL instance | 4-node CockroachDB (`RF=3`), 8 logical hash shards, Audit API, analytics dashboard | Made | 🟢 Done |
| Aggregator (P2) | One instance, every instance polls | ≥3 instances, DB-backed leader election + fencing token (core done; 10k-VU k6 pending) | Made, Fayadh | 🚧 In progress |
| Schema changes (P3) | Additive changes only | Incompatible changes rejected into a dead-letter queue, retry and replay | Theo | 🟢 Done |
| Field reports (P3) | — | `POST /field-reports`, accepted with 202, never lost or duplicated | Fayadh | 🚧 In progress |
| Satellite fire feed (P4) | — | New `satellite-ingestor`: SSE feed → fires → WILDFIRE hazard events | Theo | 🟢 Done |
| Coastal sensors (P5) | — | Sensor Pantai poller, circuit breaker, prioritised critical alerts | Fayadh | 🚧 In progress |
| Observability | JSON logs + `X-Correlation-ID` | Prometheus, Grafana dashboards, distributed tracing | Theo | 🚧 In progress |

---

## Service Status & Overview

Docker Compose still runs the whole system on one machine for development. The Kubernetes deployment is described in [Kubernetes Deployment](#kubernetes-deployment).

| Service | Port | Network | Status | Description |
| :--- | :---: | :--- | :---: | :--- |
| `bmkg-mock` | `8081` | `agency_net` | 🟢 Operational | Simulated BMKG seismic & tsunami API with `/admin/requests`, `/admin/burst`, jittered delay, and `X-BMKG-Key` auth |
| `pvmbg-mock` | `8082` | `agency_net` | 🟢 Operational | Simulated PVMBG volcanic API with `/admin/outage`, `/admin/schema-version?mode=incompatible`, `/admin/requests`, and Bearer auth |
| `canonical-store-0..3` | *internal* | `store_net` | 🟢 Operational | 4-node CockroachDB `v24.1.6` cluster (`RF=3`, 8 logical hash shards `shard-0..7`, `canonical_events`, `outbox`, `leader_leases`, `poller_watermarks`, `dead_letters`) |
| `aggregator` | *internal* (`8090`) | `agency_net`, `store_net`, `internal_api_net`, `bus_net` | 🟢 Operational | HA leader-elected poller, fenced writes, schema policy & DLQ, scatter-gather query API, Audit API, analytics dashboard, outbox relay, JetStream ingest consumer |
| `nats-jetstream` | *internal* (`4222`) | `bus_net` | 🟢 Operational | JetStream broker (`BNPB_EVENTS`, `HAZARD_INGEST` streams) |
| `stub-sink` | *internal* (`8085`) | `bus_net` | 🟢 Operational | Notification target with idempotency check |
| `notification-consumer` | *internal* (`8086`) | `bus_net` | 🟢 Operational | Durable consumer with bbolt dedup, forwards to stub sink |
| `audit-consumer` | *internal* (`8087`) | `bus_net` | 🟢 Operational | Durable consumer keeping event history in bbolt |
| `token-db` | *internal* | `token_net` | 🟢 Operational | Refresh tokens and reuse detection |
| `token-service` | `8083` | `edge_net`, `token_net` | 🟢 Operational | OAuth 2.0 token service, ES256, JWKS |
| `client-api` | `8080` | `edge_net`, `internal_api_net` | 🟢 Operational | Edge gateway with scope projection and rate limiting |
| `satellite-store` | *internal* | `satellite_net` | 🟢 Operational | PostgreSQL owned by `satellite-ingestor` only: detections, fires, offset, outbox, dead letters |
| `satellite-ingestor` | `9091` (metrics) | `agency_net`, `satellite_net`, `bus_net` | 🟢 Operational | Satellite feed client, fire clustering, publishes WILDFIRE events to `hazard.ingest.SATELIT` |

Ports `8080`–`8083` and `9091` can be changed via `.env`.

---

## Prerequisites

| Tool | Notes |
| :--- | :--- |
| Docker Engine + Docker Compose v2 | `docker compose version` |
| `bash` | Linux/macOS shell, or **Git Bash** / WSL on Windows |
| `curl`, `openssl` | `openssl` generates the ES256 signing key |
| Python 3 | Demo scripts parse JSON with it; `scripts/satellite/report.py` reads metrics |
| Go 1.25 *(optional)* | Only to run the unit and integration tests outside Docker |
| k6 *(optional)* | `scripts/demo/p2-load.sh` runs k6 from the `grafana/k6` image by default |

---

## Quick Start (Docker Compose)

### 1. Configure
```bash
cp .env.example .env
bash scripts/keys/gen-es256.sh          # copy the printed ES256_PRIVATE_KEY_B64=... line into .env
```
Then set these in `.env`:

| Variable | Value |
| :--- | :--- |
| `INTERNAL_OPS_TOKEN` | Any random string, e.g. `openssl rand -hex 32` |
| `SATELLITE_FEED_TOKEN` | The group token from the course assistants. Keep it only in `.env` |
| `SATELLITE_STORE_POSTGRES_PASSWORD` | Any password for the satellite store |

`bash scripts/demo/up.sh` still does the `.env` copy and key generation on first run.

### 2. Start
```bash
docker compose up -d --build
docker compose ps                       # wait until every service is (healthy)
```

### 3. Automated checks
| Script | Covers | Duration |
| :--- | :--- | :--- |
| `bash scripts/demo/smoke.sh` | Mocks, outage toggle, aggregator health, canonical store | ~30 s |
| `bash scripts/demo/p3.sh` | Milestone 1 authentication and scope projection checks | ~2 min |
| `bash scripts/demo/p2-load.sh` | Milestone 1 load test (the M2 10k-VU test is in [P2](#p2--high-availability-aggregator)) | ~1.5 min |
| `bash scripts/demo/p5.sh` | Milestone 1 fan-out checks | ~1.5 min |
| `python3 scripts/satellite/report.py` | Satellite ingest latency, lag and counters | instant |

### 4. Teardown
```bash
docker compose down -v           # stop and remove volumes
```

### Loading credentials for manual commands
```bash
set -a; source .env; set +a
```

---

## Kubernetes Deployment

> 🚧 **Owner: Fayadh.** To be completed: topology declaration (physical hosts, VMs, CPU/RAM per node), the one-command deploy, the namespace `sistem-informasi-kebencanaan-bnpb`, Secrets and ConfigMaps, NetworkPolicies, and the Tester Agent setup.

Values the satellite ingestor needs in the cluster:

| Item | Value |
| :--- | :--- |
| Image | `services/satellite-ingestor/Dockerfile`, built from the repository root |
| Role label | `role=consumer` |
| Secret | `SATELLITE_FEED_TOKEN`, the database password |
| Env | `DATABASE_URL`, `NATS_URL`, `SATELLITE_FEED_URL`, `METRICS_ADDR=:9090` |
| Probes | `GET /healthz` on port 9090 (checks Postgres and NATS) |
| Metrics | `GET /metrics` on port 9090 |
| Store | Its own PostgreSQL (`satellite-store`); only the ingestor may reach it |
| Replicas | 1 until the leader lease from P2 is available; the offset is in the database, so a restarted pod resumes exactly |

---

## Observability

All services write structured JSON logs to `stdout`. `X-Correlation-ID` is propagated client-api → aggregator as in Milestone 1.

```bash
docker compose logs -f aggregator satellite-ingestor
```

**Satellite ingestor metrics** (`http://localhost:9091/metrics`):

| Metric | Meaning |
| :--- | :--- |
| `satellite_ingest_latency_seconds` | Histogram: event read from the feed → result committed |
| `satellite_feed_lag_offsets` | Events the feed has published that are not committed yet |
| `satellite_event_time_lag_seconds` | Feed virtual time minus the newest committed detection time |
| `satellite_next_offset` | Next offset to read; everything before it is committed |
| `satellite_feed_messages_total{type,outcome}` | Messages by type and outcome (`processed`, `low_confidence`, `late`, `duplicate`, `unknown_key`, `dead_lettered`) |
| `satellite_durable_count{kind}` | Running totals kept in the database, so they survive restarts |
| `satellite_outbox_backlog` | Hazard messages committed but not yet acknowledged by JetStream |
| `satellite_dead_letters` | Parked feed events |

> 🚧 **Owner: Theo.** To be completed: Prometheus and Grafana in the cluster, per-service dashboards (latency, error rate, throughput, satellite lag, DLQ size, leader status, circuit breaker state), OpenTelemetry tracing to Jaeger/Tempo, and the shared metrics/tracing middleware package.

---

## Demo Scenarios

Helper used below to obtain an access token:
```bash
set -a; source .env; set +a
tok(){ curl -s -X POST http://localhost:8083/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$1&client_secret=$2" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])'; }
OPS_TOKEN=$(tok "$CLIENT_INTERNAL_OPS_ID" "$CLIENT_INTERNAL_OPS_SECRET")
```

The aggregator's internal routes are not published to the host. They are reached from inside `internal_api_net`:
```bash
incurl(){ docker run --rm --network if4031-tubes-imade_internal_api_net curlimages/curl:8.11.0 -s "$@"; }
```

### P1 — Distributed Storage

The canonical store runs as a 4-node **CockroachDB `v24.1.6`** cluster (`RF=3`, `server.time_until_store_dead = '1m15s'`) with 8 pre-split computed hash shards (`shard = mod(crc32ieee(hazard_id), 8)`, `shard-0`..`shard-7`) and deterministic `hazard_id = "hz-" + sha256(source + "|" + source_ref_id)[:16]`.

- **Kubernetes manifests:** [deploy/k8s/README.md](deploy/k8s/README.md) (`base/`, `overlays/multi-node/`, `overlays/two-node/`, `jobs/aggregator-loader.yaml`, `scripts/k8s/create-secrets.sh`).
- **Automated P1 scenario scripts (`c1`–`c7`, `MODE=compose|k8s`):** [scripts/demo/m2/p1/README.md](scripts/demo/m2/p1/README.md).

#### Historical Catalog Loader & Data Distribution

The aggregator image ships `/loader` (`services/aggregator/cmd/loader`) for idempotent bulk loading (`ON CONFLICT (shard, hazard_id) DO NOTHING`, no outbox spam unless `--with-outbox` is passed; auto-detects USGS/BMKG CSV/JSON/NDJSON headers or custom `--col-*` flags):

```bash
# 1. Generate a skewed 50,000-row sample catalog (80% Sulawesi Tengah)
python3 scripts/p1/gen-sample-catalog.py --rows 50000 --out /tmp/sample-catalog-50k.csv

# 2. Load into CockroachDB (re-running loads 0 new rows)
docker compose run --rm --entrypoint /loader \
  -v /tmp/sample-catalog-50k.csv:/data/catalog.csv:ro \
  aggregator --file /data/catalog.csv --concurrency 4 --batch-size 500

# 3. Inspect per-shard & per-node distribution (GET /internal/admin/distribution)
bash scripts/p1/distribution.sh
```

#### Audit API (`/internal/audit/*`)

All endpoints require `Authorization: Bearer <AGGREGATOR_AUDIT_TOKEN>` (returns `401` if invalid):
- `GET /internal/audit/events?source=&since=&limit=&cursor=` — paginated non-deleted events ordered by `(ingested_at ASC, hazard_id ASC)` with `partial` and `unavailable_units`.
- `GET /internal/audit/checksum?source=` — SHA-256 of sorted non-deleted `source_ref_id` values (`AUDIT_TOKEN="$AGGREGATOR_AUDIT_TOKEN" bash scripts/audit/checksum-check.sh BMKG`).
- `GET /internal/audit/location?hazard_id=...` — leaseholder (`primary`) and follower (`replica`) node placement via `SHOW RANGE`.
- `GET /internal/audit/unprocessed?source=&limit=&cursor=` — paginated `dead_letters` rows with `status IN ('retrying', 'parked')` (excludes `resolved` and `raw_payload`).

#### Analytics Dashboard (`/analytics/`)

Embedded Chart.js dashboard (no external CDN) backed by scatter-gather `GET /internal/analytics/summary`:
- `SEISMIC` events with non-null `attributes.tsunami_warning` are counted under `TSUNAMI` (mutually exclusive from `SEISMIC`).
- On shard timeout (`800 ms`), returns HTTP `200` with `partial: true`, `unavailable_units`, warning banner, and `≥` count prefixes.
- Verify totals match Audit API: `bash scripts/p1/dashboard-vs-audit.sh`.

#### JetStream Ingest Contract (`hazard.ingest.<SOURCE>`)

Non-aggregator sources (`SATELIT`, `SENSOR_PANTAI`) publish `event.IngestMessage` to stream `HAZARD_INGEST` (`hazard.ingest.>`, `Nats-Msg-Id = message_id`). Aggregator replicas share durable pull consumer `aggregator-ingest` (`256 KiB` cap, panic recovery, poison messages terminated & parked in `dead_letters`). Updates (`op: "upsert"`, full replace) and soft-deletes (`op: "delete"`, tombstone + outbox `attributes.deleted=true`) apply only when `source_version > stored source_version`.

---

### P2 — High-Availability Aggregator

- **Core HA (Made — 🟢 Done):** Aggregator runs `≥ 3` replicas behind one Service (`aggregator:8090`), with DB-backed leader election (`leader_leases` using CockroachDB HLC `now()`), monotonic `fencing_token` checked in `SERIALIZABLE` write transactions (`UpsertFencedBatch`), renew-before-poll, durable `poller_watermarks`, `GET /internal/leader`, `GET /internal/ready` (`503 DRAINING` on `SIGTERM`), and graceful shutdown drain (`SHUTDOWN_DRAIN_S=5`).
  - Automated P2 scenario scripts (`c1`–`c5`, `MODE=compose|k8s`): [scripts/demo/m2/p2/README.md](scripts/demo/m2/p2/README.md).
- **Load testing, scaling & rolling update (Fayadh — 🚧 In progress):** k6 `≥ 10,000 VU × ≥ 120 s` with scaling `3→5→2`, throughput `3 vs 1` replicas, and zero-error rolling update.

---

### P3 — Schema Changes, Dead-Letter Queue and Replay

**Policy.** The aggregator validates every PVMBG report before it is normalised:

| Change | Result |
| :--- | :--- |
| New field | Accepted, kept in `attributes` |
| Optional field missing | Accepted |
| Required field missing, field renamed, or type changed (e.g. number → string) | Rejected: **parked** in the DLQ |
| Truncated JSON, invalid UTF-8, body over 1 MiB, nesting deeper than 32 | Rejected: **parked** in the DLQ |
| Database or other temporary failure | **retrying** with backoff 5 s → 10 min, parked after 5 attempts |
| Panic while processing one message | Recovered, the message is parked; the service keeps running |

The DLQ is the `dead_letters` table. It keeps the raw message byte for byte with `reason`, `source`, `received_at`, `retryable`, `attempts` and `status` (`retrying`, `parked`, `resolved`). The poll watermark only advances once every message is either stored or safely in the DLQ, so nothing is skipped.

1. **Break the PVMBG schema at runtime:**
   ```bash
   curl -s -X POST "http://localhost:8082/admin/schema-version?mode=incompatible"
   #   volcano_id -> volcano_code, ash_column_height_m becomes a string, reported_at removed
   ```
2. **See the rejected messages parked, not crashing the aggregator** (wait ~20 s):
   ```bash
   docker compose logs --since 1m aggregator | grep -i dlq
   incurl -H "Authorization: Bearer $INTERNAL_OPS_TOKEN" "http://aggregator:8090/internal/dlq?status=parked&limit=5"
   docker compose exec -T canonical-store-0 cockroach sql --insecure -d canonical \
     -e "SELECT id, source, source_ref_id, reason, status, attempts FROM dead_letters ORDER BY id DESC LIMIT 10;"
   docker compose ps aggregator          # still running, no restart
   ```
3. **Only internal ops may inspect the DLQ:**
   ```bash
   incurl -o /dev/null -w "%{http_code}\n" "http://aggregator:8090/internal/dlq"                          # 401
   incurl -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer wrong" "http://aggregator:8090/internal/dlq"   # 401
   ```
4. **Switch the schema back and replay:**
   ```bash
   curl -s -X POST "http://localhost:8082/admin/schema-version?mode=incompatible"   # toggles off
   incurl -X POST -H "Authorization: Bearer $INTERNAL_OPS_TOKEN" "http://aggregator:8090/internal/dlq/replay?limit=100"
   ```
   Replay runs each parked message through the current normaliser. Messages it can now read become `resolved` and are upserted by `(source, source_ref_id)`, so a replay never creates duplicates. Messages it still cannot read stay `parked`. Replay keeps the order per `source_ref_id`: once one message of a report fails, later messages of the same report wait.
5. **Malformed-message tests:**
   ```bash
   cd services/aggregator && go test ./internal/schema/ ./internal/dlq/ ./internal/poller/
   ```

**Field reports (`POST /field-reports`)**

> 🚧 **Owner: Fayadh.** To be completed: 202 only after the JetStream ack, `Idempotency-Key` dedup, idempotent consumer, kill-after-202 test, field-team write scope.

---

### P4 — Satellite Fire Feed → WILDFIRE

`satellite-ingestor` reads `GET /v1/stream` from the course feed (Server-Sent Events, Bearer group token), turns VIIRS hotspots into fires, and publishes each fire change to the aggregator as a WILDFIRE hazard event.

**How it works**

| Concern | Design |
| :--- | :--- |
| Resume after restart | The next offset is stored in Postgres **in the same transaction** as the fire changes, so a restart resumes exactly and never loses or repeats a change. The client reconnects with `?from=<offset>`. |
| Feed errors | 400/401 stop the service (a reconnect cannot fix a bad token). 429/503 back off exponentially, honouring `Retry-After`. 45 s without bytes (heartbeats included) triggers a reconnect. |
| Fire rule | Dedup by `satellite\|acq_date\|acq_time\|lat\|lon`; drop confidence `l`; link detections ≤1 km and ≤360 min apart, transitively; ≥2 linked detections make a fire; single detections are counted. |
| Severity | AWAS ≥20 detections or ≥100 MW total FRP; SIAGA ≥6 or ≥30 MW; otherwise WASPADA. |
| Event time | Everything uses the detection's own acquisition time (UTC), never arrival time. The feed runs on virtual time at 60×. |
| Late data | A detection more than **2 hours** older than the newest one seen is counted as late and not clustered (`satellite_durable_count{kind="late"}`). |
| Update / retract | Changes the stored detection and reclusters only the fire it belongs to. A retracted detection is kept as a marked row. |
| Duplicates | The feed re-sends detections. A key already stored is ignored, so a re-sent copy cannot undo a later update or bring back a retracted false positive. |
| Stable fire ID | `source_ref_id` starts as a hash of the earliest detection key, then is carried over to the recomputed fire that overlaps it. Retracting the earliest detection keeps the ID. On a merge the older fire keeps its ID and the other gets a delete event; on a split the older part keeps it. |
| Area name | Nearest of the 38 provinces, by distance to the provincial capital (static table). Near a border this can name the neighbouring province. |
| Delivery to the aggregator | Each change is written to an outbox table in the same transaction, then published to `hazard.ingest.SATELIT` (`HAZARD_INGEST` stream) with `Nats-Msg-Id`. `source_version` is the feed offset, so later states always win. Deleted fires are sent as `op: delete`. |
| Bad events | Oversized lines, broken JSON, invalid detections and a panic while handling one message go to `satellite_dead_letters` with the raw bytes; the feed moves on. |

**Hazard event mapping**

| Field | Value |
| :--- | :--- |
| `hazard_type` / `source` | `WILDFIRE` / `SATELIT` |
| `latitude`, `longitude` | Centroid of the fire's detections |
| `occurred_at` | Earliest detection time |
| `area_name` | Nearest province |
| `attributes` | `detection_count`, `satellites`, `max_confidence`, `total_frp_mw`, `detection_keys` |

**Demo**

1. **Start the ingestor and watch it catch up:**
   ```bash
   docker compose up -d --build satellite-ingestor
   python3 scripts/satellite/report.py --watch 10
   #   latency p50/p95/p99, lag in offsets and feed time, late / low-confidence / duplicate counts
   ```
2. **Fires in the satellite store:**
   ```bash
   docker compose exec -T satellite-store psql -U satellite -d satellite -c "
     SELECT next_offset, late, low_conf, duplicate, unknown, invalid FROM satellite_state;"
   docker compose exec -T satellite-store psql -U satellite -d satellite -c "
     SELECT source_ref_id, severity, detection_count, round(total_frp_mw::numeric,1) AS frp_mw, occurred_at
     FROM satellite_fires ORDER BY detection_count DESC LIMIT 10;"
   ```
3. **Kill it mid-stream and restart:**
   ```bash
   docker compose kill satellite-ingestor
   docker compose start satellite-ingestor
   docker compose logs --since 1m satellite-ingestor | grep resume_offset    # continues from the committed offset
   ```
4. **Events sent to the aggregator:**
   ```bash
   docker run --rm --network if4031-tubes-imade_bus_net natsio/nats-box:latest \
     nats -s nats://nats-jetstream:4222 stream info HAZARD_INGEST
   docker run --rm --network if4031-tubes-imade_bus_net natsio/nats-box:latest \
     nats -s nats://nats-jetstream:4222 sub 'hazard.ingest.SATELIT' --count 3
   ```

**Kill-and-restart proof.** `internal/e2e` replays a recorded feed into two databases: once straight through, and once killed at random moments (some in the middle of a transaction) and restarted from the committed offset each time. Every table and the exact sequence of messages to the aggregator must match, and the fires must equal a full recluster of all stored detections.

```bash
# Record the feed (outside the repo)
timeout 60 curl -sS -N -H "Authorization: Bearer $SATELLITE_FEED_TOKEN" \
  "https://satellite.naufarrel.dev/v1/stream?from=0" > ~/satellite_stream.txt

docker run -d --name sat-pg -e POSTGRES_PASSWORD=pw -p 5433:5432 postgres:16
cd services/satellite-ingestor
SATELLITE_TEST_DATABASE_URL="postgres://postgres:pw@localhost:5433/postgres?sslmode=disable" \
SATELLITE_TEST_STREAM_FILE=~/satellite_stream.txt \
SATELLITE_TEST_EXPORT=/tmp/fires.jsonl \
  go test -v -timeout 60m ./internal/e2e/
```
Result on the practice feed (181,467 events): 36 random kills, all tables and 120,809 aggregator messages identical, 2,845 fires equal to a full recluster, about 385 events/s. `SATELLITE_TEST_EXPORT` writes the fires as JSON lines for comparison with the assistants' reference.

> **Depends on:** the aggregator consumer for `hazard.ingest.>` (Made) to store WILDFIRE events in the canonical store, and the P2 leader lease to run more than one replica.

---

### P5 — Coastal Sensors (Sensor Pantai)

> 🚧 **Owner: Fayadh.** To be completed: sensor deployment, poller with validation, critical alert path (p95 ≤5 s, no loss), circuit breaker, bulkhead, retry with jitter, prioritised load shedding, SLO and error budget, game day.

---

## Tests

```bash
# Aggregator (schema policy, DLQ, poller); the store tests need Docker
cd services/aggregator && go test ./...

# Satellite ingestor: unit tests
cd services/satellite-ingestor && go test -race ./...

# Satellite ingestor: Postgres-backed tests (store, restart, outbox)
SATELLITE_TEST_DATABASE_URL="postgres://postgres:pw@localhost:5433/postgres?sslmode=disable" \
  go test -race -p 1 ./...

# Satellite ingestor: JetStream relay test (needs a NATS server with -js)
SATELLITE_TEST_DATABASE_URL=... SATELLITE_TEST_NATS_URL=nats://localhost:4222 go test ./internal/relay/
```

---

## Configuration Variables

All variables are in [.env.example](.env.example). Milestone 1 variables are unchanged and documented in [README_M1.md](README_M1.md#configuration-variables). New in Milestone 2:

| Variable | Default | Service | Description |
| :--- | :---: | :--- | :--- |
| `AGGREGATOR_DB_DSN` | `postgresql://aggregator@canonical-store-0:26257,…/canonical?sslmode=disable` | `aggregator` | Multi-host CockroachDB connection string |
| `AGGREGATOR_AUDIT_TOKEN` | `change-me-audit-token` | `aggregator` | Bearer token required by `/internal/audit/*` and `/internal/admin/distribution` |
| `INTERNAL_OPS_TOKEN` | *(required)* | `aggregator` | Bearer token for `/internal/dlq` routes. Generate with `openssl rand -hex 32` |
| `INSTANCE_ID` | *(hostname)* | `aggregator` | Instance identifier sent in `X-Instance-ID` and stored as lease holder |
| `AGGREGATOR_LEASE_TTL_S` | `15` | `aggregator` | Leader lease TTL in seconds (must be `> 2 ×` renew interval) |
| `AGGREGATOR_LEASE_RENEW_INTERVAL_S` | `5` | `aggregator` | Leader lease renewal interval in seconds |
| `AGGREGATOR_SHUTDOWN_DRAIN_S` | `5` | `aggregator` | Graceful shutdown readiness drain period in seconds |
| `SHARD_QUERY_TIMEOUT_MS` | `800` | `aggregator` | Per-shard scatter-gather query timeout (ms) before returning `partial: true` |
| `SATELLITE_FEED_URL` | `https://satellite.naufarrel.dev` | `satellite-ingestor` | Satellite feed base URL |
| `SATELLITE_FEED_TOKEN` | *(required)* | `satellite-ingestor` | Group token from the assistants. Keep it in `.env` / a Secret only |
| `SATELLITE_METRICS_PORT` | `9091` | `satellite-ingestor` | Host port for `/metrics` and `/healthz` |
| `SATELLITE_STORE_POSTGRES_USER` / `_PASSWORD` / `_DB` | `satellite` / *(required)* / `satellite` | `satellite-store` | PostgreSQL bootstrap credentials |
| `NATS_URL` | `nats://nats-jetstream:4222` | `satellite-ingestor` | Broker for `hazard.ingest.SATELIT` |

The satellite ingestor exits at startup if `DATABASE_URL` or `SATELLITE_FEED_TOKEN` is empty, and applies its own schema on start, so `satellite-store` needs no init script.
