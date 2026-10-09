# Demo Scripts — Upstream Ingestion & Aggregator Pipeline

Self-contained, runnable evidence for the BMKG / PVMBG → aggregator → canonical-store
pipeline. The three scripts live in `scripts/demo/` and operate against the
compose project defined in `docker-compose.yml` (`name: if4031-tubes-imade`).

## Prerequisites

1. **Docker + Docker Compose** (Compose v2).
2. **`.env` file** at the repo root. `up.sh` creates it from `.env.example` on
   the first run and generates the ES256 signing key. Manually:
   ```bash
   cp .env.example .env
   bash scripts/keys/gen-es256.sh   # paste the ES256_PRIVATE_KEY_B64=... line into .env
   ```
   The scripts source `.env` themselves. For the manual `curl` commands below,
   load it into your shell first:
   ```bash
   set -a; source .env; set +a
   ```
3. **Python 3** on the host (`python3`, or `python` on Windows Git Bash; both
   are detected) — used by the scripts as a built-in JSON parser for response
   assertions (avoids requiring external tools like `jq`).
4. **Network access** on first run so Docker can pull `postgres:16-alpine`,
   `nats:2.10-alpine`, and `curlimages/curl:8.11.0` (cached after first use).
5. No host port is published for the **aggregator** (it is on
   `internal_api_net` only). `smoke.sh` handles this by running a
   `curlimages/curl` helper container on that network — see "Aggregator access"
   below.

## Scripts

| Script      | What it does                                                                 |
|-------------|------------------------------------------------------------------------------|
| `up.sh`     | Prepares `.env` on first run (copy from `.env.example`, generate ES256 key), then `docker compose up -d --build` for **all 11 services** and polls Docker health status until all are `healthy`. |
| `smoke.sh`  | Hits every health endpoint and key data endpoint for Milestone 1 (P1/P2 ingestion pipeline), asserts status + body, and prints PASS/FAIL summary. |
| `p3.sh`     | End-to-end integration demo for Problem 3 (P3): exercises OAuth 2.0 token issuance (`client_credentials` & `refresh_token`), JWT ES256 validation, per-client rate limiting, scope-based field projection, upstream credential isolation, and correlation ID propagation. |
| `p2-load.sh`| Problem 2 criteria #1–#2: recreates only `pvmbg-mock` with a 3 s delay, runs `tests/load/k6_p2.js` (SEISMIC-only + VOLCANIC in parallel, 50 VUs × 60 s) against `client-api`, then restores the delay. |
| `p5.sh`     | Problem 5: verifies the two durable consumers, stops/restarts `notification-consumer` and checks catch-up, then adds a **new** consumer on the existing stream with nats-box (no producer change). |
| `down.sh`   | `docker compose down -v --rmi local` — stops everything, removes named/anonymous volumes and locally-built images so the next `up.sh` starts from a clean state. |

## Aggregator access (no host port, distroless image)

The aggregator runs on `gcr.io/distroless/static-debian12` (no `curl`, no shell)
and publishes **no** host port. It is reachable as `http://aggregator:8090`
only from a container on the project's `internal_api_net`. `smoke.sh` detects
that network name automatically from `docker compose config` and starts a
single `curlimages/curl:8.11.0` helper container (`demo-smoke-curl`) joined to
it, then `docker exec`s `curl` from it for every aggregator request. The
container is removed on exit.

Override the image with `CURL_IMG=...` if needed.

## Expected output

`up.sh`:
```
[up] bmkg-mock is healthy
[up] pvmbg-mock is healthy
[up] canonical-store is healthy
[up] aggregator is healthy
[up] ===================================================
[up]   Ingestion pipeline is READY (all services healthy)
[up]   bmkg-mock       http://localhost:8081   (port 8081)
[up]   pvmbg-mock      http://localhost:8082   (port 8082)
[up]   aggregator      http://aggregator:8090  (internal_api_net only)
[up]   canonical-store postgresql store_net    (no host port)
[up] ===================================================
```

`smoke.sh` (summary line at the end):
```
=== BMKG mock (:8081) ===
  [PASS] GET /health -> 200 {"status":"UP"}
  [PASS] GET /seismic-events -> 200, count=44 (>=22)
=== PVMBG mock (:8082) ===
  [PASS] GET /health -> 200 {"status":"UP"}
  [PASS] GET /volcanic-reports -> 200, count=44 (>=22), volcano IDs GNG-001..GNG-005 present
=== PVMBG outage toggle (:8082) ===
  [PASS] POST /admin/outage -> 200, outage=True (toggled ON)
  [PASS] GET /volcanic-reports during outage -> 503
  [PASS] POST /admin/outage -> 200, outage=False (toggled OFF)
  [PASS] GET /volcanic-reports after outage recovery -> 200
=== Aggregator (:8090, via if4031-tubes-imade_internal_api_net) ===
  [PASS] GET /internal/health -> 200 {"status":"UP"}
  [PASS] GET /internal/events?limit=100 -> 200, meta.count=44 (>=22), sources bmkg/pvmbg UP with last_synced_at
=== Canonical store (psql) ===
  [PASS] psql \dt lists canonical_events, outbox, volcano_reference
  [PASS] SELECT count(*) FROM volcano_reference -> 5
 ==================================================
  Smoke test summary: 12 passed, 0 failed
 ==================================================
  RESULT: ALL CHECKS PASSED
```
(Exact `count` values grow over time as the background generators emit new
events — the assertions use `>=` thresholds.)

## How each check maps to the implementation

- `bmkg-mock:8081/health` and `pvmbg-mock:8082/health` are public; reached on
  the published host ports.
- `/seismic-events` requires header `X-BMKG-Key`; `/volcanic-reports` requires
  `Authorization: Bearer ...`. Values come from `.env`.
- `/internal/health` returns `{"status":"UP","time":...}` (aggregate liveness).
- Source-level health (`bmkg`/`pvmbg` `status` **UP** + `last_synced_at`) is
  returned by **GET `/internal/events`** in `meta.sources`, not in
  `/internal/health` — see
  `services/aggregator/internal/queryapi/handler.go:47-128`.
- `meta.count` is the number of events in the **returned page**
  (`handler.go:101-103`), so `smoke.sh` queries with `?limit=100` (the cap
  enforced at `handler.go:63`) to observe the full aggregate count. With
  `?limit=5` the page — and therefore `meta.count` — would be 5, not ≥22.
- `canonical-store` is queried via `docker compose exec -T canonical-store psql`
  (the `postgres:16-alpine` image ships `psql`). `down.sh -T` disables the
  pseudo-tty so it composes cleanly under `set -e`.

## Seed / timing reference

- 22 BMKG historical seismic events (`evt-bmkg-hist-001`..`022`) and 22 PVMBG
  historical volcanic reports (`rpt-pvmbg-hist-001`..`022`).
- PVMBG reports use volcano IDs `GNG-001`..`GNG-005`, which match the five rows
  seeded in `services/canonical-store/migrations/init.sql`.
- The background generators emit ≥1 new event per `BMKG_EVENT_INTERVAL_S` /
  `PVMBG_EVENT_INTERVAL_S` seconds (default 10s); the aggregator polls each
  upstream every `AGGREGATOR_POLL_INTERVAL_S` seconds (default 3s).

---

## Problem Demonstration Guide (P1, P2, P4)

This section provides exact, copy-paste runnable commands to demonstrate compliance with the evaluation criteria for **Problem 1**, **Problem 2**, and **Problem 4**.

---

### Problem 1: Arsitektur & Interoperabilitas Data (Schema Evolution)

#### 1. Baseline: Initial Schema Mapping (Both Sources)
Ensure the stack is running via `bash scripts/demo/up.sh`. Query the initial schema on PVMBG:
```bash
curl -s -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/volcanic-reports?limit=1"
```
**Expected Output:** Reports contain standard fields without `confidence_level`:
```json
[{"report_id":"rpt-pvmbg-hist-022","volcano_id":"GNG-001","alert_level":"Siaga","eruption_count_24h":2,"ash_column_height_m":750,"reported_at":"2023-07-04T19:45:00Z"}]
```

Check the canonical store to verify Aggregator maps both sources into `HazardEvent`:
```bash
# PVMBG volcanic mapping (area_name joined from volcano_reference, alert_level -> severity)
docker compose exec -T canonical-store psql -U aggregator -d canonical \
  -c "SELECT source, hazard_type, area_name, severity, attributes FROM canonical_events WHERE source = 'PVMBG' AND attributes->>'confidence_level' IS NULL ORDER BY occurred_at DESC LIMIT 1;"

# BMKG seismic mapping (magnitude, depth, tsunami correlation)
docker compose exec -T canonical-store psql -U aggregator -d canonical \
  -c "SELECT source, hazard_type, area_name, severity, attributes FROM canonical_events WHERE source = 'BMKG' ORDER BY occurred_at DESC LIMIT 1;"
```

#### 2. Trigger Schema Evolution at Runtime (No Restart / No Redeploy)
Trigger the schema change on the live PVMBG mock container:
```bash
curl -s -X POST -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/admin/schema-version"
```
**Expected Output:**
```json
{"confidence_level":true,"message":"schema version toggled"}
```

Confirm PVMBG now emits `confidence_level`:
```bash
curl -s -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/volcanic-reports?limit=1"
```

#### 3. Verify Aggregator Adapts Dynamically (No Crash, No Manual Intervention)
Wait 3–5 seconds for the next polling cycle, then inspect the canonical store:
```bash
docker compose exec -T canonical-store psql -U aggregator -d canonical \
  -c "SELECT source_ref_id, severity, attributes FROM canonical_events WHERE source = 'PVMBG' AND attributes->>'confidence_level' IS NOT NULL ORDER BY occurred_at DESC LIMIT 2;"
```
**Expected Output:**
```text
           source_ref_id            | severity |                                     attributes                                     
------------------------------------+----------+------------------------------------------------------------------------------------
 rpt-pvmbg-dyn-20261003T...-0142    | AWAS     | {"confidence_level": 0.762, "eruption_count_24h": 1, "ash_column_height_m": 564.2}
```
Aggregator continues normal ingestion without errors or service restart.

#### 4. Architecture Report Notes (P1)
- **Declared Unknown Field Policy**: *Forward to `attributes` (Tolerant Reader with Additive Passthrough)*.
- **Mechanism**: JSON Tolerant Reader pattern. In `services/aggregator/internal/agency/model.go`, `VolcanicReport.UnmarshalJSON` decodes recognized core fields into struct members and sweeps all unexpected keys into `Extra map[string]json.RawMessage`. The normalizer (`normalizer.go`) unmarshals `Extra` directly into `HazardEvent.Attributes`.
- **Supported vs Consciously Unsupported Evolutions**:
  - *Supported*: Additive fields (primitive/object/array), reordered keys, omitted optional/null fields.
  - *Consciously Unsupported*: Deletion or renaming of mandatory entity keys (`volcano_id`, `alert_level`), incompatible core type mutations (e.g. `alert_level` string changed to integer), non-JSON wire protocols.

---

### Problem 2: Concurrency, Availability, & Graceful Degradation

#### 1. Head-of-Line Blocking Prevention
Aggregator polls BMKG and PVMBG concurrently in independent goroutines per source cycle (`services/aggregator/internal/poller/poller.go`). Slow PVMBG responses never block BMKG polling.

Verify via the dedicated concurrent poller unit test:
```bash
(cd services/aggregator && go test -v -run TestPollConcurrent_BMKGNotBlockedBySlowPVMBG ./internal/poller)
```
**Live Demonstration with 3-second PVMBG Delay (through the gateway):**
```bash
bash scripts/demo/p2-load.sh
```
The script recreates only `pvmbg-mock` with `PVMBG_DELAY_MIN_MS=PVMBG_DELAY_MAX_MS=3000`,
then sends `GET /v1/events?type=SEISMIC` (BMKG-only) **and** `GET /v1/events?type=VOLCANIC`
to `client-api` at the same time, and restores the delay afterwards.
**Expected Result**: `bmkg_only_latency` p(95) < 300 ms. Reads are served from the
Canonical Store, so PVMBG's 3-second delay only affects ingestion, never the read path.

#### 2. Sustained Load Testing (≥50 VUs for ≥60s)
The same `p2-load.sh` run is the sustained test: 50 concurrent VUs (25 SEISMIC + 25
VOLCANIC) for 60 s. By default k6 runs from the `grafana/k6` image on `edge_net`
(`K6_MODE=local` uses a host k6 against `localhost:8080`). It reports and asserts:
- throughput (`http_reqs`), p50 (`med`) / p95 / p99 for every trend;
- `bmkg_only_latency`: p(95) < 300 ms;
- `error_rate`: < 1% (excluding deliberate HTTP 429 rate limiting);
- `rate_limited_total`: number of controlled 429 rejections;
- system stability with zero 5xx.

#### 3. Outage Simulation & Graceful Degradation
Simulate a full PVMBG outage at runtime:
```bash
curl -s -X POST -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/admin/outage"
```
**Verify Behavior During Outage:**
1. BMKG seismic events continue to ingest and serve without interruption.
2. PVMBG endpoint returns HTTP 503, tripping Aggregator's circuit breaker (`services/aggregator/internal/circuitbreaker/breaker.go`).
3. Aggregator Query API (`/internal/events`) continues to return HTTP 200, serving cached historical data from Canonical Store alongside explicit freshness indicators (`stale_since` and `status: "OUTAGE"`), rather than HTTP 500 or client connection timeouts:
   ```bash
   docker run --rm --net if4031-tubes-imade_internal_api_net curlimages/curl:8.11.0 -s "http://aggregator:8090/internal/events?limit=1" | python3 -m json.tool
   ```

**Outage Recovery:**
Restore PVMBG to operational status:
```bash
curl -s -X POST -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/admin/outage"
```
Aggregator probes upstream, automatically resets the circuit breaker to closed state, and resumes live updates with `status: "UP"` without any service restart.

---

### Problem 4: Arsitektur Microservice & Flexible Storage

#### 1. Independent Container Lifecycle
Demonstrate that all components are deployed in distinct containers:
```bash
docker compose ps
```
Stop, rebuild, or restart a single service independently:
```bash
# Restart PVMBG mock independently
docker compose restart pvmbg-mock

# Rebuild and restart Aggregator independently
docker compose up -d --no-deps --build aggregator
```
Verify other services (`bmkg-mock`, `canonical-store`) remain healthy and continue serving requests uninterrupted throughout the restart:
```bash
curl -s -o /dev/null -w 'BMKG HTTP: %{http_code}\n' -H "X-BMKG-Key: $BMKG_API_KEY" "http://localhost:8081/health"
```

#### 2. Flexible Storage without Schema Migration
Inspect records in PostgreSQL `canonical_events` before and after schema evolution:
```bash
docker compose exec -T canonical-store psql -U aggregator -d canonical \
  -c "SELECT source_ref_id, attributes FROM canonical_events WHERE source = 'PVMBG' ORDER BY occurred_at DESC LIMIT 4;"
```
**Expected Output:**
```text
           source_ref_id            |                                     attributes                                     
------------------------------------+------------------------------------------------------------------------------------
 rpt-pvmbg-dyn-20261003T...-0142    | {"confidence_level": 0.762, "eruption_count_24h": 1, "ash_column_height_m": 564.2}
 rpt-pvmbg-hist-022                 | {"eruption_count_24h": 2, "ash_column_height_m": 750}
```
Records before and after the schema change coexist side-by-side in the same table. The PostgreSQL `JSONB` column stores arbitrary additive attributes dynamically with no `ALTER TABLE` or database migration needed.

#### 3. Canonical Store Network Isolation
Confirm that `canonical-store` publishes **zero host ports** and is accessible strictly by `aggregator` on the internal network `store_net`:
```bash
docker inspect $(docker compose ps -q canonical-store) --format '{{json .NetworkSettings.Ports}}'
```
**Expected Output:** `{"5432/tcp":null}` (port 5432 is not bound to host interface). External consumers and client-facing APIs obtain data exclusively through Aggregator's API (`http://aggregator:8090/internal/events`).

---

## Problem 3 Demo (`p3.sh`)

`scripts/demo/p3.sh` verifies the complete token lifecycle, API gateway security, projection rules, and upstream protections against a running stack:

| # | Scenario | Expected Behavior |
|---|----------|-------------------|
| 1 | OAuth 2.0 `client_credentials` issuance | HTTP 200; only `client-field-team` receives a `refresh_token`, while `media` and `internal_ops` receive access tokens only. |
| 2 | Unauthenticated request (`GET /v1/events`) | HTTP 401 Unauthorized with header `WWW-Authenticate: Bearer error="invalid_token"`. |
| 3 | Tampered token signature | HTTP 401 Unauthorized (signature verification failure). |
| 4 | Media access (`GET /v1/events`) | HTTP 200 OK; response body is projected to summary fields only (`hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`), stripping coordinates (`latitude`, `longitude`) and attributes. |
| 5 | Media requesting full projection (`GET /v1/events?view=full`) | HTTP 403 Forbidden with `{"error": "insufficient_scope"}`. |
| 6 | Field team requesting full projection (`GET /v1/events?view=full`) | HTTP 200 OK with full fields included. |
| 7 | Expired access token | HTTP 401 Unauthorized after token TTL expires. |
| 8 | Token refresh (`grant_type=refresh_token`) | HTTP 200 OK; issues a new access token and a **rotated** refresh token. The new access token works immediately (no manual re-login). |
| 9 | Refresh token reuse detection | HTTP 400 Bad Request with `{"error": "invalid_grant"}` when attempting to reuse the previous refresh token. |
| 10 | Burst rate limiting | A parallel burst (5 × `BURST` requests, sent from a curl container on `edge_net`) yields HTTP 429 Too Many Requests with an integer `Retry-After` header. |
| 11 | Upstream credential isolation | Cross-credential presentations (PVMBG Bearer to BMKG, BMKG key to PVMBG) are both rejected with HTTP 403 Forbidden. |
| 12 | Correlation ID propagation | `X-Correlation-ID` sent by the client is forwarded to Aggregator and logged by both `client-api` and `aggregator`. |

Run the P3 demo:
```bash
bash scripts/demo/p3.sh
```

---

## Problem 5 Demo (`p5.sh`)

`scripts/demo/p5.sh` uses a `natsio/nats-box` helper container on `bus_net` (the broker publishes no host port):

| # | Scenario | Expected Behavior |
|---|----------|-------------------|
| 1 | Stream & consumers | `BNPB_EVENTS` has the durable consumers `notification-consumer` and `audit-consumer`; the stream's `last_seq` grows as the aggregator publishes. |
| 2 | Stop `notification-consumer` (`P5_STOP_SECONDS`, default 25 s) | Producer keeps publishing, `audit-consumer` keeps receiving, and messages for the stopped consumer wait in JetStream (`num_pending > 0`). |
| 3 | Start it again | `num_pending` drops back to 0: every missed event is caught up (durable consumer, 24 h retention). |
| 4 | New consumer without producer changes | `nats consumer add BNPB_EVENTS p5-demo-dashboard --filter 'bnpb.events.>'` creates a third consumer that immediately receives HazardEvents. It is removed afterwards unless `P5_KEEP_CONSUMER=1`. |

Run the P5 demo:
```bash
bash scripts/demo/p5.sh
```

---

## Re-run

```bash
cd /path/to/IF4031_Tubes_IMade
bash scripts/demo/up.sh       # prepare .env, build + start all services, wait for healthy
bash scripts/demo/smoke.sh    # P1/P2 ingest pipeline
bash scripts/demo/p3.sh       # P3 auth & projection
bash scripts/demo/p2-load.sh  # P2 load test (3 s PVMBG delay, 50 VUs x 60 s)
bash scripts/demo/p5.sh       # P5 fan-out + new consumer
bash scripts/demo/down.sh     # tear down + remove volumes (clean slate)
```

Re-run `up.sh` after `down.sh` to verify a fresh DB comes back to ≥22 events.
`up.sh` is idempotent — running it while the stack is already up is a no-op
that rebuilds images and re-confirms health.

