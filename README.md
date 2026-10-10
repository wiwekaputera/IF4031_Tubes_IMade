# IF4031 Milestone 1 — BNPB Disaster Coordination System

> **Course:** IF4031 — Arsitektur Aplikasi Terdistribusi  
> **Milestone:** 1 — Distributed Systems Foundation  

---

## Service Status & Overview

All 11 services build from a clean checkout and start with a single `docker compose up`. Every container has a health check.

| Service | Port | Network | Status | Description |
| :--- | :---: | :--- | :---: | :--- |
| `bmkg-mock` | `8081` | `agency_net` | 🟢 Operational | Simulated BMKG seismic & tsunami API with jittered delay and `X-BMKG-Key` auth |
| `pvmbg-mock` | `8082` | `agency_net` | 🟢 Operational | Simulated PVMBG volcanic API with `/admin/outage`, `/admin/schema-version`, and Bearer auth |
| `canonical-store` | *internal* | `store_net` | 🟢 Operational | PostgreSQL 16+ JSONB store (`canonical_events`, `outbox`, `volcano_reference`) |
| `aggregator` | *internal* (`8090`) | `agency_net`, `store_net`, `internal_api_net`, `bus_net` | 🟢 Operational | Poller, normalizer, circuit breaker, transactional outbox relay, and query API |
| `nats-jetstream` | *internal* (`4222`) | `bus_net` | 🟢 Operational | NATS 2.10 JetStream event broker (`BNPB_EVENTS` stream) |
| `stub-sink` | *internal* (`8085`) | `bus_net` | 🟢 Operational | Notification target endpoint with idempotency verification (`GET /received`) |
| `notification-consumer` | *internal* (`8086`) | `bus_net` | 🟢 Operational | Durable JetStream consumer with local bbolt deduplication forwarding to stub sink |
| `audit-consumer` | *internal* (`8087`) | `bus_net` | 🟢 Operational | Durable JetStream consumer appending disaster event history to local bbolt |
| `token-db` | *internal* | `token_net` | 🟢 Operational | PostgreSQL store for refresh tokens and reuse detection |
| `token-service` | `8083` | `edge_net`, `token_net` | 🟢 Operational | OAuth 2.0 token service with ECDSA P-256 (ES256) signing & JWKS endpoint |
| `client-api` | `8080` | `edge_net`, `internal_api_net` | 🟢 Operational | Edge REST gateway with scope projection, rate limiting, and aggregator proxy |

Ports `8080`–`8083` can be changed with `CLIENT_API_PORT`, `BMKG_PORT`, `PVMBG_PORT`, `TOKEN_SERVICE_PORT` in `.env`.

---

## Prerequisites

| Tool | Notes |
| :--- | :--- |
| Docker Engine + Docker Compose v2 | `docker compose version` |
| `bash` | Linux/macOS shell, or **Git Bash** / WSL on Windows |
| `curl`, `openssl` | Included in Git Bash; `openssl` is used to generate the ES256 signing key |
| Python 3 | Used by the demo scripts to parse JSON. `python3` or `python` (Windows) are both detected automatically |
| k6 *(optional)* | Not required: `scripts/demo/p2-load.sh` runs k6 from the `grafana/k6` Docker image by default |

---

## Quick Start

### 1. Start everything (recommended)
```bash
bash scripts/demo/up.sh
```
On the first run `up.sh`:
1. copies `.env.example` to `.env` if `.env` does not exist yet;
2. generates a local ES256 signing key with `scripts/keys/gen-es256.sh` and writes `ES256_PRIVATE_KEY_B64` into `.env` if it is still the `change-me-…` placeholder (token-service refuses to start without a real key);
3. builds and starts **all 11 services** and waits until every one of them is `healthy`.

### 1b. Manual alternative
```bash
cp .env.example .env
bash scripts/keys/gen-es256.sh          # copy the printed ES256_PRIVATE_KEY_B64=... line into .env
# (optional) replace the other change-me-* secrets in .env with your own values
docker compose up -d --build
docker compose ps                       # wait until every service is (healthy), ~1 min
```

### 2. Run the automated checks
| Script | Covers | Duration |
| :--- | :--- | :--- |
| `bash scripts/demo/smoke.sh` | Mocks, PVMBG outage toggle, aggregator health/events, canonical store (P1/P2 ingest) | ~30 s |
| `bash scripts/demo/p3.sh` | Problem 3 end-to-end, 16 checks (tokens, projection, expiry, refresh, reuse detection, 429, cross-credentials, correlation ID) | ~2 min |
| `bash scripts/demo/p2-load.sh` | Problem 2 criteria #1–#2: PVMBG delay 3 s + mixed SEISMIC/VOLCANIC load, 50 VUs × 60 s | ~1.5 min |
| `bash scripts/demo/p5.sh` | Problem 5: fan-out, stop/restart consumer, catch-up, **add a new consumer without changing the producer** | ~1.5 min |

Each script prints `[PASS]`/`[FAIL]` per check and exits non-zero on failure.

### 3. Teardown
```bash
bash scripts/demo/down.sh        # stop + remove volumes and locally built images
# or
docker compose down -v           # stop + remove volumes (next start begins with empty DBs)
```

### Using the credentials in manual commands
All manual commands below read secrets from `.env`, so load it into your shell first:
```bash
set -a; source .env; set +a
```

---

## Live Logging & Observability

All services use **Zerolog** to write structured JSON logs to `stdout`. Every request carries an `X-Correlation-ID` (generated if absent) that is propagated client-api → aggregator, and every outbound call logs its `latency_ms`.

```bash
# Stream aggregator logs
docker compose logs -f aggregator

# Stream multiple ingestion services together
docker compose logs -f aggregator bmkg-mock pvmbg-mock

# Trace one request across services by Correlation ID
curl -s -o /dev/null -H "Authorization: Bearer $TOKEN" -H "X-Correlation-ID: demo-123" http://localhost:8080/v1/events
docker compose logs client-api aggregator | grep demo-123
```

---

## Demo Scenarios

Detailed walkthroughs and expected outputs are also in [scripts/demo/README.md](scripts/demo/README.md). Helper used below to obtain an access token:

```bash
set -a; source .env; set +a
tok(){ curl -s -X POST http://localhost:8083/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$1&client_secret=$2" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])'; }   # Windows Git Bash: python
OPS_TOKEN=$(tok "$CLIENT_INTERNAL_OPS_ID" "$CLIENT_INTERNAL_OPS_SECRET")
```

### Problem 1 — Schema Evolution (Tolerant Reader)
Unknown fields are **forwarded into `attributes`** (policy: additive-only schema changes).
1. **Initial state:** PVMBG records have no `confidence_level`:
   ```bash
   curl -s -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" "http://localhost:8082/volcanic-reports?limit=1"
   ```
2. **Trigger the schema change at runtime (no restart):**
   ```bash
   curl -s -X POST http://localhost:8082/admin/schema-version     # {"confidence_level":true,...}
   ```
3. **Verify the aggregator adapts without restart** (wait ~20 s for new reports):
   ```bash
   curl -s -H "Authorization: Bearer $OPS_TOKEN" "http://localhost:8080/v1/events?type=VOLCANIC&limit=3"   # attributes.confidence_level
   docker compose exec -T canonical-store psql -U aggregator -d canonical \
     -c "SELECT source_ref_id, attributes FROM canonical_events WHERE source = 'PVMBG' ORDER BY ingested_at DESC LIMIT 6;"
   docker compose ps aggregator     # still the same container, not restarted
   ```
   Call `POST /admin/schema-version` again to switch the field off.

---

### Problem 2 — Concurrency, PVMBG Outage & Circuit Breaker
1. **No head-of-line blocking + sustained load (criteria #1 and #2):**
   ```bash
   bash scripts/demo/p2-load.sh
   ```
   The script recreates only `pvmbg-mock` with a fixed **3 s delay**, then runs [tests/load/k6_p2.js](tests/load/k6_p2.js) against `client-api`: SEISMIC-only and VOLCANIC requests **at the same time**, 50 VUs for 60 s. Afterwards it restores the delay from `.env`. Read from the summary:
   - `bmkg_only_latency` p(95) — must be < 300 ms (threshold enforced);
   - `http_reqs` (throughput), p50 (`med`)/p95/p99;
   - `error_rate` (excludes 429) — must be < 1 %;
   - `rate_limited_total` — number of controlled HTTP 429 rejections.
2. **Trigger a PVMBG outage:**
   ```bash
   curl -s -X POST http://localhost:8082/admin/outage      # {"outage":true}
   ```
3. **Verify graceful degradation through the gateway** (wait ~15 s for the circuit breaker to open):
   ```bash
   curl -s -w '\nHTTP %{http_code} in %{time_total}s\n' -H "Authorization: Bearer $OPS_TOKEN" "http://localhost:8080/v1/events?type=VOLCANIC&limit=1"
   #   -> 200 quickly, last stored data, meta.sources.pvmbg = {"status":"OUTAGE","stale_since":"..."}
   curl -s -w '\nHTTP %{http_code}\n' -H "Authorization: Bearer $OPS_TOKEN" "http://localhost:8080/v1/events?type=SEISMIC&limit=1"
   #   -> seismic data served normally
   ```
4. **Restore the service:**
   ```bash
   curl -s -X POST http://localhost:8082/admin/outage      # {"outage":false}
   ```
   The circuit breaker probes PVMBG every `AGGREGATOR_PVMBG_CB_PROBE_INTERVAL` seconds (30 s) and returns to `UP` without restarting anything.

---

### Problem 3 — Service Authentication & Scope Projection

Run all checks at once with `bash scripts/demo/p3.sh`, or step by step:

1. **Upstream credential isolation** — each agency rejects the other agency's credential with `403`:
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" http://localhost:8081/seismic-events
   curl -s -o /dev/null -w "%{http_code}\n" -H "X-BMKG-Key: $BMKG_API_KEY" http://localhost:8082/volcanic-reports
   ```

2. **OAuth 2.0 token issuance (`token-service` :8083):** three separate client identities.
   ```bash
   MEDIA_TOKEN=$(tok "$CLIENT_MEDIA_ID" "$CLIENT_MEDIA_SECRET")
   FIELD_RESP=$(curl -s -X POST http://localhost:8083/oauth/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=client_credentials&client_id=$CLIENT_FIELD_TEAM_ID&client_secret=$CLIENT_FIELD_TEAM_SECRET")
   FIELD_TOKEN=$(echo "$FIELD_RESP" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
   REFRESH_TOKEN=$(echo "$FIELD_RESP" | python3 -c 'import sys,json;print(json.load(sys.stdin)["refresh_token"])')
   ```
   Only `field_team` receives a `refresh_token`.

3. **Scope-based field projection (`client-api` :8080):**
   - `media` only receives summary fields (no `latitude`, `longitude`, `attributes`, `source_ref_id`):
     ```bash
     curl -s -H "Authorization: Bearer $MEDIA_TOKEN" http://localhost:8080/v1/events
     ```
   - `media` explicitly requesting raw data (`view=full`) is rejected with `403 insufficient_scope`:
     ```bash
     curl -s -w "\nHTTP: %{http_code}\n" -H "Authorization: Bearer $MEDIA_TOKEN" "http://localhost:8080/v1/events?view=full"
     ```
   - `field_team` (and `internal_ops`) receive every field:
     ```bash
     curl -s -H "Authorization: Bearer $FIELD_TOKEN" "http://localhost:8080/v1/events?view=full"
     ```

4. **Token expiry, refresh rotation & reuse detection:**
   ```bash
   sleep 66      # ACCESS_TOKEN_TTL=60 (+5 s leeway)
   curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $FIELD_TOKEN" http://localhost:8080/v1/events   # 401 expired
   curl -s -X POST http://localhost:8083/oauth/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=refresh_token&client_id=$CLIENT_FIELD_TEAM_ID&client_secret=$CLIENT_FIELD_TEAM_SECRET&refresh_token=$REFRESH_TOKEN"
   #   -> 200: new access token + rotated refresh token, no manual re-login
   ```
   Presenting the **old** refresh token again returns `400 invalid_grant` and revokes the client's refresh tokens (theft/reuse detection).

5. **Rate limiting:** each client has a token bucket (`CLIENT_API_RPS_LIMIT`=100 req/s, `CLIENT_API_BURST`=20). Excess requests get `429 Too Many Requests` with `Retry-After`. `p3.sh` scenario 10 fires a parallel burst from inside the Docker network to show this.

6. **No committed secrets:**
   ```bash
   git ls-files | grep -E '\.env$|\.pem$|\.key$'      # empty: only .env.example is tracked
   ```

---

### Problem 4 — Microservice Isolation & Flexible Storage
1. **Stop, rebuild and restart one service without touching the others:**
   ```bash
   docker compose ps --format '{{.Service}} {{.CreatedAt}}'    # note creation times
   docker compose stop aggregator
   # while the aggregator is down, services that do not depend on it keep serving:
   curl -s -o /dev/null -w "token-service: %{http_code}\n" -X POST http://localhost:8083/oauth/token \
     -d "grant_type=client_credentials&client_id=$CLIENT_MEDIA_ID&client_secret=$CLIENT_MEDIA_SECRET"
   curl -s -o /dev/null -w "bmkg-mock: %{http_code}\n" http://localhost:8081/health
   docker compose build aggregator
   docker compose up -d --no-deps aggregator
   docker compose ps --format '{{.Service}} {{.CreatedAt}}'    # only aggregator changed
   ```
2. **Zero-migration dynamic storage:** records from before and after the P1 schema change coexist in `canonical_events.attributes` (JSONB):
   ```bash
   docker compose exec -T canonical-store psql -U aggregator -d canonical \
     -c "SELECT source_ref_id, attributes FROM canonical_events WHERE source = 'PVMBG' ORDER BY ingested_at DESC LIMIT 8;"
   ```
3. **Canonical Store is only reachable by the aggregator:** it lives on the internal `store_net`, which only `aggregator` joins. `client-api` reads through `http://aggregator:8090/internal/events`.

---

### Problem 5 — Fan-out Event Distribution (NATS JetStream)
Run all checks at once with `bash scripts/demo/p5.sh`, or step by step:

1. **Two independent durable consumers on the same stream:**
   ```bash
   docker compose logs -f notification-consumer audit-consumer    # same event_key reaches both
   ```
2. **Stop one consumer** — the producer and `audit-consumer` keep running:
   ```bash
   docker compose stop notification-consumer
   ```
3. **Restart it and watch it catch up** — the durable consumer resumes from its last ack; JetStream keeps messages for 24 h:
   ```bash
   sleep 30
   docker compose start notification-consumer
   docker compose logs --since 1m notification-consumer
   ```
4. **Add a new consumer without changing producer code** (nats-box CLI on `bus_net`):
   ```bash
   # live subscriber on the existing subject
   docker run --rm -it --network if4031-tubes-imade_bus_net natsio/nats-box:latest \
     nats -s nats://nats-jetstream:4222 sub 'bnpb.events.>'

   # or a new durable consumer on the existing stream (replays history)
   docker run --rm -it --network if4031-tubes-imade_bus_net natsio/nats-box:latest sh -c '
     nats -s nats://nats-jetstream:4222 consumer add BNPB_EVENTS dashboard-demo \
       --pull --filter "bnpb.events.>" --deliver all --ack explicit --defaults &&
     nats -s nats://nats-jetstream:4222 consumer next BNPB_EVENTS dashboard-demo --count 3'
   ```
5. **Notifications accepted by the stub sink:**
   ```bash
   docker run --rm --network if4031-tubes-imade_bus_net curlimages/curl:8.11.0 -s http://stub-sink:8085/received
   ```

---

## Load Testing (Problem 2)

[tests/load/k6_p2.js](tests/load/k6_p2.js) runs two scenarios in parallel against `client-api`: `seismic_only` (`type=SEISMIC`) and `volcanic` (`type=VOLCANIC`), 25 + 25 VUs for 60 s. Each VU fetches its own token and refreshes it before expiry. HTTP 429 is counted separately (`rate_limited_total`) and is not treated as a failure.

```bash
bash scripts/demo/p2-load.sh                   # k6 in Docker on edge_net (default, no local k6 needed)
K6_MODE=local bash scripts/demo/p2-load.sh     # use a locally installed k6 against localhost:8080
VUS=50 DURATION=90s bash scripts/demo/p2-load.sh
```

The default Docker mode measures the gateway itself. With `K6_MODE=local` on Windows/macOS, Docker Desktop's host port forwarding adds latency under heavy load, so expect higher percentiles there.

---

## Configuration Variables

All variables live in [.env.example](.env.example). `docker-compose.yml` maps the prefixed names in `.env` to the short names each service reads (e.g. `CLIENT_API_RPS_LIMIT` → `RPS_LIMIT`).

Every service validates its configuration at startup (fail-fast):
- **Credentials, signing keys and DSNs have no default and are required.** If one is empty, the service logs `failed to load configuration` and exits. This applies to the mock keys, the aggregator's upstream credentials and `DB_DSN`, `TOKEN_DB_DSN`, `ES256_PRIVATE_KEY_B64`, and the three client secrets.
- **Numeric values must be valid integers in range** (e.g. intervals, timeouts and limits > 0, delay max ≥ min). An invalid value stops the service instead of being silently replaced.
- Everything else (ports, URLs, client IDs, issuer/audience, log level) falls back to the default shown below.

| Variable | Default | Service | Description |
| :--- | :---: | :--- | :--- |
| `BMKG_API_KEY` | `change-me-bmkg-api-key` | `bmkg-mock` | API key BMKG accepts in `X-BMKG-Key` |
| `BMKG_DELAY_MIN_MS` / `BMKG_DELAY_MAX_MS` | `50` / `150` | `bmkg-mock` | Response delay range (ms) |
| `BMKG_EVENT_INTERVAL_S` | `10` | `bmkg-mock` | New seismic event every N seconds |
| `BMKG_PORT` | `8081` | `bmkg-mock` | Host + container port |
| `PVMBG_BEARER_TOKEN` | `change-me-pvmbg-bearer-token` | `pvmbg-mock` | Bearer token PVMBG accepts |
| `PVMBG_DELAY_MIN_MS` / `PVMBG_DELAY_MAX_MS` | `500` / `3000` | `pvmbg-mock` | Response delay range (ms) |
| `PVMBG_EVENT_INTERVAL_S` | `10` | `pvmbg-mock` | New volcanic report every N seconds |
| `PVMBG_PORT` | `8082` | `pvmbg-mock` | Host + container port |
| `AGGREGATOR_BMKG_BASE_URL` | `http://bmkg-mock:8081` | `aggregator` | BMKG base URL |
| `AGGREGATOR_BMKG_API_KEY` | = `BMKG_API_KEY` | `aggregator` | Credential the aggregator sends to BMKG |
| `AGGREGATOR_BMKG_TIMEOUT_MS` | `1000` | `aggregator` | Per-request timeout to BMKG |
| `AGGREGATOR_PVMBG_BASE_URL` | `http://pvmbg-mock:8082` | `aggregator` | PVMBG base URL |
| `AGGREGATOR_PVMBG_BEARER_TOKEN` | = `PVMBG_BEARER_TOKEN` | `aggregator` | Credential the aggregator sends to PVMBG |
| `AGGREGATOR_PVMBG_TIMEOUT_MS` | `4000` | `aggregator` | Per-request timeout to PVMBG |
| `AGGREGATOR_PVMBG_CB_THRESHOLD` | `3` | `aggregator` | Consecutive failures that open the circuit breaker |
| `AGGREGATOR_PVMBG_CB_PROBE_INTERVAL` | `30` | `aggregator` | Seconds between half-open probes |
| `AGGREGATOR_POLL_INTERVAL_S` | `3` | `aggregator` | Polling interval (spec: 2–5 s) |
| `AGGREGATOR_DB_DSN` | `postgres://aggregator:…@canonical-store:5432/canonical` | `aggregator` | Canonical store connection string |
| `AGGREGATOR_NATS_URL` | `nats://nats-jetstream:4222` | `aggregator` | Broker URL for the outbox relay |
| `AGGREGATOR_PORT` | `8090` | `aggregator` | Internal query API port |
| `CANONICAL_STORE_POSTGRES_USER` / `_PASSWORD` / `_DB` | `aggregator` / `change-me-pg-password` / `canonical` | `canonical-store` | PostgreSQL bootstrap credentials |
| `TOKEN_DB_DSN` | `postgres://tokenservice:…@token-db:5432/tokendb` | `token-service` | Token DB connection string |
| `ES256_PRIVATE_KEY_B64` | *(generate)* | `token-service` | Base64 PKCS#8 PEM P-256 key (`scripts/keys/gen-es256.sh`, done automatically by `up.sh`) |
| `ACCESS_TOKEN_TTL` | `60` | `token-service` | Access token lifetime (seconds) |
| `REFRESH_TOKEN_TTL_DAYS` | `7` | `token-service` | Refresh token lifetime (days) |
| `CLIENT_MEDIA_ID` / `CLIENT_MEDIA_SECRET` | `client-media` / `change-me-media-secret` | `token-service` | Media client credentials (scope `media`) |
| `CLIENT_FIELD_TEAM_ID` / `CLIENT_FIELD_TEAM_SECRET` | `client-field-team` / `change-me-field-team-secret` | `token-service` | Field team credentials (scope `field_team`, refresh allowed) |
| `CLIENT_INTERNAL_OPS_ID` / `CLIENT_INTERNAL_OPS_SECRET` | `client-internal-ops` / `change-me-internal-ops-secret` | `token-service` | Internal ops credentials (scope `internal_ops`) |
| `TOKEN_ISSUER` / `TOKEN_AUDIENCE` | `bnpb-token-service` / `bnpb-client-api` | `token-service`, `client-api` | JWT `iss` / `aud`; passed to both services, must match |
| `TOKEN_SERVICE_PORT` | `8083` | `token-service` | Host + container port |
| `TOKEN_DB_POSTGRES_USER` / `_PASSWORD` / `_DB` | `tokenservice` / `change-me-token-pg-password` / `tokendb` | `token-db` | PostgreSQL bootstrap credentials |
| `CLIENT_API_AGGREGATOR_BASE_URL` | `http://aggregator:8090` | `client-api` | Aggregator query API |
| `CLIENT_API_JWKS_URL` | `http://token-service:8083/.well-known/jwks.json` | `client-api` | Public keys used to verify JWTs |
| `CLIENT_API_RPS_LIMIT` | `100` | `client-api` | Token bucket refill rate per client (req/s) |
| `CLIENT_API_BURST` | `20` | `client-api` | Token bucket capacity per client |
| `CLIENT_API_PORT` | `8080` | `client-api` | Host + container port |
| `NATS_URL` | `nats://nats-jetstream:4222` | consumers | Broker URL |
| `NOTIFICATION_CONSUMER_STUB_SINK_URL` | `http://stub-sink:8085` | `notification-consumer` | Notification target |
| `NOTIFICATION_CONSUMER_BBOLT_PATH` | `/data/notification.db` | `notification-consumer` | Local dedup store |
| `AUDIT_CONSUMER_BBOLT_PATH` | `/data/audit.db` | `audit-consumer` | Local audit history store |
| `STUB_SINK_PORT` | `8085` | `stub-sink` | Internal port |
| `LOG_LEVEL` | `info` | all | Zerolog level |
