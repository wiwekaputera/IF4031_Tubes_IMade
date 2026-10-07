# IF4031 Milestone 1 — BNPB Disaster Coordination System

> **Course:** IF4031 — Arsitektur Aplikasi Terdistribusi  
> **Milestone:** 1 — Distributed Systems Foundation  
> **Architecture & Contracts:** [ARCHITECTURE.md](ARCHITECTURE.md)  

---

## Service Status & Overview

All 11 microservices in the distributed system compile, have container health checks, and build cleanly in Docker Compose.

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
| `token-db` | *internal* | `token_net` | 🟢 Operational | PostgreSQL 16 for refresh token persistence and rotation audit |
| `token-service` | `8083` | `edge_net`, `token_net` | 🟢 Operational | OAuth2 client credentials, refresh token rotation, ES256 signing, and JWKS |
| `client-api` | `8080` | `edge_net`, `internal_api_net` | 🟡 Gateway Active | Edge HTTP gateway with health check; scope projection & rate limiter in progress |

---

## Quick Start

### 1. Configure Environment Variables
Copy the template and verify secrets/keys (the default values work out of the box for local development):
```bash
cp .env.example .env
```

### 2. Launch the System

#### Option A: Launch Complete Distributed System (All 11 Services)
To build and spin up the complete cluster including OAuth2 token service and consumers:
```bash
docker compose up -d --build
```

#### Option B: Launch Upstream Ingestion & Aggregator Pipeline
To focus specifically on upstream ingestion (BMKG, PVMBG, Canonical Store, Aggregator, JetStream), use the automated runner:
```bash
bash scripts/demo/up.sh
```

### 3. Verify Operational Readiness

#### Automated Smoke Test Suite
Run the 12-check automated smoke test suite to validate upstream health, seed data ingestion, outage simulation, and PostgreSQL storage:
```bash
bash scripts/demo/smoke.sh
```

#### Run Unit Tests Locally
All services can also be tested natively without Docker:
```bash
for d in services/*; do
  if [ -f "$d/go.mod" ]; then
    echo "Testing $d..."
    (cd "$d" && go test ./...)
  fi
done
```

### 4. Teardown
To cleanly stop containers, remove networks, and wipe volumes:
```bash
bash scripts/demo/down.sh
# or
docker compose down -v
```

---

## Live Logging & Observability

All services utilize **Zerolog** to output structured JSON logs directly to `stdout`.

To follow logs in real time:
```bash
# Stream aggregator logs
docker compose logs -f aggregator

# Stream multiple ingestion services together
docker compose logs -f aggregator bmkg-mock pvmbg-mock

# Pretty-print structured JSON with jq
docker compose logs -f aggregator | jq .

# Trace a specific request across all services by Correlation ID
docker compose logs -f | grep "<correlation-id>"
```

---

## Demo Scenarios

Detailed execution walkthroughs and expected outputs are documented in [scripts/demo/README.md](scripts/demo/README.md).

### Problem 1 — Schema Evolution (Tolerant Reader)
1. **Initial state:** Query PVMBG mock; records contain standard fields without `confidence_level`:
   ```bash
   curl -s -H "Authorization: Bearer test-pvmbg-token" "http://localhost:8082/volcanic-reports?limit=1"
   ```
2. **Trigger schema change at runtime:**
   ```bash
   curl -s -X POST -H "Authorization: Bearer test-pvmbg-token" "http://localhost:8082/admin/schema-version"
   ```
3. **Verify Aggregator adapts without restart:**
   Inspect the canonical PostgreSQL store; `confidence_level` dynamically lands in `attributes` (`JSONB`) with zero DDL migrations:
   ```bash
   docker compose exec -T canonical-store psql -U aggregator -d canonical \
     -c "SELECT source_ref_id, severity, attributes FROM canonical_events WHERE source = 'PVMBG' AND attributes->>'confidence_level' IS NOT NULL ORDER BY occurred_at DESC LIMIT 2;"
   ```

---

### Problem 2 — Concurrency, PVMBG Outage & Circuit Breaker
1. **Head-of-line blocking prevention:**
   PVMBG's configured delay (up to 3s) does not degrade BMKG latency (verified p95 < 150 ms):
   ```bash
   curl -o /dev/null -s -w 'BMKG latency: %{time_total}s\n' -H "X-BMKG-Key: test-bmkg-key" "http://localhost:8081/seismic-events?limit=5"
   ```
2. **Trigger PVMBG Outage:**
   ```bash
   curl -s -X POST -H "Authorization: Bearer test-pvmbg-token" "http://localhost:8082/admin/outage"
   ```
3. **Verify Graceful Degradation:**
   Aggregator trips its circuit breaker to `OUTAGE`. Querying Aggregator's internal query API serves cached historical data with `status: "OUTAGE"` and `stale_since` metadata rather than failing with HTTP 500 or timing out:
   ```bash
   docker run --rm --net if4031-tubes-imade_internal_api_net curlimages/curl:8.11.0 -s "http://aggregator:8090/internal/events?limit=1"
   ```
4. **Restore Service:**
   ```bash
   curl -s -X POST -H "Authorization: Bearer test-pvmbg-token" "http://localhost:8082/admin/outage"
   ```
   The circuit breaker probes upstream and automatically returns to `UP` state without service restart.

---

### Problem 3 — Service Authentication, OAuth2 & Scope Projection

#### 1. Upstream Credential Isolation
* Submitting PVMBG Bearer token to BMKG returns `403 Forbidden`:
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer test-pvmbg-token" http://localhost:8081/seismic-events
  ```
* Submitting BMKG API Key to PVMBG returns `403 Forbidden`:
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" -H "X-BMKG-Key: test-bmkg-key" http://localhost:8082/volcanic-reports
  ```

#### 2. Downstream OAuth2 Token Service (`token-service`)
* **Obtain Access Token (Client Credentials Grant):**
  ```bash
  # Media Client (scope: media)
  curl -s -X POST http://localhost:8083/oauth/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=media-client&client_secret=secret-media" | jq .

  # Field Team Client (scope: field_team, includes refresh_token)
  curl -s -X POST http://localhost:8083/oauth/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=field-team-client&client_secret=secret-field-team" | jq .
  ```
* **Retrieve Active JWKS Public Keys:**
  ```bash
  curl -s http://localhost:8083/.well-known/jwks.json | jq .
  ```
* **Refresh Token Rotation & Theft Detection:**
  Send the issued refresh token to `/oauth/token` with `grant_type=refresh_token`. Reusing an invalidated refresh token immediately triggers theft detection, revoking the token family.

---

### Problem 4 — Microservice Isolation & Flexible Storage
1. **Independent Process Lifecycles:**
   Restart the Aggregator container independently without affecting upstream mocks or the database:
   ```bash
   docker compose restart aggregator
   ```
   Verify upstream mocks stay healthy throughout the restart:
   ```bash
   curl -s http://localhost:8081/health
   ```
2. **Transactional Outbox & Zero-DDL Dynamic Storage:**
   Pre- and post-schema evolution records coexist seamlessly inside PostgreSQL `canonical_events`:
   ```bash
   docker compose exec -T canonical-store psql -U aggregator -d canonical \
     -c "SELECT source_ref_id, attributes FROM canonical_events WHERE source = 'PVMBG' ORDER BY occurred_at DESC LIMIT 4;"
   ```

---

### Problem 5 — Fan-out Event Distribution (NATS JetStream)
1. **Follow Durable Consumers:**
   ```bash
   docker compose logs -f aggregator notification-consumer audit-consumer stub-sink
   ```
2. **Inspect Notifications Accepted by Stub Sink:**
   ```bash
   docker run --rm --network if4031-tubes-imade_bus_net curlimages/curl:8.11.0 -s http://stub-sink:8085/received | jq .
   ```
3. **Fault Tolerance (Stop One Consumer):**
   ```bash
   docker compose stop notification-consumer
   ```
   *The producer and `audit-consumer` continue processing unaffected.*
4. **Restart Consumer & Verify Catch-up:**
   ```bash
   sleep 10
   docker compose start notification-consumer
   docker compose logs -f notification-consumer
   ```
   The durable consumer retrieves missed messages from JetStream without message loss or duplication.

---

## Load Testing (Problem 2)

Load testing is authored in [tests/load/k6_p2.js](tests/load/k6_p2.js).

To execute the sustained load test (50 VUs for 60s):
```bash
# 1. Obtain Bearer token from token-service
TOKEN=$(curl -s -X POST http://localhost:8083/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=field-team-client&client_secret=secret-field-team" \
  | jq -r .access_token)

# 2. Run k6 with the active token
k6 run -e TOKEN=$TOKEN tests/load/k6_p2.js
```

---

## Key Configuration Variables

All configuration is managed through environment variables defined in [.env.example](.env.example):

| Variable | Default | Service | Description |
| :--- | :---: | :--- | :--- |
| `BMKG_API_KEY` | `test-bmkg-key` | `bmkg-mock`, `aggregator` | API key for BMKG authentication |
| `PVMBG_BEARER_TOKEN` | `test-pvmbg-token` | `pvmbg-mock`, `aggregator` | Bearer token for PVMBG authentication |
| `POLL_INTERVAL_S` | `3` | `aggregator` | Upstream polling interval in seconds |
| `PVMBG_CB_THRESHOLD` | `3` | `aggregator` | Consecutive failure threshold to trip circuit breaker |
| `PVMBG_CB_PROBE_INTERVAL` | `30` | `aggregator` | Half-open probe interval in seconds |
| `TOKEN_DB_DSN` | `postgres://token_user:...` | `token-service` | PostgreSQL connection string for refresh tokens |
| `ES256_PRIVATE_KEY_B64` | `...` | `token-service` | Base64-encoded SEC1 EC private key for JWT signing |
| `ACCESS_TOKEN_TTL` | `60` | `token-service` | Access token TTL in seconds (default: 60s) |
| `REFRESH_TOKEN_TTL_DAYS`| `7` | `token-service` | Refresh token lifespan in days |
| `CLIENT_API_RPS_LIMIT` | `100` | `client-api` | Token bucket request rate limit (req/s) |
| `CLIENT_API_BURST` | `20` | `client-api` | Token bucket maximum burst capacity |
