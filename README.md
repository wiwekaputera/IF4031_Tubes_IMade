# IF4031 Milestone 1 — BNPB Disaster Coordination System

> **Course:** IF4031 — Arsitektur Aplikasi Terdistribusi  
> **Milestone:** 1 — Distributed Systems Foundation  
> **Source of Truth:** [Specification.md](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/Specification.md)  
> **Architecture & Contracts:** [ARCHITECTURE.md](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/ARCHITECTURE.md)  
> **Work Distribution:** [WORKDIST.md](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/WORKDIST.md)  

---

## Service Status & Overview

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
| `token-db` | *internal* | `token_net` | 🟡 Schema Ready | PostgreSQL schema initialized for refresh token storage |
| `token-service` | `8083` | `edge_net`, `token_net` | 🔴 In Development | OAuth2 client credentials & refresh token service (Person B) |
| `client-api` | `8080` | `edge_net`, `internal_api_net` | 🔴 In Development | Edge REST gateway with scope projection and rate limiting (Person B) |

---

## Quick Start (Current Operational Pipeline)

Because edge services (`token-service` and `client-api`) are currently in development, running a full `docker compose up --build` across all 11 services will encounter stub build errors. Use the automated demo scripts to run and test the complete upstream ingestion, storage, and event-distribution pipeline.

### 1. Configure Environment Variables
```bash
cp .env.example .env
# Edit .env to set local passwords and keys (see .env.example for descriptions)
```

### 2. Launch Operational Services
Use the automated startup script which launches and health-checks `bmkg-mock`, `pvmbg-mock`, `canonical-store`, `aggregator`, and their dependencies (`nats-jetstream`):
```bash
bash scripts/demo/up.sh
```
*Alternatively, launch via Docker Compose directly:*
```bash
docker compose up -d --build canonical-store nats-jetstream bmkg-mock pvmbg-mock aggregator stub-sink notification-consumer audit-consumer
```

### 3. Verify with the Automated Smoke Suite
Run the 12-check automated smoke test suite to validate upstream health, seed data ingestion, outage simulation, and PostgreSQL storage:
```bash
bash scripts/demo/smoke.sh
```

### 4. Teardown
To cleanly stop containers and wipe volumes:
```bash
bash scripts/demo/down.sh
```

---

## Demo Scenarios

Detailed execution steps and expected output are documented in [scripts/demo/README.md](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/scripts/demo/README.md).

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
   Inspect the canonical PostgreSQL store; `confidence_level` dynamically lands in `attributes` (JSONB) with zero schema migration:
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
   Aggregator trips its circuit breaker to `OUTAGE`. Querying Aggregator's internal query API serves cached historical data with `status: "OUTAGE"` and `stale_since` metadata rather than failing with HTTP 500 or timeout:
   ```bash
   docker run --rm --net if4031-tubes-imade_internal_api_net curlimages/curl:8.11.0 -s "http://aggregator:8090/internal/events?limit=1"
   ```
4. **Restore Service:**
   ```bash
   curl -s -X POST -H "Authorization: Bearer test-pvmbg-token" "http://localhost:8082/admin/outage"
   ```
   The circuit breaker probes upstream and automatically returns to `UP` state without restart.

---

### Problem 3 — Service Authentication & Scope Projection
* **Upstream Credential Isolation (Operational):**
  * Submitting PVMBG Bearer token to BMKG returns `403 Forbidden`:
    ```bash
    curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer test-pvmbg-token" http://localhost:8081/seismic-events
    ```
  * Submitting BMKG API Key to PVMBG returns `403 Forbidden`:
    ```bash
    curl -s -o /dev/null -w "%{http_code}\n" -H "X-BMKG-Key: test-bmkg-key" http://localhost:8082/volcanic-reports
    ```
* **Downstream Edge Authentication (In Development):**
  * `token-service` (`:8083`) and `client-api` (`:8080`) scope enforcement (Media Ringkasan-only vs Field Team raw attributes) are currently being finalized by Person B.

---

### Problem 4 — Microservice Isolation & Flexible Storage
1. **Independent Process Lifecycles:**
   Restart the Aggregator container independently without affecting upstream mocks or the database:
   ```bash
   docker compose restart aggregator
   ```
   Verify other services stay healthy throughout the restart:
   ```bash
   curl -s http://localhost:8081/health
   ```
2. **Zero-DDL Dynamic Storage:**
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
   docker run --rm --network if4031-tubes-imade_bus_net curlimages/curl:8.11.0 -s http://stub-sink:8085/received
   ```
3. **Stop One Consumer (Producer and other consumer unaffected):**
   ```bash
   docker compose stop notification-consumer
   ```
4. **Restart Consumer & Verify Catch-up:**
   ```bash
   sleep 10
   docker compose start notification-consumer
   docker compose logs -f notification-consumer
   ```
   The durable consumer retrieves missed events from JetStream without message loss or duplication.

---

## Load Testing (Problem 2)

Load testing is authored in [tests/load/k6_p2.js](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/tests/load/k6_p2.js). Once `client-api` is deployed:
```bash
# Obtain Bearer token from token-service
TOKEN=$(curl -s -X POST http://localhost:8083/oauth/token \
  -d "grant_type=client_credentials&client_id=$CLIENT_FIELD_TEAM_ID&client_secret=$CLIENT_FIELD_TEAM_SECRET" \
  | jq -r .access_token)

# Run 50 VUs sustained for 60s
k6 run -e TOKEN=$TOKEN tests/load/k6_p2.js
```

---

## Key Configuration Variables

All configuration is managed through environment variables defined in [.env.example](file:///Users/Agung/Documents/wiwekaputera/ITB/semester-7/aat/IF4031_Tubes_IMade/.env.example):

| Variable | Default | Service | Description |
| :--- | :---: | :--- | :--- |
| `BMKG_API_KEY` | — | `bmkg-mock`, `aggregator` | API key for BMKG authentication |
| `PVMBG_BEARER_TOKEN` | — | `pvmbg-mock`, `aggregator` | Bearer token for PVMBG authentication |
| `POLL_INTERVAL_S` | `3` | `aggregator` | Upstream polling interval in seconds |
| `PVMBG_CB_THRESHOLD` | `3` | `aggregator` | Consecutive failure threshold to trip circuit breaker |
| `PVMBG_CB_PROBE_INTERVAL` | `30` | `aggregator` | Half-open probe interval in seconds |
| `ACCESS_TOKEN_TTL` | `60` | `token-service` | Access token TTL in seconds (default: 60s) |
| `CLIENT_API_RPS_LIMIT` | `100` | `client-api` | Rate limit token bucket requests/sec |
