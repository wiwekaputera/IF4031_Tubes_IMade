# IF4031 Milestone 1 — BNPB Disaster Coordination System

> **Course:** IF4031 — Arsitektur Aplikasi Terdistribusi  
> **Milestone:** 1 — Distributed Systems Foundation

---

## Quick Start

```bash
# 1. Copy and fill environment variables
cp .env.example .env
# Edit .env — fill in all secrets (see .env.example for descriptions)

# 2. Start all services
docker compose up --build

# 3. Verify system health
curl http://localhost:8080/health      # client-api
curl http://localhost:8083/health      # token-service
curl http://localhost:8081/health      # bmkg-mock
curl http://localhost:8082/health      # pvmbg-mock
```

---

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full design, contracts, and decisions.

## Work Distribution

See [WORKDIST.md](./WORKDIST.md) for the team member ownership map.

---

## Configuration

All configuration is via environment variables. See [.env.example](./.env.example) for the complete list with descriptions.

| Variable | Service | Description |
| :--- | :--- | :--- |
| `BMKG_API_KEY` | bmkg-mock, aggregator | API key for BMKG authentication |
| `PVMBG_BEARER_TOKEN` | pvmbg-mock, aggregator | Bearer token for PVMBG authentication |
| `ACCESS_TOKEN_TTL` | token-service | Access token TTL in seconds (default: 60) |
| `POLL_INTERVAL_S` | aggregator | Polling interval in seconds (default: 3) |
| `CLIENT_API_RPS_LIMIT` | client-api | Rate limit (requests/sec, default: 100) |

---

## Demo Scenarios

### Problem 1 — Schema Evolution
```bash
# 1. Check current volcanic events (no confidence_level)
curl -H "Authorization: Bearer <token>" "http://localhost:8080/v1/events?type=VOLCANIC"

# 2. Trigger schema update on PVMBG (confidence_level now appears)
curl -X POST http://localhost:8082/admin/schema-version

# 3. Wait one poll interval, then verify confidence_level lands in attributes
curl -H "Authorization: Bearer <token>" "http://localhost:8080/v1/events?type=VOLCANIC"
```

### Problem 2 — PVMBG Outage
```bash
# 1. Enable outage
curl -X POST http://localhost:8082/admin/outage

# 2. Query volcanic events — should return stale data with stale_since, not 500
curl -H "Authorization: Bearer <token>" "http://localhost:8080/v1/events?type=VOLCANIC"

# 3. Disable outage
curl -X POST http://localhost:8082/admin/outage
```

### Problem 3 — Token Expiry & Refresh
```bash
# 1. Get initial token (Field Team)
curl -X POST http://localhost:8083/oauth/token \
  -d "grant_type=client_credentials&client_id=$CLIENT_FIELD_TEAM_ID&client_secret=$CLIENT_FIELD_TEAM_SECRET"

# 2. Wait 60 seconds for access token to expire

# 3. Refresh without re-login
curl -X POST http://localhost:8083/oauth/token \
  -d "grant_type=refresh_token&refresh_token=<refresh_token>"
```

### Problem 5 — Fan-out Demo
```bash
# 1. Start the ingestion and event-distribution services
docker compose up -d canonical-store nats-jetstream bmkg-mock pvmbg-mock aggregator \
  stub-sink notification-consumer audit-consumer

# 2. Follow the independent durable consumers receiving published events
docker compose logs -f aggregator notification-consumer audit-consumer stub-sink

# 3. Inspect notifications accepted by the internal stub sink
docker run --rm --network if4031-tubes-imade_bus_net curlimages/curl:8.10.1 \
  http://stub-sink:8085/received

# 4. Stop one consumer; audit-consumer and the producer keep running
docker compose stop notification-consumer

# 5. Let more events arrive, then restart it. Its durable subscription catches up.
sleep 10
docker compose logs --since 10s aggregator audit-consumer
docker compose start notification-consumer
docker compose logs -f notification-consumer

# Stop the demo stack when finished
docker compose down
```

### Load Test (Problem 2)
```bash
# Requires k6: https://k6.io/docs/get-started/installation/
TOKEN=$(curl -s -X POST http://localhost:8083/oauth/token \
  -d "grant_type=client_credentials&client_id=$CLIENT_FIELD_TEAM_ID&client_secret=$CLIENT_FIELD_TEAM_SECRET" \
  | jq -r .access_token)

k6 run -e TOKEN=$TOKEN tests/load/k6_p2.js
```

---

## Ports

| Service | Host Port | Notes |
| :--- | :--- | :--- |
| `client-api` | 8080 | Main public endpoint |
| `bmkg-mock` | 8081 | Demo / dev access only |
| `pvmbg-mock` | 8082 | Demo / dev access only |
| `token-service` | 8083 | Token issuance + JWKS |

All other services are internal (no host port).
