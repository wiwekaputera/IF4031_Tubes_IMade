# Work Distribution — IF4031 Milestone 1

**Team size:** 3 people  
**Deliverable:** All 5 Problems (P1–P5) + Report + Demo  
**Reference:** [ARCHITECTURE.md](./ARCHITECTURE.md)

---

## Division Philosophy

Split follows **vertical ownership** — each person owns a full slice of the system end-to-end (implementation + testing + report section), not a horizontal layer. This prevents bottlenecks and makes independent deployment demonstrations straightforward.

---

## Person A — Upstream & Ingestion Core

**Owns:** Mock services, Aggregator (polling + normalization + canonical store), P1 + P2 + P4 core

### Services
| Service | Responsibility |
| :--- | :--- |
| `bmkg-mock` | Full implementation: seed data, `/seismic-events`, `/tsunami-warnings`, credential enforcement, jittered delay |
| `pvmbg-mock` | Full implementation: seed data, `/volcanic-reports`, `/admin/outage`, `/admin/schema-version`, credential enforcement, random delay |
| `aggregator` (Poller + Normalizer) | Polling goroutines, severity mapping, `hazard_id` generation, JSONB upsert, `volcano_reference` lookup, circuit breaker, outbox transaction |
| `canonical-store` | DB init SQL (`init.sql`: all tables including `volcano_reference` seed) |

### Problems Covered
| Problem | Contribution |
| :--- | :--- |
| **P1** — Schema Interoperability | Tolerant reader design, `confidence_level` landing in `attributes`, schema evolution demo |
| **P2** — Concurrency & Graceful Degradation | Per-source goroutines, BMKG/PVMBG isolation, circuit breaker, stale-data response with `stale_since` |
| **P4** — Microservice & Flexible Storage | Independent Aggregator container restart demo, JSONB dynamic field demo, storage justification |

### Report Sections
- P1: rancangan, alasan, batasan, bukti
- P2: model konkurensi, circuit breaker, degradasi
- P4: batas service, storage comparison

### Load Testing Support
- Set up mock seed data volume & timing for k6 scenarios
- Provide `/admin/outage` trigger script for P2 demo

---

## Person B — Auth, Client API & Security

**Owns:** Token service, client-api, rate limiting, scope enforcement, P3

### Services
| Service | Responsibility |
| :--- | :--- |
| `token-service` | Client-credentials grant, refresh-token grant, ES256 JWT signing, JWKS endpoint, refresh token storage in `token-db` |
| `token-db` | PostgreSQL schema for refresh token table |
| `client-api` | REST gateway, JWT verification (local JWKS), scope-based field projection (`media` / `field_team` / `internal_ops`), rate limiting middleware (token bucket), `X-Correlation-ID` propagation |

### Problems Covered
| Problem | Contribution |
| :--- | :--- |
| **P3** — Service-to-Service Auth & Trust | Upstream credential isolation (via mock enforcement by Person A, demo by Person B), downstream token lifecycle, scope enforcement demo, refresh flow demo, `.env.example` |

### Report Sections
- P3: mekanisme auth, domain of trust, alasan tidak membuka celah baru
- Bagian Umum: daftar teknologi (auth-related), asumsi

### Additional
- Generate `.env.example` with all variables across all services
- Generate ES256 key pair for dev (document key rotation process)
- Write curl/k6 demo scripts for P3 scenarios (token expiry, refresh, scope rejection)

---

## Person C — Event Distribution, Orchestration & Integration

**Owns:** NATS, consumers, stub sink, docker-compose, load testing, P5, integration glue

### Services
| Service | Responsibility |
| :--- | :--- |
| `aggregator` (Outbox Relay) | Outbox relay goroutine (polling outbox table → publish to NATS with `Nats-Msg-Id`) |
| `nats-jetstream` | NATS server config (`nats-server.conf`), stream creation at startup |
| `notification-consumer` | Durable consumer, idempotency (bbolt), `POST /notify` to `stub-sink` |
| `audit-consumer` | Durable consumer, idempotency (bbolt), append to event history |
| `stub-sink` | HTTP stub service (`POST /notify`, `GET /received`, `GET /health`) |

### Problems Covered
| Problem | Contribution |
| :--- | :--- |
| **P5** — Fan-out / Pub-Sub | Full NATS fan-out demo, consumer stop/restart demo, new consumer addition demo without producer change |

### Orchestration
- `docker-compose.yml` — full compose file: all containers, networks, volumes, healthchecks, `depends_on`
- Multi-stage `Dockerfile` for each service (distroless static images)
- `README.md` — system startup guide, configuration variables, demo scenario trigger instructions

### Load Testing
- `tests/load/k6_p2.js` — k6 script with 50 VUs, 60 s sustained, BMKG-only vs mixed scenarios
- Document p50/p95/p99 reporting in Report P2

### Report Sections
- P5: broker rationale, at-least-once semantics, idempotency design
- Bagian Umum: diagram arsitektur, deskripsi umum sistem, petunjuk menjalankan
- Bagian Umum: Tabel kontribusi, halaman sampul

---

## Shared Responsibilities

| Task | Owner |
| :--- | :--- |
| `/contracts/` module (struct definitions, JSON tags, enums) | **A** writes initial structs, **B** adds auth-related types, **C** adds event envelope |
| Structured logging (`zerolog` setup, `X-Correlation-ID` middleware) | **B** defines middleware; **A** and **C** integrate in their services |
| `/health` endpoints on all services | Each person implements for their own services |
| `.env.example` | **B** owns final file; **A** and **C** contribute their service vars |
| Security review for report | All three contribute; **B** leads (auth is the main trust boundary) |

---

## Timeline Suggestion

```
Week 1 — Foundation
  All:  scaffold repo, contracts module, docker-compose skeleton
  A:    bmkg-mock + pvmbg-mock (seed data, auth, delay)
  B:    token-service + token-db schema
  C:    nats-jetstream config + stub-sink

Week 2 — Core Implementation
  A:    aggregator polling + normalization + canonical store
  B:    client-api (auth middleware + scope projection)
  C:    outbox relay + notification-consumer + audit-consumer

Week 3 — Integration & Demo Prep
  All:  docker-compose integration, end-to-end smoke test
  A:    P1 + P2 demo scenarios + report sections
  B:    P3 demo scenarios (expiry, refresh, scope) + .env.example + report
  C:    P5 demo scenarios + k6 load test + P4 container restart demo

Week 4 — Report & Polish
  C:    Bagian Umum (diagram, deskripsi, petunjuk)
  All:  Review each other's report sections
  All:  Final demo rehearsal
```

---

## Quick Reference: Who Owns What

```
services/
├── bmkg-mock/              → A
├── pvmbg-mock/             → A
├── aggregator/
│   ├── poller/             → A
│   ├── normalizer/         → A
│   ├── query-api/          → A
│   └── outbox-relay/       → C
├── canonical-store/        → A (init.sql)
├── client-api/             → B
├── token-service/          → B
├── token-db/               → B (schema)
├── nats-jetstream/         → C (config)
├── notification-consumer/  → C
├── audit-consumer/         → C
└── stub-sink/              → C

contracts/                  → shared (A leads)
tests/load/                 → C
docker-compose.yml          → C
.env.example                → B
README.md                   → C
ARCHITECTURE.md             → all (finalized)
WORKDIST.md                 → reference only
```
