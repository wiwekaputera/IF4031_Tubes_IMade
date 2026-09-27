# Demo Scripts — Person A Stack Walkthrough

Self-contained, runnable evidence for the BMKG → aggregator → canonical-store
pipeline. The three scripts live in `scripts/demo/` and operate against the
compose project defined in `docker-compose.yml` (`name: if4031-tubes-imade`).

## Prerequisites

1. **Docker + Docker Compose** (Compose v2).
2. **`.env` file** at the repo root. Copy from `.env.example` and set real
   values. The demo reads `BMKG_API_KEY` and `PVMBG_BEARER_TOKEN` from it.
   ```bash
   cp .env.example .env   # then edit the "change-me-*" values
   ```
   The shipped `.env` already contains working demo values
   (`BMKG_API_KEY=test-bmkg-key`, `PVMBG_BEARER_TOKEN=test-pvmbg-token`,
   `CANONICAL_STORE_POSTGRES_PASSWORD=password`).
3. **`python3`** on the host — used only to assert on JSON responses
   (jq is not required).
4. **Network access** on first run so Docker can pull `postgres:16-alpine`,
   `nats:2.10-alpine`, and `curlimages/curl:8.11.0` (cached after first use).
5. No host port is published for the **aggregator** (it is on
   `internal_api_net` only). `smoke.sh` handles this by running a
   `curlimages/curl` helper container on that network — see "Aggregator access"
   below.

## Scripts

| Script      | What it does                                                                 |
|-------------|------------------------------------------------------------------------------|
| `up.sh`     | `docker compose up -d --build bmkg-mock pvmbg-mock canonical-store aggregator` (nats-jetstream comes up as an aggregator dependency). Polls `docker compose ps --format Health` until all four are `healthy`, then prints a readiness banner. |
| `smoke.sh`  | Hits every health endpoint and key data endpoint, asserts status + body, and prints a PASS/FAIL summary with a final exit code. Waits for the aggregator poller's first cycle to ingest the seeded events. |
| `down.sh`   | `docker compose down -v --rmi local` — stops everything, removes named/anonymous volumes and locally-built images so the next `up.sh` starts from a clean DB. |

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
[up]   Person A stack is READY (all services healthy)
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
=== Aggregator (:8090, via if4031-tubes-imade_internal_api_net) ===
  [PASS] GET /internal/health -> 200 {"status":"UP"}
  [PASS] GET /internal/events?limit=100 -> 200, meta.count=44 (>=22), sources bmkg/pvmbg UP with last_synced_at
=== Canonical store (psql) ===
  [PASS] psql \dt lists canonical_events, outbox, volcano_reference
  [PASS] SELECT count(*) FROM volcano_reference -> 5
==================================================
  Smoke test summary: 8 passed, 0 failed
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

## Re-run

```bash
cd /path/to/IF4031_Tubes_IMade
bash scripts/demo/up.sh     # build + start + wait for healthy
bash scripts/demo/smoke.sh  # exercise endpoints, assert, summarize
bash scripts/demo/down.sh   # tear down + remove volumes (clean slate)
```

Re-run `up.sh` after `down.sh` to verify a fresh DB comes back to ≥22 events.
`up.sh` is idempotent — running it while the stack is already up is a no-op
that rebuilds images and re-confirms health.
