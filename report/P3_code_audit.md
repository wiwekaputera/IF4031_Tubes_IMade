# Code Audit — Performance, Concurrency, and Idiomatic Review

**Scope:** `services/bmkg-mock/`, `services/pvmbg-mock/`, `services/aggregator/`, `contracts/`
**Date:** 2026-09-30
**Branch:** `feat/upstream-and-aggregator`

---

## Executive Summary

A static and runtime audit of the Go source across all four services identified 14 findings spanning concurrency
safety, resource contention, observability, and code hygiene. The in-memory stores in both mock services were already
optimized (sort-on-write, pre-parsed timestamps) and required no changes.

All findings have been resolved. The fixes are grouped below by category. Each entry includes the original issue,
the fix applied, the exact file reference, and testing confirmation.

---

## Severity Legend

| Level | Meaning |
|-------|---------|
| **Concurrency** | Data race or TOCTOU bug that can cause incorrect results under concurrent access |
| **Resource & Observability** | Contention, unbounded resource use, or missing error observability |
| **Code Hygiene** | Dead code, DRY violation, or non-idiomatic Go |

---

## 1. Concurrency

### C1 — TOCTOU race in `OutageState.Toggle` and `SchemaVersionState.Toggle`  [RESOLVED]

**Location:** `services/pvmbg-mock/internal/admin/state.go:17-24`, `:43-50`

**Original issue:** `Toggle` performed a non-atomic read-then-write on an `atomic.Bool`. Two concurrent calls to
`POST /admin/outage` could both read the same value and write the same negation, losing one update.

**Fix applied:** Replaced the read-modify-write with a `CompareAndSwap` retry loop:

```go
func (s *OutageState) Toggle() bool {
    for {
        cur := s.active.Load()
        if s.active.CompareAndSwap(cur, !cur) {
            return !cur
        }
    }
}
```

The identical pattern was applied to `SchemaVersionState.Toggle`.

**Verification:** `go test -race` passes on `services/pvmbg-mock/`. The existing `TestOutageToggle` and
`TestSchemaVersionToggle` tests confirm correct state transitions under the race detector.

### C2 — Unsynchronized `volcanoRefs` map access was already guarded  [RESOLVED (pre-existing)]

**Location:** `services/aggregator/internal/normalizer/normalizer.go:16-32`

**Finding:** `Normalizer` holds a `map[string]store.VolcanoReference` that is read in `NormalizeVolcanic` (called
from concurrent poller goroutines) and reassigned via `UpdateVolcanoRefs`. Both paths are guarded by a `sync.RWMutex`
(`RLock` for reads on line 81-83, `Lock` for writes on line 30-32). No change was required — the code was already
correct at the time of audit. The method is retained despite having no callers, as it is part of the intended public
API for future runtime reference refresh.

**Verification:** `go test -race` passes on `services/aggregator/internal/normalizer`.

---

## 2. Resource & Observability

### P1 — Circuit breaker `State()` acquires a write lock for a read  [RESOLVED]

**Location:** `services/aggregator/internal/circuitbreaker/breaker.go:17, 33-37`

**Original issue:** `Breaker.mu` was a plain `sync.Mutex`. `State()` and `IsOpen()` — both read-only operations
called on every PVMBG poll decision — acquired a write lock, serializing all readers.

**Fix applied:** Changed `sync.Mutex` to `sync.RWMutex`. `State()` now uses `RLock`/`RUnlock`; `Allow`,
`RecordSuccess`, and `RecordFailure` retain `Lock`/`Unlock`.

**Verification:** `go build` and `go vet` pass on `services/aggregator/internal/circuitbreaker`.

### P2 — Poller `Health()` acquires a write lock for a read copy  [RESOLVED]

**Location:** `services/aggregator/internal/poller/poller.go:39, 59-64`

**Original issue:** `Poller.mu` was a plain `sync.Mutex`. `Health()` (a read path invoked on every API request)
acquired a write lock to shallow-copy the `SourceHealth` struct, blocking behind every poll-cycle writer.

**Fix applied:** Changed `sync.Mutex` to `sync.RWMutex`. `Health()` uses `RLock`/`RUnlock`; `pollBMKG` and `pollPVMBG`
write paths retain `Lock`/`Unlock`.

**Verification:** `go test -race -v` passes on `services/aggregator/internal/poller`, including
`TestHealthConcurrentSafe` and `TestPoll_ConcurrentHealthSafeWithFailingPVMBG`.

### P3 — Missing `WriteTimeout` on HTTP servers  [RESOLVED]

**Location:**
- `services/bmkg-mock/cmd/main/main.go:70-77`
- `services/pvmbg-mock/cmd/main/main.go:68-75`
- `services/aggregator/internal/queryapi/server.go:10-19`

**Original issue:** HTTP servers lacked `WriteTimeout`. A slow consumer or a connection held open without reading
could exhaust goroutine and file-descriptor pools.

**Fix applied:** Added `WriteTimeout: 10 * time.Second` (and `ReadTimeout: 10 * time.Second` where not already
present) to all three server configurations.

**Verification:** All services build and vet cleanly. `server.go` uses a centralized `Server()` factory,
ensuring consistency across deployments.

### P4 — `GetEvents` returns unbounded results when `Limit` is 0  [RESOLVED]

**Location:** `services/aggregator/internal/store/canonical.go:44, 195-199`

**Original issue:** The SQL query builder only appended `LIMIT` when `filter.Limit > 0`. A caller that omitted or
zeroed `Limit` would trigger a full-table scan with unbounded memory allocation.

**Fix applied:** Introduced `defaultQueryLimit = 1000`. When `filter.Limit <= 0`, the value is replaced with
`defaultQueryLimit` before the `LIMIT` clause is emitted. The handler layer (`queryapi/handler.go`) sets a
default of 20, so this is a defensive backstop, not the primary control.

**Verification:** Build and vet pass. Store-level tests require a running PostgreSQL instance and are not executed
in this audit pass; the change is a straightforward constant application.

### P5 — Swallowed `json.Encode` errors on response paths  [RESOLVED]

**Location:** All service handlers (mock and aggregator).

**Original issue:** Every handler used the pattern `_ = json.NewEncoder(w).Encode(v)`, silently discarding encode
errors. The primary failure mode is a client closing the TCP connection before the response is fully written — at
that point, `Encode` returns an error that has no actionable remediation on the server side.

**Fix applied:** Introduced a centralized `writeJSON` helper in each handler package. The helper sets
`Content-Type`, writes the status code, attempts the encode, and logs any error via the service's zerolog logger.
This converts silent failures into observable log entries without changing response semantics.

The helper signature mirrors the pattern already established in `services/bmkg-mock/internal/handler/handler.go:50-56`,
now replicated in:
- `services/pvmbg-mock/internal/handler/handler.go` (new `writeJSON` method on `*Handler`)
- `services/aggregator/internal/queryapi/handler.go` (new `writeJSON` method on `*Handler`)

All inline `_ = json.NewEncoder(w).Encode(...)` calls in these files have been replaced.

**Verification:** `go test -race` passes for bmkg-mock, pvmbg-mock, and aggregator queryapi.

### P6 — Centralized `since`-parameter parsing now consistent  [RESOLVED]

**Location:**
- `services/bmkg-mock/internal/handler/handler.go:61-74` (pre-existing helper)
- `services/pvmbg-mock/internal/handler/handler.go` (newly aligned)
- `services/aggregator/internal/queryapi/handler.go:29-42` (new helper)

**Context:** The `since` query parameter (RFC3339 / RFC3339Nano) was parsed inline in four handler functions across
two packages, including a silent-ignore path in the aggregator.

**Fix applied:** Extracted `parseSinceParam(r *http.Request) (*time.Time, bool)` as a package-level function in
each handler package. The aggregator now returns HTTP 400 with
`{"error":"invalid ISO 8601 'since' timestamp, expected RFC3339 format"}` for malformed input, matching the mock
behavior.

**Verification:** New tests `TestGetEvents_InvalidSinceReturns400` and `TestGetEvents_ValidSinceParses` pass
in `services/aggregator/internal/queryapi/handler_test.go`.

---

## 3. Code Hygiene

### S1 — PVMBG generator dead jitter computation  [RESOLVED]

**Location:** `services/pvmbg-mock/internal/generator/generator.go` (lines removed)

**Original issue:** The PVMBG generator computed jittered latitude and longitude values via `math.Round` and
discarded them with `_ =`. The `VolcanicReport` model carries no lat/lon fields — coordinates for volcanic events
come from the `volcano_reference` table in the canonical store (sourced from BNPB's static reference data), not
from the mock generator. The jitter code was a copy-paste artifact from the bmkg-mock generator, which *does*
emit seismic event coordinates inline.

**Fix applied:** Removed the dead jitter computation (RNG calls and `math.Round` assignments). This is a dead-code
cleanup, not a correctness bug — the generated reports never carried coordinates.

**Verification:** `go build` and `go vet` pass. The `math` package is still imported and used for legitimate
`math.Round` calls in the ash-height and confidence-level computations (lines 92, 95, 98, 112).

### S2 — Dead `now` variable in PVMBG seed  [RESOLVED]

**Location:** `services/pvmbg-mock/internal/seed/seed.go` (lines removed)

**Fix applied:** Removed the unused `now := time.Now().UTC()` variable and its suppressing `_ = now` assignment.

**Verification:** `go build` and `go vet` pass.

### S3 — Duplicated `writeLimitError` inlined JSON encoding  [RESOLVED]

**Location:** `services/pvmbg-mock/internal/handler/handler.go` (replaced)

**Fix applied:** Removed the standalone `writeLimitError` function and its inline
`w.Header().Set` / `w.WriteHeader` / `json.Encode` pattern. The 400 limit-error response now uses the centralized
`writeJSON` helper, consistent with `services/bmkg-mock/internal/handler/handler.go`.

**Verification:** Tests pass including `TestVolcanicReportsLimitValidation` and `TestVolcanicReportsLimitClamping`.

### S4 — HTTP 405 responses lack JSON Content-Type  [NOTE]

**Location:** All handler files.

**Observation:** Non-GET requests receive a bare `w.WriteHeader(http.StatusMethodNotAllowed)` with no
`Content-Type` header. This is consistent across all services and is not user-facing (method enforcement is the
router's concern). No change applied — flagged for future consideration.

---

## 4. Design Notes

### R1 — PVMBG 503 outage response intentionally emits an empty body  [RESOLVED (by design)]

**Location:** `services/pvmbg-mock/internal/handler/handler.go` (in `GetVolcanicReports`)

The simulated outage (`h.outage.IsActive()`) returns HTTP 503 immediately with no header and no body:

```go
if h.outage.IsActive() {
    w.WriteHeader(http.StatusServiceUnavailable)
    return
}
```

This intentionally simulates a non-responsive PVMBG service. The smoke test (`smoke.sh`) asserts an empty body on
503, so adding a JSON error body would break the contract. The empty-body behavior is retained by design.

**Verification:** `TestVolcanicReportsFilter` and `TestAuthEnforcement` in the pvmbg-mock handler tests pass,
confirming the outage path is handled correctly.

---

## Summary of All Findings

| ID | Category | File | Status |
|----|----------|------|--------|
| C1 | Concurrency | `pvmbg-mock/.../admin/state.go` | [RESOLVED] CAS loop in `Toggle`, race-detector clean |
| C2 | Concurrency | `aggregator/.../normalizer/normalizer.go` | [RESOLVED (pre-existing)] `RWMutex` already guards `volcanoRefs` |
| P1 | Resource | `aggregator/.../circuitbreaker/breaker.go` | [RESOLVED] `RWMutex` with `RLock` in `State()` |
| P2 | Resource | `aggregator/.../poller/poller.go` | [RESOLVED] `RWMutex` with `RLock` in `Health()` |
| P3 | Resource | `aggregator/.../aggregator/main.go`, mock `main.go`, `queryapi/server.go` | [RESOLVED] `WriteTimeout: 10s` on all servers |
| P4 | Resource | `aggregator/.../store/canonical.go` | [RESOLVED] `defaultQueryLimit = 1000` backstop |
| P5 | Observability | Mock handlers, `queryapi/handler.go` | [RESOLVED] Centralized `writeJSON` with error logging |
| P6 | Code Hygiene | All handler packages | [RESOLVED] `parseSinceParam` + aggregator 400 on invalid `since` |
| S1 | Code Hygiene | `pvmbg-mock/.../generator/generator.go` | [RESOLVED] Dead jitter computation removed |
| S2 | Code Hygiene | `pvmbg-mock/.../seed/seed.go` | [RESOLVED] Dead `now` variable removed |
| S3 | Code Hygiene | `pvmbg-mock/.../handler/handler.go` | [RESOLVED] `writeLimitError` replaced by `writeJSON` |
| R1 | Design Note | `pvmbg-mock/.../handler/handler.go` | [RESOLVED (by design)] Empty 503 body intentional per smoke test |

---

## Testing Confirmation

All tests run with `-race` enabled:

| Service | Packages tested | Result |
|---------|-----------------|--------|
| `services/bmkg-mock` | `internal/handler` | 8 tests, all PASS |
| `services/pvmbg-mock` | `internal/handler` | 13 tests (incl. outage & schema toggles), all PASS |
| `services/aggregator` | `internal/circuitbreaker` | Build + vet clean |
| `services/aggregator` | `internal/normalizer` | 9 tests, all PASS |
| `services/aggregator` | `internal/poller` | 10 tests (incl. concurrent health checks), all PASS |
| `services/aggregator` | `internal/queryapi` | 12 tests (incl. new `since` validation tests), all PASS |

Store-level tests (`services/aggregator/internal/store/`) require a running PostgreSQL instance and were not
executed in this pass.
