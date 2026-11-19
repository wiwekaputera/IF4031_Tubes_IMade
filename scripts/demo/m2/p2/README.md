# Problem 2 (P2) — Demo & Evidence Scripts

Automated scenario scripts for IF4031 Milestone 2 Problem 2 (*Dua Nakhoda, Satu Kapal*), covering criteria 1–5 (single entrypoint readiness removal, exactly-once upstream polling, leader failover <= 60 s, clock-skew safety, and frozen/suspended leader fencing).

## Modes

Every script supports both **Docker Compose** and **Kubernetes** (`k3s` / `kind`):

- `MODE=compose` *(auto-selected when no `aggregator` Deployment is reachable via `kubectl`)*: runs against the CockroachDB Docker Compose stack scaled to 3 `aggregator` replicas (`docker compose up -d --scale aggregator=3`).
- `MODE=k8s`: runs against `kubectl -n ${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}` using `kubectl port-forward`, `kubectl exec`, and in-cluster probes.

## Prerequisites

- `.env` (or exported env vars) containing `BMKG_API_KEY` / `AGGREGATOR_BMKG_API_KEY`, `PVMBG_BEARER_TOKEN` / `AGGREGATOR_PVMBG_BEARER_TOKEN`, and `AUDIT_TOKEN` / `AGGREGATOR_AUDIT_TOKEN`.
- Running stack:
  - Compose: `bash scripts/demo/up.sh && docker compose up -d --scale aggregator=3 --no-recreate`
  - Kubernetes: `kubectl apply -k deploy/k8s/overlays/<topology>` (with 3 `aggregator` replicas)
- `bash`, `curl`, `python3`, `docker` (compose) or `kubectl` (k8s).

## Execution Order

Run all 5 scripts sequentially with a shared `RUN_ID` so all evidence is grouped under `evidence/p2/<run-id>/`:

```bash
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
bash scripts/demo/m2/p2/c1-entrypoint.sh    # ~20 s (3 replicas, drain/kill removes endpoint, 0 non-200)
bash scripts/demo/m2/p2/c2-exactly-once.sh  # ~65 s (mock reset, 60s window, duplicates: [], 0 DB dups)
bash scripts/demo/m2/p2/c3-failover.sh      # ~35 s (SIGKILL/force-delete leader, takeover <= 60s, 0 gaps)
bash scripts/demo/m2/p2/c4-clock-skew.sh    # ~5 s compose / ~50 s k8s (DB now() authority + disjoint tokens)
bash scripts/demo/m2/p2/c5-freeze.sh        # ~40 s (pause/suspend leader > 15s TTL, 0 post-wake writes)
```

## Expected Outputs

| Script | Criterion | Key Outputs in `evidence/p2/<run-id>/` | Expected Result |
| :--- | :--- | :--- | :--- |
| `c1-entrypoint.sh` | 1. Single entrypoint & readiness drain | `c1-topology-before.txt`, `c1-endpoints-before.txt`, `c1-endpoints-during.txt`, `c1-endpoints-after.txt`, `c1-requests.csv`, `c1-verification.txt` | 3 replicas behind `aggregator:8090`; draining/deleted instance returns `503 DRAINING` and drops from endpoints while request loop gets 100% HTTP 200 |
| `c2-exactly-once.sh` | 2. Exactly-once fetch & 0 DB duplicates | `c2-bmkg-requests.json`, `c2-pvmbg-requests.json`, `c2-lease-and-watermarks.txt`, `c2-db-duplicates.txt`, `c2-verification.txt` | Both mocks report `duplicates: []` and a single `by_instance` key; `canonical_events` has 0 duplicate `(source, source_ref_id)` rows |
| `c3-failover.sh` | 3. Abrupt failover <= 60 s | `c3-state-before.txt`, `c3-state-after.txt`, `c3-leader-fencing.log`, `c3-db-source-refs.csv`, `c3-verification.txt` | Surviving replica logs `leader_acquired` with incremented `fencing_token` in <= 60 s (~15–20 s); 0 missing events and 0 duplicates |
| `c4-clock-skew.sh` | 4. Clock skew (+45 s / -30 s) | `c4-db-vs-leader-before.txt`, `c4-db-vs-leader-after.txt`, `c4-write-intervals.txt`, `c4-leader-fencing.log`, `c4-crdb-skew-logs.txt`, `c4-verification.txt` | Lease expiry uses CRDB `now()` only; distinct `(fencing_token, written_by)` epochs have strictly disjoint `updated_at` intervals |
| `c5-freeze.sh` | 5. Frozen leader fencing | `c5-state-before.txt`, `c5-state-during-freeze.txt`, `c5-state-after.txt`, `c5-woken-leader.log`, `c5-verification.txt` | Surviving replica takes over after TTL expiry; woken ex-leader logs `leader_lost` (or `write rejected: stale fencing token`) and writes 0 rows |

## Lease Timing & Trade-off (`LEASE_TTL_S=15`, `LEASE_RENEW_INTERVAL_S=5`)

- **Chosen parameters**:
  - `LEASE_TTL_S = 15s`
  - `LEASE_RENEW_INTERVAL_S = 5s`
  - Local monotonic `safetyMargin = 2s` (`ttl / 5` capped at `2s` in `lease.Elector`)
  - `SHUTDOWN_DRAIN_S = 5s` (graceful `SIGTERM` releases the lease immediately after stopping workers)
- **Failover budget**: When a leader crashes or freezes without releasing the lease, the worst-case time for a survivor to take over is `LEASE_TTL_S + LEASE_RENEW_INTERVAL_S = 15s + 5s = 20s` (plus 1–2 s DB round-trip), well inside the `<= 60s` requirement.
- **Renewal margin**: With `LEASE_RENEW_INTERVAL_S = 5s` and `LEASE_TTL_S = 15s` (`TTL > 2 * renew`, enforced in `config.Load()`), a leader gets **3 renewal attempts** before its lease expires. Transient GC pauses or single CockroachDB `40001` retry/network hiccups under 10 s do not cause spurious leadership flapping.
- **Trade-off**:
  - **Shorter TTL** (e.g., 5 s / 1.5 s renew) gives faster crash detection, at the cost of 3× higher lease-row write QPS and false failovers during brief network jitter or DB range-lease moves.
  - **Longer TTL** (e.g., 45 s / 15 s renew) minimizes background DB writes and false failovers, but pushes worst-case failover close to 60 s (45 + 15 = 60 s), leaving zero headroom under load.
  - `15s / 5s` sits in the sweet spot: negligible DB load (0.2 writes/sec for one row), strong resistance to <10 s jitter, and ~15–20 s crash failover with 40 s of safety margin below the 60 s SLA.
- **Time source**: All lease checks (`AcquireOrRenewLease` and `verifyLeaseTx` inside `UpsertFencedBatch`) use CockroachDB's HLC `now()`, never aggregator OS wall clocks. CockroachDB enforces `max-offset = 500 ms` across store nodes (shutting down any CRDB node that drifts past 500 ms), so DB lease time remains globally bounded.

## Remaining Runs Required on Fayadh's Real k3s VMs

Docker Compose shares a single host Linux kernel across all containers, so Compose baseline runs prove all DB fencing, mock deduplication, abrupt `SIGKILL` failover, and `docker pause` freeze semantics, while the following must still be executed on the multi-VM `k3s` cluster (`MODE=k8s`):

1. **`c1-entrypoint.sh` (`MODE=k8s`)**: Verify the 3 `aggregator` pods are scheduled across distinct VM nodes (`kubectl get pods -o wide -l app=aggregator`) and `kube-proxy` drops the unready pod from `Endpoints`/`EndpointSlice`.
2. **`c3-failover.sh` (`MODE=k8s`, `KILL_MODE=node`)**: Re-run leader failover by powering off/stopping the entire VM hosting the leader pod (in addition to `KILL_MODE=pod`).
3. **`c4-clock-skew.sh` (`MODE=k8s`)**: Inject actual `+45s` then `-30s` OS clock skew on the leader's VM (`timedatectl set-ntp false && sudo date -s ...`, or via Tester Agent) and capture both aggregator disjoint intervals and CockroachDB's `>500ms` max-offset self-shutdown behavior on that VM.
4. **`c5-freeze.sh` (`MODE=k8s`)**: Suspend the VM hosting the leader pod (`VBoxManage controlvm <vm> pause` / `virsh suspend` / UTM Pause) for > 15 s and resume it to verify the hypervisor-level VM freeze case.
5. **Criteria 6–8 (Fayadh)**: Sustained 10,000-VU k6 load testing with live `3 -> 5 -> 2` scaling, 3x vs 1x throughput comparison (`>= 2.0x`), and zero-downtime rolling update.
