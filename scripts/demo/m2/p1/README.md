# Problem 1 (P1) — Demo & Evidence Scripts

Automated scenario scripts for IF4031 Milestone 2 Problem 1 (*Yang Pulang Membawa Laptopnya*), covering criteria 1–7.

## Modes

Every script supports both **Docker Compose** and **Kubernetes** (`k3s` / `kind`):

- `MODE=compose` *(auto-selected when no `canonical-store` StatefulSet is reachable via `kubectl`)*: runs against the 4-node CockroachDB Docker Compose stack (`canonical-store-0..3`) and `aggregator` on `internal_api_net`.
- `MODE=k8s`: runs against `kubectl -n ${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}` using `kubectl port-forward` and `kubectl exec`.

## Prerequisites

- `.env` (or exported env vars) containing `AUDIT_TOKEN` / `AGGREGATOR_AUDIT_TOKEN`.
- Running stack (`bash scripts/demo/up.sh` for compose, or `kubectl apply -k deploy/k8s/overlays/<topology>` for k8s).
- `bash`, `curl`, `python3`, `docker` (compose) or `kubectl` (k8s).

## Execution Order

Run all 7 scripts sequentially with a shared `RUN_ID` so evidence is grouped in `evidence/p1/<run-id>/`:

```bash
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
bash scripts/demo/m2/p1/c1-load-and-distribution.sh   # ~25 s
bash scripts/demo/m2/p1/c2-skew.sh                    # ~45 s
bash scripts/demo/m2/p1/c3-cross-shard-read.sh        # ~5 s
bash scripts/demo/m2/p1/c4-node-loss.sh               # ~120 s (waits for RF=3 up-replication)
bash scripts/demo/m2/p1/c5-add-node.sh                # ~70 s (rebalance snapshots every 15s)
bash scripts/demo/m2/p1/c6-survive-next-loss.sh       # ~30 s (decommissions dead node + 2nd loss drill)
bash scripts/demo/m2/p1/c7-dashboard.sh               # ~45 s (analytics vs audit + partial banner check)
```

> **Note:** `c4-node-loss.sh` kills and wipes `canonical-store-3` and leaves it stopped (`LEAVE_VICTIM_STOPPED=1`) so `c5-add-node.sh` can immediately demonstrate joining an empty node without stopping reads or writes, followed by `c6-survive-next-loss.sh` decommissioning the dead node ID and restoring 4 healthy nodes. Set `LEAVE_VICTIM_STOPPED=0` if running `c4` standalone.

## Script Reference & Expected Outputs

| Script | Criterion | Key Outputs in `evidence/p1/<run-id>/` | Typical Time |
| :--- | :--- | :--- | :--- |
| `c1-load-and-distribution.sh` | 1. Catalog load & spread | `c1-loader.log`, `c1-loader-idempotent.log`, `c1-distribution.json`, `c1-distribution.txt`, `c1-show-ranges.txt` | ~25 s |
| `c2-skew.sh` | 2. Skew handling & burst QPS | `c2-load-qps.csv`, `c2-burst-qps.csv`, `c2-area-counts.csv`, `c2-shard-counts.csv`, `c2-skew-summary.txt`, `c2-skew-summary.json` | ~45 s |
| `c3-cross-shard-read.sh` | 3. Cross-shard pagination | `c3-page1-latest50.json`, `c3-pagination-timings.csv`, `c3-verification.txt` | ~5 s |
| `c4-node-loss.sh` | 4. Node loss + wipe | `c4-checksum-before.json`, `c4-checksum-after.json`, `c4-hammer.csv`, `c4-recovery.log`, `c4-verification.txt` | ~120 s |
| `c5-add-node.sh` | 5. Add empty node live | `c5-load-requests.csv`, `c5-rebalance-snapshots.txt`, `c5-rebalance-snapshots.jsonl`, `c5-verification.txt` | ~70 s |
| `c6-survive-next-loss.sh` | 6. Survive next loss | `c6-decommission.log`, `c6-ranges-healthy.txt`, `c6-hammer.csv`, `c6-checksum-*.json`, `c6-verification.txt` | ~30 s |
| `c7-dashboard.sh` | 7. Analytics dashboard | `c7-analytics-summary-*.json`, `c7-dashboard-vs-audit.txt`, `c7-analytics-partial.json`, `c7-partial-check.txt` | ~45 s |

## Screenshots for Video / Report

1. **Distribution & Ranges**: terminal output of `c1-distribution.txt` and `c1-show-ranges.txt` (showing all 8 shards and < 100% ranges per node).
2. **Skew Comparison**: `c2-skew-summary.txt` comparing actual hash-sharded balance vs hypothetical `area_name` partitioning.
3. **Node Loss Resilience**: `c4-verification.txt` and `c4-hammer.csv` (0 HTTP 5xx, identical SHA-256 checksums before/after).
4. **Live Rebalance**: `c5-rebalance-snapshots.txt` showing replicas and leases moving onto the newly joined store while read/write load continues with 0 errors.
5. **Analytics Dashboard (`http://localhost:8090/analytics/`)**:
   - Normal view (`c7-dashboard-vs-audit.txt` printing `MATCH`).
   - Partial availability banner (`Data belum lengkap — unit tidak terjangkau: ...` with `≥` prefixes) captured while 2 storage nodes are temporarily stopped.
