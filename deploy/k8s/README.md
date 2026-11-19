# Kubernetes Deployment (`deploy/k8s/`)

Kustomize manifests for deploying the BNPB **canonical-store** (CockroachDB) and **aggregator** into the `sistem-informasi-kebencanaan-bnpb` namespace.

## Directory Layout

- `base/` — Shared Kustomize base:
  - `namespace.yaml` — `sistem-informasi-kebencanaan-bnpb` namespace.
  - `canonical-store/` — 4-node CockroachDB StatefulSet (`cockroachdb/cockroach:v24.1.6`), headless & public ClusterIP services, node-local `hostPath` storage (`/var/lib/bnpb/canonical-store` with `subPathExpr: $(POD_NAME)`), `PodDisruptionBudget`, `NetworkPolicy` (`canonical-store-ingress`), and `canonical-store-init` Job (runs `cockroach init` + `migrations/001_init.sql`).
  - `aggregator/` — 3-replica `aggregator` Deployment (`RollingUpdate` with `maxUnavailable: 0, maxSurge: 1`), `ConfigMap`, `secret.example.yaml`, and `ClusterIP` Service on port `8090`.
- `overlays/multi-node/` — For clusters with ≥ 3 Kubernetes nodes (default Raft RF=3 across nodes).
- `overlays/two-node/` — For 2-node clusters (control-plane + 1 worker). Pins 2 of 3 Raft replicas to `CONTROL_PLANE_NODE` (set in `configmap-zone.yaml`) so losing the worker node preserves Raft quorum. **Caveat:** the control-plane node holds a copy of every range.
- `jobs/aggregator-loader.yaml` — Standalone Job template running `/loader` with `role: aggregator`.

> **Note:** Keep `base/canonical-store/migrations/001_init.sql` in sync with `services/canonical-store/migrations/001_init.sql`.

## Prerequisites

1. Build or load the aggregator image (`bnpb/aggregator:latest`) onto your cluster nodes:
   ```bash
   docker build -t bnpb/aggregator:latest -f services/aggregator/Dockerfile .
   ```
2. Create the `aggregator-secrets` Secret from `.env` (never commit real secrets):
   ```bash
   ./scripts/k8s/create-secrets.sh .env
   ```

## Deploy (Single Command)

For a **multi-node** (≥ 3 nodes) cluster:
```bash
kubectl apply -k deploy/k8s/overlays/multi-node
```

For a **two-node** cluster (first update `CONTROL_PLANE_NODE` in `deploy/k8s/overlays/two-node/configmap-zone.yaml`):
```bash
kubectl apply -k deploy/k8s/overlays/two-node
```

## Accessing Dashboard & Audit API

Port-forward the `aggregator` Service locally:
```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/aggregator 8090:8090
```
- Analytics dashboard: `http://localhost:8090/analytics/`
- Health / Ready / Leader: `http://localhost:8090/internal/health`, `/internal/ready`, `/internal/leader`
- Audit API: `Authorization: Bearer <AUDIT_TOKEN>` on `/internal/audit/events`, `/internal/audit/checksum`, `/internal/audit/location`, `/internal/audit/unprocessed`

## Running the Historical Catalog Loader Job

```bash
kubectl apply -f deploy/k8s/jobs/aggregator-loader.yaml
```
