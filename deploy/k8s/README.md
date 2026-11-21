# Kubernetes Deployment (`deploy/k8s/`)

Kustomize manifests for the whole BNPB system in the namespace `sistem-informasi-kebencanaan-bnpb`. The one-command deploy is `scripts/k8s/up.sh` (see the main [README](../../README.md#kubernetes-deployment)).

## Directory Layout

| Path | Contents | `role` |
| :--- | :--- | :--- |
| `base/namespace.yaml` | Namespace | — |
| `base/canonical-store/` | 4-node CockroachDB StatefulSet (`hostPath` `/var/lib/bnpb/canonical-store`), headless + public Services, PDB, NetworkPolicy, init Job (`cockroach init` + `migrations/001_init.sql`) | `store` |
| `base/aggregator/` | 3-replica Deployment (`maxUnavailable: 0`, `maxSurge: 1`, readiness `/internal/ready`, `preStop`), ConfigMap, Service `aggregator:8090` | `aggregator` |
| `base/mocks/` | `bmkg-mock`, `pvmbg-mock` (1 replica each, NodePort 30081/30082 for admin endpoints) | `mock` |
| `base/token-db/` | PostgreSQL StatefulSet (`hostPath` `/var/lib/bnpb/token-db`), init SQL ConfigMap, NetworkPolicy | `store` |
| `base/token-service/` | 2-replica Deployment, NodePort 30083 | `api` |
| `base/client-api/` | 2-replica Deployment, NodePort 30080 | `api` |
| `base/nats-jetstream/` | JetStream StatefulSet (`hostPath` `/var/lib/bnpb/nats-jetstream`) | `broker` |
| `base/consumers/` | `notification-consumer`, `audit-consumer` (bbolt on `hostPath`), `stub-sink` | `consumer`, `other` |
| `base/satellite/` | `satellite-store` PostgreSQL (`hostPath`), NetworkPolicy, `satellite-ingestor` | `store`, `consumer` |
| `base/observability/` | Prometheus (pod discovery via annotations, NodePort 30090), Grafana (NodePort 30300), Jaeger (NodePort 30686) | `other` |
| `overlays/two-node/` | Control plane + 1 worker. Pins 2 of 3 CockroachDB replicas of every range to the control-plane node so losing the worker keeps Raft quorum. `up.sh` fills in the node name | — |
| `overlays/multi-node/` | ≥ 3 nodes, default RF=3 placement | — |
| `jobs/aggregator-loader.yaml` | Historical catalog loader Job | `aggregator` |

> Keep `base/canonical-store/migrations/001_init.sql` in sync with `services/canonical-store/migrations/001_init.sql`, and `base/token-db/migrations/init.sql` with `services/token-service/migrations/init.sql`.

## Placement decisions

| Component | Placement | Why |
| :--- | :--- | :--- |
| `canonical-store` (4 pods) | Spread over both nodes | Sharded + replicated (P1). Survives the loss of a node |
| `aggregator` (3 pods), `token-service`, `client-api` (2 pods each) | Spread with soft anti-affinity | Stateless; a lost node only removes some replicas |
| `token-db`, `satellite-store`, `nats-jetstream`, `notification-consumer`, `audit-consumer`, `prometheus` | Pinned to the control-plane node (`nodeSelector: node-role.kubernetes.io/control-plane: "true"`) | Single instance with node-local data (`hostPath`). Node-kill scenarios only target worker nodes, so pinning keeps their data. A pod kill restarts them on the same node with the same data |
| Mocks, `stub-sink`, `satellite-ingestor`, Grafana, Jaeger | Anywhere | Stateless or rebuilt on start; the ingestor's offset lives in `satellite-store` |

## Resources

| Class | `role` | Memory requests / limits | Components |
| :--- | :--- | :--- | :--- |
| Aggregator | `aggregator` | 256Mi / 512Mi | aggregator |
| Storage (per instance) | `store` | 512Mi / 1Gi | canonical-store, token-db, satellite-store |
| Broker or stream processor | `broker`, `consumer` | 512Mi / 1Gi | nats-jetstream, satellite-ingestor |
| Consumer or event processor | `consumer` | 128Mi / 256Mi | notification-consumer, audit-consumer |
| Auth and client-facing API | `api` | 128Mi / 256Mi | token-service, client-api |
| Mock | `mock` | 64Mi / 128Mi | bmkg-mock, pvmbg-mock |
| Other | `other` | 128Mi / 256Mi | stub-sink, prometheus, grafana, jaeger |

## Storage ownership (NetworkPolicy)

k3s enforces NetworkPolicy with its embedded controller. Each store accepts connections from its owner only:

| Store | Allowed client |
| :--- | :--- |
| `canonical-store` | `aggregator` (and the store's own pods / init Job) |
| `token-db` | `token-service` |
| `satellite-store` | `satellite-ingestor` |

No policy selects the `tester-agent` namespace or the mocks, so the Tester Agent can still call the mock admin endpoints. `scripts/k8s/verify.sh` proves the policies with a foreign probe pod.

## Secrets and configuration

- Non-secret settings live in one ConfigMap per service (`<service>-config`).
- Credentials are created from `.env` by `scripts/k8s/create-secrets.sh`, one Secret per service: `aggregator-secrets`, `bmkg-mock-secrets`, `pvmbg-mock-secrets`, `token-db-secrets`, `token-service-secrets`, `satellite-store-secrets`, `satellite-ingestor-secrets`, `grafana-secrets`. No real Secret is committed; `base/aggregator/secret.example.yaml` shows the shape.
- Grafana dashboards are loaded from `monitoring/grafana` into the ConfigMaps `grafana-dashboards` and `grafana-provisioning` by `up.sh`, so Compose and Kubernetes use the same files.

## Images

k3s runs containerd, so images are built with Docker and imported on every node by `scripts/k8s/build-images.sh` (over SSH with `K3S_NODES`, or locally when run on a node). All manifests use `bnpb/<service>:latest` with `imagePullPolicy: IfNotPresent`.

## Accessing internal endpoints

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/aggregator 8090:8090
```
- Analytics dashboard: `http://localhost:8090/analytics/`
- Health / Ready / Leader: `/internal/health`, `/internal/ready`, `/internal/leader`
- Audit API: `Authorization: Bearer <AUDIT_TOKEN>` on `/internal/audit/events`, `/checksum`, `/location`, `/unprocessed`

## Historical Catalog Loader Job

```bash
kubectl apply -f deploy/k8s/jobs/aggregator-loader.yaml
```
