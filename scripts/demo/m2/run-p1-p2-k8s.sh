#!/usr/bin/env bash
# Single-command runner for Made's P1 (c1..c7) and P2 core (c1..c5) on a 2-VM k3s cluster.
#
# Usage:
#   K3S_NODES="ubuntu@<node-a-ip> ubuntu@<node-b-ip>" bash scripts/demo/m2/run-p1-p2-k8s.sh
#
# Optional env vars:
#   CATALOG_FILE=/path/to/assistant.csv   (default: generates 50k skewed sample CSV)
#   RUN_ID=k8s-20261122                   (default: k8s-<UTC timestamp>)
#   SKIP_P1=1                             (skip P1 c1..c7)
#   SKIP_P2=1                             (skip P2 c1..c5)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

export MODE=k8s
export NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
export RUN_ID="${RUN_ID:-k8s-$(date -u +%Y%m%dT%H%M%SZ)}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

step() { printf '\n\033[1m==== %s ====\033[0m\n' "$*"; }

command -v kubectl >/dev/null || { echo "ERROR: kubectl not found" >&2; exit 1; }
kubectl get nodes >/dev/null || { echo "ERROR: kubectl cannot reach cluster" >&2; exit 1; }

mkdir -p "evidence/p1/${RUN_ID}" "evidence/p2/${RUN_ID}"
{
  echo "=== kubectl get nodes -o wide ==="
  kubectl get nodes -o wide
  echo ""
  echo "=== kubectl -n ${NAMESPACE} get pods -o wide ==="
  kubectl -n "$NAMESPACE" get pods -o wide
} | tee "evidence/p1/${RUN_ID}/cluster-topology.txt"

# Stage catalog.csv onto every VM in K3S_NODES so aggregator-loader finds it regardless of scheduling.
if [[ "${SKIP_P1:-0}" != "1" ]]; then
  CATALOG_FILE="${CATALOG_FILE:-/tmp/p1-catalog-${RUN_ID}.csv}"
  if [[ ! -f "$CATALOG_FILE" ]]; then
    step "Generating 50,000-row skewed earthquake catalog -> ${CATALOG_FILE}"
    python3 scripts/p1/gen-sample-catalog.py --rows 50000 --out "$CATALOG_FILE"
  fi
  if [[ -n "${K3S_NODES:-}" ]]; then
    step "Copying catalog.csv to /var/lib/bnpb/catalog/catalog.csv on K3S_NODES"
    for target in $K3S_NODES; do
      echo "  -> $target"
      scp -q "$CATALOG_FILE" "${target}:/tmp/catalog.csv"
      ssh "$target" "sudo mkdir -p /var/lib/bnpb/catalog && sudo mv /tmp/catalog.csv /var/lib/bnpb/catalog/catalog.csv"
    done
  else
    echo "WARN: K3S_NODES not set; assuming /var/lib/bnpb/catalog/catalog.csv already exists on nodes."
  fi

  export CATALOG_FILE
  for s in \
    c1-load-and-distribution.sh \
    c2-skew.sh \
    c3-cross-shard-read.sh \
    c4-node-loss.sh \
    c5-add-node.sh \
    c6-survive-next-loss.sh \
    c7-dashboard.sh; do
    step "P1: ${s}"
    bash "scripts/demo/m2/p1/${s}"
  done
fi

if [[ "${SKIP_P2:-0}" != "1" ]]; then
  for s in c1-entrypoint.sh c2-exactly-once.sh c3-failover.sh; do
    step "P2: ${s}"
    bash "scripts/demo/m2/p2/${s}"
  done

  # On a 2-node cluster (control-plane + 1 worker), 2 of 3 CockroachDB replicas live on
  # the control-plane node. Skewing (>500ms) or pausing the control-plane VM would break
  # Raft quorum, so ensure the active leader pod is on a worker node before c4/c5.
  ensure_leader_on_worker() {
    local node_count cp_node leader_pod leader_node
    node_count="$(kubectl get nodes --no-headers | wc -l | tr -d ' ')"
    [[ "$node_count" -ne 2 ]] && return 0
    cp_node="$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    [[ -z "$cp_node" ]] && return 0
    for attempt in 1 2 3 4 5; do
      leader_pod="$(kubectl -n "$NAMESPACE" exec canonical-store-0 -- /cockroach/cockroach sql --insecure --format=tsv \
        --execute="SELECT holder FROM canonical.leader_leases WHERE name='ingest-poller' AND expires_at > now() LIMIT 1;" \
        2>/dev/null | tail -n +2 | head -n 1 | tr -d '\r' || true)"
      [[ -z "$leader_pod" ]] && { sleep 3; continue; }
      leader_node="$(kubectl -n "$NAMESPACE" get pod "$leader_pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
      if [[ -n "$leader_node" && "$leader_node" != "$cp_node" ]]; then
        echo "  Leader ${leader_pod} is on worker node '${leader_node}' (safe for 2-node skew/freeze)."
        return 0
      fi
      echo "  Leader ${leader_pod} is on control-plane '${cp_node}'; rotating pod (attempt ${attempt}/5) so a worker pod holds the lease..."
      kubectl -n "$NAMESPACE" delete pod "$leader_pod" --wait=false >/dev/null 2>&1 || true
      sleep 8
    done
    echo "WARN: leader is still on control-plane '${cp_node}'; proceed carefully on 2-node clusters." >&2
  }

  step "P2: c4-clock-skew.sh"
  ensure_leader_on_worker
  SSH_ARGS=()
  if [[ -n "${K3S_NODES:-}" ]]; then
    # Query active leader pod (holder == INSTANCE_ID == pod name in K8s) and match its node IP against K3S_NODES.
    LEADER_POD="$(kubectl -n "$NAMESPACE" exec canonical-store-0 -- /cockroach/cockroach sql --insecure --format=tsv \
      --execute="SELECT holder FROM canonical.leader_leases WHERE name='ingest-poller' AND expires_at > now() LIMIT 1;" \
      2>/dev/null | tail -n +2 | head -n 1 | tr -d '\r' || true)"
    if [[ -n "$LEADER_POD" ]]; then
      NODE_NAME="$(kubectl -n "$NAMESPACE" get pod "$LEADER_POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
      NODE_IP="$(kubectl get node "$NODE_NAME" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)"
      for target in $K3S_NODES; do
        u="${target%@*}"; h="${target#*@}"
        if [[ "$h" == "$NODE_IP" || "$h" == "$NODE_NAME" ]]; then
          SSH_ARGS=(SSH_SKEW=1 "SSH_USER=${u}" "SSH_HOST=${h}")
          break
        fi
      done
    fi
  fi
  if [[ ${#SSH_ARGS[@]} -gt 0 ]]; then
    env "${SSH_ARGS[@]}" bash scripts/demo/m2/p2/c4-clock-skew.sh
  else
    bash scripts/demo/m2/p2/c4-clock-skew.sh
  fi

  step "P2: c5-freeze.sh"
  ensure_leader_on_worker
  bash scripts/demo/m2/p2/c5-freeze.sh
fi

step "DONE — Evidence saved for RUN_ID=${RUN_ID}"
echo "  P1: evidence/p1/${RUN_ID}/"
echo "  P2: evidence/p2/${RUN_ID}/"
echo "Reminder: grab 2 screenshots of /analytics/ (normal + partial banner) if not yet captured."
