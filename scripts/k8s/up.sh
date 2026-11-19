#!/usr/bin/env bash
# One-command deploy of the whole BNPB system to Kubernetes (k3s).
#
#   K3S_NODES="ubuntu@<node-a-ip> ubuntu@<node-b-ip>" ./scripts/k8s/up.sh
#
# Steps:
#   1. build every service image and import it into the k3s nodes
#   2. create the Secrets from .env
#   3. create the Grafana ConfigMaps from monitoring/grafana
#   4. render the Kustomize overlay and apply it
#   5. wait until every workload is ready, then print the entry points
#
# Variables:
#   OVERLAY=two-node     two-node (control plane + 1 worker) or multi-node (>= 3 nodes)
#   ENV_FILE=.env
#   SKIP_BUILD=1         reuse the images already on the nodes
#   K3S_NODES=...        SSH targets for the image import (see build-images.sh)
#   CONTROL_PLANE_NODE=  node name for the two-node CockroachDB zone config;
#                        detected from the node-role.kubernetes.io/control-plane label
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

NAMESPACE="sistem-informasi-kebencanaan-bnpb"
OVERLAY="${OVERLAY:-two-node}"
ENV_FILE="${ENV_FILE:-.env}"
TIMEOUT="${TIMEOUT:-600s}"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

command -v kubectl >/dev/null || { echo "Error: kubectl not found" >&2; exit 1; }
kubectl get nodes >/dev/null || { echo "Error: kubectl cannot reach the cluster" >&2; exit 1; }
[[ -d "deploy/k8s/overlays/$OVERLAY" ]] || { echo "Error: unknown OVERLAY '$OVERLAY'" >&2; exit 1; }

FIRST_DEPLOY=1
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 && FIRST_DEPLOY=0

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  step "Building and importing images"
  bash scripts/k8s/build-images.sh
fi

step "Creating Secrets from $ENV_FILE"
bash scripts/k8s/create-secrets.sh "$ENV_FILE"

step "Creating Grafana ConfigMaps from monitoring/grafana"
kubectl -n "$NAMESPACE" create configmap grafana-dashboards \
  --from-file=monitoring/grafana/dashboards \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create configmap grafana-provisioning \
  --from-file=datasources.yml=monitoring/grafana/provisioning/datasources/datasources.yml \
  --from-file=dashboards.yml=monitoring/grafana/provisioning/dashboards/dashboards.yml \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  configmap/grafana-dashboards, configmap/grafana-provisioning"

step "Applying overlay '$OVERLAY'"
rendered="$(kubectl kustomize "deploy/k8s/overlays/$OVERLAY")"
if [[ "$OVERLAY" == "two-node" ]]; then
  cp_node="${CONTROL_PLANE_NODE:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane \
    -o jsonpath='{.items[0].metadata.name}')}"
  [[ -n "$cp_node" ]] || { echo "Error: no control-plane node found; set CONTROL_PLANE_NODE" >&2; exit 1; }
  echo "  control-plane node: $cp_node"
  rendered="$(printf '%s\n' "$rendered" | sed -E "s/^([[:space:]]*CONTROL_PLANE_NODE:).*/\1 \"${cp_node}\"/")"
fi
printf '%s\n' "$rendered" | kubectl apply -f -

if [[ "$FIRST_DEPLOY" == "0" && "${SKIP_BUILD:-0}" != "1" ]]; then
  step "Restarting Deployments so they pick up the rebuilt :latest images"
  kubectl -n "$NAMESPACE" rollout restart deployment
fi

step "Waiting for workloads (timeout $TIMEOUT each)"
not_ready=()
for kind in statefulset deployment; do
  for name in $(kubectl -n "$NAMESPACE" get "$kind" -o jsonpath='{.items[*].metadata.name}'); do
    kubectl -n "$NAMESPACE" rollout status "$kind/$name" --timeout="$TIMEOUT" \
      || not_ready+=("$kind/$name")
  done
done
kubectl -n "$NAMESPACE" wait --for=condition=complete job/canonical-store-init --timeout="$TIMEOUT" \
  || not_ready+=("job/canonical-store-init")

NODE_IP="$(kubectl get nodes -l node-role.kubernetes.io/control-plane \
  -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"

step "Deployed"
kubectl -n "$NAMESPACE" get pods -o wide -L role
cat <<EOF

Entry points (NodePort, any node IP works; control plane: ${NODE_IP}):
  client-api     http://${NODE_IP}:30080
  token-service  http://${NODE_IP}:30083
  bmkg-mock      http://${NODE_IP}:30081   (admin endpoints)
  pvmbg-mock     http://${NODE_IP}:30082   (admin endpoints)
  grafana        http://${NODE_IP}:30300
  prometheus     http://${NODE_IP}:30090
  jaeger         http://${NODE_IP}:30686
  aggregator     kubectl -n ${NAMESPACE} port-forward svc/aggregator 8090:8090

Check the platform set-up: bash scripts/k8s/verify.sh
EOF

if [[ ${#not_ready[@]} -gt 0 ]]; then
  echo
  echo "Not ready within $TIMEOUT: ${not_ready[*]}"
  echo "Inspect with: kubectl -n $NAMESPACE logs <kind>/<name> --previous"
  exit 1
fi
