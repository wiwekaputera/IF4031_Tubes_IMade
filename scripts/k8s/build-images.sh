#!/usr/bin/env bash
# Build every BNPB service image and load it into the k3s nodes.
#
# k3s runs containerd, not Docker, so images built with `docker build` must be
# imported on every node that may run the pod. Two ways are supported:
#
#   K3S_NODES="ubuntu@192.168.56.10 ubuntu@192.168.56.11" ./scripts/k8s/build-images.sh
#       Streams the images over SSH into each node (`sudo k3s ctr images import`).
#       The SSH user needs passwordless sudo for `k3s`.
#
#   ./scripts/k8s/build-images.sh            (run on a k3s node, K3S_NODES empty)
#       Imports into the local k3s only. Run it on every node, or use K3S_NODES.
#
# Other variables:
#   TAG=latest          image tag (the manifests use :latest)
#   SERVICES="a b"      build only these services
#   SKIP_IMPORT=1       build only
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TAG="${TAG:-latest}"
ALL_SERVICES="bmkg-mock pvmbg-mock aggregator token-service client-api stub-sink notification-consumer audit-consumer satellite-ingestor"
SERVICES="${SERVICES:-$ALL_SERVICES}"

images=()
for svc in $SERVICES; do
  img="bnpb/${svc}:${TAG}"
  echo "==> docker build ${img}"
  docker build -q -t "$img" -f "services/${svc}/Dockerfile" . >/dev/null
  images+=("$img")
done

if [[ "${SKIP_IMPORT:-0}" == "1" ]]; then
  echo "Built ${#images[@]} images (import skipped)."
  exit 0
fi

if [[ -n "${K3S_NODES:-}" ]]; then
  for node in $K3S_NODES; do
    echo "==> importing ${#images[@]} images into ${node}"
    docker save "${images[@]}" | ssh "$node" "sudo k3s ctr images import -"
  done
elif command -v k3s >/dev/null 2>&1; then
  echo "==> importing ${#images[@]} images into the local k3s"
  docker save "${images[@]}" | sudo k3s ctr images import -
else
  echo "Error: set K3S_NODES (ssh targets) or run this on a k3s node." >&2
  exit 1
fi

echo "Images ready: ${images[*]}"
