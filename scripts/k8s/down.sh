#!/usr/bin/env bash
# Remove the BNPB system from the cluster.
#
# Data in hostPath directories (/var/lib/bnpb/* on each node) is kept, so the
# next `up.sh` starts with the same stores. Pass WIPE_DATA=1 with K3S_NODES set
# to delete it as well.
set -euo pipefail

NAMESPACE="sistem-informasi-kebencanaan-bnpb"

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true

if [[ "${WIPE_DATA:-0}" == "1" ]]; then
  if [[ -z "${K3S_NODES:-}" ]]; then
    echo "WIPE_DATA=1 needs K3S_NODES (ssh targets). Or run on each node: sudo rm -rf /var/lib/bnpb" >&2
    exit 1
  fi
  for node in $K3S_NODES; do
    echo "==> wiping /var/lib/bnpb on ${node}"
    ssh "$node" "sudo rm -rf /var/lib/bnpb"
  done
else
  echo "hostPath data kept under /var/lib/bnpb on each node (WIPE_DATA=1 to remove)."
fi
