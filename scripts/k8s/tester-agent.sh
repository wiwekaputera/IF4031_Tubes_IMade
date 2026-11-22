#!/usr/bin/env bash
# Install and drive the assistants' Tester Agent.
#
# The image and manifest are used exactly as published (pinned commit); nothing
# in this repository restricts the agent: no NetworkPolicy selects its
# namespace, no taint is added and no RBAC is changed.
#
#   ./scripts/k8s/tester-agent.sh install          apply the official manifest
#   ./scripts/k8s/tester-agent.sh status           pods per node
#   ./scripts/k8s/tester-agent.sh list             list scenarios
#   ./scripts/k8s/tester-agent.sh practice <name>  run a practice scenario
#   ./scripts/k8s/tester-agent.sh logs             action log of the agent on every node
set -euo pipefail

# Git Bash on Windows would turn "/app" into a Windows path; keep it literal.
export MSYS_NO_PATHCONV=1

MANIFEST_URL="https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml"
NS="tester-agent"

cmd="${1:-status}"
case "$cmd" in
  install)
    kubectl apply -f "$MANIFEST_URL"
    kubectl -n "$NS" rollout status daemonset/tester-agent --timeout=300s
    kubectl -n "$NS" get pods -o wide
    ;;
  status)
    kubectl -n "$NS" get pods -o wide
    ;;
  list)
    kubectl exec -n "$NS" daemonset/tester-agent -- /app list
    ;;
  practice)
    [[ -n "${2:-}" ]] || { echo "Usage: $0 practice <scenario-name>" >&2; exit 1; }
    kubectl exec -n "$NS" daemonset/tester-agent -- /app run practice "$2"
    ;;
  logs)
    # One pod per node: print each pod's log (last 200 lines), node by node.
    for pod in $(kubectl -n "$NS" get pods -o name); do
      kubectl -n "$NS" logs "$pod" --all-containers --prefix --tail="${TAIL:-200}"
    done
    ;;
  *)
    echo "Unknown command: $cmd (install|status|list|practice <name>|logs)" >&2
    exit 1
    ;;
esac
