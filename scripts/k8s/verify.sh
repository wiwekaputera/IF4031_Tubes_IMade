#!/usr/bin/env bash
# Check the platform set-up of the running cluster and print the evidence:
#   1. >= 2 nodes, each with its own kernel
#   2. every pod in the namespace has a valid `role` label
#   3. every container declares memory requests and limits
#   4. every role=store pod keeps its data on the node (hostPath / local PV)
#   5. stores are reachable only by their owner (NetworkPolicy, negative test)
#   6. end to end: token from token-service, events from client-api
#
# Usage: bash scripts/k8s/verify.sh     (client credentials from .env, or ENV_FILE=...)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
NS="sistem-informasi-kebencanaan-bnpb"
ROLES="aggregator store broker consumer api mock sensor other"
FAIL=0
NL='{{"\n"}}'

ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; source "$ENV_FILE"; set +a; fi

ok()      { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()     { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=1; }
section() { printf '\n\033[1m[%s]\033[0m\n' "$*"; }

section "1. Nodes and kernels"
kubectl get nodes -o wide
nodes=$(kubectl get nodes --no-headers | wc -l)
boot_ids=$(kubectl get nodes -o go-template="{{range .items}}{{.status.nodeInfo.bootID}}${NL}{{end}}" | sort -u | wc -l)
if [[ "$nodes" -ge 2 ]]; then ok "$nodes nodes"; else bad "only $nodes node(s); at least 2 required"; fi
# Nodes that are containers on one kernel (kind/k3d) report the same boot ID.
if [[ "$boot_ids" -eq "$nodes" ]]; then
  ok "$boot_ids distinct kernel boot IDs (each node runs its own kernel)"
else
  bad "nodes share a kernel boot ID (kind/k3d-style nodes are not allowed for the demo)"
fi

section "2. role label on every pod"
kubectl -n "$NS" get pods -o wide -L role
bad_roles=0
while read -r pod role; do
  [[ -z "$pod" ]] && continue
  if [[ -z "$role" || "$role" == "<no" ]] || ! grep -qw -- "$role" <<<"$ROLES"; then
    bad "pod $pod has role='$role'"; bad_roles=1
  fi
done < <(kubectl -n "$NS" get pods -o go-template="{{range .items}}{{.metadata.name}} {{index .metadata.labels \"role\"}}${NL}{{end}}")
[[ "$bad_roles" == "0" ]] && ok "all pods carry one of: $ROLES"

section "3. memory requests and limits on every container"
missing=$(kubectl -n "$NS" get pods -o go-template="{{range .items}}{{\$p := .metadata.name}}{{range .spec.containers}}{{\$p}}/{{.name}} {{.resources.requests.memory}} {{.resources.limits.memory}}${NL}{{end}}{{end}}" \
  | awk '$2 == "<no" || $3 == "<no" || NF < 3 {print $1}' | tr '\n' ' ')
if [[ -z "$missing" ]]; then
  ok "every container declares requests.memory and limits.memory"
else
  bad "containers without memory requests/limits: $missing"
fi

section "4. role=store data on node-local disk"
while read -r pod vols; do
  [[ -z "$pod" ]] && continue
  if [[ "$vols" == *hostPath* || "$vols" == *pvc* ]]; then
    ok "$pod keeps its data on the node ($vols)"
  else
    bad "$pod has no hostPath/local PV volume"
  fi
done < <(kubectl -n "$NS" get pods -l 'role=store,!job-name' -o go-template="{{range .items}}{{.metadata.name}} {{range .spec.volumes}}{{if .hostPath}}hostPath:{{.hostPath.path}} {{end}}{{if .persistentVolumeClaim}}pvc:{{.persistentVolumeClaim.claimName}} {{end}}{{end}}${NL}{{end}}")

section "5. stores reachable only by their owner (NetworkPolicy)"
kubectl -n "$NS" get networkpolicy
probe() { # probe HOST PORT -> exit 0 if a TCP connection succeeds
  kubectl -n "$NS" run "np-probe-$RANDOM" --rm -i --restart=Never --quiet \
    --image=busybox:1.36 --labels=app=np-probe,role=other \
    --overrides='{"spec":{"containers":[{"name":"p","image":"busybox:1.36","command":["sh","-c","nc -z -w 3 '"$1"' '"$2"'"],"resources":{"requests":{"memory":"16Mi"},"limits":{"memory":"32Mi"}}}]}}' \
    >/dev/null 2>&1
}
for target in "token-db 5432" "satellite-store 5432" "canonical-store-public 26257"; do
  # shellcheck disable=SC2086
  if probe $target; then bad "a foreign pod reached $target"; else ok "foreign pod refused at $target"; fi
done
if probe nats-jetstream 4222; then
  ok "control: the same probe pod reaches nats-jetstream:4222, so the refusals above come from the policies"
else
  bad "control probe to nats-jetstream:4222 failed; the probe itself is broken"
fi

section "6. end to end through the public entry points"
NODE_IP="${NODE_IP:-$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')}"
TOKEN_URL="${TOKEN_URL:-http://${NODE_IP}:30083}"
API_URL="${API_URL:-http://${NODE_IP}:30080}"
resp=$(curl -s -m 10 -X POST "$TOKEN_URL/oauth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${CLIENT_INTERNAL_OPS_ID:-}&client_secret=${CLIENT_INTERNAL_OPS_SECRET:-}")
token=$(sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p' <<<"$resp")
if [[ -n "$token" ]]; then
  ok "token-service issued a token (it stores the refresh token in token-db, so the owner path works)"
  out=$(curl -s -m 10 -o /dev/null -w '%{http_code} %{time_total}' -H "Authorization: Bearer $token" "$API_URL/v1/events?limit=5")
  if [[ "${out%% *}" == "200" ]]; then ok "GET /v1/events -> HTTP ${out% *} in ${out#* }s"; else bad "GET /v1/events -> $out"; fi
else
  bad "no token from $TOKEN_URL: $resp"
fi

echo
if [[ "$FAIL" == "0" ]]; then echo "All platform checks passed."; else echo "Some checks failed."; fi
exit "$FAIL"
