#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
NAMESPACE="sistem-informasi-kebencanaan-bnpb"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file '$ENV_FILE' not found. Copy .env.example to .env first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

BMKG_KEY="${AGGREGATOR_BMKG_API_KEY:-${BMKG_API_KEY:-}}"
PVMBG_TOKEN="${AGGREGATOR_PVMBG_BEARER_TOKEN:-${PVMBG_BEARER_TOKEN:-}}"
AUDIT_TOK="${AGGREGATOR_AUDIT_TOKEN:-${AUDIT_TOKEN:-}}"
DEFAULT_DSN="postgresql://aggregator@canonical-store-0.canonical-store:26257,canonical-store-1.canonical-store:26257,canonical-store-2.canonical-store:26257,canonical-store-3.canonical-store:26257/canonical?sslmode=disable"
DB_DSN_VAL="${K8S_DB_DSN:-$DEFAULT_DSN}"

if [[ -z "$BMKG_KEY" || -z "$PVMBG_TOKEN" || -z "$AUDIT_TOK" ]]; then
  echo "Error: BMKG_API_KEY, PVMBG_BEARER_TOKEN, and AUDIT_TOKEN must be set in $ENV_FILE" >&2
  exit 1
fi

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" create secret generic aggregator-secrets \
  --from-literal=BMKG_API_KEY="$BMKG_KEY" \
  --from-literal=PVMBG_BEARER_TOKEN="$PVMBG_TOKEN" \
  --from-literal=AUDIT_TOKEN="$AUDIT_TOK" \
  --from-literal=DB_DSN="$DB_DSN_VAL" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret 'aggregator-secrets' applied in namespace '$NAMESPACE'."
