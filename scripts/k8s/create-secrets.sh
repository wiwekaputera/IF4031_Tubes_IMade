#!/usr/bin/env bash
# Create (or update) every Kubernetes Secret the BNPB system needs, from .env.
# Credentials never go into the manifests; each service gets its own Secret
# holding only the values it uses.
#
# Usage: ./scripts/k8s/create-secrets.sh [.env]
set -euo pipefail

ENV_FILE="${1:-.env}"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file '$ENV_FILE' not found. Copy .env.example to .env first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# require NAME... : fail when a variable is empty, warn when it is still the
# change-me placeholder from .env.example (fine for a local test, not for the demo).
require() {
  local missing=0
  for name in "$@"; do
    local value="${!name:-}"
    if [[ -z "$value" ]]; then
      echo "Error: $name is empty in $ENV_FILE" >&2
      missing=1
    elif [[ "$value" == change-me* ]]; then
      echo "Warning: $name still has the placeholder value from .env.example" >&2
    fi
  done
  return "$missing"
}

# apply_secret NAME KEY=VALUE... : idempotent create-or-update.
apply_secret() {
  local name="$1"; shift
  local args=()
  for kv in "$@"; do args+=("--from-literal=$kv"); done
  kubectl -n "$NAMESPACE" create secret generic "$name" "${args[@]}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "  secret/$name"
}

BMKG_API_KEY="${AGGREGATOR_BMKG_API_KEY:-${BMKG_API_KEY:-}}"
PVMBG_BEARER_TOKEN="${AGGREGATOR_PVMBG_BEARER_TOKEN:-${PVMBG_BEARER_TOKEN:-}}"
AUDIT_TOKEN="${AGGREGATOR_AUDIT_TOKEN:-${AUDIT_TOKEN:-}}"
TOKEN_DB_POSTGRES_USER="${TOKEN_DB_POSTGRES_USER:-tokenservice}"
TOKEN_DB_POSTGRES_DB="${TOKEN_DB_POSTGRES_DB:-tokendb}"
SATELLITE_STORE_POSTGRES_USER="${SATELLITE_STORE_POSTGRES_USER:-satellite}"
SATELLITE_STORE_POSTGRES_DB="${SATELLITE_STORE_POSTGRES_DB:-satellite}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"

require BMKG_API_KEY PVMBG_BEARER_TOKEN INTERNAL_OPS_TOKEN AUDIT_TOKEN \
  TOKEN_DB_POSTGRES_PASSWORD ES256_PRIVATE_KEY_B64 \
  CLIENT_MEDIA_ID CLIENT_MEDIA_SECRET CLIENT_FIELD_TEAM_ID CLIENT_FIELD_TEAM_SECRET \
  CLIENT_INTERNAL_OPS_ID CLIENT_INTERNAL_OPS_SECRET \
  SATELLITE_STORE_POSTGRES_PASSWORD SATELLITE_FEED_TOKEN GRAFANA_ADMIN_PASSWORD \
  PANTAI_AUTH_TOKEN

# The aggregator connects to every CockroachDB node; the driver fails over
# between the hosts listed here.
DEFAULT_DSN="postgresql://aggregator@canonical-store-0.canonical-store:26257,canonical-store-1.canonical-store:26257,canonical-store-2.canonical-store:26257,canonical-store-3.canonical-store:26257/canonical?sslmode=disable"
TOKEN_DB_DSN="postgres://${TOKEN_DB_POSTGRES_USER}:${TOKEN_DB_POSTGRES_PASSWORD}@token-db:5432/${TOKEN_DB_POSTGRES_DB}?sslmode=disable"
SATELLITE_DATABASE_URL="postgres://${SATELLITE_STORE_POSTGRES_USER}:${SATELLITE_STORE_POSTGRES_PASSWORD}@satellite-store:5432/${SATELLITE_STORE_POSTGRES_DB}?sslmode=disable"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "Applying Secrets in namespace '$NAMESPACE':"

apply_secret aggregator-secrets \
  BMKG_API_KEY="$BMKG_API_KEY" \
  PVMBG_BEARER_TOKEN="$PVMBG_BEARER_TOKEN" \
  INTERNAL_OPS_TOKEN="$INTERNAL_OPS_TOKEN" \
  AUDIT_TOKEN="$AUDIT_TOKEN" \
  DB_DSN="${K8S_DB_DSN:-$DEFAULT_DSN}"

apply_secret bmkg-mock-secrets BMKG_API_KEY="$BMKG_API_KEY"
apply_secret pvmbg-mock-secrets PVMBG_BEARER_TOKEN="$PVMBG_BEARER_TOKEN"

apply_secret token-db-secrets \
  POSTGRES_USER="$TOKEN_DB_POSTGRES_USER" \
  POSTGRES_PASSWORD="$TOKEN_DB_POSTGRES_PASSWORD" \
  POSTGRES_DB="$TOKEN_DB_POSTGRES_DB"

apply_secret token-service-secrets \
  TOKEN_DB_DSN="$TOKEN_DB_DSN" \
  ES256_PRIVATE_KEY_B64="$ES256_PRIVATE_KEY_B64" \
  CLIENT_MEDIA_ID="$CLIENT_MEDIA_ID" \
  CLIENT_MEDIA_SECRET="$CLIENT_MEDIA_SECRET" \
  CLIENT_FIELD_TEAM_ID="$CLIENT_FIELD_TEAM_ID" \
  CLIENT_FIELD_TEAM_SECRET="$CLIENT_FIELD_TEAM_SECRET" \
  CLIENT_INTERNAL_OPS_ID="$CLIENT_INTERNAL_OPS_ID" \
  CLIENT_INTERNAL_OPS_SECRET="$CLIENT_INTERNAL_OPS_SECRET"

apply_secret satellite-store-secrets \
  POSTGRES_USER="$SATELLITE_STORE_POSTGRES_USER" \
  POSTGRES_PASSWORD="$SATELLITE_STORE_POSTGRES_PASSWORD" \
  POSTGRES_DB="$SATELLITE_STORE_POSTGRES_DB"

apply_secret satellite-ingestor-secrets \
  DATABASE_URL="$SATELLITE_DATABASE_URL" \
  SATELLITE_FEED_TOKEN="$SATELLITE_FEED_TOKEN"

# Name and key fixed by the coastal sensor's example manifest.
apply_secret pantai-token token="$PANTAI_AUTH_TOKEN"

apply_secret grafana-secrets \
  GF_SECURITY_ADMIN_USER="$GRAFANA_ADMIN_USER" \
  GF_SECURITY_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD"

echo "Done."
