set -euo pipefail

if ! command -v openssl >/dev/null 2>&1; then
    echo "Error: openssl command not found. Please install openssl or run from Git Bash." >&2
    exit 1
fi

# Generate P-256 private key and convert to unencrypted PKCS#8 PEM
KEY_PEM=$(openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt | tr -d '\r')

# Base64 encode without newlines
if printf "" | base64 -w0 >/dev/null 2>&1; then
    KEY_B64=$(printf "%s" "$KEY_PEM" | base64 -w0)
else
    KEY_B64=$(printf "%s" "$KEY_PEM" | base64 | tr -d '\r\n')
fi

echo "# Generated ES256 Key Pair (for development/demo only)"
echo "# Copy the line below and paste it into your .env file:"
echo ""
echo "ES256_PRIVATE_KEY_B64=$KEY_B64"
