package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/keys"
)

func JWKSHandler(km *keys.KeyManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_ = json.NewEncoder(w).Encode(km.JWKS())
	}
}
