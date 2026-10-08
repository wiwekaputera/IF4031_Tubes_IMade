package httpapi

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthHandler returns an HTTP handler for GET /health.
func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "UP",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	}
}
