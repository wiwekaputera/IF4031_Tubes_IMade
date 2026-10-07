package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HealthHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		dbStatus := "UP"
		overallStatus := "UP"
		statusCode := http.StatusOK

		if db == nil {
			dbStatus = "UNINITIALIZED"
			overallStatus = "DOWN"
			statusCode = http.StatusServiceUnavailable
		} else if err := db.Ping(ctx); err != nil {
			dbStatus = "DOWN"
			overallStatus = "DOWN"
			statusCode = http.StatusServiceUnavailable
		}

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   overallStatus,
			"database": dbStatus,
			"time":     time.Now().UTC().Format(time.RFC3339),
		})
	}
}
