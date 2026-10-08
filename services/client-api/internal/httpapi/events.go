package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
)

// EventsHandler handles GET /v1/events by proxying to the Aggregator internal API.
func EventsHandler(aggClient upstream.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":             "method_not_allowed",
				"error_description": "only GET method is supported",
			})
			return
		}

		corrID := CorrelationIDFromContext(r.Context())
		if corrID == "" {
			corrID = r.Header.Get("X-Correlation-ID")
		}
		if corrID != "" {
			w.Header().Set("X-Correlation-ID", corrID)
		}

		resp, err := aggClient.GetEvents(r.Context(), r.URL.RawQuery, corrID)
		if err != nil {
			var upErr *upstream.UpstreamError
			if errors.As(err, &upErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(upErr.StatusCode)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":             upErr.ErrorCode,
					"error_description": upErr.Message,
				})
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":             "bad_gateway",
				"error_description": "failed to contact upstream aggregator",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
