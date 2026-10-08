package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/projection"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

// EventsHandler handles GET /v1/events by validating view permissions, proxying to Aggregator,
// and applying server-side field projection based on client scope.
func EventsHandler(aggClient upstream.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
				Error:            "method_not_allowed",
				ErrorDescription: "only GET method is supported",
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

		// Validate scope and view parameter
		scope := auth.ScopeFromContext(r.Context())
		viewParam := r.URL.Query().Get("view")
		view, err := projection.ValidateViewScope(scope, viewParam)
		if err != nil {
			if errors.Is(err, projection.ErrInsufficientScope) {
				w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
					Error:            contractsauth.ErrInsufficientScope,
					ErrorDescription: "media scope is not authorized to request full view",
				})
				return
			}
			if errors.Is(err, projection.ErrUnknownScope) {
				w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
					Error:            contractsauth.ErrInsufficientScope,
					ErrorDescription: "unauthorized or unrecognized client scope",
				})
				return
			}
			if errors.Is(err, projection.ErrInvalidView) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
					Error:            contractsauth.ErrInvalidRequest,
					ErrorDescription: err.Error(),
				})
				return
			}
		}

		resp, err := aggClient.GetEvents(r.Context(), r.URL.RawQuery, corrID)
		if err != nil {
			var upErr *upstream.UpstreamError
			if errors.As(err, &upErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(upErr.StatusCode)
				_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
					Error:            upErr.ErrorCode,
					ErrorDescription: upErr.Message,
				})
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
				Error:            "bad_gateway",
				ErrorDescription: "failed to contact upstream aggregator",
			})
			return
		}

		// Apply server-side projection per client scope
		resp.Data = projection.ProjectEvents(resp.Data, scope, view)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
