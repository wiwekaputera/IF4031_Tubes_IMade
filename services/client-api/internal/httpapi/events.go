package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/projection"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// EventsOption configures EventsHandler.
type EventsOption func(*eventsOptions)

type eventsOptions struct {
	sensorStatus func() *hazard.SourceHealthMeta
}

// WithSensorStatus adds meta.sources.sensor_pantai to every response, so a
// client sees at once whether coastal data is current. The status comes from
// the sensor poller; client-api never waits on the sensor itself.
func WithSensorStatus(fn func() *hazard.SourceHealthMeta) EventsOption {
	return func(o *eventsOptions) { o.sensorStatus = fn }
}

// EventsHandler handles GET /v1/events by validating view permissions, proxying to Aggregator,
// and applying server-side field projection based on client scope.
func EventsHandler(aggClient upstream.Client, opts ...EventsOption) http.HandlerFunc {
	var o eventsOptions
	for _, opt := range opts {
		opt(&o)
	}
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

		// 1. Validate query parameters (type, limit, since, hazard_id, view)
		queryParams, err := ValidateEventQuery(r.URL.Query())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
				Error:            contractsauth.ErrInvalidRequest,
				ErrorDescription: err.Error(),
			})
			return
		}

		// 2. Validate scope permissions against requested view
		scope := auth.ScopeFromContext(r.Context())
		view, err := projection.ValidateViewScope(scope, queryParams.View)
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
		}

		// 3. Call upstream Aggregator with sanitized query
		resp, err := aggClient.GetEvents(r.Context(), queryParams.ToUpstreamQuery(), corrID)
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

		if o.sensorStatus != nil {
			if meta := o.sensorStatus(); meta != nil {
				if resp.Meta == nil {
					resp.Meta = map[string]interface{}{}
				}
				sources, _ := resp.Meta["sources"].(map[string]interface{})
				if sources == nil {
					sources = map[string]interface{}{}
				}
				sources["sensor_pantai"] = meta
				resp.Meta["sources"] = sources
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
