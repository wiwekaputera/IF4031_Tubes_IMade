package handler

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/admin"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

// Handler coordinates HTTP requests for the pvmbg-mock service.
type Handler struct {
	store         store.Store
	cfg           *config.Config
	logger        zerolog.Logger
	outage        *admin.OutageState
	schemaVersion *admin.SchemaVersionState
}

// NewHandler creates a new HTTP Handler instance.
func NewHandler(s store.Store, cfg *config.Config, outage *admin.OutageState, schemaVersion *admin.SchemaVersionState, logger zerolog.Logger) *Handler {
	return &Handler{
		store:         s,
		cfg:           cfg,
		logger:        logger.With().Str("component", "handler").Logger(),
		outage:        outage,
		schemaVersion: schemaVersion,
	}
}

// parseLimit extracts and validates the 'limit' query parameter.
// Returns the resolved limit and true on success, or 0 and false on invalid input.
func parseLimit(limitStr string, defaultLimit, maxLimit int) (int, bool) {
	if limitStr == "" {
		return defaultLimit, true
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		return 0, false
	}
	if limit > maxLimit {
		return maxLimit, true
	}
	return limit, true
}

// writeJSON encodes v as JSON with the given status code. Encode errors are
// logged rather than silently discarded.
func (h *Handler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error().Err(err).Msg("failed to encode JSON response")
	}
}

// parseSinceParam extracts and validates the 'since' query parameter.
// Returns the parsed time and true on success (nil, true when since is absent).
// Returns nil and false on invalid input.
func parseSinceParam(r *http.Request) (*time.Time, bool) {
	sinceParam := r.URL.Query().Get("since")
	if sinceParam == "" {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339, sinceParam)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, sinceParam)
	}
	if err != nil {
		return nil, false
	}
	return &t, true
}

// Health handles GET /health without requiring authentication (no simulated latency).
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

// GetVolcanicReports handles GET /volcanic-reports?since=<ISO8601>&limit=<int>.
func (h *Handler) GetVolcanicReports(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if h.outage.IsActive() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	sinceTime, ok := parseSinceParam(r)
	if !ok {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid ISO 8601 'since' timestamp, expected RFC3339 format (e.g. 2026-09-22T20:00:00Z)",
		})
		return
	}

	limit, ok := parseLimit(r.URL.Query().Get("limit"), h.cfg.DefaultPageLimit, h.cfg.MaxPageLimit)
	if !ok {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("invalid 'limit' parameter: must be a positive integer between 1 and %d", h.cfg.MaxPageLimit),
		})
		return
	}

	reports := h.store.GetVolcanicReports(sinceTime, limit)
	if reports == nil {
		reports = make([]model.VolcanicReport, 0)
	}

	if h.schemaVersion.IsEnabled() {
		for i := range reports {
			if reports[i].ConfidenceLevel == nil {
				conf := math.Round((0.5+float64(i%5)*0.1)*1000) / 1000
				if conf > 1.0 {
					conf = 1.0
				}
				reports[i].ConfidenceLevel = &conf
			}
		}
	}

	h.writeJSON(w, http.StatusOK, reports)
}

// ToggleOutage handles POST /admin/outage — toggles the outage state.
func (h *Handler) ToggleOutage(w http.ResponseWriter, r *http.Request) {
	state := h.outage.Toggle()
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "outage toggled",
		"outage":  state,
	})
}

// ToggleSchemaVersion handles POST /admin/schema-version — toggles whether
// confidence_level appears in volcanic report responses.
func (h *Handler) ToggleSchemaVersion(w http.ResponseWriter, r *http.Request) {
	state := h.schemaVersion.Toggle()
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":          "schema version toggled",
		"confidence_level": state,
	})
}

// Routes builds and returns the http.Handler with all routes and middleware configured.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	// Public endpoint (no auth, no simulated latency)
	mux.HandleFunc("/health", h.Health)

	// Public admin control plane (no auth)
	mux.HandleFunc("/admin/outage", h.ToggleOutage)
	mux.HandleFunc("/admin/schema-version", h.ToggleSchemaVersion)

	// Protected data endpoint: auth + latency (jitter only on /volcanic-reports when outage is OFF)
	dataMiddleware := Chain(AuthMiddleware(h.cfg.BearerToken), LatencyMiddleware(h.cfg.DelayMinMs, h.cfg.DelayMaxMs, h.outage))
	mux.Handle("/volcanic-reports", dataMiddleware(http.HandlerFunc(h.GetVolcanicReports)))

	// Global middleware applied from outermost to innermost:
	// 1. Correlation ID
	// 2. Zerolog Request Logging
	globalMiddleware := Chain(CorrelationIDMiddleware, LoggingMiddleware(h.logger, "pvmbg-mock"))
	return globalMiddleware(mux)
}
