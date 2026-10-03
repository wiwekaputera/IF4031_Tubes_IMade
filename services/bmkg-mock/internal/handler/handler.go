package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

// Handler coordinates HTTP requests for the bmkg-mock service.
type Handler struct {
	store  store.Store
	cfg    *config.Config
	logger zerolog.Logger
}

// NewHandler creates a new HTTP Handler instance.
func NewHandler(s store.Store, cfg *config.Config, logger zerolog.Logger) *Handler {
	return &Handler{
		store:  s,
		cfg:    cfg,
		logger: logger.With().Str("component", "handler").Logger(),
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

// Health handles GET /health without requiring authentication.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

// GetSeismicEvents handles GET /seismic-events?since=<ISO8601>&limit=<int>.
func (h *Handler) GetSeismicEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
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

	events := h.store.GetSeismicEvents(sinceTime, limit)
	if events == nil {
		events = make([]model.SeismicEvent, 0)
	}

	h.writeJSON(w, http.StatusOK, events)
}

// GetTsunamiWarnings handles GET /tsunami-warnings?since=<ISO8601>&limit=<int>.
func (h *Handler) GetTsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
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

	warnings := h.store.GetTsunamiWarnings(sinceTime, limit)
	if warnings == nil {
		warnings = make([]model.TsunamiWarning, 0)
	}

	h.writeJSON(w, http.StatusOK, warnings)
}

// Routes builds and returns the http.Handler with all routes and middleware configured.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	// Public endpoint (no auth, no simulated latency)
	mux.HandleFunc("/health", h.Health)

	// Protected data endpoints: auth + latency jitter
	dataMiddleware := Chain(AuthMiddleware(h.cfg.APIKey), LatencyMiddleware(h.cfg.DelayMinMs, h.cfg.DelayMaxMs))
	mux.Handle("/seismic-events", dataMiddleware(http.HandlerFunc(h.GetSeismicEvents)))
	mux.Handle("/tsunami-warnings", dataMiddleware(http.HandlerFunc(h.GetTsunamiWarnings)))

	// Global middleware applied from outermost to innermost:
	// 1. Correlation ID
	// 2. Zerolog Request Logging
	globalMiddleware := Chain(CorrelationIDMiddleware, LoggingMiddleware(h.logger))
	return globalMiddleware(mux)
}
