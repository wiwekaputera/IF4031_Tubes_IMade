package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
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

// Health handles GET /health without requiring authentication.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP"})
}

// GetSeismicEvents handles GET /seismic-events?since=<ISO8601>.
func (h *Handler) GetSeismicEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var sinceTime *time.Time
	sinceParam := r.URL.Query().Get("since")
	if sinceParam != "" {
		t, err := time.Parse(time.RFC3339, sinceParam)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, sinceParam)
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "invalid ISO 8601 'since' timestamp, expected RFC3339 format (e.g. 2026-09-22T20:00:00Z)",
			})
			return
		}
		sinceTime = &t
	}

	events := h.store.GetSeismicEvents(sinceTime)
	if events == nil {
		events = make([]agency.SeismicEvent, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(events)
}

// GetTsunamiWarnings handles GET /tsunami-warnings?since=<ISO8601>.
func (h *Handler) GetTsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var sinceTime *time.Time
	sinceParam := r.URL.Query().Get("since")
	if sinceParam != "" {
		t, err := time.Parse(time.RFC3339, sinceParam)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, sinceParam)
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "invalid ISO 8601 'since' timestamp, expected RFC3339 format (e.g. 2026-09-22T20:00:00Z)",
			})
			return
		}
		sinceTime = &t
	}

	warnings := h.store.GetTsunamiWarnings(sinceTime)
	if warnings == nil {
		warnings = make([]agency.TsunamiWarning, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(warnings)
}

// Routes builds and returns the http.Handler with all routes and middleware configured.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	// Public endpoint (no simulated latency)
	mux.HandleFunc("/health", h.Health)

	// Protected endpoints (authenticated via X-BMKG-Key)
	// Latency jitter is applied only to data endpoints, not /health.
	authMiddleware := func(handlerFunc http.HandlerFunc) http.Handler {
		return AuthMiddleware(h.cfg.APIKey, http.HandlerFunc(handlerFunc))
	}

	seismicHandler := LatencyMiddleware(h.cfg.DelayMinMs, h.cfg.DelayMaxMs, authMiddleware(h.GetSeismicEvents))
	tsunamiHandler := LatencyMiddleware(h.cfg.DelayMinMs, h.cfg.DelayMaxMs, authMiddleware(h.GetTsunamiWarnings))

	mux.Handle("/seismic-events", seismicHandler)
	mux.Handle("/tsunami-warnings", tsunamiHandler)

	// Global middleware chain applied from outermost to innermost:
	// 1. Correlation ID
	// 2. Zerolog Request Logging
	var handler http.Handler = mux
	handler = LoggingMiddleware(h.logger, handler)
	handler = CorrelationIDMiddleware(handler)

	return handler
}
