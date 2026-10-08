package queryapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type SourceHealthFunc func() (bmkgStatus, pvmbgStatus string, bmkgLastSynced, pvmbgLastSynced time.Time, pvmbgStaleSince *time.Time)

type Handler struct {
	repo             store.CanonicalRepo
	logger           zerolog.Logger
	sourceHealthFunc SourceHealthFunc
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error().Err(err).Msg("failed to encode JSON response")
	}
}

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

func New(repo store.CanonicalRepo, logger zerolog.Logger, healthFn SourceHealthFunc) *Handler {
	return &Handler{
		repo:             repo,
		logger:           logger.With().Str("component", "queryapi").Logger(),
		sourceHealthFunc: healthFn,
	}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	h.writeJSON(w, http.StatusOK, hazard.HealthResponse{
		Status: "UP",
		Time:   time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *Handler) GetEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	filter := store.EventFilter{}
	filter.HazardType = r.URL.Query().Get("type")
	filter.HazardID = r.URL.Query().Get("hazard_id")
	filter.Limit = 20
	if limStr := r.URL.Query().Get("limit"); limStr != "" {
		if lim, err := strconv.Atoi(limStr); err == nil && lim > 0 && lim <= 100 {
			filter.Limit = lim
		}
	}

	sinceTime, ok := parseSinceParam(r)
	if !ok {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid ISO 8601 'since' timestamp, expected RFC3339 format",
		})
		return
	}
	if sinceTime != nil {
		filter.Since = sinceTime
	}

	ctx := r.Context()
	events, err := h.repo.GetEvents(ctx, filter)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to fetch events")
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	if events == nil {
		events = make([]hazard.HazardEvent, 0)
	}

	resp := hazard.EventsResponse{
		Data: events,
		Meta: hazard.EventsMeta{
			Count: len(events),
		},
	}

	if h.sourceHealthFunc != nil {
		bmkgStatus, pvmbgStatus, bmkgLastSynced, pvmbgLastSynced, pvmbgStaleSince := h.sourceHealthFunc()

		bmkgMeta := hazard.SourceHealthMeta{
			Status: bmkgStatus,
		}
		if !bmkgLastSynced.IsZero() {
			bmkgMeta.LastSyncedAt = bmkgLastSynced.UTC().Format(time.RFC3339)
		} else {
			bmkgMeta.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		}

		pvmbgMeta := hazard.SourceHealthMeta{
			Status: pvmbgStatus,
		}
		if pvmbgStaleSince != nil {
			staleStr := pvmbgStaleSince.UTC().Format(time.RFC3339)
			pvmbgMeta.StaleSince = &staleStr
		} else if !pvmbgLastSynced.IsZero() {
			pvmbgMeta.LastSyncedAt = pvmbgLastSynced.UTC().Format(time.RFC3339)
		} else {
			pvmbgMeta.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
		}

		resp.Meta.Sources = &hazard.SourcesMeta{
			BMKG:  bmkgMeta,
			PVMBG: pvmbgMeta,
		}
	}

	h.writeJSON(w, http.StatusOK, resp)
}

func Routes(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/health", h.Health)
	mux.HandleFunc("/internal/events", h.GetEvents)

	var handler http.Handler = mux
	handler = LoggingMiddleware(h.logger, handler)
	handler = CorrelationIDMiddleware(handler)
	return handler
}
