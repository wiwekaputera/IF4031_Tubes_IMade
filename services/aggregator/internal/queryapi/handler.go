package queryapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
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

type healthResponse struct {
	Status string `json:"status"`
	Time   string `json:"time"`
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	h.writeJSON(w, http.StatusOK, healthResponse{
		Status: "UP",
		Time:   time.Now().UTC().Format(time.RFC3339),
	})
}

type eventsResponse struct {
	Data []map[string]interface{} `json:"data"`
	Meta map[string]interface{}   `json:"meta"`
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

	data := make([]map[string]interface{}, len(events))
	for i, e := range events {
		data[i] = map[string]interface{}{
			"hazard_id":     e.HazardID,
			"source":        string(e.Source),
			"hazard_type":   string(e.HazardType),
			"severity":      string(e.Severity),
			"area_name":     e.AreaName,
			"occurred_at":   e.OccurredAt,
			"ingested_at":   e.IngestedAt,
			"source_ref_id": e.SourceRefID,
			"latitude":      e.Latitude,
			"longitude":     e.Longitude,
			"attributes":    e.Attributes,
		}
	}

	resp := eventsResponse{
		Data: data,
		Meta: map[string]interface{}{
			"count": len(data),
		},
	}

	if h.sourceHealthFunc != nil {
		bmkgStatus, pvmbgStatus, bmkgLastSynced, pvmbgLastSynced, pvmbgStaleSince := h.sourceHealthFunc()
		bmkgMeta := map[string]interface{}{
			"status": bmkgStatus,
		}
		if !bmkgLastSynced.IsZero() {
			bmkgMeta["last_synced_at"] = bmkgLastSynced.UTC().Format(time.RFC3339)
		} else {
			bmkgMeta["last_synced_at"] = time.Now().UTC().Format(time.RFC3339)
		}

		pvmbgMeta := map[string]interface{}{
			"status": pvmbgStatus,
		}
		if pvmbgStaleSince != nil {
			pvmbgMeta["stale_since"] = pvmbgStaleSince.Format(time.RFC3339)
		} else if !pvmbgLastSynced.IsZero() {
			pvmbgMeta["last_synced_at"] = pvmbgLastSynced.UTC().Format(time.RFC3339)
		} else {
			pvmbgMeta["last_synced_at"] = time.Now().UTC().Format(time.RFC3339)
		}

		resp.Meta["sources"] = map[string]interface{}{
			"bmkg":  bmkgMeta,
			"pvmbg": pvmbgMeta,
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
