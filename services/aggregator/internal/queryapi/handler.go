package queryapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

type SourceHealthFunc func() (bmkg, pvmbg string, pvmbgStaleSince *time.Time)

type Handler struct {
	repo            store.CanonicalRepo
	logger          zerolog.Logger
	sourceHealthFunc SourceHealthFunc
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(healthResponse{
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
	if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
		if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			filter.Since = &t
		}
	}

	ctx := r.Context()
	events, err := h.repo.GetEvents(ctx, filter)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to fetch events")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
		return
	}

	data := make([]map[string]interface{}, len(events))
	for i, e := range events {
		data[i] = map[string]interface{}{
			"hazard_id":    e.HazardID,
			"source":       string(e.Source),
			"hazard_type":  string(e.HazardType),
			"severity":     string(e.Severity),
			"area_name":    e.AreaName,
			"occurred_at":  e.OccurredAt,
			"ingested_at":  e.IngestedAt,
			"source_ref_id": e.SourceRefID,
			"latitude":     e.Latitude,
			"longitude":    e.Longitude,
		}
	}

	resp := eventsResponse{
		Data: data,
		Meta: map[string]interface{}{
			"count": len(data),
		},
	}

	if h.sourceHealthFunc != nil {
		bmkgStatus, pvmbgStatus, pvmbgStaleSince := h.sourceHealthFunc()
		sources := map[string]interface{}{
			"bmkg": map[string]interface{}{
				"status":         bmkgStatus,
				"last_synced_at": time.Now().UTC().Format(time.RFC3339),
			},
		}
		pvmbgMeta := map[string]interface{}{
			"status": pvmbgStatus,
		}
		if pvmbgStaleSince != nil {
			pvmbgMeta["stale_since"] = pvmbgStaleSince.Format(time.RFC3339)
		} else {
			pvmbgMeta["last_synced_at"] = time.Now().UTC().Format(time.RFC3339)
		}
		sources["pvmbg"] = pvmbgMeta
		resp.Meta["sources"] = sources
	}

	w.Header().Set("Content-Type", "application/json")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
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
