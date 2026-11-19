package queryapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

func parseISO8601(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, raw)
	}
	return t.UTC(), err
}

// GetAnalyticsSummary handles GET /internal/analytics/summary.
func (h *Handler) GetAnalyticsSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	filter := store.AnalyticsFilter{}

	toTime := time.Now().UTC()
	if toStr := q.Get("to"); toStr != "" {
		t, err := parseISO8601(toStr)
		if err != nil {
			h.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid ISO 8601 'to' timestamp, expected RFC3339 format",
			})
			return
		}
		toTime = t
	}
	filter.To = toTime

	if fromStr := q.Get("from"); fromStr != "" {
		if fromStr == "all" {
			filter.All = true
		} else {
			t, err := parseISO8601(fromStr)
			if err != nil {
				h.writeJSON(w, http.StatusBadRequest, map[string]string{
					"error": "invalid ISO 8601 'from' timestamp, expected RFC3339 format",
				})
				return
			}
			filter.From = &t
		}
	} else if rangePreset := q.Get("range"); rangePreset != "" {
		switch rangePreset {
		case "all":
			filter.All = true
		case "24h":
			t := toTime.Add(-24 * time.Hour)
			filter.From = &t
		case "7d":
			t := toTime.Add(-7 * 24 * time.Hour)
			filter.From = &t
		case "30d":
			t := toTime.Add(-30 * 24 * time.Hour)
			filter.From = &t
		default:
			h.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid 'range' preset, expected 24h, 7d, 30d, or all",
			})
			return
		}
	}

	if filter.From != nil && filter.From.After(filter.To) {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "'from' must be before or equal to 'to'",
		})
		return
	}

	if bucket := q.Get("bucket"); bucket != "" {
		if bucket != "hour" && bucket != "day" {
			h.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid 'bucket', expected hour or day",
			})
			return
		}
		filter.Bucket = bucket
	}

	if topStr := q.Get("top_areas"); topStr != "" {
		top, err := strconv.Atoi(topStr)
		if err != nil || top < 1 || top > 100 {
			h.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid 'top_areas', expected integer between 1 and 100",
			})
			return
		}
		filter.TopAreas = top
	}

	ctx := store.WithCorrelationID(r.Context(), correlationIDFromContext(r.Context()))
	summary, err := h.repo.GetAnalyticsSummary(ctx, filter)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to compute analytics summary")
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	h.writeJSON(w, http.StatusOK, summary)
}
