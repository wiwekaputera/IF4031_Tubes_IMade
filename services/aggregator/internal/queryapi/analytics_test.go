package queryapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

func TestGetAnalyticsSummary_RangeParsingAndParams(t *testing.T) {
	repo := &fakeQueryRepo{
		analyticsResp: store.AnalyticsSummary{
			From:             "2026-11-11T00:00:00Z",
			To:               "2026-11-18T00:00:00Z",
			Bucket:           "day",
			Total:            12,
			ByCategory:       map[string]int64{"SEISMIC": 12},
			Series:           map[string][]store.AnalyticsSeriesPoint{},
			BySeverity:       map[string]int64{"NORMAL": 12},
			ByArea:           []store.AnalyticsAreaCount{{AreaName: "Maluku", Count: 12}},
			GeneratedAt:      "2026-11-18T00:00:01Z",
			Partial:          false,
			UnavailableUnits: []string{},
		},
	}
	h := New(repo, zerolog.Nop(), nil)
	mux := Routes(h)

	req := httptest.NewRequest(http.MethodGet, "/internal/analytics/summary?from=2026-11-11T00:00:00Z&to=2026-11-18T00:00:00Z&bucket=hour&top_areas=15", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}

	if repo.lastAnalyticsFilter.Bucket != "hour" {
		t.Fatalf("Bucket = %q, want hour", repo.lastAnalyticsFilter.Bucket)
	}
	if repo.lastAnalyticsFilter.TopAreas != 15 {
		t.Fatalf("TopAreas = %d, want 15", repo.lastAnalyticsFilter.TopAreas)
	}
	if repo.lastAnalyticsFilter.From == nil || repo.lastAnalyticsFilter.From.Format(time.RFC3339) != "2026-11-11T00:00:00Z" {
		t.Fatalf("From = %v, want 2026-11-11T00:00:00Z", repo.lastAnalyticsFilter.From)
	}
}

func TestGetAnalyticsSummary_AllPreset(t *testing.T) {
	repo := &fakeQueryRepo{
		analyticsResp: store.AnalyticsSummary{
			Partial:          true,
			UnavailableUnits: []string{"shard-2"},
		},
	}
	h := New(repo, zerolog.Nop(), nil)
	mux := Routes(h)

	req := httptest.NewRequest(http.MethodGet, "/internal/analytics/summary?range=all", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !repo.lastAnalyticsFilter.All {
		t.Fatalf("All = false, want true")
	}

	var body store.AnalyticsSummary
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.Partial || len(body.UnavailableUnits) != 1 || body.UnavailableUnits[0] != "shard-2" {
		t.Fatalf("unexpected partial body: %+v", body)
	}
}

func TestGetAnalyticsSummary_InvalidParams(t *testing.T) {
	repo := &fakeQueryRepo{}
	h := New(repo, zerolog.Nop(), nil)
	mux := Routes(h)

	cases := []string{
		"/internal/analytics/summary?from=not-a-date",
		"/internal/analytics/summary?to=not-a-date",
		"/internal/analytics/summary?from=2026-11-18T00:00:00Z&to=2026-11-17T00:00:00Z",
		"/internal/analytics/summary?range=99y",
		"/internal/analytics/summary?bucket=week",
		"/internal/analytics/summary?top_areas=0",
		"/internal/analytics/summary?top_areas=101",
	}

	for _, url := range cases {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("GET %s: status = %d, want 400", url, rr.Code)
		}
	}
}
