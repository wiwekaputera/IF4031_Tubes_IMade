package queryapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeQueryRepo struct {
	mu         sync.Mutex
	events     []hazard.HazardEvent
	lastFilter store.EventFilter
	getErr     error
}

func (f *fakeQueryRepo) HealthCheck(context.Context) error                                     { return nil }
func (f *fakeQueryRepo) UpsertEvent(context.Context, store.UpsertRequest) (string, error)     { return "", nil }
func (f *fakeQueryRepo) GetVolcanoReferences(context.Context) (map[string]store.VolcanoReference, error) {
	return nil, nil
}
func (f *fakeQueryRepo) GetLatestEventTime(context.Context, string) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeQueryRepo) GetEvents(_ context.Context, filter store.EventFilter) ([]hazard.HazardEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter
	if f.getErr != nil {
		return nil, f.getErr
	}
	var out []hazard.HazardEvent
	for _, e := range f.events {
		if filter.HazardType != "" && string(e.HazardType) != filter.HazardType {
			continue
		}
		if filter.HazardID != "" && e.HazardID != filter.HazardID {
			continue
		}
		if filter.Since != nil {
			t, err := time.Parse(time.RFC3339, e.OccurredAt)
			if err != nil || t.Before(*filter.Since) {
				continue
			}
		}
		out = append(out, e)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (f *fakeQueryRepo) lastFilterCopy() store.EventFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastFilter
}

func testEvents() []hazard.HazardEvent {
	return []hazard.HazardEvent{
		{HazardID: "E-001", Source: hazard.SourceBMKG, HazardType: hazard.HazardTypeSeismic, Severity: hazard.SeverityWaspada, AreaName: "Jawa", OccurredAt: "2026-09-21T10:00:00Z", Attributes: map[string]interface{}{"magnitude": 5.5, "depth_km": 10.0, "potential_tsunami": false}},
		{HazardID: "E-002", Source: hazard.SourcePVMBG, HazardType: hazard.HazardTypeVolcanic, Severity: hazard.SeverityNormal, AreaName: "Merapi", OccurredAt: "2026-09-21T11:00:00Z", Attributes: map[string]interface{}{"eruption_count_24h": 3}},
		{HazardID: "E-003", Source: hazard.SourceBMKG, HazardType: hazard.HazardTypeSeismic, Severity: hazard.SeveritySiaga, AreaName: "Sulawesi", OccurredAt: "2026-09-21T12:00:00Z", Attributes: map[string]interface{}{"magnitude": 6.5}},
	}
}

func TestGetEvents_AttributesAndLimitClamping(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	// Attributes decoding & meta.count
	w := httptest.NewRecorder()
	h.GetEvents(w, httptest.NewRequest(http.MethodGet, "/internal/events", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Data) != 3 || resp.Meta["count"] != float64(3) {
		t.Fatalf("expected 3 events, got %d", len(resp.Data))
	}
	attrs := resp.Data[0]["attributes"].(map[string]interface{})
	if attrs["magnitude"] != 5.5 || attrs["potential_tsunami"] != false {
		t.Errorf("unexpected attributes: %+v", attrs)
	}

	// Limit clamping table
	limitTests := []struct {
		param string
		want  int
	}{
		{"", 20}, {"0", 20}, {"-5", 20}, {"101", 20}, {"abc", 20},
		{"1", 1}, {"50", 50}, {"100", 100},
	}
	for _, tc := range limitTests {
		url := "/internal/events"
		if tc.param != "" {
			url += "?limit=" + tc.param
		}
		rec := httptest.NewRecorder()
		h.GetEvents(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if got := repo.lastFilterCopy().Limit; got != tc.want {
			t.Errorf("limit %q: got %d, want %d", tc.param, got, tc.want)
		}
	}

	// Limit slices returned results
	recLimit1 := httptest.NewRecorder()
	h.GetEvents(recLimit1, httptest.NewRequest(http.MethodGet, "/internal/events?limit=1", nil))
	var resp1 struct{ Data []map[string]interface{} }
	_ = json.NewDecoder(recLimit1.Body).Decode(&resp1)
	if len(resp1.Data) != 1 {
		t.Errorf("expected 1 event, got %d", len(resp1.Data))
	}
}

func TestGetEvents_Filtering(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	t.Run("filter by type", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.GetEvents(rec, httptest.NewRequest(http.MethodGet, "/internal/events?type=SEISMIC", nil))
		var resp struct{ Data []map[string]interface{} }
		_ = json.NewDecoder(rec.Body).Decode(&resp)
		if len(resp.Data) != 2 || repo.lastFilterCopy().HazardType != "SEISMIC" {
			t.Fatalf("expected 2 SEISMIC events, got %d", len(resp.Data))
		}
	})

	t.Run("filter by hazard_id and combined", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.GetEvents(rec, httptest.NewRequest(http.MethodGet, "/internal/events?type=VOLCANIC&hazard_id=E-002", nil))
		var resp struct{ Data []map[string]interface{} }
		_ = json.NewDecoder(rec.Body).Decode(&resp)
		if len(resp.Data) != 1 || resp.Data[0]["hazard_id"] != "E-002" {
			t.Fatalf("expected event E-002, got %+v", resp.Data)
		}
	})

	t.Run("filter by since timestamp", func(t *testing.T) {
		// Invalid timestamp
		for _, bad := range []string{"not-a-timestamp", "yesterday"} {
			recBad := httptest.NewRecorder()
			h.GetEvents(recBad, httptest.NewRequest(http.MethodGet, "/internal/events?since="+bad, nil))
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for bad since %s, got %d", bad, recBad.Code)
			}
		}

		// Valid timestamp
		sinceStr := "2026-09-21T10:30:00Z"
		recOk := httptest.NewRecorder()
		h.GetEvents(recOk, httptest.NewRequest(http.MethodGet, "/internal/events?since="+sinceStr, nil))
		if recOk.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid since, got %d", recOk.Code)
		}
		wantT, _ := time.Parse(time.RFC3339, sinceStr)
		if got := repo.lastFilterCopy().Since; got == nil || !got.Equal(wantT) {
			t.Errorf("expected filter.Since %v, got %v", wantT, got)
		}
	})
}

func TestGetEvents_MetaSources(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	staleTime := time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC)
	syncTime := time.Date(2026, 9, 21, 10, 35, 0, 0, time.UTC)

	// 1. With healthFn showing BMKG UP and PVMBG DOWN/Stale
	healthFn := func() (string, string, time.Time, time.Time, *time.Time) {
		return "UP", "DOWN", syncTime, time.Time{}, &staleTime
	}
	h := New(repo, zerolog.Nop(), healthFn)
	rec := httptest.NewRecorder()
	h.GetEvents(rec, httptest.NewRequest(http.MethodGet, "/internal/events", nil))

	var typedResp hazard.EventsResponse
	if err := json.NewDecoder(rec.Body).Decode(&typedResp); err != nil {
		t.Fatalf("failed to decode typed response: %v", err)
	}
	if typedResp.Meta.Sources == nil {
		t.Fatal("expected typedResp.Meta.Sources not nil")
	}
	if typedResp.Meta.Sources.BMKG.Status != "UP" || typedResp.Meta.Sources.PVMBG.Status != "DOWN" {
		t.Fatalf("unexpected meta.sources: %+v", typedResp.Meta.Sources)
	}
	if typedResp.Meta.Sources.PVMBG.StaleSince == nil || *typedResp.Meta.Sources.PVMBG.StaleSince != staleTime.Format(time.RFC3339) {
		t.Errorf("expected stale_since %s, got %v", staleTime.Format(time.RFC3339), typedResp.Meta.Sources.PVMBG.StaleSince)
	}

	// 2. Without healthFn, sources omitted from meta
	hNil := New(repo, zerolog.Nop(), nil)
	recNil := httptest.NewRecorder()
	hNil.GetEvents(recNil, httptest.NewRequest(http.MethodGet, "/internal/events", nil))
	var typedRespNil hazard.EventsResponse
	if err := json.NewDecoder(recNil.Body).Decode(&typedRespNil); err != nil {
		t.Fatalf("failed to decode nil response: %v", err)
	}
	if typedRespNil.Meta.Sources != nil {
		t.Error("expected sources omitted (nil) when healthFn is nil")
	}
}

func TestHealthAndRepoError(t *testing.T) {
	h := New(&fakeQueryRepo{getErr: context.DeadlineExceeded}, zerolog.Nop(), nil)

	// Health endpoint allows only GET
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		w := httptest.NewRecorder()
		h.Health(w, httptest.NewRequest(m, "/internal/health", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", m, w.Code)
		}
	}
	wHealth := httptest.NewRecorder()
	h.Health(wHealth, httptest.NewRequest(http.MethodGet, "/internal/health", nil))
	if wHealth.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", wHealth.Code)
	}

	// Repo error returns 500
	wErr := httptest.NewRecorder()
	h.GetEvents(wErr, httptest.NewRequest(http.MethodGet, "/internal/events", nil))
	if wErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on repo error, got %d", wErr.Code)
	}
}
