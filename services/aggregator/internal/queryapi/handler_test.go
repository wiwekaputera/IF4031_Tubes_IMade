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

func (f *fakeQueryRepo) HealthCheck(context.Context) error { return nil }

func (f *fakeQueryRepo) UpsertEvent(context.Context, store.UpsertRequest) (string, error) {
	return "", nil
}

func (f *fakeQueryRepo) GetVolcanoReferences(context.Context) (map[string]store.VolcanoReference, error) {
	return nil, nil
}

func (f *fakeQueryRepo) GetEvents(_ context.Context, filter store.EventFilter) ([]hazard.HazardEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter
	if f.getErr != nil {
		return nil, f.getErr
	}
	result := f.events
	if filter.HazardType != "" {
		out := make([]hazard.HazardEvent, 0, len(result))
		for _, e := range result {
			if string(e.HazardType) == filter.HazardType {
				out = append(out, e)
			}
		}
		result = out
	}
	if filter.HazardID != "" {
		out := make([]hazard.HazardEvent, 0, len(result))
		for _, e := range result {
			if e.HazardID == filter.HazardID {
				out = append(out, e)
			}
		}
		result = out
	}
	if filter.Since != nil {
		out := make([]hazard.HazardEvent, 0, len(result))
		for _, e := range result {
			t, err := time.Parse(time.RFC3339, e.OccurredAt)
			if err != nil {
				continue
			}
			if !t.Before(*filter.Since) {
				out = append(out, e)
			}
		}
		result = out
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}
	return result, nil
}

func (f *fakeQueryRepo) lastFilterCopy() store.EventFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastFilter
}

func testEvents() []hazard.HazardEvent {
	return []hazard.HazardEvent{
		{
			HazardID:   "E-001",
			Source:     hazard.SourceBMKG,
			HazardType: hazard.HazardTypeSeismic,
			Severity:   hazard.SeverityWaspada,
			AreaName:   "Jawa",
			OccurredAt: "2026-09-21T10:00:00Z",
			IngestedAt: "2026-09-21T10:01:00Z",
			Attributes: map[string]interface{}{
				"magnitude":         5.5,
				"depth_km":          10.0,
				"potential_tsunami": false,
			},
		},
		{
			HazardID:   "E-002",
			Source:     hazard.SourcePVMBG,
			HazardType: hazard.HazardTypeVolcanic,
			Severity:   hazard.SeverityNormal,
			AreaName:   "Gunung Merapi",
			OccurredAt: "2026-09-21T11:00:00Z",
			IngestedAt: "2026-09-21T11:01:00Z",
			Attributes: map[string]interface{}{
				"eruption_count_24h":  3,
				"ash_column_height_m": 5000.0,
			},
		},
		{
			HazardID:   "E-003",
			Source:     hazard.SourceBMKG,
			HazardType: hazard.HazardTypeSeismic,
			Severity:   hazard.SeveritySiaga,
			AreaName:   "Sulawesi",
			OccurredAt: "2026-09-21T12:00:00Z",
			IngestedAt: "2026-09-21T12:01:00Z",
			Attributes: map[string]interface{}{
				"magnitude":         6.5,
				"depth_km":          5.0,
				"potential_tsunami": false,
			},
		},
	}
}

func TestGetEvents_ReturnsAttributes(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Data) != 3 {
		t.Fatalf("expected 3 events, got %d", len(resp.Data))
	}

	attrs, ok := resp.Data[0]["attributes"].(map[string]interface{})
	if !ok {
		t.Fatal("expected attributes field as object in first event")
	}
	if attrs["magnitude"] != float64(5.5) {
		t.Errorf("expected magnitude 5.5, got %v", attrs["magnitude"])
	}
	if attrs["depth_km"] != float64(10.0) {
		t.Errorf("expected depth_km 10.0, got %v", attrs["depth_km"])
	}
	if attrs["potential_tsunami"] != false {
		t.Errorf("expected potential_tsunami false, got %v", attrs["potential_tsunami"])
	}

	if resp.Meta["count"] != float64(3) {
		t.Errorf("expected meta.count 3, got %v", resp.Meta["count"])
	}
}

func TestGetEvents_LimitClamping(t *testing.T) {
	tests := []struct {
		name       string
		limitParam string
		wantLimit  int
	}{
		{"default when absent", "", 20},
		{"reject zero", "0", 20},
		{"reject negative", "-5", 20},
		{"reject over 100", "101", 20},
		{"reject non-numeric", "abc", 20},
		{"accept 1", "1", 1},
		{"accept 50", "50", 50},
		{"accept 100", "100", 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeQueryRepo{events: testEvents()}
			h := New(repo, zerolog.Nop(), nil)

			url := "/internal/events"
			if tc.limitParam != "" {
				url += "?limit=" + tc.limitParam
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()
			h.GetEvents(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}

			got := repo.lastFilterCopy().Limit
			if got != tc.wantLimit {
				t.Errorf("filter.Limit = %d, want %d", got, tc.wantLimit)
			}
		})
	}
}

func TestGetEvents_LimitClampsResultCount(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events?limit=1", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if len(resp.Data) != 1 {
		t.Errorf("expected 1 event with limit=1, got %d", len(resp.Data))
	}
	if resp.Meta["count"] != float64(1) {
		t.Errorf("expected meta.count 1, got %v", resp.Meta["count"])
	}
}

func TestGetEvents_TypeFilter(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events?type=SEISMIC", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if resp.Meta["count"] != float64(2) {
		t.Errorf("expected count 2 (SEISMIC), got %v", resp.Meta["count"])
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp.Data))
	}
	for i, e := range resp.Data {
		if e["hazard_type"] != "SEISMIC" {
			t.Errorf("event %d: expected SEISMIC type, got %v", i, e["hazard_type"])
		}
	}

	if got := repo.lastFilterCopy().HazardType; got != "SEISMIC" {
		t.Errorf("filter.HazardType = %q, want SEISMIC", got)
	}
}

func TestGetEvents_HazardIDFilter(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events?hazard_id=E-002", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if resp.Meta["count"] != float64(1) {
		t.Errorf("expected count 1, got %v", resp.Meta["count"])
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Data))
	}
	if resp.Data[0]["hazard_id"] != "E-002" {
		t.Errorf("expected hazard_id E-002, got %v", resp.Data[0]["hazard_id"])
	}
	if resp.Data[0]["source"] != "PVMBG" {
		t.Errorf("expected source PVMBG, got %v", resp.Data[0]["source"])
	}

	if got := repo.lastFilterCopy().HazardID; got != "E-002" {
		t.Errorf("filter.HazardID = %q, want E-002", got)
	}
}

func TestGetEvents_TypeAndHazardIDFilterCombined(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events?type=VOLCANIC&hazard_id=E-002", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data []map[string]interface{} `json:"data"`
		Meta map[string]interface{}   `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if resp.Meta["count"] != float64(1) {
		t.Errorf("expected count 1, got %v", resp.Meta["count"])
	}

	f := repo.lastFilterCopy()
	if f.HazardType != "VOLCANIC" {
		t.Errorf("filter.HazardType = %q, want VOLCANIC", f.HazardType)
	}
	if f.HazardID != "E-002" {
		t.Errorf("filter.HazardID = %q, want E-002", f.HazardID)
	}
}

func TestHealth_405OnNonGET(t *testing.T) {
	h := New(&fakeQueryRepo{}, zerolog.Nop(), nil)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/internal/health", nil)
		w := httptest.NewRecorder()
		h.Health(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", method, w.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/health", nil)
	w := httptest.NewRecorder()
	h.Health(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET: expected 200, got %d", w.Code)
	}

	var resp healthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}
	if resp.Status != "UP" {
		t.Errorf("expected status UP, got %q", resp.Status)
	}
}

func TestGetEvents_MetaSourcesRealTimestamps(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}

	staleTime := time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC)
	healthFn := func() (bmkg, pvmbg string, pvmbgStaleSince *time.Time) {
		return "UP", "DOWN", &staleTime
	}

	h := New(repo, zerolog.Nop(), healthFn)
	req := httptest.NewRequest(http.MethodGet, "/internal/events", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Meta map[string]interface{} `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	sources, ok := resp.Meta["sources"].(map[string]interface{})
	if !ok {
		t.Fatal("expected sources in meta")
	}

	bmkg, ok := sources["bmkg"].(map[string]interface{})
	if !ok {
		t.Fatal("expected bmkg source")
	}
	if bmkg["status"] != "UP" {
		t.Errorf("expected bmkg status UP, got %v", bmkg["status"])
	}
	bmkgLastSynced, ok := bmkg["last_synced_at"].(string)
	if !ok {
		t.Fatal("expected bmkg last_synced_at")
	}
	if _, err := time.Parse(time.RFC3339, bmkgLastSynced); err != nil {
		t.Errorf("invalid bmkg last_synced_at %q: %v", bmkgLastSynced, err)
	}

	pvmbg, ok := sources["pvmbg"].(map[string]interface{})
	if !ok {
		t.Fatal("expected pvmbg source")
	}
	if pvmbg["status"] != "DOWN" {
		t.Errorf("expected pvmbg status DOWN, got %v", pvmbg["status"])
	}
	pvmbgStale, hasStale := pvmbg["stale_since"]
	if !hasStale {
		t.Fatal("expected pvmbg stale_since from sourceHealthFunc")
	}
	expectedStale := staleTime.Format(time.RFC3339)
	if pvmbgStale != expectedStale {
		t.Errorf("expected pvmbg stale_since %q, got %v", expectedStale, pvmbgStale)
	}
	if _, hasLastSynced := pvmbg["last_synced_at"]; hasLastSynced {
		t.Error("did not expect last_synced_at when stale_since is present")
	}
}

func TestGetEvents_NoSourceHealthFunc(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Meta map[string]interface{} `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if _, hasSources := resp.Meta["sources"]; hasSources {
		t.Error("did not expect sources in meta when sourceHealthFunc is nil")
	}
}

func TestGetEvents_MetaSourcesLastSyncedWhenNotStale(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	healthFn := func() (bmkg, pvmbg string, pvmbgStaleSince *time.Time) {
		return "UP", "UP", nil
	}

	h := New(repo, zerolog.Nop(), healthFn)
	req := httptest.NewRequest(http.MethodGet, "/internal/events", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Meta map[string]interface{} `json:"meta"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	sources := resp.Meta["sources"].(map[string]interface{})
	pvmbg := sources["pvmbg"].(map[string]interface{})

	if pvmbg["status"] != "UP" {
		t.Errorf("expected pvmbg status UP, got %v", pvmbg["status"])
	}
	if _, hasStale := pvmbg["stale_since"]; hasStale {
		t.Error("did not expect stale_since when pvmbgStaleSince is nil")
	}
	lastSynced, ok := pvmbg["last_synced_at"].(string)
	if !ok {
		t.Fatal("expected pvmbg last_synced_at when not stale")
	}
	if _, err := time.Parse(time.RFC3339, lastSynced); err != nil {
		t.Errorf("invalid pvmbg last_synced_at %q: %v", lastSynced, err)
	}
}

func TestGetEvents_RepoError(t *testing.T) {
	repo := &fakeQueryRepo{getErr: context.DeadlineExceeded}
	h := New(repo, zerolog.Nop(), nil)

	req := httptest.NewRequest(http.MethodGet, "/internal/events", nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestGetEvents_InvalidSinceReturns400(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	for _, sinceParam := range []string{"not-a-timestamp", "2026-13-45T99:99:99Z", "yesterday"} {
		req := httptest.NewRequest(http.MethodGet, "/internal/events?since="+sinceParam, nil)
		w := httptest.NewRecorder()
		h.GetEvents(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("since=%q: expected 400, got %d", sinceParam, w.Code)
		}

		var errResp map[string]string
		if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
			t.Fatalf("since=%q: failed to decode error response: %v", sinceParam, err)
		}
		if errResp["error"] != "invalid ISO 8601 'since' timestamp, expected RFC3339 format" {
			t.Errorf("since=%q: expected error message, got %q", sinceParam, errResp["error"])
		}
	}
}

func TestGetEvents_ValidSinceParses(t *testing.T) {
	repo := &fakeQueryRepo{events: testEvents()}
	h := New(repo, zerolog.Nop(), nil)

	sinceStr := "2026-09-21T10:30:00Z"
	req := httptest.NewRequest(http.MethodGet, "/internal/events?since="+sinceStr, nil)
	w := httptest.NewRecorder()
	h.GetEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	wantSince, _ := time.Parse(time.RFC3339, sinceStr)
	if got := repo.lastFilterCopy().Since; got == nil || !got.Equal(wantSince) {
		t.Errorf("filter.Since = %v, want ~%v", got, wantSince)
	}
}
