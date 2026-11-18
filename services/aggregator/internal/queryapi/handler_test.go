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
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/audit"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeQueryRepo struct {
	mu               sync.Mutex
	events           []hazard.HazardEvent
	lastFilter       store.ListFilter
	getErr           error
	partial          bool
	unavailableUnits []string
	nextCursor       *string
	leaseInfo        store.LeaseInfo
	leaseErr         error
}

func (f *fakeQueryRepo) HealthCheck(context.Context) error                                     { return nil }
func (f *fakeQueryRepo) UpsertEvent(context.Context, store.UpsertRequest) (string, error)     { return "", nil }
func (f *fakeQueryRepo) UpsertFencedBatch(context.Context, store.Fence, string, time.Time, []store.UpsertRequest) error {
	return nil
}
func (f *fakeQueryRepo) GetVolcanoReferences(context.Context) (map[string]store.VolcanoReference, error) {
	return nil, nil
}
func (f *fakeQueryRepo) GetLatestEventTime(context.Context, string) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeQueryRepo) GetPollerWatermark(context.Context, string) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeQueryRepo) GetLease(context.Context, string) (store.LeaseInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaseInfo, f.leaseErr
}
func (f *fakeQueryRepo) ListAuditEvents(context.Context, store.AuditEventsFilter) (audit.EventsPage, error) {
	return audit.EventsPage{}, nil
}
func (f *fakeQueryRepo) ComputeChecksum(context.Context, string) (audit.Checksum, error) {
	return audit.Checksum{}, nil
}
func (f *fakeQueryRepo) LocateEvent(context.Context, string) (audit.LocationResponse, error) {
	return audit.LocationResponse{}, nil
}
func (f *fakeQueryRepo) GetEvents(_ context.Context, filter store.EventFilter) ([]hazard.HazardEvent, error) {
	res, err := f.ListEvents(context.Background(), store.ListFilter{
		HazardType: filter.HazardType,
		HazardID:   filter.HazardID,
		Since:      filter.Since,
		Limit:      filter.Limit,
	})
	return res.Events, err
}

func (f *fakeQueryRepo) ListEvents(_ context.Context, filter store.ListFilter) (store.ListResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter
	if f.getErr != nil {
		return store.ListResult{}, f.getErr
	}
	if filter.Cursor == "bad-cursor" {
		return store.ListResult{}, store.ErrInvalidCursor
	}
	var out []hazard.HazardEvent
	for _, e := range f.events {
		if filter.HazardType != "" && string(e.HazardType) != filter.HazardType {
			continue
		}
		if filter.HazardID != "" && e.HazardID != filter.HazardID {
			continue
		}
		if filter.Source != "" && string(e.Source) != filter.Source {
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
	return store.ListResult{
		Events:           out,
		NextCursor:       f.nextCursor,
		Partial:          f.partial,
		UnavailableUnits: f.unavailableUnits,
	}, nil
}

func (f *fakeQueryRepo) lastFilterCopy() store.ListFilter {
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

func TestGetEvents_ScatterMetaAndCursor(t *testing.T) {
	nextCur := "eyJ0IjoiMjAyNi0wOS0yMVQxMDowMDowMFoiLCJpZCI6IkUtMDAxIn0"
	repo := &fakeQueryRepo{
		events:           testEvents(),
		partial:          true,
		unavailableUnits: []string{"shard-2", "shard-5"},
		nextCursor:       &nextCur,
	}
	h := New(repo, zerolog.Nop(), nil)

	// Partial response returns 200 with meta.partial, unavailable_units, next_cursor
	rec := httptest.NewRecorder()
	h.GetEvents(rec, httptest.NewRequest(http.MethodGet, "/internal/events?source=BMKG", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on partial result, got %d", rec.Code)
	}
	if got := repo.lastFilterCopy().Source; got != "BMKG" {
		t.Errorf("expected source filter BMKG, got %q", got)
	}
	var resp hazard.EventsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !resp.Meta.Partial {
		t.Error("expected meta.partial=true")
	}
	if len(resp.Meta.UnavailableUnits) != 2 || resp.Meta.UnavailableUnits[0] != "shard-2" {
		t.Errorf("unexpected unavailable_units: %+v", resp.Meta.UnavailableUnits)
	}
	if resp.Meta.NextCursor == nil || *resp.Meta.NextCursor != nextCur {
		t.Errorf("unexpected next_cursor: %v", resp.Meta.NextCursor)
	}

	// Bad cursor returns 400
	recBad := httptest.NewRecorder()
	h.GetEvents(recBad, httptest.NewRequest(http.MethodGet, "/internal/events?cursor=bad-cursor", nil))
	if recBad.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid cursor, got %d", recBad.Code)
	}
}

func TestReady(t *testing.T) {
	h := New(&fakeQueryRepo{}, zerolog.Nop(), nil)

	// Starts not-ready -> 503
	rec1 := httptest.NewRecorder()
	h.Ready(rec1, httptest.NewRequest(http.MethodGet, "/internal/ready", nil))
	if rec1.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 before SetReady(true), got %d", rec1.Code)
	}

	// Ready -> 200
	h.SetReady(true)
	rec2 := httptest.NewRecorder()
	h.Ready(rec2, httptest.NewRequest(http.MethodGet, "/internal/ready", nil))
	if rec2.Code != http.StatusOK {
		t.Errorf("expected 200 after SetReady(true), got %d", rec2.Code)
	}

	// Draining -> 503
	h.SetReady(false)
	rec3 := httptest.NewRecorder()
	h.Ready(rec3, httptest.NewRequest(http.MethodGet, "/internal/ready", nil))
	if rec3.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when draining, got %d", rec3.Code)
	}
}

func TestLeader(t *testing.T) {
	exp := time.Date(2026, 11, 18, 12, 0, 15, 0, time.UTC)
	repo := &fakeQueryRepo{
		leaseInfo: store.LeaseInfo{
			Name:         store.DefaultLeaseName,
			Holder:       "agg-1",
			FencingToken: 7,
			ExpiresAt:    exp,
			IsActive:     true,
		},
	}

	h1 := New(repo, zerolog.Nop(), nil).WithInstanceID("agg-1")
	rec1 := httptest.NewRecorder()
	h1.Leader(rec1, httptest.NewRequest(http.MethodGet, "/internal/leader", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec1.Code)
	}
	var resp1 leaderResponse
	if err := json.NewDecoder(rec1.Body).Decode(&resp1); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp1.InstanceID != "agg-1" || !resp1.IsLeader || resp1.FencingToken != 7 {
		t.Errorf("unexpected leader response for holder: %+v", resp1)
	}

	h2 := New(repo, zerolog.Nop(), nil).WithInstanceID("agg-2")
	rec2 := httptest.NewRecorder()
	h2.Leader(rec2, httptest.NewRequest(http.MethodGet, "/internal/leader", nil))
	var resp2 leaderResponse
	if err := json.NewDecoder(rec2.Body).Decode(&resp2); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp2.InstanceID != "agg-2" || resp2.IsLeader || resp2.FencingToken != 7 {
		t.Errorf("unexpected leader response for follower: %+v", resp2)
	}
}
