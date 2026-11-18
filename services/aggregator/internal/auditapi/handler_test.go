package auditapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/audit"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeAuditRepo struct {
	lastEventsFilter store.AuditEventsFilter
	eventsPage       audit.EventsPage
	eventsErr        error

	lastChecksumSource string
	checksumResp       audit.Checksum
	checksumErr        error

	lastLocationID string
	locationResp   audit.LocationResponse
	locationErr    error
}

func (f *fakeAuditRepo) HealthCheck(context.Context) error                                 { return nil }
func (f *fakeAuditRepo) UpsertEvent(context.Context, store.UpsertRequest) (string, error)  { return "", nil }
func (f *fakeAuditRepo) UpsertFencedBatch(context.Context, store.Fence, string, time.Time, []store.UpsertRequest) error {
	return nil
}
func (f *fakeAuditRepo) GetEvents(context.Context, store.EventFilter) ([]hazard.HazardEvent, error) {
	return nil, nil
}
func (f *fakeAuditRepo) ListEvents(context.Context, store.ListFilter) (store.ListResult, error) {
	return store.ListResult{}, nil
}
func (f *fakeAuditRepo) GetVolcanoReferences(context.Context) (map[string]store.VolcanoReference, error) {
	return nil, nil
}
func (f *fakeAuditRepo) GetLatestEventTime(context.Context, string) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeAuditRepo) GetPollerWatermark(context.Context, string) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeAuditRepo) GetLease(context.Context, string) (store.LeaseInfo, error) {
	return store.LeaseInfo{}, nil
}

func (f *fakeAuditRepo) ListAuditEvents(_ context.Context, filter store.AuditEventsFilter) (audit.EventsPage, error) {
	f.lastEventsFilter = filter
	return f.eventsPage, f.eventsErr
}

func (f *fakeAuditRepo) ComputeChecksum(_ context.Context, source string) (audit.Checksum, error) {
	f.lastChecksumSource = source
	return f.checksumResp, f.checksumErr
}

func (f *fakeAuditRepo) LocateEvent(_ context.Context, hazardID string) (audit.LocationResponse, error) {
	f.lastLocationID = hazardID
	return f.locationResp, f.locationErr
}

func newTestMux(repo store.CanonicalRepo) *http.ServeMux {
	h := New(repo, "secret-audit-token", zerolog.Nop())
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func TestAuditAuth(t *testing.T) {
	mux := newTestMux(&fakeAuditRepo{})
	endpoints := []string{
		"/internal/audit/events",
		"/internal/audit/checksum",
		"/internal/audit/location?hazard_id=hz-1",
	}

	for _, ep := range endpoints {
		t.Run("missing_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", rec.Code)
			}
			var body audit.ErrorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal error body: %v", err)
			}
			if body.Error.Code != "unauthorized" || body.Error.Message != "token tidak valid" {
				t.Errorf("unexpected error body: %+v", body)
			}
		})

		t.Run("wrong_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep, nil)
			req.Header.Set("Authorization", "Bearer wrong-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", rec.Code)
			}
		})

		t.Run("valid_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep, nil)
			req.Header.Set("Authorization", "Bearer secret-audit-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestAuditEvents_ValidationAndResponse(t *testing.T) {
	repo := &fakeAuditRepo{
		eventsPage: audit.EventsPage{
			Items: []hazard.HazardEvent{
				{
					HazardID:    "hz-1",
					Source:      hazard.SourceBMKG,
					SourceRefID: "eq-1",
					HazardType:  hazard.HazardTypeSeismic,
					Severity:    hazard.SeverityWaspada,
					AreaName:    "Jawa",
					OccurredAt:  "2026-11-12T02:43:00Z",
					IngestedAt:  "2026-11-12T02:51:08Z",
				},
			},
			Partial:          true,
			UnavailableUnits: []string{"shard-2"},
		},
	}
	mux := newTestMux(repo)

	cases := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{"default limit 100", "", http.StatusOK},
		{"max limit 1000", "?limit=1000", http.StatusOK},
		{"limit 0", "?limit=0", http.StatusBadRequest},
		{"limit 1001", "?limit=1001", http.StatusBadRequest},
		{"limit non-int", "?limit=abc", http.StatusBadRequest},
		{"bad source", "?source=UNKNOWN", http.StatusBadRequest},
		{"valid source", "?source=BMKG", http.StatusOK},
		{"bad since", "?since=not-a-time", http.StatusBadRequest},
		{"valid since", "?since=2026-11-12T00:00:00Z", http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/internal/audit/events"+tc.query, nil)
			req.Header.Set("Authorization", "Bearer secret-audit-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("query %q: expected %d, got %d (%s)", tc.query, tc.wantStatus, rec.Code, rec.Body.String())
			}
			if tc.wantStatus == http.StatusBadRequest {
				var eb audit.ErrorBody
				if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != "bad_request" {
					t.Errorf("expected bad_request ErrorBody, got %s", rec.Body.String())
				}
			}
		})
	}

	// Verify default limit=100 was passed and partial response is preserved.
	req := httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if repo.lastEventsFilter.Limit != 100 {
		t.Errorf("expected default limit 100, got %d", repo.lastEventsFilter.Limit)
	}
	var got audit.EventsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !got.Partial || len(got.UnavailableUnits) != 1 || got.UnavailableUnits[0] != "shard-2" {
		t.Errorf("unexpected partial metadata: %+v", got)
	}

	// Verify invalid cursor maps to 400.
	repo.eventsErr = store.ErrInvalidCursor
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/events?cursor=bad", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid cursor, got %d", rec.Code)
	}
}

func TestAuditChecksum(t *testing.T) {
	src := "SATELIT"
	repo := &fakeAuditRepo{
		checksumResp: audit.Checksum{
			Source:           &src,
			Count:            3,
			Checksum:         "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
			Algorithm:        "sha256",
			AsOf:             "2026-11-12T03:00:00Z",
			UnavailableUnits: []string{},
		},
	}
	mux := newTestMux(repo)

	req := httptest.NewRequest(http.MethodGet, "/internal/audit/checksum?source=INVALID", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid source, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/internal/audit/checksum?source=SATELIT", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if repo.lastChecksumSource != "SATELIT" {
		t.Errorf("expected source SATELIT, got %q", repo.lastChecksumSource)
	}
}

func TestAuditLocation(t *testing.T) {
	repo := &fakeAuditRepo{
		locationErr: store.ErrNotFound,
	}
	mux := newTestMux(repo)

	// Missing hazard_id -> 400
	req := httptest.NewRequest(http.MethodGet, "/internal/audit/location", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when hazard_id missing, got %d", rec.Code)
	}

	// Not found -> 404
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/location?hazard_id=hz-missing", nil)
	req.Header.Set("Authorization", "Bearer secret-audit-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when not found, got %d", rec.Code)
	}
	var eb audit.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != "not_found" {
		t.Errorf("expected not_found ErrorBody, got %s", rec.Body.String())
	}
}
