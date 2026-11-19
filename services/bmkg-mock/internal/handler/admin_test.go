package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

func newTestBMKGHandler() (*Handler, *store.MemoryStore) {
	memStore := store.NewMemoryStore(1000)
	cfg := &config.Config{
		Port:       "8081",
		APIKey:     "test-key",
		DelayMinMs: 0,
		DelayMaxMs: 0,
	}
	return NewHandler(memStore, cfg, zerolog.Nop()), memStore
}

func TestRequestRecorder_RingBufferBound(t *testing.T) {
	rec := NewRequestRecorder(3)
	for i := 0; i < 5; i++ {
		rec.Record("inst-a", "/seismic-events", "2026-01-01T00:00:00Z", 200)
	}

	snap := rec.Snapshot(10 * time.Second)
	if snap.Total != 5 {
		t.Fatalf("expected total 5, got %d", snap.Total)
	}
	if len(snap.Recent) != 3 {
		t.Fatalf("expected 3 recent records (ring buffer bound), got %d", len(snap.Recent))
	}
	if snap.ByInstance["inst-a"] != 5 {
		t.Fatalf("expected inst-a count 5, got %d", snap.ByInstance["inst-a"])
	}
}

func TestAdminRequests_DuplicateDetectionAndReset(t *testing.T) {
	h, _ := newTestBMKGHandler()
	router := h.Routes()

	doDataReq := func(instanceID, since string) {
		req := httptest.NewRequest(http.MethodGet, "/seismic-events?since="+since, nil)
		req.Header.Set("X-BMKG-Key", "test-key")
		if instanceID != "" {
			req.Header.Set("X-Instance-ID", instanceID)
		}
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
	}

	// Single instance only -> no duplicates
	doDataReq("agg-1", "2026-01-01T00:00:00Z")
	doDataReq("agg-1", "2026-01-01T00:00:00Z")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/requests", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp RequestsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Total != 2 || resp.ByInstance["agg-1"] != 2 {
		t.Fatalf("unexpected counts: %+v", resp)
	}
	if len(resp.Duplicates) != 0 {
		t.Fatalf("expected 0 duplicates for single instance, got %+v", resp.Duplicates)
	}

	// Second instance with same (path, since) -> duplicate detected
	doDataReq("agg-2", "2026-01-01T00:00:00Z")

	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/requests", nil))
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(resp.Duplicates) != 1 {
		t.Fatalf("expected 1 duplicate, got %+v", resp.Duplicates)
	}
	if resp.Duplicates[0].Path != "/seismic-events" || resp.Duplicates[0].Count != 3 {
		t.Fatalf("unexpected duplicate entry: %+v", resp.Duplicates[0])
	}

	// Reset clears everything
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/admin/requests/reset", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 on reset, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/requests", nil))
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Total != 0 || len(resp.Recent) != 0 || len(resp.Duplicates) != 0 {
		t.Fatalf("expected empty state after reset, got %+v", resp)
	}
}

func TestAdminBurst_ValidationAndGeneration(t *testing.T) {
	h, memStore := newTestBMKGHandler()
	router := h.Routes()

	badPayloads := []string{
		`{invalid`,
		`{"region_name":"","count":10,"duration_s":0,"lat":-0.9,"lon":119.8}`,
		`{"region_name":"Sulawesi Tengah","count":0,"duration_s":0,"lat":-0.9,"lon":119.8}`,
		`{"region_name":"Sulawesi Tengah","count":100001,"duration_s":0,"lat":-0.9,"lon":119.8}`,
		`{"region_name":"Sulawesi Tengah","count":10,"duration_s":0,"lat":95,"lon":119.8}`,
	}
	for _, body := range badPayloads {
		req := httptest.NewRequest(http.MethodPost, "/admin/burst", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d", body, rr.Code)
		}
	}

	validBody := `{"region_name":"Sulawesi Tengah","count":25,"duration_s":0,"lat":-0.9,"lon":119.8}`
	req := httptest.NewRequest(http.MethodPost, "/admin/burst", bytes.NewBufferString(validBody))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}

	seismicCount, _ := memStore.Count()
	if seismicCount != 25 {
		t.Fatalf("expected 25 seismic events in store, got %d", seismicCount)
	}
	events := memStore.GetSeismicEvents(nil, 100)
	for _, ev := range events {
		if ev.RegionName != "Sulawesi Tengah" {
			t.Fatalf("expected region Sulawesi Tengah, got %s", ev.RegionName)
		}
	}
}
