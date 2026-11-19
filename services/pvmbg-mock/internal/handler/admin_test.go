package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/admin"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

func TestPVMBGAdminRequests_DuplicateDetectionAndReset(t *testing.T) {
	memStore := store.NewMemoryStore(1000)
	cfg := &config.Config{
		Port:        "8082",
		BearerToken: "test-token",
		DelayMinMs:  0,
		DelayMaxMs:  0,
	}
	h := NewHandler(memStore, cfg, admin.NewOutageState(), admin.NewSchemaVersionState(), zerolog.Nop())
	router := h.Routes()

	doDataReq := func(instanceID, since string) {
		req := httptest.NewRequest(http.MethodGet, "/volcanic-reports?since="+since, nil)
		req.Header.Set("Authorization", "Bearer test-token")
		if instanceID != "" {
			req.Header.Set("X-Instance-ID", instanceID)
		}
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
	}

	doDataReq("agg-1", "2026-01-01T00:00:00Z")
	doDataReq("agg-2", "2026-01-01T00:00:00Z")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/requests", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp RequestsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Total != 2 || resp.ByInstance["agg-1"] != 1 || resp.ByInstance["agg-2"] != 1 {
		t.Fatalf("unexpected counts: %+v", resp)
	}
	if len(resp.Duplicates) != 1 || resp.Duplicates[0].Path != "/volcanic-reports" {
		t.Fatalf("expected 1 duplicate on /volcanic-reports, got %+v", resp.Duplicates)
	}

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
