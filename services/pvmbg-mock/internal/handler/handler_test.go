package handler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/admin"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/handler"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/seed"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

func setupTestApp() (*handler.Handler, store.Store, *config.Config, *admin.OutageState, *admin.SchemaVersionState) {
	cfg := &config.Config{
		Port:             "8082",
		BearerToken:      "test-pvmbg-token",
		DelayMinMs:       10,
		DelayMaxMs:       30,
		EventIntervalS:   1,
		DefaultPageLimit: config.DefaultPageLimit,
		MaxPageLimit:     config.MaxPageLimit,
		LogLevel:         "disabled",
	}
	memStore := store.NewMemoryStore(config.MaxStoreCapacity)
	seed.PopulateSeeds(memStore)
	outage := admin.NewOutageState()
	schemaVersion := admin.NewSchemaVersionState()
	h := handler.NewHandler(memStore, cfg, outage, schemaVersion, zerolog.Nop())
	return h, memStore, cfg, outage, schemaVersion
}

func doReq(h *handler.Handler, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func TestHealthEndpoint(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	start := time.Now()
	rec := doReq(h, http.MethodGet, "/health", nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed >= time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("health should have no simulated delay, took %v", elapsed)
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "UP" {
		t.Errorf("expected UP, got %q", resp["status"])
	}
}

func TestAuthEnforcement(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	tests := []struct {
		name    string
		headers map[string]string
		status  int
	}{
		{"missing token", nil, http.StatusUnauthorized},
		{"invalid token", map[string]string{"Authorization": "Bearer bad"}, http.StatusUnauthorized},
		{"BMKG key cross-credential", map[string]string{"X-BMKG-Key": "bmkg-key"}, http.StatusForbidden},
		{"valid Bearer with BMKG key forbidden", map[string]string{"Authorization": "Bearer " + cfg.BearerToken, "X-BMKG-Key": "bmkg"}, http.StatusForbidden},
		{"valid Bearer", map[string]string{"Authorization": "Bearer " + cfg.BearerToken}, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(h, http.MethodGet, "/volcanic-reports", tc.headers)
			if rec.Code != tc.status {
				t.Errorf("expected %d, got %d", tc.status, rec.Code)
			}
		})
	}
}

func TestVolcanicReportsEndpoint(t *testing.T) {
	h, memStore, cfg, _, _ := setupTestApp()
	authHeader := map[string]string{"Authorization": "Bearer " + cfg.BearerToken}

	t.Run("fetch and order", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		var reports []model.VolcanicReport
		_ = json.Unmarshal(rec.Body.Bytes(), &reports)
		if len(reports) < 20 {
			t.Errorf("expected >= 20 reports, got %d", len(reports))
		}
		for i := 1; i < len(reports); i++ {
			p, _ := time.Parse(time.RFC3339, reports[i-1].ReportedAt)
			c, _ := time.Parse(time.RFC3339, reports[i].ReportedAt)
			if c.Before(p) {
				t.Fatalf("reports not in chronological order at %d", i)
			}
		}
	})

	t.Run("since filtering", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/volcanic-reports?since=2099-01-01T00:00:00Z", authHeader)
		var future []model.VolcanicReport
		_ = json.Unmarshal(rec.Body.Bytes(), &future)
		if len(future) != 0 {
			t.Errorf("expected 0 reports, got %d", len(future))
		}
		recBad := doReq(h, http.MethodGet, "/volcanic-reports?since=invalid-date", authHeader)
		if recBad.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid since, got %d", recBad.Code)
		}
	})

	t.Run("limit pagination and clamping", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/volcanic-reports?limit=5", authHeader)
		var limReports []model.VolcanicReport
		_ = json.Unmarshal(rec.Body.Bytes(), &limReports)
		if len(limReports) != 5 {
			t.Errorf("expected 5 reports, got %d", len(limReports))
		}

		for _, badLimit := range []string{"0", "-1", "abc"} {
			if recBad := doReq(h, http.MethodGet, "/volcanic-reports?limit="+badLimit, authHeader); recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for limit=%s, got %d", badLimit, recBad.Code)
			}
		}

		for i := 0; i < config.MaxPageLimit; i++ {
			memStore.AddVolcanicReport(model.VolcanicReport{
				ReportID:   fmt.Sprintf("rpt-clamped-%04d", i),
				VolcanoID:  "GNG-001",
				ReportedAt: time.Now().Add(-time.Duration(i) * time.Minute).Format(time.RFC3339),
			})
		}
		recClamp := doReq(h, http.MethodGet, "/volcanic-reports?limit=9999", authHeader)
		var clamped []model.VolcanicReport
		_ = json.Unmarshal(recClamp.Body.Bytes(), &clamped)
		if len(clamped) != config.MaxPageLimit {
			t.Errorf("expected limit clamped to %d, got %d", config.MaxPageLimit, len(clamped))
		}
	})
}

func TestOutageToggle(t *testing.T) {
	h, _, cfg, outage, _ := setupTestApp()
	authHeader := map[string]string{"Authorization": "Bearer " + cfg.BearerToken}

	recToggle1 := doReq(h, http.MethodPost, "/admin/outage", nil)
	if recToggle1.Code != http.StatusOK || !outage.IsActive() {
		t.Fatalf("expected outage activated, code %d", recToggle1.Code)
	}

	rec503 := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
	if rec503.Code != http.StatusServiceUnavailable || rec503.Body.Len() != 0 {
		t.Errorf("expected 503 empty body during outage, got %d %q", rec503.Code, rec503.Body.String())
	}

	recToggle2 := doReq(h, http.MethodPost, "/admin/outage", nil)
	if recToggle2.Code != http.StatusOK || outage.IsActive() {
		t.Fatalf("expected outage deactivated, code %d", recToggle2.Code)
	}

	rec200 := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
	if rec200.Code != http.StatusOK {
		t.Errorf("expected 200 after outage cleared, got %d", rec200.Code)
	}
}

func TestSchemaVersionToggle(t *testing.T) {
	h, _, cfg, _, schemaVersion := setupTestApp()
	authHeader := map[string]string{"Authorization": "Bearer " + cfg.BearerToken}

	// Schema version OFF by default
	rec1 := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
	var raw1 []map[string]interface{}
	_ = json.Unmarshal(rec1.Body.Bytes(), &raw1)
	if _, ok := raw1[0]["confidence_level"]; ok {
		t.Error("expected confidence_level absent when schema version OFF")
	}

	// Toggle ON
	recToggle := doReq(h, http.MethodPost, "/admin/schema-version", nil)
	if recToggle.Code != http.StatusOK || !schemaVersion.IsEnabled() {
		t.Fatalf("expected schema version enabled, code %d", recToggle.Code)
	}

	rec2 := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
	var raw2 []map[string]interface{}
	_ = json.Unmarshal(rec2.Body.Bytes(), &raw2)
	clVal, ok := raw2[0]["confidence_level"].(float64)
	if !ok || clVal < 0.0 || clVal > 1.0 {
		t.Errorf("expected confidence_level in [0, 1] when schema version ON, got %v", raw2[0]["confidence_level"])
	}

	// Toggle OFF again
	doReq(h, http.MethodPost, "/admin/schema-version", nil)
	rec3 := doReq(h, http.MethodGet, "/volcanic-reports", authHeader)
	var raw3 []map[string]interface{}
	_ = json.Unmarshal(rec3.Body.Bytes(), &raw3)
	if _, ok := raw3[0]["confidence_level"]; ok {
		t.Error("expected confidence_level absent after toggle OFF")
	}
}

func TestCorrelationIDAndLatency(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	headers := map[string]string{
		"Authorization":     "Bearer " + cfg.BearerToken,
		"X-Correlation-ID":  "trace-pvmbg-1234",
	}
	start := time.Now()
	rec := doReq(h, http.MethodGet, "/volcanic-reports", headers)
	elapsed := time.Since(start)

	if rec.Header().Get("X-Correlation-ID") != "trace-pvmbg-1234" {
		t.Errorf("expected correlation ID preserved, got %q", rec.Header().Get("X-Correlation-ID"))
	}
	if elapsed < time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected simulated latency >= %dms, took %v", cfg.DelayMinMs, elapsed)
	}
}
