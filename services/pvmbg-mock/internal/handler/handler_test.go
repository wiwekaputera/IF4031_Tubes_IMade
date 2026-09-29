package handler_test

import (
	"encoding/json"
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

func testStart() time.Time { return time.Now() }

func testElapsed(start time.Time) time.Duration { return time.Since(start) }

func setupTestApp() (*handler.Handler, store.Store, *config.Config, *admin.OutageState, *admin.SchemaVersionState) {
	cfg := &config.Config{
		Port:           "8082",
		BearerToken:    "test-pvmbg-token",
		DelayMinMs:     10,
		DelayMaxMs:     30,
		EventIntervalS: 1,
		LogLevel:       "disabled",
	}
	memStore := store.NewMemoryStore()
	seed.PopulateSeeds(memStore)
	outage := admin.NewOutageState()
	schemaVersion := admin.NewSchemaVersionState()
	logger := zerolog.Nop()
	h := handler.NewHandler(memStore, cfg, outage, schemaVersion, logger)
	return h, memStore, cfg, outage, schemaVersion
}

func TestHealthEndpoint(t *testing.T) {
	h, _, _, _, _ := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "UP" {
		t.Errorf("expected status 'UP', got '%s'", resp["status"])
	}
}

func TestAuthEnforcement(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	router := h.Routes()

	tests := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
	}{
		{
			name:           "missing Authorization returns 401",
			headers:        map[string]string{},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid Bearer token returns 401",
			headers: map[string]string{
				"Authorization": "Bearer wrong-token",
			},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name: "X-BMKG-Key cross-credential returns 403 even with no Bearer",
			headers: map[string]string{
				"X-BMKG-Key": "bmkg-key",
			},
			expectedStatus: http.StatusForbidden,
		},
		{
			name: "X-BMKG-Key cross-credential returns 403 even with valid Bearer",
			headers: map[string]string{
				"Authorization": "Bearer " + cfg.BearerToken,
				"X-BMKG-Key":    "bmkg-key",
			},
			expectedStatus: http.StatusForbidden,
		},
		{
			name: "valid Bearer token returns 200 OK",
			headers: map[string]string{
				"Authorization": "Bearer " + cfg.BearerToken,
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.expectedStatus {
				t.Errorf("expected status %d, got %d, body: %s", tc.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestVolcanicReportsFilter(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var allReports []model.VolcanicReport
	if err := json.Unmarshal(rec.Body.Bytes(), &allReports); err != nil {
		t.Fatalf("failed to decode volcanic reports: %v", err)
	}
	if len(allReports) < 20 {
		t.Errorf("expected at least 20 seeded reports, got %d", len(allReports))
	}

	futureReq := httptest.NewRequest(http.MethodGet, "/volcanic-reports?since=2099-01-01T00:00:00Z", nil)
	futureReq.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	futureRec := httptest.NewRecorder()
	router.ServeHTTP(futureRec, futureReq)

	var futureReports []model.VolcanicReport
	_ = json.Unmarshal(futureRec.Body.Bytes(), &futureReports)
	if len(futureReports) != 0 {
		t.Errorf("expected 0 reports in future, got %d", len(futureReports))
	}

	invalidReq := httptest.NewRequest(http.MethodGet, "/volcanic-reports?since=invalid-date", nil)
	invalidReq.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)

	if invalidRec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for invalid since, got %d", invalidRec.Code)
	}
}

func TestOutageToggle(t *testing.T) {
	h, _, cfg, outage, _ := setupTestApp()
	router := h.Routes()

	outageReq := httptest.NewRequest(http.MethodPost, "/admin/outage", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, outageReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for outage toggle, got %d", rec.Code)
	}
	if !outage.IsActive() {
		t.Errorf("expected outage to be active after toggle")
	}

	dataReq := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	dataReq.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	dataRec := httptest.NewRecorder()
	router.ServeHTTP(dataRec, dataReq)

	if dataRec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 during outage, got %d", dataRec.Code)
	}
	if dataRec.Body.Len() != 0 {
		t.Errorf("expected empty body during outage, got %s", dataRec.Body.String())
	}

	outageReq2 := httptest.NewRequest(http.MethodPost, "/admin/outage", nil)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, outageReq2)
	if outage.IsActive() {
		t.Errorf("expected outage to be inactive after second toggle")
	}

	dataRec2 := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	dataRec2.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, dataRec2)
	if rec3.Code != http.StatusOK {
		t.Errorf("expected 200 after outage cleared, got %d", rec3.Code)
	}
}

func TestSchemaVersionToggle(t *testing.T) {
	h, _, cfg, _, schemaVersion := setupTestApp()
	router := h.Routes()

	// Default: OFF -> confidence_level should be absent
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var reports []model.VolcanicReport
	_ = json.Unmarshal(rec.Body.Bytes(), &reports)
	if len(reports) == 0 {
		t.Fatalf("expected reports, got none")
	}
	for _, r := range reports {
		if r.ConfidenceLevel != nil {
			t.Errorf("expected confidence_level nil when schema version OFF, got %v", r.ConfidenceLevel)
		}
	}

	// Toggle ON
	toggleReq := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	toggleRec := httptest.NewRecorder()
	router.ServeHTTP(toggleRec, toggleReq)
	if !schemaVersion.IsEnabled() {
		t.Errorf("expected schema version to be enabled after toggle")
	}

	// After toggle: confidence_level should be present
	req2 := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req2.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	var reports2 []model.VolcanicReport
	_ = json.Unmarshal(rec2.Body.Bytes(), &reports2)
	if len(reports2) == 0 {
		t.Fatalf("expected reports, got none")
	}
	for _, r := range reports2 {
		if r.ConfidenceLevel == nil {
			t.Errorf("expected confidence_level present when schema version ON, got nil")
		} else if *r.ConfidenceLevel < 0.0 || *r.ConfidenceLevel > 1.0 {
			t.Errorf("expected confidence_level in [0.0, 1.0], got %v", *r.ConfidenceLevel)
		}
	}

	// Verify via raw JSON that the field is present
	var raw []map[string]interface{}
	_ = json.Unmarshal(rec2.Body.Bytes(), &raw)
	if _, ok := raw[0]["confidence_level"]; !ok {
		t.Errorf("expected 'confidence_level' key in JSON response when schema version ON")
	}

	// Toggle OFF again
	toggleReq2 := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	router.ServeHTTP(httptest.NewRecorder(), toggleReq2)
	if schemaVersion.IsEnabled() {
		t.Errorf("expected schema version to be disabled after second toggle")
	}

	req3 := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req3.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, req3)

	var raw3 []map[string]interface{}
	_ = json.Unmarshal(rec3.Body.Bytes(), &raw3)
	if _, ok := raw3[0]["confidence_level"]; ok {
		t.Errorf("expected 'confidence_level' key absent in JSON response when schema version OFF")
	}
}

func TestCorrelationIDPropagation(t *testing.T) {
	h, _, _, _, _ := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	corrID := rec.Header().Get("X-Correlation-ID")
	if corrID == "" {
		t.Errorf("expected X-Correlation-ID header in response")
	}

	customID := "custom-trace-uuid-1234"
	reqCustom := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	reqCustom.Header.Set("Authorization", "Bearer test-pvmbg-token")
	reqCustom.Header.Set("X-Correlation-ID", customID)
	recCustom := httptest.NewRecorder()
	router.ServeHTTP(recCustom, reqCustom)

	if recCustom.Header().Get("X-Correlation-ID") != customID {
		t.Errorf("expected correlation ID %s, got %s", customID, recCustom.Header().Get("X-Correlation-ID"))
	}
}

func TestLatencyJitter(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	router := h.Routes()

	start := testStart()
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	elapsed := testElapsed(start)
	if elapsed < time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected latency >= %dms on data endpoint, got %v", cfg.DelayMinMs, elapsed)
	}
}

func TestHealthHasNoLatency(t *testing.T) {
	h, _, cfg, _, _ := setupTestApp()
	router := h.Routes()

	start := testStart()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	elapsed := testElapsed(start)
	if elapsed >= time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected no latency on /health, but took %v (min delay is %dms)", elapsed, cfg.DelayMinMs)
	}
}
