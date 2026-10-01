package handler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/handler"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/seed"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

func setupTestApp() (*handler.Handler, store.Store, *config.Config) {
	cfg := &config.Config{
		Port:             "8081",
		APIKey:           "test-bmkg-key",
		DelayMinMs:       10,
		DelayMaxMs:       30,
		EventIntervalS:   1,
		DefaultPageLimit: config.DefaultPageLimit,
		MaxPageLimit:     config.MaxPageLimit,
		LogLevel:         "disabled",
	}
	memStore := store.NewMemoryStore(config.MaxStoreCapacity)
	seed.PopulateSeeds(memStore)
	logger := zerolog.Nop()
	h := handler.NewHandler(memStore, cfg, logger)
	return h, memStore, cfg
}

func TestHealthEndpoint(t *testing.T) {
	h, _, _ := setupTestApp()
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
	h, _, cfg := setupTestApp()
	router := h.Routes()

	tests := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
	}{
		{
			name:           "missing X-BMKG-Key returns 401",
			headers:        map[string]string{},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid X-BMKG-Key returns 401",
			headers: map[string]string{
				"X-BMKG-Key": "wrong-key",
			},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name: "cross-credential Bearer returns 403 Forbidden even with no key",
			headers: map[string]string{
				"Authorization": "Bearer pvmbg-token-123",
			},
			expectedStatus: http.StatusForbidden,
		},
		{
			name: "cross-credential Bearer returns 403 Forbidden even with valid BMKG key",
			headers: map[string]string{
				"X-BMKG-Key":    cfg.APIKey,
				"Authorization": "Bearer pvmbg-token-123",
			},
			expectedStatus: http.StatusForbidden,
		},
		{
			name: "valid X-BMKG-Key returns 200 OK",
			headers: map[string]string{
				"X-BMKG-Key": cfg.APIKey,
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
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

func TestSeismicEventsFilter(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	// 1. Fetch all events (should be >= 20 seeded records)
	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var allEvents []model.SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &allEvents); err != nil {
		t.Fatalf("failed to decode seismic events: %v", err)
	}
	if len(allEvents) < 20 {
		t.Errorf("expected at least 20 seeded events, got %d", len(allEvents))
	}

	// 2. Fetch with since far in the future -> 0 events
	futureReq := httptest.NewRequest(http.MethodGet, "/seismic-events?since=2099-01-01T00:00:00Z", nil)
	futureReq.Header.Set("X-BMKG-Key", cfg.APIKey)
	futureRec := httptest.NewRecorder()
	router.ServeHTTP(futureRec, futureReq)

	var futureEvents []model.SeismicEvent
	_ = json.Unmarshal(futureRec.Body.Bytes(), &futureEvents)
	if len(futureEvents) != 0 {
		t.Errorf("expected 0 events in future, got %d", len(futureEvents))
	}

	// 3. Invalid since parameter -> 400 Bad Request
	invalidReq := httptest.NewRequest(http.MethodGet, "/seismic-events?since=invalid-date", nil)
	invalidReq.Header.Set("X-BMKG-Key", cfg.APIKey)
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)

	if invalidRec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for invalid since, got %d", invalidRec.Code)
	}
}

func TestTsunamiWarningsEndpoint(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/tsunami-warnings", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var warnings []model.TsunamiWarning
	if err := json.Unmarshal(rec.Body.Bytes(), &warnings); err != nil {
		t.Fatalf("failed to decode tsunami warnings: %v", err)
	}
	if len(warnings) == 0 {
		t.Errorf("expected seeded tsunami warnings, got 0")
	}

	// Verify related_event_id is present
	for _, w := range warnings {
		if w.RelatedEventID == "" {
			t.Errorf("warning %s has empty RelatedEventID", w.WarningID)
		}
	}
}

func TestCorrelationIDPropagation(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	corrID := rec.Header().Get("X-Correlation-ID")
	if corrID == "" {
		t.Errorf("expected X-Correlation-ID header in response")
	}

	// Test custom correlation ID preserved
	customID := "custom-trace-uuid-1234"
	reqCustom := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	reqCustom.Header.Set("X-BMKG-Key", cfg.APIKey)
	reqCustom.Header.Set("X-Correlation-ID", customID)
	recCustom := httptest.NewRecorder()
	router.ServeHTTP(recCustom, reqCustom)

	if recCustom.Header().Get("X-Correlation-ID") != customID {
		t.Errorf("expected correlation ID %s, got %s", customID, recCustom.Header().Get("X-Correlation-ID"))
	}
}

func TestLatencyJitter(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	// Latency jitter should apply to data endpoints, not /health.
	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	elapsed := time.Since(start)
	if elapsed < time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected latency >= %dms on data endpoint, got %v", cfg.DelayMinMs, elapsed)
	}
}

func TestHealthHasNoLatency(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	// /health must respond immediately — no simulated latency.
	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	elapsed := time.Since(start)
	if elapsed >= time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected no latency on /health, but took %v (min delay is %dms)", elapsed, cfg.DelayMinMs)
	}
}

func TestSeismicEventsPaginationLimit(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/seismic-events?limit=5", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var events []model.SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to decode seismic events: %v", err)
	}
	if len(events) != 5 {
		t.Errorf("expected exactly 5 events, got %d", len(events))
	}
}

func TestSeismicEventsLimitValidation(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	tests := []struct {
		name  string
		limit string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"non-integer", "abc"},
		{"float", "5.5"},
		{"empty-after-zero", "0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/seismic-events?limit="+tc.limit, nil)
			req.Header.Set("X-BMKG-Key", cfg.APIKey)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for limit=%s, got %d", tc.limit, rec.Code)
			}

			var errResp map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if errResp["error"] == "" {
				t.Error("expected non-empty error message in response body")
			}
		})
	}
}

func TestSeismicEventsLimitClamping(t *testing.T) {
	h, memStore, cfg := setupTestApp()
	router := h.Routes()

	for i := 0; i < 90; i++ {
		memStore.AddSeismicEvent(model.SeismicEvent{
			EventID:    fmt.Sprintf("evt-extra-%04d", i),
			OccurredAt: time.Now().Add(-time.Duration(i) * time.Minute).Format(time.RFC3339),
		})
	}

	req := httptest.NewRequest(http.MethodGet, "/seismic-events?limit=9999", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var events []model.SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to decode seismic events: %v", err)
	}
	if len(events) != config.MaxPageLimit {
		t.Errorf("expected %d events (clamped to MAX_PAGE_LIMIT), got %d", config.MaxPageLimit, len(events))
	}
}

func TestSeismicEventsDefaultLimit(t *testing.T) {
	h, memStore, cfg := setupTestApp()
	router := h.Routes()

	for i := 0; i < 90; i++ {
		memStore.AddSeismicEvent(model.SeismicEvent{
			EventID:    fmt.Sprintf("evt-extra-%04d", i),
			OccurredAt: time.Now().Add(-time.Duration(i) * time.Minute).Format(time.RFC3339),
		})
	}

	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var events []model.SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to decode seismic events: %v", err)
	}
	if len(events) > config.DefaultPageLimit {
		t.Errorf("expected at most %d events (DEFAULT_PAGE_LIMIT), got %d", config.DefaultPageLimit, len(events))
	}
	if len(events) < 20 {
		t.Errorf("expected at least 20 seeded events, got %d", len(events))
	}
}

func TestTsunamiWarningsPaginationLimit(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/tsunami-warnings?limit=2", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var warnings []model.TsunamiWarning
	if err := json.Unmarshal(rec.Body.Bytes(), &warnings); err != nil {
		t.Fatalf("failed to decode tsunami warnings: %v", err)
	}
	if len(warnings) != 2 {
		t.Errorf("expected exactly 2 warnings, got %d", len(warnings))
	}
}

func TestTsunamiWarningsLimitValidation(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	tests := []struct {
		name  string
		limit string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"non-integer", "abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tsunami-warnings?limit="+tc.limit, nil)
			req.Header.Set("X-BMKG-Key", cfg.APIKey)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for limit=%s, got %d", tc.limit, rec.Code)
			}
		})
	}
}

func TestSeismicEventsChronologicalOrder(t *testing.T) {
	h, _, cfg := setupTestApp()
	router := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", cfg.APIKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var events []model.SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to decode seismic events: %v", err)
	}

	for i := 1; i < len(events); i++ {
		prev, _ := time.Parse(time.RFC3339, events[i-1].OccurredAt)
		curr, _ := time.Parse(time.RFC3339, events[i].OccurredAt)
		if curr.Before(prev) {
			t.Errorf("events not in chronological order: event %d (%s) before event %d (%s)",
				i, events[i].OccurredAt, i-1, events[i-1].OccurredAt)
		}
	}
}
