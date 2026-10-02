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
	return handler.NewHandler(memStore, cfg, zerolog.Nop()), memStore, cfg
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
	h, _, cfg := setupTestApp()
	start := time.Now()
	rec := doReq(h, http.MethodGet, "/health", nil)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if elapsed >= time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("health endpoint should have no simulated latency, took %v", elapsed)
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "UP" {
		t.Errorf("expected status UP, got %q", resp["status"])
	}
}

func TestAuthEnforcement(t *testing.T) {
	h, _, cfg := setupTestApp()
	tests := []struct {
		name    string
		headers map[string]string
		status  int
	}{
		{"missing key", nil, http.StatusUnauthorized},
		{"invalid key", map[string]string{"X-BMKG-Key": "wrong"}, http.StatusUnauthorized},
		{"cross-credential Bearer", map[string]string{"Authorization": "Bearer tok"}, http.StatusForbidden},
		{"valid key with Bearer forbidden", map[string]string{"X-BMKG-Key": cfg.APIKey, "Authorization": "Bearer tok"}, http.StatusForbidden},
		{"valid key", map[string]string{"X-BMKG-Key": cfg.APIKey}, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(h, http.MethodGet, "/seismic-events", tc.headers)
			if rec.Code != tc.status {
				t.Errorf("expected %d, got %d", tc.status, rec.Code)
			}
		})
	}
}

func TestSeismicEventsEndpoint(t *testing.T) {
	h, memStore, cfg := setupTestApp()
	authHeader := map[string]string{"X-BMKG-Key": cfg.APIKey}

	t.Run("fetch and order", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/seismic-events", authHeader)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		var events []model.SeismicEvent
		_ = json.Unmarshal(rec.Body.Bytes(), &events)
		if len(events) < 20 {
			t.Errorf("expected >= 20 events, got %d", len(events))
		}
		for i := 1; i < len(events); i++ {
			p, _ := time.Parse(time.RFC3339, events[i-1].OccurredAt)
			c, _ := time.Parse(time.RFC3339, events[i].OccurredAt)
			if c.Before(p) {
				t.Fatalf("events not in chronological order at %d", i)
			}
		}
	})

	t.Run("since filtering", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/seismic-events?since=2099-01-01T00:00:00Z", authHeader)
		var future []model.SeismicEvent
		_ = json.Unmarshal(rec.Body.Bytes(), &future)
		if len(future) != 0 {
			t.Errorf("expected 0 events, got %d", len(future))
		}
		recBad := doReq(h, http.MethodGet, "/seismic-events?since=invalid-date", authHeader)
		if recBad.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid since, got %d", recBad.Code)
		}
	})

	t.Run("limit pagination and clamping", func(t *testing.T) {
		rec := doReq(h, http.MethodGet, "/seismic-events?limit=5", authHeader)
		var limEvents []model.SeismicEvent
		_ = json.Unmarshal(rec.Body.Bytes(), &limEvents)
		if len(limEvents) != 5 {
			t.Errorf("expected 5 events, got %d", len(limEvents))
		}

		for _, badLimit := range []string{"0", "-1", "abc"} {
			if recBad := doReq(h, http.MethodGet, "/seismic-events?limit="+badLimit, authHeader); recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for limit=%s, got %d", badLimit, recBad.Code)
			}
		}

		for i := 0; i < 90; i++ {
			memStore.AddSeismicEvent(model.SeismicEvent{
				EventID:    fmt.Sprintf("evt-clamped-%04d", i),
				OccurredAt: time.Now().Add(-time.Duration(i) * time.Minute).Format(time.RFC3339),
			})
		}
		recClamp := doReq(h, http.MethodGet, "/seismic-events?limit=9999", authHeader)
		var clamped []model.SeismicEvent
		_ = json.Unmarshal(recClamp.Body.Bytes(), &clamped)
		if len(clamped) != config.MaxPageLimit {
			t.Errorf("expected limit clamped to %d, got %d", config.MaxPageLimit, len(clamped))
		}
	})
}

func TestTsunamiWarningsEndpoint(t *testing.T) {
	h, _, cfg := setupTestApp()
	authHeader := map[string]string{"X-BMKG-Key": cfg.APIKey}

	rec := doReq(h, http.MethodGet, "/tsunami-warnings?limit=2", authHeader)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var warnings []model.TsunamiWarning
	_ = json.Unmarshal(rec.Body.Bytes(), &warnings)
	if len(warnings) != 2 {
		t.Fatalf("expected 2 warnings, got %d", len(warnings))
	}
	for _, w := range warnings {
		if w.RelatedEventID == "" {
			t.Errorf("warning %s missing RelatedEventID", w.WarningID)
		}
	}
	if recBad := doReq(h, http.MethodGet, "/tsunami-warnings?limit=0", authHeader); recBad.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for limit=0, got %d", recBad.Code)
	}
}

func TestCorrelationIDAndLatency(t *testing.T) {
	h, _, cfg := setupTestApp()
	headers := map[string]string{
		"X-BMKG-Key":        cfg.APIKey,
		"X-Correlation-ID":  "trace-bmkg-1234",
	}
	start := time.Now()
	rec := doReq(h, http.MethodGet, "/seismic-events", headers)
	elapsed := time.Since(start)

	if rec.Header().Get("X-Correlation-ID") != "trace-bmkg-1234" {
		t.Errorf("expected correlation ID preserved, got %q", rec.Header().Get("X-Correlation-ID"))
	}
	if elapsed < time.Duration(cfg.DelayMinMs)*time.Millisecond {
		t.Errorf("expected simulated latency >= %dms, took %v", cfg.DelayMinMs, elapsed)
	}
}
