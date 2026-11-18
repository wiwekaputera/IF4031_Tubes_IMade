package analytics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnalyticsRoutes(t *testing.T) {
	h := New()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Simulate LoggingMiddleware setting Content-Type: application/json before routing.
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mux.ServeHTTP(w, r)
	})

	t.Run("redirect /analytics to /analytics/", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/analytics", nil)
		rr := httptest.NewRecorder()
		wrapped.ServeHTTP(rr, req)
		if rr.Code != http.StatusMovedPermanently {
			t.Fatalf("status = %d, want 301", rr.Code)
		}
		if loc := rr.Header().Get("Location"); loc != "/analytics/" {
			t.Fatalf("Location = %q, want /analytics/", loc)
		}
	})

	t.Run("serve index.html", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/analytics/", nil)
		rr := httptest.NewRecorder()
		wrapped.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
			t.Fatalf("Content-Type = %q, want text/html", ct)
		}
		if !strings.Contains(rr.Body.String(), "BNPB Hazard Analytics") {
			t.Fatalf("body missing dashboard title")
		}
	})

	t.Run("serve vendored chart.umd.min.js", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/analytics/chart.umd.min.js", nil)
		rr := httptest.NewRecorder()
		wrapped.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
			t.Fatalf("Content-Type = %q, want javascript", ct)
		}
	})
}
