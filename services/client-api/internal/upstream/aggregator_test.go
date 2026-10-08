package upstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestAggregatorClient_Success(t *testing.T) {
	var capturedCorrID string
	var capturedQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCorrID = r.Header.Get("X-Correlation-ID")
		capturedQuery = r.URL.RawQuery

		resp := EventsResponse{
			Data: []map[string]interface{}{
				{
					"hazard_id":   "h-001",
					"source":      "BMKG",
					"hazard_type": "SEISMIC",
					"severity":    "WASPADA",
				},
			},
			Meta: map[string]interface{}{
				"count": 1,
				"sources": map[string]interface{}{
					"bmkg": map[string]interface{}{
						"status": "UP",
					},
					"pvmbg": map[string]interface{}{
						"status":      "OUTAGE",
						"stale_since": "2026-09-21T22:35:00Z",
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(Options{
		BaseURL: server.URL,
		Timeout: 1 * time.Second,
		Logger:  zerolog.Nop(),
	})

	corrID := "test-corr-id-999"
	query := "type=SEISMIC&limit=10"

	resp, err := client.GetEvents(context.Background(), query, corrID)
	if err != nil {
		t.Fatalf("unexpected error from GetEvents: %v", err)
	}

	if capturedCorrID != corrID {
		t.Fatalf("expected X-Correlation-ID %q, got %q", corrID, capturedCorrID)
	}
	if capturedQuery != query {
		t.Fatalf("expected query %q, got %q", query, capturedQuery)
	}

	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 event in Data, got %d", len(resp.Data))
	}
	if resp.Meta == nil {
		t.Fatal("expected non-nil Meta")
	}

	// Verify meta sources preserved
	sources, ok := resp.Meta["sources"].(map[string]interface{})
	if !ok {
		t.Fatal("expected sources map in meta")
	}
	pvmbg, ok := sources["pvmbg"].(map[string]interface{})
	if !ok || pvmbg["stale_since"] != "2026-09-21T22:35:00Z" {
		t.Fatalf("expected pvmbg stale_since to be preserved, got %v", pvmbg)
	}
}

func TestAggregatorClient_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(Options{
		BaseURL: server.URL,
		Timeout: 20 * time.Millisecond, // very short timeout
		Logger:  zerolog.Nop(),
	})

	_, err := client.GetEvents(context.Background(), "", "test-corr")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	upErr, ok := err.(*UpstreamError)
	if !ok {
		t.Fatalf("expected *UpstreamError, got %T: %v", err, err)
	}
	if upErr.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("expected status 504 Gateway Timeout, got %d", upErr.StatusCode)
	}
}

func TestAggregatorClient_ConnectionRefused(t *testing.T) {
	client := NewClient(Options{
		BaseURL: "http://127.0.0.1:54321", // non-existent server
		Timeout: 50 * time.Millisecond,
		Logger:  zerolog.Nop(),
	})

	_, err := client.GetEvents(context.Background(), "", "test-corr")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}

	upErr, ok := err.(*UpstreamError)
	if !ok {
		t.Fatalf("expected *UpstreamError, got %T", err)
	}
	if upErr.StatusCode != http.StatusServiceUnavailable && upErr.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("expected 503 or 504 status, got %d", upErr.StatusCode)
	}
}

func TestAggregatorClient_Upstream500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"db connection failed"}`))
	}))
	defer server.Close()

	client := NewClient(Options{
		BaseURL: server.URL,
		Timeout: 1 * time.Second,
		Logger:  zerolog.Nop(),
	})

	_, err := client.GetEvents(context.Background(), "", "test-corr")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	upErr, ok := err.(*UpstreamError)
	if !ok {
		t.Fatalf("expected *UpstreamError, got %T", err)
	}
	if upErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 status code from upstream, got %d", upErr.StatusCode)
	}
}

func TestAggregatorClient_MalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()

	client := NewClient(Options{
		BaseURL: server.URL,
		Timeout: 1 * time.Second,
		Logger:  zerolog.Nop(),
	})

	_, err := client.GetEvents(context.Background(), "", "test-corr")
	if err == nil {
		t.Fatal("expected error on malformed JSON, got nil")
	}

	upErr, ok := err.(*UpstreamError)
	if !ok {
		t.Fatalf("expected *UpstreamError, got %T", err)
	}
	if upErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway for malformed JSON, got %d", upErr.StatusCode)
	}
}
