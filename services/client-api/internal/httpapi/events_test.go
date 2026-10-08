package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
)

type mockAggregatorClient struct {
	getEventsFunc func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error)
}

func (m *mockAggregatorClient) GetEvents(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
	return m.getEventsFunc(ctx, rawQuery, correlationID)
}

func TestEventsHandler_Success(t *testing.T) {
	mockClient := &mockAggregatorClient{
		getEventsFunc: func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
			return &upstream.EventsResponse{
				Data: []map[string]interface{}{
					{"hazard_id": "test-1"},
				},
				Meta: map[string]interface{}{
					"count": 1,
					"sources": map[string]interface{}{
						"pvmbg": map[string]interface{}{
							"status":      "OUTAGE",
							"stale_since": "2026-09-21T22:35:00Z",
						},
					},
				},
			}, nil
		},
	}

	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events?type=SEISMIC", nil)
	req.Header.Set("X-Correlation-ID", "corr-req-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	if corr := rec.Header().Get("X-Correlation-ID"); corr != "corr-req-123" {
		t.Fatalf("expected X-Correlation-ID %q, got %q", "corr-req-123", corr)
	}

	var resp upstream.EventsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Data))
	}
	sources := resp.Meta["sources"].(map[string]interface{})
	pvmbg := sources["pvmbg"].(map[string]interface{})
	if pvmbg["stale_since"] != "2026-09-21T22:35:00Z" {
		t.Fatalf("expected stale_since preserved, got %v", pvmbg["stale_since"])
	}
}

func TestEventsHandler_MethodNotAllowed(t *testing.T) {
	mockClient := &mockAggregatorClient{}
	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET" {
		t.Fatalf("expected Allow: GET, got %q", allow)
	}
}

func TestEventsHandler_UpstreamError(t *testing.T) {
	mockClient := &mockAggregatorClient{
		getEventsFunc: func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
			return nil, &upstream.UpstreamError{
				StatusCode: http.StatusServiceUnavailable,
				ErrorCode:  "upstream_unavailable",
				Message:    "aggregator service is unavailable",
			}
		},
	}

	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var errResp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to parse json error: %v", err)
	}
	if errResp["error"] != "upstream_unavailable" {
		t.Fatalf("expected upstream_unavailable, got %v", errResp["error"])
	}
}
