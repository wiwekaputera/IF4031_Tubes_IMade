package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

type mockAggregatorClient struct {
	getEventsFunc func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error)
}

func (m *mockAggregatorClient) GetEvents(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
	return m.getEventsFunc(ctx, rawQuery, correlationID)
}

func sampleRawEvents() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"hazard_id":     "h-100",
			"source":        "BMKG",
			"hazard_type":   "SEISMIC",
			"severity":      "SIAGA",
			"area_name":     "Sulawesi Tengah",
			"occurred_at":   "2026-09-21T20:00:00Z",
			"ingested_at":   "2026-09-21T20:00:05Z",
			"source_ref_id": "bmkg-001",
			"latitude":      -1.4,
			"longitude":     119.8,
			"attributes":    map[string]interface{}{"magnitude": 6.7},
		},
	}
}

func withAuthScope(req *http.Request, scope contractsauth.Scope) *http.Request {
	claims := &auth.AccessTokenClaims{
		Scope: scope,
	}
	claims.Subject = "test-client"
	return req.WithContext(auth.ContextWithClaims(req.Context(), claims))
}

func TestEventsHandler_MediaScope_SummaryProjection(t *testing.T) {
	mockClient := &mockAggregatorClient{
		getEventsFunc: func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
			return &upstream.EventsResponse{
				Data: sampleRawEvents(),
				Meta: map[string]interface{}{
					"count": 1,
					"sources": map[string]interface{}{
						"bmkg": map[string]interface{}{"status": "UP"},
					},
				},
			}, nil
		},
	}

	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req = withAuthScope(req, contractsauth.ScopeMedia)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp upstream.EventsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Data))
	}

	evt := resp.Data[0]
	if _, ok := evt["hazard_id"]; !ok {
		t.Error("expected hazard_id to be present")
	}
	if _, ok := evt["latitude"]; ok {
		t.Error("SECURITY VIOLATION: latitude leaked to media scope")
	}
	if _, ok := evt["longitude"]; ok {
		t.Error("SECURITY VIOLATION: longitude leaked to media scope")
	}
	if _, ok := evt["attributes"]; ok {
		t.Error("SECURITY VIOLATION: attributes leaked to media scope")
	}
}

func TestEventsHandler_MediaScope_ViewFull_Returns403(t *testing.T) {
	mockClient := &mockAggregatorClient{
		getEventsFunc: func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
			t.Fatal("upstream should not be called when media requests view=full")
			return nil, nil
		},
	}

	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events?view=full", nil)
	req = withAuthScope(req, contractsauth.ScopeMedia)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got %d", rec.Code)
	}

	authHeader := rec.Header().Get("WWW-Authenticate")
	if authHeader != `Bearer error="insufficient_scope"` {
		t.Fatalf("expected WWW-Authenticate: Bearer error=\"insufficient_scope\", got %q", authHeader)
	}

	var oauthErr contractsauth.OAuthError
	if err := json.NewDecoder(rec.Body).Decode(&oauthErr); err != nil {
		t.Fatalf("failed to parse json error: %v", err)
	}
	if oauthErr.Error != contractsauth.ErrInsufficientScope {
		t.Fatalf("expected error insufficient_scope, got %q", oauthErr.Error)
	}
}

func TestEventsHandler_FieldTeam_ViewFull_Allowed(t *testing.T) {
	mockClient := &mockAggregatorClient{
		getEventsFunc: func(ctx context.Context, rawQuery, correlationID string) (*upstream.EventsResponse, error) {
			return &upstream.EventsResponse{
				Data: sampleRawEvents(),
				Meta: map[string]interface{}{"count": 1},
			}, nil
		},
	}

	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events?view=full", nil)
	req = withAuthScope(req, contractsauth.ScopeFieldTeam)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp upstream.EventsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	evt := resp.Data[0]
	if evt["latitude"] != -1.4 {
		t.Fatalf("expected latitude -1.4, got %v", evt["latitude"])
	}
	if evt["longitude"] != 119.8 {
		t.Fatalf("expected longitude 119.8, got %v", evt["longitude"])
	}
	if evt["attributes"] == nil {
		t.Fatal("expected attributes to be present for field_team")
	}
}

func TestEventsHandler_UnknownScope_Returns403(t *testing.T) {
	mockClient := &mockAggregatorClient{}
	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req = withAuthScope(req, "unauthorized_scope")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for unknown scope, got %d", rec.Code)
	}
}

func TestEventsHandler_InvalidViewParam_Returns400(t *testing.T) {
	mockClient := &mockAggregatorClient{}
	handler := EventsHandler(mockClient)

	req := httptest.NewRequest(http.MethodGet, "/v1/events?view=invalid_view", nil)
	req = withAuthScope(req, contractsauth.ScopeFieldTeam)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
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
	req = withAuthScope(req, contractsauth.ScopeFieldTeam)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var errResp contractsauth.OAuthError
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to parse json error: %v", err)
	}
	if errResp.Error != "upstream_unavailable" {
		t.Fatalf("expected upstream_unavailable, got %v", errResp.Error)
	}
}
