package dlq

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type listStore struct{ fakeStore }

func (l *listStore) List(_ context.Context, status string, _ int) ([]Row, error) {
	return []Row{{ID: 7, Status: status, Reason: "missing_required:volcano_id"}}, nil
}

func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	h, err := NewHTTPHandler(&listStore{}, func(context.Context, []byte) error { return nil }, "secret")
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestNewHTTPHandlerRequiresToken(t *testing.T) {
	if _, err := NewHTTPHandler(&listStore{}, nil, ""); !errors.Is(err, ErrNoToken) {
		t.Fatalf("err = %v, want ErrNoToken", err)
	}
}

func TestRoutesRejectMissingToken(t *testing.T) {
	srv := newTestServer(t)
	for _, req := range []*http.Request{
		httptest.NewRequest("GET", "/internal/dlq", nil),
		httptest.NewRequest("POST", "/internal/dlq/replay", nil),
		httptest.NewRequest("GET", "/internal/dlq", nil),
	} {
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s with wrong token: code %d, want 401", req.Method, req.URL.Path, rec.Code)
		}
	}
}

func TestListDefaultsToParkedAndRejectsBadStatus(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest("GET", "/internal/dlq", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"parked"`) {
		t.Fatalf("default list: code %d body %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest("GET", "/internal/dlq?status=bogus", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad status: code %d, want 400", rec.Code)
	}
}

func TestReplayEndpointRuns(t *testing.T) {
	s := &listStore{}
	s.parked = []Row{{ID: 1, Source: "PVMBG", SourceRefID: "R-1", Raw: []byte("x")}}
	srv, err := NewHTTPHandler(s, func(context.Context, []byte) error { return nil }, "secret")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/internal/dlq/replay", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"Resolved":1`) {
		t.Fatalf("replay: code %d body %s", rec.Code, rec.Body.String())
	}
}
