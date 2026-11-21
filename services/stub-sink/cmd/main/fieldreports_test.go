package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

const reportJSON = `{"report_id":"r1","client_id":"team-a","category":"DAMAGE","severity":"HIGH",` +
	`"area_name":"Palu","latitude":-0.9,"longitude":119.87,"description":"x",` +
	`"reported_at":"2026-11-20T02:55:00Z","accepted_at":"2026-11-20T03:00:00Z"}`

func send(t *testing.T, h http.Handler, key, body string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestFieldReportInboxKeepsEachReportOnce(t *testing.T) {
	h := newSink(zerolog.Nop()).routes()
	if code := send(t, h, "team-a/r1", reportJSON); code != http.StatusAccepted {
		t.Fatalf("first delivery: %d", code)
	}
	if code := send(t, h, "team-a/r1", reportJSON); code != http.StatusOK {
		t.Fatalf("repeated delivery: %d", code)
	}
	if code := send(t, h, "team-b/r1", reportJSON); code != http.StatusBadRequest {
		t.Fatalf("key not matching client_id/report_id must be rejected: %d", code)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/field-reports", nil))
	var out struct {
		Count      int `json:"count"`
		Duplicates int `json:"duplicates_ignored"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 || out.Duplicates != 1 {
		t.Fatalf("count=%d duplicates=%d, want 1 and 1", out.Count, out.Duplicates)
	}
}
