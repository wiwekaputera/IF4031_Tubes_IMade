package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

func TestNotifyDeduplicatesByEventVersion(t *testing.T) {
	server := httptest.NewServer(newSink(zerolog.Nop()).routes())
	defer server.Close()

	body := []byte(`{"event_id":"hazard-1","version":2,"trace_id":"corr-1","data":{}}`)
	for _, test := range []struct {
		wantStatus int
		wantLabel  string
	}{{http.StatusAccepted, "accepted"}, {http.StatusOK, "duplicate"}} {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/notify", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "hazard-1:2")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != test.wantStatus || result["status"] != test.wantLabel {
			t.Fatalf("response = (%d, %q), want (%d, %q)", resp.StatusCode, result["status"], test.wantStatus, test.wantLabel)
		}
	}

	resp, err := http.Get(server.URL + "/received")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var received struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&received); err != nil {
		t.Fatal(err)
	}
	if received.Count != 1 {
		t.Fatalf("received count = %d, want 1 after duplicate POST", received.Count)
	}
}

func TestNotifyRejectsMissingOrMismatchedIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(newSink(zerolog.Nop()).routes())
	defer server.Close()

	for _, key := range []string{"", "hazard-1:1"} {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/notify", bytes.NewBufferString(`{"event_id":"hazard-1","version":2,"data":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status for key %q = %d, want %d", key, resp.StatusCode, http.StatusBadRequest)
		}
	}
}
