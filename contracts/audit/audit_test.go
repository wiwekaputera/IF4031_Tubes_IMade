package audit

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEmptySlicesMarshalAsEmptyArrays(t *testing.T) {
	ep, err := json.Marshal(EventsPage{})
	if err != nil {
		t.Fatalf("marshal EventsPage: %v", err)
	}
	s := string(ep)
	if !strings.Contains(s, `"items":[]`) {
		t.Errorf("EventsPage{} missing \"items\":[]: %s", s)
	}
	if !strings.Contains(s, `"unavailable_units":[]`) {
		t.Errorf("EventsPage{} missing \"unavailable_units\":[]: %s", s)
	}

	cs, err := json.Marshal(Checksum{})
	if err != nil {
		t.Fatalf("marshal Checksum: %v", err)
	}
	if !strings.Contains(string(cs), `"unavailable_units":[]`) {
		t.Errorf("Checksum{} missing \"unavailable_units\":[]: %s", string(cs))
	}

	lr, err := json.Marshal(LocationResponse{})
	if err != nil {
		t.Fatalf("marshal LocationResponse: %v", err)
	}
	if !strings.Contains(string(lr), `"locations":[]`) {
		t.Errorf("LocationResponse{} missing \"locations\":[]: %s", string(lr))
	}

	up, err := json.Marshal(UnprocessedPage{})
	if err != nil {
		t.Fatalf("marshal UnprocessedPage: %v", err)
	}
	if !strings.Contains(string(up), `"items":[]`) {
		t.Errorf("UnprocessedPage{} missing \"items\":[]: %s", string(up))
	}
}
