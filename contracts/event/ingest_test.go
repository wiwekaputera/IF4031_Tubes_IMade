package event

import (
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func TestIngestSubject(t *testing.T) {
	if got := IngestSubject(hazard.SourceSatelit); got != "hazard.ingest.SATELIT" {
		t.Fatalf("IngestSubject(SATELIT) = %q, want %q", got, "hazard.ingest.SATELIT")
	}
}

func TestIngestMessage_Validate(t *testing.T) {
	validUpsert := IngestMessage{
		MessageID:     "msg-1",
		Op:            IngestOpUpsert,
		Source:        hazard.SourceSatelit,
		SourceRefID:   "hotspot-1",
		SourceVersion: 1,
		HazardType:    hazard.HazardTypeWildfire,
		Severity:      hazard.SeveritySiaga,
		AreaName:      "Riau",
		Latitude:      0.5,
		Longitude:     101.4,
		OccurredAt:    "2026-11-17T10:00:00Z",
	}
	if err := validUpsert.Validate(); err != nil {
		t.Fatalf("valid upsert failed: %v", err)
	}

	validDelete := IngestMessage{
		Op:            IngestOpDelete,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   "tide-42",
		SourceVersion: 2,
	}
	if err := validDelete.Validate(); err != nil {
		t.Fatalf("valid delete failed: %v", err)
	}

	bad := []struct {
		name string
		fn   func(*IngestMessage)
	}{
		{"bad source", func(m *IngestMessage) { m.Source = "NOPE" }},
		{"empty ref", func(m *IngestMessage) { m.SourceRefID = "" }},
		{"zero version", func(m *IngestMessage) { m.SourceVersion = 0 }},
		{"bad op", func(m *IngestMessage) { m.Op = "patch" }},
		{"empty message_id", func(m *IngestMessage) { m.MessageID = "" }},
		{"bad hazard_type", func(m *IngestMessage) { m.HazardType = "NOPE" }},
		{"bad severity", func(m *IngestMessage) { m.Severity = "NOPE" }},
		{"empty area", func(m *IngestMessage) { m.AreaName = "" }},
		{"lat too high", func(m *IngestMessage) { m.Latitude = 91 }},
		{"lon too low", func(m *IngestMessage) { m.Longitude = -181 }},
		{"bad occurred_at", func(m *IngestMessage) { m.OccurredAt = "not-a-time" }},
	}

	for _, tc := range bad {
		m := validUpsert
		tc.fn(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
}

func TestIngestRejectValidation(t *testing.T) {
	ok := IngestMessage{
		MessageID: "reject:obs-1", Op: IngestOpReject, Source: hazard.SourceSensorPantai,
		SourceRefID: "obs-1", Reason: "expired", Raw: []byte(`{"observation_id":"obs-1"}`),
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid reject rejected: %v", err)
	}
	noRef := ok
	noRef.SourceRefID = ""
	if err := noRef.Validate(); err != nil {
		t.Fatalf("a reject without source_ref_id (unparseable data) must be allowed: %v", err)
	}
	for name, mutate := range map[string]func(*IngestMessage){
		"no reason":  func(m *IngestMessage) { m.Reason = "" },
		"no raw":     func(m *IngestMessage) { m.Raw = nil },
		"no id":      func(m *IngestMessage) { m.MessageID = "" },
		"bad source": func(m *IngestMessage) { m.Source = "X" },
	} {
		m := ok
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
