package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeIngestStore struct {
	applied bool
	err     error
	panic   bool
	calls   []event.IngestMessage
}

func (s *fakeIngestStore) ApplyIngest(_ context.Context, msg event.IngestMessage) (bool, error) {
	s.calls = append(s.calls, msg)
	if s.panic {
		panic("boom")
	}
	return s.applied, s.err
}

type fakeMsg struct {
	subject  string
	data     []byte
	msgID    string
	acked    bool
	naked    bool
	nakDelay time.Duration
	termed   bool
}

func (m *fakeMsg) Subject() string { return m.subject }
func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) MsgID() string   { return m.msgID }
func (m *fakeMsg) Ack() error {
	m.acked = true
	return nil
}
func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *fakeMsg) Term() error {
	m.termed = true
	return nil
}

func validUpsertJSON(t *testing.T, extraFields bool) []byte {
	t.Helper()
	m := map[string]any{
		"message_id":     "msg-1",
		"op":             "upsert",
		"source":         "SATELIT",
		"source_ref_id":  "wf-1",
		"source_version": 1,
		"hazard_type":    "WILDFIRE",
		"severity":       "SIAGA",
		"area_name":      "Riau",
		"latitude":       0.5,
		"longitude":      101.4,
		"occurred_at":    "2026-11-18T10:00:00Z",
		"attributes":     map[string]any{"detections": 3},
	}
	if extraFields {
		m["unknown_future_field"] = "tolerant-reader"
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestProcessMessage_ValidUpsertToleratesUnknownFieldsAndAcks(t *testing.T) {
	st := &fakeIngestStore{applied: true}
	c := New(nil, st, zerolog.Nop())
	msg := &fakeMsg{
		subject: "hazard.ingest.SATELIT",
		data:    validUpsertJSON(t, true),
		msgID:   "sat-1",
	}

	c.ProcessMessage(context.Background(), msg)

	if !msg.acked || msg.termed || msg.naked {
		t.Fatalf("msg state = (acked=%v, termed=%v, naked=%v), want acked=true", msg.acked, msg.termed, msg.naked)
	}
	if len(st.calls) != 1 || st.calls[0].Source != hazard.SourceSatelit {
		t.Fatalf("store calls = %+v, want 1 SATELIT call", st.calls)
	}
}

func TestProcessMessage_ValidDeleteAndStaleNoOpAck(t *testing.T) {
	st := &fakeIngestStore{applied: false}
	c := New(nil, st, zerolog.Nop())
	payload, _ := json.Marshal(event.IngestMessage{
		Op:            event.IngestOpDelete,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   "sp-1",
		SourceVersion: 2,
	})
	msg := &fakeMsg{subject: "hazard.ingest.SENSOR_PANTAI", data: payload, msgID: "sp-2"}

	c.ProcessMessage(context.Background(), msg)

	if !msg.acked || msg.termed || msg.naked {
		t.Fatalf("msg state = (acked=%v, termed=%v, naked=%v), want acked=true", msg.acked, msg.termed, msg.naked)
	}
	if len(st.calls) != 1 || st.calls[0].Op != event.IngestOpDelete {
		t.Fatalf("store calls = %+v, want 1 delete call", st.calls)
	}
}

func TestProcessMessage_TransientStoreErrorNaksWithDelay(t *testing.T) {
	st := &fakeIngestStore{err: errors.New("connection refused")}
	c := New(nil, st, zerolog.Nop())
	msg := &fakeMsg{
		subject: "hazard.ingest.SATELIT",
		data:    validUpsertJSON(t, false),
		msgID:   "sat-err",
	}

	c.ProcessMessage(context.Background(), msg)

	if !msg.naked || msg.nakDelay != defaultNakDelay || msg.acked || msg.termed {
		t.Fatalf("msg state = (naked=%v, delay=%v, acked=%v, termed=%v), want naked=true delay=%v",
			msg.naked, msg.nakDelay, msg.acked, msg.termed, defaultNakDelay)
	}
}

func TestProcessMessage_PoisonMessagesAreTerminated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "garbage bytes", data: []byte("not-json-at-all")},
		{name: "validation failure", data: []byte(`{"op":"upsert","source":"UNKNOWN","source_ref_id":"x","source_version":1}`)},
		{name: "oversized payload", data: bytes.Repeat([]byte("a"), MaxPayloadBytes+1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeIngestStore{applied: true}
			c := New(nil, st, zerolog.Nop())
			msg := &fakeMsg{
				subject: "hazard.ingest.SATELIT",
				data:    tc.data,
				msgID:   "poison-1",
			}

			c.ProcessMessage(context.Background(), msg)

			if !msg.termed || msg.acked || msg.naked {
				t.Fatalf("msg state = (termed=%v, acked=%v, naked=%v), want termed=true", msg.termed, msg.acked, msg.naked)
			}
			if len(st.calls) != 0 {
				t.Fatalf("store calls = %d, want 0 for poison message", len(st.calls))
			}
		})
	}
}

func TestProcessMessage_RecoversPanicAndTerminates(t *testing.T) {
	st := &fakeIngestStore{panic: true}
	c := New(nil, st, zerolog.Nop())
	msg := &fakeMsg{
		subject: "hazard.ingest.SATELIT",
		data:    validUpsertJSON(t, false),
		msgID:   "panic-1",
	}

	c.ProcessMessage(context.Background(), msg)

	if !msg.termed || msg.acked || msg.naked {
		t.Fatalf("msg state = (termed=%v, acked=%v, naked=%v), want termed=true", msg.termed, msg.acked, msg.naked)
	}
}

type recordedUnprocessed struct {
	source      string
	sourceRefID string
	raw         []byte
	reason      string
}

type recordingIngestStore struct {
	fakeIngestStore
	recorded []recordedUnprocessed
	recErr   error
}

func (r *recordingIngestStore) RecordUnprocessed(_ context.Context, source, sourceRefID string, raw []byte, reason string) error {
	r.recorded = append(r.recorded, recordedUnprocessed{
		source:      source,
		sourceRefID: sourceRefID,
		raw:         raw,
		reason:      reason,
	})
	return r.recErr
}

func TestConsumer_RecordsPoisonMessages(t *testing.T) {
	st := &recordingIngestStore{}
	c := New(nil, st, zerolog.Nop())

	// Invalid JSON -> Term() + RecordUnprocessed with inferred source from subject.
	badJSON := &fakeMsg{
		subject: "hazard.ingest.SATELIT",
		msgID:   "m-bad",
		data:    []byte("{not-json"),
	}
	c.ProcessMessage(context.Background(), badJSON)
	if !badJSON.termed {
		t.Fatalf("expected bad JSON to be termed")
	}
	if len(st.recorded) != 1 || st.recorded[0].source != "SATELIT" || st.recorded[0].reason != "invalid json" {
		t.Fatalf("unexpected recorded poison message: %+v", st.recorded)
	}

	// Best-effort: even if RecordUnprocessed returns an error, Term() still happens.
	st.recErr = errors.New("db unavailable")
	oversized := &fakeMsg{
		subject: "hazard.ingest.SENSOR_PANTAI",
		msgID:   "m-big",
		data:    make([]byte, MaxPayloadBytes+1),
	}
	c.ProcessMessage(context.Background(), oversized)
	if !oversized.termed {
		t.Fatalf("expected oversized message to be termed even when RecordUnprocessed fails")
	}
	if len(st.recorded) != 2 || st.recorded[1].source != "SENSOR_PANTAI" || st.recorded[1].reason != "payload exceeds max size" {
		t.Fatalf("unexpected second recorded poison message: %+v", st.recorded)
	}
}
