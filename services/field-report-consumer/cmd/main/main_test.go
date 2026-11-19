package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
)

type fakeMsg struct {
	acked, termed bool
	naks          int
	lastDelay     time.Duration
}

func (m *fakeMsg) Ack(...nats.AckOpt) error  { m.acked = true; return nil }
func (m *fakeMsg) Term(...nats.AckOpt) error { m.termed = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration, _ ...nats.AckOpt) error {
	m.naks++
	m.lastDelay = d
	return nil
}

func reportData(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(fieldreport.Report{ReportID: "r1", ClientID: "team-a", Severity: "HIGH", AcceptedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newTestConsumer(t *testing.T, sink http.HandlerFunc) (*consumer, *httptest.Server) {
	t.Helper()
	store, err := openStore(filepath.Join(t.TempDir(), "dedup.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.db.Close() })
	srv := httptest.NewServer(sink)
	t.Cleanup(srv.Close)
	return &consumer{store: store, sinkURL: srv.URL, httpClient: srv.Client(), logger: zerolog.Nop()}, srv
}

func TestDeliversOnceAcrossRedeliveries(t *testing.T) {
	var calls atomic.Int32
	c, _ := newTestConsumer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Idempotency-Key") != "team-a/r1" {
			t.Errorf("Idempotency-Key = %q", r.Header.Get("Idempotency-Key"))
		}
		w.WriteHeader(http.StatusAccepted)
	})
	data := reportData(t)

	first := &fakeMsg{}
	if out := c.handle(context.Background(), first, data, 1); out != "ok" || !first.acked {
		t.Fatalf("first delivery: outcome %s acked %v", out, first.acked)
	}
	// Same report delivered again (e.g. the ack was lost when the pod died).
	again := &fakeMsg{}
	if out := c.handle(context.Background(), again, data, 2); out != "duplicate" || !again.acked {
		t.Fatalf("redelivery: outcome %s acked %v", out, again.acked)
	}
	if calls.Load() != 1 {
		t.Fatalf("receiver called %d times, want 1", calls.Load())
	}
}

func TestReceiverDownIsRetriedNotAcked(t *testing.T) {
	c, _ := newTestConsumer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	msg := &fakeMsg{}
	if out := c.handle(context.Background(), msg, reportData(t), 3); out != "retry" {
		t.Fatalf("outcome %s", out)
	}
	if msg.acked || msg.naks != 1 || msg.lastDelay != 4*time.Second {
		t.Fatalf("acked=%v naks=%d delay=%v", msg.acked, msg.naks, msg.lastDelay)
	}
	if done, _ := c.store.IsProcessed("team-a/r1"); done {
		t.Fatal("an undelivered report must not be marked processed")
	}
}

func TestUndecodableMessageIsTerminated(t *testing.T) {
	c, _ := newTestConsumer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	msg := &fakeMsg{}
	if out := c.handle(context.Background(), msg, []byte(`{"report_id":`), 1); out != "invalid" || !msg.termed {
		t.Fatalf("outcome %s termed %v", out, msg.termed)
	}
}

func TestDedupSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dedup.db")
	s1, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.MarkProcessed("team-a/r1"); err != nil {
		t.Fatal(err)
	}
	s1.db.Close()
	s2, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.db.Close()
	if done, err := s2.IsProcessed("team-a/r1"); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
}

func TestRetryDelayBacksOffAndCaps(t *testing.T) {
	want := map[uint64]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 10: maxRetryDelay}
	for n, d := range want {
		if got := retryDelay(n); got != d {
			t.Errorf("retryDelay(%d) = %v, want %v", n, got, d)
		}
	}
}
