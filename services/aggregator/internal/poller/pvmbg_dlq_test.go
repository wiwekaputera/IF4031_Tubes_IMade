package poller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

// recordingRepo wraps fakeRepo and records DLQ writes and injected failures.
type recordingRepo struct {
	*fakeRepo
	mu          sync.Mutex
	dlq         []store.DeadLetter
	upsertErr   error
	dlqErr      error
	panicUpsert bool
}

func (r *recordingRepo) UpsertEvent(ctx context.Context, rec store.UpsertRequest) (string, error) {
	if r.panicUpsert {
		panic("boom")
	}
	if r.upsertErr != nil {
		return "", r.upsertErr
	}
	return r.fakeRepo.UpsertEvent(ctx, rec)
}

func (r *recordingRepo) InsertDeadLetter(_ context.Context, dl store.DeadLetter) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dlqErr != nil {
		return r.dlqErr
	}
	r.dlq = append(r.dlq, dl)
	return nil
}

func pvmbgServerReturning(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
}

const mixedPage = `[
  {"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"},
  {"report_id":"R-2","volcano_code":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T11:00:00Z"},
  {"report_id":"R-3","volcano_id":"GNG-002","alert_level":"WASPADA","reported_at":"2026-11-17T12:00:00Z"},
  {"report_id":"R-4","volcano_id":"GNG-001","alert_level":"SIAGA","ash_column_height_m":"high","reported_at":"2026-11-17T13:00:00Z"}
]`

func TestPVMBGParksRejectedAndKeepsValid(t *testing.T) {
	srv := pvmbgServerReturning(t, mixedPage)
	defer srv.Close()

	p := newPollerWithBreaker("", srv.URL, 5, time.Second)
	repo := &recordingRepo{fakeRepo: &fakeRepo{}}
	p.repo = repo

	p.pollPVMBG(context.Background(), "dlq-mixed", time.Time{}, 0)

	if repo.fakeRepo.upserts != 2 {
		t.Fatalf("upserts = %d, want 2", repo.fakeRepo.upserts)
	}
	if len(repo.dlq) != 2 {
		t.Fatalf("dlq rows = %d, want 2", len(repo.dlq))
	}
	want := []struct{ ref, reason string }{
		{"R-2", "missing_required:volcano_id"},
		{"R-4", "type_mismatch:ash_column_height_m"},
	}
	for i, w := range want {
		got := repo.dlq[i]
		if got.SourceRefID != w.ref || got.Reason != w.reason || got.Retryable {
			t.Errorf("dlq[%d] = {ref:%q reason:%q retryable:%v}, want {ref:%q reason:%q retryable:false}",
				i, got.SourceRefID, got.Reason, got.Retryable, w.ref, w.reason)
		}
		if got.Source != "PVMBG" || len(got.Raw) == 0 {
			t.Errorf("dlq[%d] missing source or raw payload", i)
		}
	}
	// R-2 is rejected but its timestamp still advances the watermark.
	if p.Health().PVMBG.LastSynced.Before(time.Date(2026, 11, 17, 13, 0, 0, 0, time.UTC)) {
		t.Errorf("watermark = %v, want at least the last report time", p.Health().PVMBG.LastSynced)
	}
}

func TestPVMBGUpsertFailureIsQueuedForRetry(t *testing.T) {
	srv := pvmbgServerReturning(t, `[{"report_id":"R-9","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"}]`)
	defer srv.Close()

	p := newPollerWithBreaker("", srv.URL, 5, time.Second)
	repo := &recordingRepo{fakeRepo: &fakeRepo{}, upsertErr: errors.New("db down")}
	p.repo = repo

	p.pollPVMBG(context.Background(), "dlq-retry", time.Time{}, 0)

	if len(repo.dlq) != 1 {
		t.Fatalf("dlq rows = %d, want 1", len(repo.dlq))
	}
	got := repo.dlq[0]
	if !got.Retryable || got.SourceRefID != "R-9" {
		t.Errorf("dlq row = {ref:%q retryable:%v}, want {ref:R-9 retryable:true}", got.SourceRefID, got.Retryable)
	}
}

func TestPVMBGWatermarkHeldWhenDLQWriteFails(t *testing.T) {
	srv := pvmbgServerReturning(t, mixedPage)
	defer srv.Close()

	p := newPollerWithBreaker("", srv.URL, 5, time.Second)
	repo := &recordingRepo{fakeRepo: &fakeRepo{}, dlqErr: errors.New("db down")}
	p.repo = repo

	p.pollPVMBG(context.Background(), "dlq-fail", time.Time{}, 0)

	if !p.Health().PVMBG.LastSynced.IsZero() {
		t.Fatalf("watermark advanced to %v although DLQ write failed", p.Health().PVMBG.LastSynced)
	}
}

func TestPanicInMessageIsParkedNotFatal(t *testing.T) {
	srv := pvmbgServerReturning(t, `[
  {"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"},
  {"report_id":"R-2","volcano_id":"GNG-002","alert_level":"WASPADA","reported_at":"2026-11-17T11:00:00Z"}
]`)
	defer srv.Close()

	p := newPollerWithBreaker("", srv.URL, 5, time.Second)
	repo := &recordingRepo{fakeRepo: &fakeRepo{}, panicUpsert: true}
	p.repo = repo

	p.pollPVMBG(context.Background(), "dlq-panic", time.Time{}, 0)

	if len(repo.dlq) != 2 {
		t.Fatalf("dlq rows = %d, want 2 (both messages parked, poll survived)", len(repo.dlq))
	}
	for i, row := range repo.dlq {
		if row.Retryable || !strings.HasPrefix(row.Reason, "panic: boom") {
			t.Errorf("dlq[%d] = {retryable:%v reason:%q}, want permanent panic reason", i, row.Retryable, row.Reason)
		}
	}
}
