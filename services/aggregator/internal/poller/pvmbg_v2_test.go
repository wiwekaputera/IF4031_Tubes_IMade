package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/dlq"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

// captureRepo keeps the last record stored, to check what replay wrote.
type captureRepo struct {
	*fakeRepo
	mu   sync.Mutex
	last *store.UpsertRequest
}

func (r *captureRepo) UpsertEvent(ctx context.Context, rec store.UpsertRequest) (string, error) {
	r.mu.Lock()
	r.last = &rec
	r.mu.Unlock()
	return r.fakeRepo.UpsertEvent(ctx, rec)
}

// A report exactly as the PVMBG mock sends it in incompatible mode.
const v2Raw = `{"report_id":"PVMBG-V2-1","volcano_code":"GNG-001","alert_level":"SIAGA","eruption_count_24h":3,"ash_column_height_m":"1500.0"}`

// Without the adapter, a parked v2 report stays rejected on replay.
func TestReplayKeepsV2ParkedWithoutAdapter(t *testing.T) {
	p := newPollerWithBreaker("", "http://unused", 5, time.Second)
	repo := &captureRepo{fakeRepo: &fakeRepo{}}
	p.repo = repo

	err := p.PVMBGRetryHandler()(context.Background(), []byte(v2Raw))
	var perm *dlq.PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("err = %v, want a permanent rejection", err)
	}
	if repo.last != nil {
		t.Fatal("v2 report was stored without the adapter")
	}
}

// With the adapter on, replay stores the v2 report, and the missing
// reported_at becomes the time the message was first received, not the time
// of the replay.
func TestReplayStoresV2WithAdapterUsingReceivedTime(t *testing.T) {
	p := newPollerWithBreaker("", "http://unused", 5, time.Second)
	p.cfg.PVMBGSchemaV2Adapter = true
	repo := &captureRepo{fakeRepo: &fakeRepo{}}
	p.repo = repo

	received := time.Date(2026, 11, 22, 3, 4, 5, 0, time.UTC)
	ctx := dlq.WithReceivedAt(context.Background(), received)
	if err := p.PVMBGRetryHandler()(ctx, []byte(v2Raw)); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if repo.last == nil {
		t.Fatal("nothing stored")
	}
	if repo.last.SourceRefID != "PVMBG-V2-1" || !repo.last.OccurredAt.Equal(received) {
		t.Fatalf("stored %s at %s, want PVMBG-V2-1 at %s", repo.last.SourceRefID, repo.last.OccurredAt, received)
	}
	if repo.last.Attributes["reported_at_source"] != "received_at" {
		t.Fatalf("attributes = %v, want reported_at_source=received_at", repo.last.Attributes)
	}
}
