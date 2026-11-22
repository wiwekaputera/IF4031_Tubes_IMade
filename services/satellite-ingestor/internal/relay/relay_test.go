package relay

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
)

type fakeStore struct {
	rows      []store.OutboxRow
	published map[int64]bool
}

func (f *fakeStore) PendingOutbox(_ context.Context, limit int) ([]store.OutboxRow, error) {
	var out []store.OutboxRow
	for _, r := range f.rows {
		if !f.published[r.ID] && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) MarkPublished(_ context.Context, ids ...int64) error {
	for _, id := range ids {
		f.published[id] = true
	}
	return nil
}

type fakePub struct {
	sent   []string
	failOn string
}

func (p *fakePub) Publish(_ context.Context, subject, id string, _ []byte) error {
	if id == p.failOn {
		p.failOn = "" // fail once
		return errors.New("broker unavailable")
	}
	p.sent = append(p.sent, subject+" "+id)
	return nil
}

func rows(n int) []store.OutboxRow {
	var out []store.OutboxRow
	for i := 1; i <= n; i++ {
		out = append(out, store.OutboxRow{ID: int64(i), MessageID: fmt.Sprintf("m%d", i), Subject: "hazard.ingest.SATELIT"})
	}
	return out
}

func TestPublishesInOrderAndMarks(t *testing.T) {
	s := &fakeStore{rows: rows(3), published: map[int64]bool{}}
	p := &fakePub{}
	r := &Relay{Store: s, Publisher: p}
	n, err := r.PublishPending(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if fmt.Sprint(p.sent) != "[hazard.ingest.SATELIT m1 hazard.ingest.SATELIT m2 hazard.ingest.SATELIT m3]" {
		t.Fatalf("sent = %v", p.sent)
	}
	if len(s.published) != 3 {
		t.Fatalf("marked %d rows, want 3", len(s.published))
	}
}

func TestFailureStopsBatchAndRetriesSameRow(t *testing.T) {
	s := &fakeStore{rows: rows(3), published: map[int64]bool{}}
	p := &fakePub{failOn: "m2"}
	r := &Relay{Store: s, Publisher: p}

	n, err := r.PublishPending(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("first pass n=%d err=%v, want 1 and an error", n, err)
	}
	if s.published[3] {
		t.Fatal("m3 was published before m2: order broken")
	}
	if _, err := r.PublishPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(p.sent) != "[hazard.ingest.SATELIT m1 hazard.ingest.SATELIT m2 hazard.ingest.SATELIT m3]" {
		t.Fatalf("sent = %v", p.sent)
	}
}

// fireRows builds outbox rows for the given fires, one row per entry, in order.
func fireRows(fires ...string) []store.OutboxRow {
	var out []store.OutboxRow
	for i, f := range fires {
		out = append(out, store.OutboxRow{
			ID: int64(i + 1), MessageID: fmt.Sprintf("%s:%d", f, i+1),
			Subject: "hazard.ingest.SATELIT", SourceRefID: f,
		})
	}
	return out
}

func TestSendsOnlyNewestRowPerFire(t *testing.T) {
	s := &fakeStore{rows: fireRows("a", "b", "a", "a", "b", "c"), published: map[int64]bool{}}
	p := &fakePub{}
	superseded := 0
	r := &Relay{Store: s, Publisher: p, OnSuperseded: func(n int) { superseded += n }}

	n, err := r.PublishPending(context.Background())
	if err != nil || n != 6 {
		t.Fatalf("n=%d err=%v, want all 6 rows cleared", n, err)
	}
	// The newest rows keep their original order: a:4 comes before b:5.
	want := "[hazard.ingest.SATELIT a:4 hazard.ingest.SATELIT b:5 hazard.ingest.SATELIT c:6]"
	if fmt.Sprint(p.sent) != want {
		t.Fatalf("sent = %v, want %s", p.sent, want)
	}
	if len(s.published) != 6 || superseded != 3 {
		t.Fatalf("marked %d rows, superseded %d; want 6 and 3", len(s.published), superseded)
	}
}

func TestSupersededRowsStayPendingWhenNewestFails(t *testing.T) {
	s := &fakeStore{rows: fireRows("a", "a", "b"), published: map[int64]bool{}}
	p := &fakePub{failOn: "a:2"}
	r := &Relay{Store: s, Publisher: p}

	if _, err := r.PublishPending(context.Background()); err == nil {
		t.Fatal("want an error from the failed publish")
	}
	if s.published[1] || s.published[2] || s.published[3] {
		t.Fatalf("rows marked after a failed publish: %v", s.published)
	}
	if _, err := r.PublishPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(p.sent) != "[hazard.ingest.SATELIT a:2 hazard.ingest.SATELIT b:3]" {
		t.Fatalf("sent = %v", p.sent)
	}
	if len(s.published) != 3 {
		t.Fatalf("marked %d rows, want 3", len(s.published))
	}
}

func TestRowsWithoutFireIDAreNeverMerged(t *testing.T) {
	s := &fakeStore{rows: rows(3), published: map[int64]bool{}}
	p := &fakePub{}
	if _, err := (&Relay{Store: s, Publisher: p}).PublishPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 3 {
		t.Fatalf("sent %d messages, want 3", len(p.sent))
	}
}
