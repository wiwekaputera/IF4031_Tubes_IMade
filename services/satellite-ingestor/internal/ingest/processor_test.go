package ingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
)

// memStore is an in-memory Store. Commit applies every part of a Commit at once.
type memStore struct {
	st     State
	dets   map[string]fire.Detection
	fires  map[string]fire.Fire
	outbox []Outgoing
}

func newMemStore() *memStore {
	return &memStore{dets: map[string]fire.Detection{}, fires: map[string]fire.Fire{}}
}

func (m *memStore) State(context.Context) (State, error) { return m.st, nil }

func (m *memStore) Get(_ context.Context, key string) (fire.Detection, bool, error) {
	d, ok := m.dets[key]
	return d, ok, nil
}

func (m *memStore) Window(_ context.Context, since time.Time) ([]fire.Detection, error) {
	var out []fire.Detection
	for _, d := range m.dets {
		t, err := d.Time()
		if err != nil {
			return nil, err
		}
		if !t.Before(since) && !d.IsLowConfidence() {
			out = append(out, d)
		}
	}
	return out, nil
}

func (m *memStore) Fires(_ context.Context, since time.Time) ([]fire.Fire, error) {
	var out []fire.Fire
	for _, f := range m.fires {
		if !f.LastSeenAt.Before(since) {
			out = append(out, f)
		}
	}
	return out, nil
}

func (m *memStore) Commit(_ context.Context, c Commit) error {
	for _, e := range c.Edits {
		d, ok := m.dets[e.Key]
		if !ok {
			continue
		}
		if e.Retract {
			delete(m.dets, e.Key)
			continue
		}
		if e.FRP != nil {
			d.FRP = *e.FRP
		}
		if e.Confidence != nil {
			d.Confidence = *e.Confidence
		}
		m.dets[e.Key] = d
	}
	if c.Accepted != nil {
		m.dets[c.Accepted.Key()] = *c.Accepted
	}

	for _, id := range c.Deletes {
		delete(m.fires, id)
	}
	for _, f := range c.Upserts {
		m.fires[f.SourceRefID] = f
	}
	m.outbox = append(m.outbox, c.Outbox...)

	m.st.NextOffset = c.NextOffset
	m.st.Watermark = c.Watermark
	m.st.Invalid += c.Counts.Invalid
	m.st.LowConf += c.Counts.LowConf
	m.st.Late += c.Counts.Late
	m.st.Unknown += c.Counts.Unknown
	return nil
}

func msg(offset int64, sat, date string, tm int, lat, lon, conf string) feed.Message {
	raw, _ := json.Marshal(map[string]any{
		"satellite": sat, "acq_date": date, "acq_time": tm,
		"latitude": json.Number(lat), "longitude": json.Number(lon),
		"confidence": conf, "frp": 5.0,
	})
	return feed.Message{Type: "detection", Offset: offset, Detection: raw}
}

// editMsg builds an update or retract event shaped like the live feed.
func editMsg(offset int64, typ, key string, changes map[string]any) feed.Message {
	body := map[string]any{"offset": offset, "key": key}
	if typ == "retract" {
		body["reason"] = "false_positive"
	} else {
		body["changes"] = changes
	}
	raw, _ := json.Marshal(body)
	return feed.Message{Type: typ, Offset: offset, Data: raw}
}

func newProc(t *testing.T) (*Processor, *memStore) {
	t.Helper()
	s := newMemStore()
	p, err := New(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return p, s
}

const (
	keyA = "N21|2026-11-22|2310|-1.98905|112.64071"
	keyB = "N20|2026-11-22|2320|-1.98273|112.63871"
)

// twoNearbyFires feeds A and B, which together form one fire.
func twoNearbyFires(t *testing.T, p *Processor) {
	t.Helper()
	ctx := context.Background()
	if err := p.Handle(ctx, msg(0, "N21", "2026-11-22", 2310, "-1.98905", "112.64071", "n")); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, msg(1, "N20", "2026-11-22", 2320, "-1.98273", "112.63871", "h")); err != nil {
		t.Fatal(err)
	}
}

func TestTwoNearbyDetectionsFormOneFire(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()

	if err := p.Handle(ctx, msg(0, "N21", "2026-11-22", 2310, "-1.98905", "112.64071", "n")); err != nil {
		t.Fatal(err)
	}
	if len(s.fires) != 0 {
		t.Fatalf("single detection made a fire: %d", len(s.fires))
	}
	twoNearbyFires(t, p) // feeds the same first detection again, which must not double count
	if len(s.fires) != 1 {
		t.Fatalf("fires = %d, want 1", len(s.fires))
	}
	if s.st.NextOffset != 2 {
		t.Fatalf("NextOffset = %d, want 2", s.st.NextOffset)
	}
}

func TestLowConfidenceAdvancesOffsetButIsNotStored(t *testing.T) {
	p, s := newProc(t)
	if err := p.Handle(context.Background(), msg(0, "N21", "2026-11-22", 2310, "-1.98", "112.64", "l")); err != nil {
		t.Fatal(err)
	}
	if len(s.dets) != 0 || s.st.LowConf != 1 || s.st.NextOffset != 1 {
		t.Fatalf("dets=%d lowconf=%d next=%d", len(s.dets), s.st.LowConf, s.st.NextOffset)
	}
}

func TestInvalidRowIsCountedAndDoesNotStall(t *testing.T) {
	p, s := newProc(t)
	bad := msg(0, "N21", "2026-13-40", 2310, "-1.98", "112.64", "n") // impossible date
	if err := p.Handle(context.Background(), bad); err != nil {
		t.Fatalf("invalid row returned error (would stall the feed): %v", err)
	}
	if s.st.Invalid != 1 || s.st.NextOffset != 1 {
		t.Fatalf("invalid=%d next=%d", s.st.Invalid, s.st.NextOffset)
	}
}

func TestLateDetectionIsCountedNotClustered(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()

	// Newest detection at 23:10 UTC sets the watermark.
	if err := p.Handle(ctx, msg(0, "N21", "2026-11-22", 2310, "-1.98", "112.64", "n")); err != nil {
		t.Fatal(err)
	}
	// 17:00 is more than LateWindow (2h) behind the watermark.
	if err := p.Handle(ctx, msg(1, "N20", "2026-11-22", 1700, "-1.98", "112.64", "n")); err != nil {
		t.Fatal(err)
	}
	if s.st.Late != 1 {
		t.Fatalf("late = %d, want 1", s.st.Late)
	}
	if len(s.dets) != 1 {
		t.Fatalf("late detection was stored: %d dets", len(s.dets))
	}
	if s.st.NextOffset != 2 {
		t.Fatalf("NextOffset = %d, want 2", s.st.NextOffset)
	}
}

func TestRetractRemovesDetectionAndFire(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()
	twoNearbyFires(t, p)

	if err := p.Handle(ctx, editMsg(2, "retract", keyA, nil)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.dets[keyA]; ok {
		t.Fatal("retracted detection still stored")
	}
	if len(s.fires) != 0 {
		t.Fatalf("fires = %d after retract, want 0 (stale fire not pruned)", len(s.fires))
	}
	if s.st.NextOffset != 3 {
		t.Fatalf("NextOffset = %d, want 3", s.st.NextOffset)
	}
}

func TestUpdateToLowConfidenceDropsFire(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()
	twoNearbyFires(t, p)

	if err := p.Handle(ctx, editMsg(2, "update", keyB, map[string]any{"confidence": "l"})); err != nil {
		t.Fatal(err)
	}
	if len(s.fires) != 0 {
		t.Fatalf("fires = %d after B dropped to low confidence, want 0", len(s.fires))
	}
	if d := s.dets[keyB]; d.Confidence != "l" {
		t.Fatalf("stored confidence = %q, want l (row kept, excluded from clustering)", d.Confidence)
	}
}

func TestUpdateChangesFRP(t *testing.T) {
	p, s := newProc(t)
	twoNearbyFires(t, p)

	frp := 9.5
	if err := p.Handle(context.Background(), editMsg(2, "update", keyA, map[string]any{"frp": frp})); err != nil {
		t.Fatal(err)
	}
	if got := s.dets[keyA].FRP; got != 9.5 {
		t.Fatalf("FRP = %v, want 9.5", got)
	}
	if len(s.fires) != 1 {
		t.Fatalf("fires = %d, want 1 (frp change must not drop the fire)", len(s.fires))
	}
}

func TestEditForUnknownKeyIsCounted(t *testing.T) {
	p, s := newProc(t)
	if err := p.Handle(context.Background(), editMsg(0, "retract", "X|2026-11-22|0000|0|0", nil)); err != nil {
		t.Fatalf("unknown key stalled the feed: %v", err)
	}
	if s.st.Unknown != 1 || s.st.NextOffset != 1 {
		t.Fatalf("unknown=%d next=%d", s.st.Unknown, s.st.NextOffset)
	}
}

func TestReplayedDetectionDoesNotDuplicateFire(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()

	a := msg(0, "N21", "2026-11-22", 2310, "-1.98905", "112.64071", "n")
	b := msg(1, "N20", "2026-11-22", 2320, "-1.98273", "112.63871", "h")
	for _, m := range []feed.Message{a, b, b} { // b delivered twice
		if err := p.Handle(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.dets) != 2 {
		t.Fatalf("stored detections = %d, want 2", len(s.dets))
	}
	if len(s.fires) != 1 {
		t.Fatalf("fires = %d, want 1 after replay", len(s.fires))
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	p, s := newProc(t)
	ctx := context.Background()
	if err := p.Handle(ctx, msg(0, "N21", "2026-11-22", 2310, "-1.98", "112.64", "n")); err != nil {
		t.Fatal(err)
	}
	// A new processor built from the same store resumes at the committed offset.
	p2, err := New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := p2.NextOffset(ctx)
	if next != 1 {
		t.Fatalf("resume offset = %d, want 1", next)
	}
}
