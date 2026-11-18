package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
)

// at builds a detection message at a time given as "2006-01-02 15:04".
func at(offset int64, ts, lat, lon string) feed.Message {
	t, err := time.Parse("2006-01-02 15:04", ts)
	if err != nil {
		panic(err)
	}
	hhmm := t.Hour()*100 + t.Minute()
	return msg(offset, "N21", t.Format("2006-01-02"), hhmm, lat, lon, "n")
}

func key(ts, lat, lon string) string {
	t, _ := time.Parse("2006-01-02 15:04", ts)
	return fmt.Sprintf("N21|%s|%d|%s|%s", t.Format("2006-01-02"), t.Hour()*100+t.Minute(), lat, lon)
}

func decode(t *testing.T, o Outgoing) event.IngestMessage {
	t.Helper()
	var m event.IngestMessage
	if err := json.Unmarshal(o.Payload, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func feedAll(t *testing.T, p *Processor, ms ...feed.Message) {
	t.Helper()
	for _, m := range ms {
		if err := p.Handle(context.Background(), m); err != nil {
			t.Fatalf("offset %d: %v", m.Offset, err)
		}
	}
}

func onlyFireID(t *testing.T, s *memStore) string {
	t.Helper()
	if len(s.fires) != 1 {
		t.Fatalf("fires = %d, want 1", len(s.fires))
	}
	for id := range s.fires {
		return id
	}
	return ""
}

func TestOutboxCarriesWildfireMessage(t *testing.T) {
	p, s := newProc(t)
	twoNearbyFires(t, p)

	if len(s.outbox) != 1 {
		t.Fatalf("outbox = %d messages, want 1", len(s.outbox))
	}
	o := s.outbox[0]
	m := decode(t, o)
	if o.Subject != "hazard.ingest.SATELIT" || o.MessageID != m.MessageID {
		t.Fatalf("subject=%q id=%q", o.Subject, o.MessageID)
	}
	if m.Op != event.IngestOpUpsert || m.HazardType != "WILDFIRE" || m.Source != "SATELIT" {
		t.Fatalf("message = %+v", m)
	}
	if m.AreaName != "Kalimantan Tengah" {
		t.Fatalf("area = %q", m.AreaName)
	}
	if m.SourceVersion != 2 || m.SourceRefID != onlyFireID(t, s) {
		t.Fatalf("version=%d ref=%q", m.SourceVersion, m.SourceRefID)
	}
	if m.Attributes["detection_count"] != float64(2) {
		t.Fatalf("attributes = %v", m.Attributes)
	}
}

func TestUnchangedFireIsNotRepublished(t *testing.T) {
	p, s := newProc(t)
	twoNearbyFires(t, p)
	before := len(s.outbox)

	// A lone detection far away changes no fire.
	feedAll(t, p, msg(2, "N21", "2026-11-22", 2330, "-5.0", "120.0", "n"))
	if len(s.outbox) != before {
		t.Fatalf("outbox grew from %d to %d for an unrelated detection", before, len(s.outbox))
	}
}

func TestIDSurvivesRetractOfEarliestDetection(t *testing.T) {
	p, s := newProc(t)
	feedAll(t, p,
		at(0, "2026-11-22 00:00", "-1.9800", "112.6400"),
		at(1, "2026-11-22 00:10", "-1.9850", "112.6400"),
		at(2, "2026-11-22 00:20", "-1.9900", "112.6400"),
	)
	id := onlyFireID(t, s)

	feedAll(t, p, editMsg(3, "retract", key("2026-11-22 00:00", "-1.9800", "112.6400"), nil))
	if got := onlyFireID(t, s); got != id {
		t.Fatalf("fire ID changed from %s to %s after the earliest detection was retracted", id, got)
	}
	last := decode(t, s.outbox[len(s.outbox)-1])
	if last.Op != event.IngestOpUpsert || last.SourceRefID != id || last.Attributes["detection_count"] != float64(2) {
		t.Fatalf("last message = %+v", last)
	}
}

func TestMergeKeepsOlderIDAndDeletesTheOther(t *testing.T) {
	p, s := newProc(t)
	feedAll(t, p,
		at(0, "2026-11-22 00:00", "-1.9800", "112.6400"),
		at(1, "2026-11-22 00:10", "-1.9850", "112.6400"),
		at(2, "2026-11-22 01:00", "-2.0000", "112.6400"), // 1.7 km away: a separate fire
		at(3, "2026-11-22 01:10", "-2.0050", "112.6400"),
	)
	if len(s.fires) != 2 {
		t.Fatalf("fires = %d before the bridge, want 2", len(s.fires))
	}
	older := decode(t, s.outbox[0]).SourceRefID
	newer := decode(t, s.outbox[1]).SourceRefID

	// Within 1 km of both fires, so they merge.
	feedAll(t, p, at(4, "2026-11-22 01:20", "-1.9925", "112.6400"))
	if got := onlyFireID(t, s); got != older {
		t.Fatalf("merged fire ID = %s, want the older fire's %s", got, older)
	}
	tail := s.outbox[len(s.outbox)-2:]
	up, del := decode(t, tail[0]), decode(t, tail[1])
	if up.Op != event.IngestOpUpsert || up.SourceRefID != older || up.Attributes["detection_count"] != float64(5) {
		t.Fatalf("upsert = %+v", up)
	}
	if del.Op != event.IngestOpDelete || del.SourceRefID != newer {
		t.Fatalf("delete = %+v, want delete of %s", del, newer)
	}
}

func TestFireLongerThanWindowStaysOneFire(t *testing.T) {
	p, s := newProc(t)
	start, _ := time.Parse("2006-01-02 15:04", "2026-11-22 00:00")
	var ms []feed.Message
	for i := 0; i < 10; i++ { // every 5 hours for 45 hours
		ts := start.Add(time.Duration(i) * 5 * time.Hour).Format("2006-01-02 15:04")
		ms = append(ms, at(int64(i), ts, "-1.9800", fmt.Sprintf("112.64%02d", i)))
	}
	feedAll(t, p, ms...)

	id := onlyFireID(t, s)
	if n := s.fires[id].DetectionCount; n != 10 {
		t.Fatalf("detection_count = %d, want 10", n)
	}
	for _, o := range s.outbox {
		if m := decode(t, o); m.SourceRefID != id {
			t.Fatalf("a second fire ID %s was published for one long fire", m.SourceRefID)
		}
	}
}

func TestVersionsOnlyGrow(t *testing.T) {
	p, s := newProc(t)
	feedAll(t, p,
		at(0, "2026-11-22 00:00", "-1.9800", "112.6400"),
		at(1, "2026-11-22 00:10", "-1.9850", "112.6400"),
		at(2, "2026-11-22 00:20", "-1.9900", "112.6400"),
		editMsg(3, "update", key("2026-11-22 00:10", "-1.9850", "112.6400"), map[string]any{"frp": 50.0}),
	)
	var prev int64
	for _, o := range s.outbox {
		v := decode(t, o).SourceVersion
		if v <= prev {
			t.Fatalf("version %d after %d", v, prev)
		}
		prev = v
	}
}
