package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
)

// The store test needs a real Postgres. It runs only when
// SATELLITE_TEST_DATABASE_URL is set, for example in the docker compose stack.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SATELLITE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("SATELLITE_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	schema, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE satellite_fire_members, satellite_fires, satellite_detections, satellite_outbox, satellite_dead_letters; UPDATE satellite_state SET next_offset = 0, watermark = NULL, invalid = 0, low_conf = 0, late = 0, unknown = 0 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func feedMsg(offset int64, sat, date string, tm int, lat, lon, conf string) feed.Message {
	raw, _ := json.Marshal(map[string]any{
		"satellite": sat, "acq_date": date, "acq_time": tm,
		"latitude": json.Number(lat), "longitude": json.Number(lon),
		"confidence": conf, "frp": 5.0,
	})
	return feed.Message{Type: "detection", Offset: offset, Detection: raw}
}

// TestRestartResumesAndKeepsFire checks that a processor restarted from the
// database continues at the committed offset and does not duplicate the fire.
func TestRestartResumesAndKeepsFire(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	s := NewSatellite(pool)

	p, err := ingest.New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, feedMsg(0, "N21", "2026-11-22", 2310, "-1.98905", "112.64071", "n")); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, feedMsg(1, "N20", "2026-11-22", 2320, "-1.98273", "112.63871", "h")); err != nil {
		t.Fatal(err)
	}

	// Simulate a restart: a fresh processor reads its state from the database.
	p2, err := ingest.New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := p2.NextOffset(ctx)
	if next != 2 {
		t.Fatalf("resume offset = %d, want 2", next)
	}
	// Replaying the last message after restart must not add a second fire.
	if err := p2.Handle(ctx, feedMsg(1, "N20", "2026-11-22", 2320, "-1.98273", "112.63871", "h")); err != nil {
		t.Fatal(err)
	}

	var fires, dets int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM satellite_fires`).Scan(&fires); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM satellite_detections`).Scan(&dets); err != nil {
		t.Fatal(err)
	}
	if fires != 1 || dets != 2 {
		t.Fatalf("fires=%d dets=%d, want 1 and 2", fires, dets)
	}
}

func outboxCount(t *testing.T, pool *pgxpool.Pool) (total, pending int) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT count(*), count(*) FILTER (WHERE published_at IS NULL) FROM satellite_outbox`).Scan(&total, &pending)
	if err != nil {
		t.Fatal(err)
	}
	return total, pending
}

// TestOutboxAndRestartDoNotRepublish checks that the fire change is queued in
// the outbox with the commit, that the relay methods drain it, and that a
// processor restarted from the database sees the stored fire as unchanged.
func TestOutboxAndRestartDoNotRepublish(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	s := NewSatellite(pool)

	p, err := ingest.New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []feed.Message{
		feedMsg(0, "N21", "2026-11-22", 2310, "-1.98905", "112.64071", "n"),
		feedMsg(1, "N20", "2026-11-22", 2320, "-1.98273", "112.63871", "h"),
	} {
		if err := p.Handle(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if total, pending := outboxCount(t, pool); total != 1 || pending != 1 {
		t.Fatalf("outbox total=%d pending=%d, want 1 and 1", total, pending)
	}

	rows, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Subject != "hazard.ingest.SATELIT" {
		t.Fatalf("pending = %+v err=%v", rows, err)
	}
	var msg map[string]any
	if err := json.Unmarshal(rows[0].Payload, &msg); err != nil || msg["hazard_type"] != "WILDFIRE" {
		t.Fatalf("payload = %s err=%v", rows[0].Payload, err)
	}
	if err := s.MarkPublished(ctx, rows[0].ID); err != nil {
		t.Fatal(err)
	}

	// After a restart, a detection far away must not republish the stored fire.
	p2, err := ingest.New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Handle(ctx, feedMsg(2, "N21", "2026-11-22", 2330, "-5.0", "120.0", "n")); err != nil {
		t.Fatal(err)
	}
	if total, pending := outboxCount(t, pool); total != 1 || pending != 0 {
		t.Fatalf("outbox total=%d pending=%d after restart, want 1 and 0 (fire republished)", total, pending)
	}
}
