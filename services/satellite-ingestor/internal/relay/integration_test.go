package relay

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/migrations"
)

func detection(offset int64, sat string, tm int, lat, lon string) feed.Message {
	raw, _ := json.Marshal(map[string]any{
		"satellite": sat, "acq_date": "2026-11-22", "acq_time": tm,
		"latitude": json.Number(lat), "longitude": json.Number(lon),
		"confidence": "n", "frp": 5.0,
	})
	return feed.Message{Type: "detection", Offset: offset, Detection: raw}
}

// TestRelayPublishesOnceThroughJetStream runs against a real Postgres and a
// real JetStream server. It needs SATELLITE_TEST_DATABASE_URL and
// SATELLITE_TEST_NATS_URL, and is skipped otherwise.
func TestRelayPublishesOnceThroughJetStream(t *testing.T) {
	dsn, natsURL := os.Getenv("SATELLITE_TEST_DATABASE_URL"), os.Getenv("SATELLITE_TEST_NATS_URL")
	if dsn == "" || natsURL == "" {
		t.Skip("SATELLITE_TEST_DATABASE_URL or SATELLITE_TEST_NATS_URL not set")
	}
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, migrations.Init); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE satellite_fires, satellite_detections, satellite_outbox;
		UPDATE satellite_state SET next_offset = 0, watermark = NULL, invalid = 0, low_conf = 0, late = 0, unknown = 0 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = js.DeleteStream(event.IngestStreamName) // start from an empty stream
	if err := EnsureIngestStream(js); err != nil {
		t.Fatal(err)
	}
	if err := EnsureIngestStream(js); err != nil { // second call must leave it alone
		t.Fatalf("ensure on an existing stream: %v", err)
	}

	s := store.NewSatellite(pool)
	p, err := ingest.New(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []feed.Message{
		detection(0, "N21", 2310, "-1.98905", "112.64071"),
		detection(1, "N20", 2320, "-1.98273", "112.63871"),
	} {
		if err := p.Handle(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	r := &Relay{Store: s, Publisher: NewJetStream(js)}
	if n, err := r.PublishPending(ctx); err != nil || n != 1 {
		t.Fatalf("published %d, err %v; want 1", n, err)
	}

	// Simulate a crash after the broker acknowledged but before the row was
	// marked: the relay sends it again, and JetStream must drop the duplicate.
	if _, err := pool.Exec(ctx, `UPDATE satellite_outbox SET published_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.PublishPending(ctx); err != nil {
		t.Fatal(err)
	}

	info, err := js.StreamInfo(event.IngestStreamName)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream holds %d messages, want 1 (duplicate not dropped)", info.State.Msgs)
	}

	sub, err := js.SubscribeSync(event.IngestSubject("SATELIT"), nats.DeliverAll())
	if err != nil {
		t.Fatal(err)
	}
	got, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var m event.IngestMessage
	if err := json.Unmarshal(got.Data, &m); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("published message fails the contract: %v", err)
	}
	if m.HazardType != "WILDFIRE" || m.Op != event.IngestOpUpsert || got.Header.Get(nats.MsgIdHdr) != m.MessageID {
		t.Fatalf("message = %+v header id = %q", m, got.Header.Get(nats.MsgIdHdr))
	}
}
