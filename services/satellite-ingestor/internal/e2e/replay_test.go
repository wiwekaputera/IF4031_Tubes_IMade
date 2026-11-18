package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/migrations"
)

// recording is a feed captured with curl, split into raw SSE blocks by offset.
type recording struct {
	blocks []string // blocks[i] is the event with offset i, including its blank line
}

func loadRecording(t *testing.T, path string, limit int) recording {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var rec recording
	var cur strings.Builder
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), feed.MaxLineBytes+1024)
	for sc.Scan() {
		line := sc.Text()
		cur.WriteString(line + "\n")
		if line != "" {
			continue
		}
		block := cur.String()
		cur.Reset()
		if !strings.HasPrefix(block, "id: ") {
			continue
		}
		id, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(block[4:], "\n", 2)[0]))
		if err != nil || id != len(rec.blocks) {
			t.Fatalf("recording is not contiguous at block %d (id %q)", len(rec.blocks), block[:20])
		}
		rec.blocks = append(rec.blocks, block)
		if limit > 0 && len(rec.blocks) >= limit {
			break
		}
	}
	// A final block without its blank line was cut off by curl and is ignored.
	return rec
}

// serve replays the recording from ?from= and then closes the stream, the way
// a dropped connection would end. The client reconnects from its offset.
func (rec recording) serve() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		bw := bufio.NewWriterSize(w, 256*1024)
		for i := from; i < len(rec.blocks); i++ {
			if _, err := bw.WriteString(rec.blocks[i]); err != nil {
				return
			}
		}
		bw.Flush()
	}))
}

// freshDB creates (or empties) a database next to the one in dsn.
func freshDB(t *testing.T, dsn, name string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, migrations.Init); err != nil {
		t.Fatal(err)
	}
	return pool
}

func client(url string) *feed.Client {
	c := feed.NewClient(url, "test")
	c.MinBackoff = time.Millisecond
	c.MaxBackoff = 10 * time.Millisecond
	c.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	return c
}

// runSegment processes the stream until it is complete or ctx ends, starting
// from whatever the database has committed, like a freshly started process.
func runSegment(ctx context.Context, pool *pgxpool.Pool, url string, end int64) (done bool, err error) {
	proc, err := ingest.New(ctx, store.NewSatellite(pool))
	if err != nil {
		return false, err
	}
	if proc.State().NextOffset >= end {
		return true, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = client(url).Run(ctx, proc.NextOffset, func(ctx context.Context, m feed.Message) error {
		if err := proc.Handle(ctx, m); err != nil {
			return err
		}
		if m.Offset+1 >= end {
			cancel()
		}
		return nil
	})
	if proc.State().NextOffset >= end {
		return true, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, nil
	}
	return false, err
}

// snapshot is everything the processor wrote, in a comparable form.
func snapshot(t *testing.T, pool *pgxpool.Pool) map[string][]string {
	t.Helper()
	queries := map[string]string{
		"state":      `SELECT next_offset, watermark, invalid, low_conf, late, unknown, duplicate FROM satellite_state`,
		"detections": `SELECT det_key, confidence, frp, acquired_at, retracted FROM satellite_detections ORDER BY det_key`,
		"fires": `SELECT source_ref_id, center_lat, center_lon, occurred_at, last_seen_at, detection_count,
		                  satellites, max_confidence, total_frp_mw, detection_keys, severity
		           FROM satellite_fires ORDER BY source_ref_id`,
		"outbox":       `SELECT message_id, subject, payload::text FROM satellite_outbox ORDER BY id`,
		"dead_letters": `SELECT feed_offset, event_type, reason, encode(raw_payload, 'hex') FROM satellite_dead_letters ORDER BY feed_offset`,
	}
	out := map[string][]string{}
	for name, q := range queries {
		rows, err := pool.Query(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(vals)
			out[name] = append(out[name], string(b))
		}
		rows.Close()
	}
	return out
}

// TestKillAndRestartGivesIdenticalResult replays a recorded feed twice: once
// without interruption, and once cut off at random moments (including in the
// middle of a database transaction) and restarted from the committed offset
// each time. Every table, and the exact sequence of messages sent to the
// aggregator, must come out identical.
//
// It needs SATELLITE_TEST_DATABASE_URL and SATELLITE_TEST_STREAM_FILE (a feed
// captured with curl). SATELLITE_TEST_STREAM_LIMIT caps the number of events;
// SATELLITE_TEST_EXPORT writes the uninterrupted run's fires as JSON lines.
func TestKillAndRestartGivesIdenticalResult(t *testing.T) {
	dsn, path := os.Getenv("SATELLITE_TEST_DATABASE_URL"), os.Getenv("SATELLITE_TEST_STREAM_FILE")
	if dsn == "" || path == "" {
		t.Skip("SATELLITE_TEST_DATABASE_URL or SATELLITE_TEST_STREAM_FILE not set")
	}
	limit, _ := strconv.Atoi(os.Getenv("SATELLITE_TEST_STREAM_LIMIT"))
	rec := loadRecording(t, path, limit)
	end := int64(len(rec.blocks))
	if end == 0 {
		t.Fatal("recording has no events")
	}
	srv := rec.serve()
	defer srv.Close()
	ctx := context.Background()

	// Run A: straight through.
	a := freshDB(t, dsn, "satellite_replay_a")
	startA := time.Now()
	for {
		done, err := runSegment(ctx, a, srv.URL, end)
		if err != nil {
			t.Fatalf("run A: %v", err)
		}
		if done {
			break
		}
	}
	durA := time.Since(startA)

	// Run B: killed at random moments, restarted from the database each time.
	b := freshDB(t, dsn, "satellite_replay_b")
	rng := rand.New(rand.NewSource(42))
	var kills atomic.Int64
	segment := durA / 20
	if segment < 20*time.Millisecond {
		segment = 20 * time.Millisecond
	}
	for {
		life := time.Duration(rng.Int63n(int64(segment))) + time.Millisecond
		segCtx, cancel := context.WithTimeout(ctx, life)
		done, err := runSegment(segCtx, b, srv.URL, end)
		cancel()
		if err != nil {
			t.Fatalf("run B: %v", err)
		}
		if done {
			break
		}
		kills.Add(1)
	}

	sa, sb := snapshot(t, a), snapshot(t, b)
	for _, table := range []string{"state", "detections", "fires", "outbox", "dead_letters"} {
		ra, rb := sa[table], sb[table]
		if len(ra) != len(rb) {
			t.Errorf("%s: %d rows uninterrupted, %d rows after kills", table, len(ra), len(rb))
			continue
		}
		for i := range ra {
			if ra[i] != rb[i] {
				t.Errorf("%s row %d differs:\n  uninterrupted: %s\n  after kills:   %s", table, i, ra[i], rb[i])
				break
			}
		}
	}

	// The incremental result must equal clustering every stored detection at
	// once: same groups of detections, fire by fire.
	all, err := store.NewSatellite(a).Detections(ctx, allKeys(t, a))
	if err != nil {
		t.Fatal(err)
	}
	comps, _ := fire.Cluster(all)
	want := map[string]bool{}
	for _, c := range comps {
		want[strings.Join(fire.Summarize(c).DetectionKeys, ",")] = true
	}
	got := map[string]bool{}
	rows, err := a.Query(ctx, `SELECT array_to_string(detection_keys, ',') FROM satellite_fires`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		got[k] = true
	}
	rows.Close()
	if len(got) != len(want) {
		t.Errorf("incremental run has %d fires, full recluster has %d", len(got), len(want))
	}
	for k := range want {
		if !got[k] {
			t.Errorf("full recluster has a fire the incremental run lacks: %.120s", k)
			break
		}
	}

	t.Logf("events=%d kills=%d uninterrupted=%s (%.0f events/s)", end, kills.Load(),
		durA.Round(time.Millisecond), float64(end)/durA.Seconds())
	t.Logf("state=%s", sa["state"][0])
	t.Logf("detections=%d fires=%d messages=%d dead_letters=%d",
		len(sa["detections"]), len(sa["fires"]), len(sa["outbox"]), len(sa["dead_letters"]))

	if out := os.Getenv("SATELLITE_TEST_EXPORT"); out != "" {
		if err := os.WriteFile(out, []byte(strings.Join(sa["fires"], "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("fires written to %s", out)
	}
	if kills.Load() == 0 {
		t.Fatal("run B was never interrupted; the test proved nothing")
	}
}

func allKeys(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT det_key FROM satellite_detections WHERE lower(confidence) NOT IN ('l', 'low') AND NOT retracted`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}
