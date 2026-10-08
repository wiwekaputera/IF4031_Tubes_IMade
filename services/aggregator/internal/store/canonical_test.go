package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

var (
	testPool *pgxpool.Pool
	testRepo *PostgresRepo
)

func findInitSQLPath() (string, error) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get test file path")
	}
	repoRoot := filepath.Dir(testFile)
	for i := 0; i < 4; i++ {
		repoRoot = filepath.Dir(repoRoot)
	}
	path := filepath.Join(repoRoot, "services", "canonical-store", "migrations", "init.sql")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("init.sql not found at %s: %w", path, err)
	}
	return path, nil
}

func waitForPostgres(dsn string, timeout time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			conn.Close(ctx)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for postgres at %s", dsn)
}

func TestMain(m *testing.M) {
	if _, err := exec.LookPath("docker"); err != nil {
		fmt.Fprintf(os.Stderr, "docker not found: %v\n", err)
		os.Exit(1)
	}
	initSQLPath, err := findInitSQLPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to find init.sql: %v\n", err)
		os.Exit(1)
	}

	exec.Command("docker", "rm", "-f", "if4031-canonical-test").Run()
	cmd := exec.Command("docker", "run", "-d", "--name", "if4031-canonical-test",
		"-e", "POSTGRES_PASSWORD=pg", "-e", "POSTGRES_USER=aggregator", "-e", "POSTGRES_DB=canonical",
		"-p", "5432:5432", "-v", initSQLPath+":/docker-entrypoint-initdb.d/init.sql:ro",
		"postgres:16-alpine",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n%s\n", err, out)
		os.Exit(1)
	}
	containerID := strings.TrimSpace(string(out))
	dsn := "postgresql://aggregator:pg@localhost:5432/canonical?sslmode=disable"

	if err := waitForPostgres(dsn, 120*time.Second); err != nil {
		exec.Command("docker", "rm", "-f", containerID).Run()
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		exec.Command("docker", "rm", "-f", containerID).Run()
		os.Exit(1)
	}
	testPool = pool
	testRepo = NewPostgresRepo(pool, zerolog.Nop())

	code := m.Run()
	pool.Close()
	exec.Command("docker", "rm", "-f", containerID).Run()
	os.Exit(code)
}

func truncateTables(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := testPool.Exec(ctx, "TRUNCATE TABLE canonical_events, outbox"); err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}
}

func TestUpsertEvent_PreservesHazardID(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hazardID1 := uuid.NewString()
	sourceRefID := "bmkg-test-" + uuid.NewString()
	req := UpsertRequest{
		HazardID:    hazardID1,
		Source:      "BMKG",
		SourceRefID: sourceRefID,
		HazardType:  hazard.HazardTypeSeismic,
		Severity:    hazard.SeverityNormal,
		AreaName:    "Jakarta",
		OccurredAt:  time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
		Sources:     map[string]interface{}{"bmkg": map[string]interface{}{"event_id": sourceRefID}},
		Attributes:  map[string]interface{}{"confidence_level": "high"},
	}

	if id, err := testRepo.UpsertEvent(ctx, req); err != nil || id != hazardID1 {
		t.Fatalf("first upsert failed: id=%s, err=%v", id, err)
	}

	// Second upsert with modified attributes and new UUID should preserve original HazardID
	req2 := req
	req2.HazardID = uuid.NewString()
	req2.Sources = map[string]interface{}{"pvmbg": map[string]interface{}{"val": 2}}
	req2.Attributes = map[string]interface{}{"confidence_level": "medium", "extra": true}

	if id, err := testRepo.UpsertEvent(ctx, req2); err != nil || id != hazardID1 {
		t.Fatalf("expected preserved hazard_id %s, got %s (err: %v)", hazardID1, id, err)
	}

	var storedID, outboxKey string
	var version int64
	var sourcesJSON, attrsJSON []byte
	if err := testPool.QueryRow(ctx, "SELECT hazard_id, version, sources, attributes FROM canonical_events WHERE source = $1 AND source_ref_id = $2", "BMKG", sourceRefID).Scan(&storedID, &version, &sourcesJSON, &attrsJSON); err != nil || storedID != hazardID1 || version != 2 {
		t.Fatalf("canonical row mismatch: id=%s, ver=%d, err=%v", storedID, version, err)
	}
	if err := testPool.QueryRow(ctx, "SELECT event_key FROM outbox WHERE event_key = $1", hazardID1+":2").Scan(&outboxKey); err != nil {
		t.Fatalf("outbox row missing for version 2: %v", err)
	}

	var sources, attrs map[string]interface{}
	_ = json.Unmarshal(sourcesJSON, &sources)
	_ = json.Unmarshal(attrsJSON, &attrs)
	if _, ok := sources["bmkg"]; !ok || sources["pvmbg"] == nil || attrs["confidence_level"] != "medium" || attrs["extra"] != true {
		t.Errorf("merging failed: sources=%+v, attrs=%+v", sources, attrs)
	}
}

func TestUpsertEvent_GuardsVersionAndOutboxOnUnchangedRePoll(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hazardID := uuid.NewString()
	req := UpsertRequest{
		HazardID:    hazardID,
		Source:      "BMKG",
		SourceRefID: "evt-idem-001",
		HazardType:  hazard.HazardTypeSeismic,
		Severity:    hazard.SeveritySiaga,
		AreaName:    "Sulawesi",
		OccurredAt:  time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
		Sources:     map[string]interface{}{"bmkg": map[string]interface{}{"event_id": "evt-idem-001"}},
		Attributes:  map[string]interface{}{"magnitude": 6.7},
	}

	if id1, err := testRepo.UpsertEvent(ctx, req); err != nil || id1 != hazardID {
		t.Fatalf("initial upsert failed: %v", err)
	}
	var outboxCount int
	if _ = testPool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE event_key LIKE $1", hazardID+"%").Scan(&outboxCount); outboxCount != 1 {
		t.Fatalf("expected 1 outbox row initially, got %d", outboxCount)
	}

	// Re-poll the exact same unchanged record with a new transient HazardID
	reqRePoll := req
	reqRePoll.HazardID = uuid.NewString()
	if id2, err := testRepo.UpsertEvent(ctx, reqRePoll); err != nil || id2 != hazardID {
		t.Fatalf("re-poll failed: id=%s, err=%v", id2, err)
	}
	var version int64
	if _ = testPool.QueryRow(ctx, "SELECT version FROM canonical_events WHERE hazard_id = $1", hazardID).Scan(&version); version != 1 {
		t.Fatalf("expected version to remain 1 on unchanged re-poll, got %d", version)
	}
	if _ = testPool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE event_key LIKE $1", hazardID+"%").Scan(&outboxCount); outboxCount != 1 {
		t.Fatalf("expected still 1 outbox row on unchanged re-poll, got %d", outboxCount)
	}
}

func TestGetEvents_Filters(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts1, ts2, ts3 := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC), time.Date(2025, 6, 2, 10, 0, 0, 0, time.UTC), time.Date(2025, 6, 3, 10, 0, 0, 0, time.UTC)
	idA, idB, idC := uuid.NewString(), uuid.NewString(), uuid.NewString()

	events := []UpsertRequest{
		{HazardID: idA, Source: "BMKG", SourceRefID: "s-a-" + idA, HazardType: hazard.HazardTypeSeismic, Severity: hazard.SeverityNormal, AreaName: "AreaA", OccurredAt: ts1},
		{HazardID: idB, Source: "PVMBG", SourceRefID: "s-b-" + idB, HazardType: hazard.HazardTypeVolcanic, Severity: hazard.SeverityWaspada, AreaName: "AreaB", OccurredAt: ts2},
		{HazardID: idC, Source: "BMKG", SourceRefID: "s-c-" + idC, HazardType: hazard.HazardTypeSeismic, Severity: hazard.SeveritySiaga, AreaName: "AreaC", OccurredAt: ts3},
	}
	for _, e := range events {
		if _, err := testRepo.UpsertEvent(ctx, e); err != nil {
			t.Fatalf("seed upsert failed: %v", err)
		}
	}

	// 1. Hazard type filter
	if res, err := testRepo.GetEvents(ctx, EventFilter{HazardType: "SEISMIC"}); err != nil || len(res) != 2 {
		t.Fatalf("expected 2 SEISMIC events, got %d (err: %v)", len(res), err)
	}
	// 2. Hazard ID filter
	if res, err := testRepo.GetEvents(ctx, EventFilter{HazardID: idB}); err != nil || len(res) != 1 || res[0].HazardID != idB {
		t.Fatalf("expected 1 event matching idB, got %+v", res)
	}
	// 3. Since filter (ts2 - 1h includes ts2, ts3)
	since := ts2.Add(-time.Hour)
	if res, err := testRepo.GetEvents(ctx, EventFilter{Since: &since}); err != nil || len(res) != 2 {
		t.Fatalf("expected 2 events since ts2, got %d (err: %v)", len(res), err)
	}
	// 4. Limit & DESC order (idC newest, then idB)
	if res, err := testRepo.GetEvents(ctx, EventFilter{Limit: 2}); err != nil || len(res) != 2 || res[0].HazardID != idC || res[1].HazardID != idB {
		t.Fatalf("expected limit 2 ordered DESC (idC, idB), got %+v", res)
	}
}

func TestGetVolcanoReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	refs, err := testRepo.GetVolcanoReferences(ctx)
	if err != nil || len(refs) != 5 {
		t.Fatalf("expected 5 volcano references, got %d (err: %v)", len(refs), err)
	}
	for _, id := range []string{"GNG-001", "GNG-002", "GNG-003", "GNG-004", "GNG-005"} {
		ref, ok := refs[id]
		if !ok || ref.Name == "" || (ref.Latitude == 0 && ref.Longitude == 0) {
			t.Errorf("invalid volcano ref for %s: %+v", id, ref)
		}
	}
}

func TestOutboxPendingRowsAreOnlyClearedAfterMarkPublished(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hazardID := uuid.NewString()
	if _, err := testRepo.UpsertEvent(ctx, UpsertRequest{
		HazardID:    hazardID,
		Source:      "BMKG",
		SourceRefID: "outbox-test-" + hazardID,
		HazardType:  hazard.HazardTypeSeismic,
		Severity:    hazard.SeverityNormal,
		AreaName:    "Jakarta",
		OccurredAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("UpsertEvent() error = %v", err)
	}

	rows, err := testRepo.GetUnpublishedOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetUnpublishedOutbox() error = %v", err)
	}
	if len(rows) != 1 || rows[0].EventKey != hazardID+":1" || len(rows[0].Payload) == 0 {
		t.Fatalf("pending rows = %+v, want one event with key %q", rows, hazardID+":1")
	}

	if err := testRepo.MarkOutboxPublished(ctx, rows[0].ID); err != nil {
		t.Fatalf("MarkOutboxPublished() error = %v", err)
	}
	rows, err = testRepo.GetUnpublishedOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("GetUnpublishedOutbox() after mark error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("pending rows after marking = %d, want 0", len(rows))
	}
}

func TestGetLatestEventTime(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. When empty, should return zero time and no error
	latest, err := testRepo.GetLatestEventTime(ctx, "PVMBG")
	if err != nil {
		t.Fatalf("expected no error for empty table, got %v", err)
	}
	if !latest.IsZero() {
		t.Fatalf("expected zero time for empty table, got %v", latest)
	}

	// 2. Insert records with different occurred_at timestamps
	t1 := time.Date(2026, 10, 8, 2, 10, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 8, 2, 20, 0, 0, time.UTC)
	t3 := time.Date(2026, 10, 8, 2, 15, 0, 0, time.UTC)

	events := []UpsertRequest{
		{HazardID: uuid.NewString(), Source: "PVMBG", SourceRefID: "pvmbg-1", HazardType: hazard.HazardTypeVolcanic, Severity: hazard.SeverityNormal, AreaName: "Area1", OccurredAt: t1},
		{HazardID: uuid.NewString(), Source: "PVMBG", SourceRefID: "pvmbg-2", HazardType: hazard.HazardTypeVolcanic, Severity: hazard.SeveritySiaga, AreaName: "Area2", OccurredAt: t2},
		{HazardID: uuid.NewString(), Source: "PVMBG", SourceRefID: "pvmbg-3", HazardType: hazard.HazardTypeVolcanic, Severity: hazard.SeverityWaspada, AreaName: "Area3", OccurredAt: t3},
		{HazardID: uuid.NewString(), Source: "BMKG", SourceRefID: "bmkg-1", HazardType: hazard.HazardTypeSeismic, Severity: hazard.SeverityNormal, AreaName: "Area4", OccurredAt: t1},
	}
	for _, e := range events {
		if _, err := testRepo.UpsertEvent(ctx, e); err != nil {
			t.Fatalf("failed to insert event: %v", err)
		}
	}

	// 3. Query PVMBG max time
	latestPVMBG, err := testRepo.GetLatestEventTime(ctx, "PVMBG")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !latestPVMBG.Equal(t2) {
		t.Errorf("expected latest PVMBG time %v, got %v", t2, latestPVMBG)
	}

	// 4. Query BMKG max time
	latestBMKG, err := testRepo.GetLatestEventTime(ctx, "BMKG")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !latestBMKG.Equal(t1) {
		t.Errorf("expected latest BMKG time %v, got %v", t1, latestBMKG)
	}
}

