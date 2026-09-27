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
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			conn.Close(ctx)
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for postgres at %s: %w", dsn, lastErr)
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
		"-e", "POSTGRES_PASSWORD=pg",
		"-e", "POSTGRES_USER=aggregator",
		"-e", "POSTGRES_DB=canonical",
		"-p", "5432:5432",
		"-v", initSQLPath+":/docker-entrypoint-initdb.d/init.sql:ro",
		"postgres:16-alpine",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n%s\n", err, output)
		os.Exit(1)
	}
	containerID := strings.TrimSpace(string(output))

	dsn := "postgresql://aggregator:pg@localhost:5432/canonical?sslmode=disable"

	if err := waitForPostgres(dsn, 120*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to postgres: %v\n", err)
		exec.Command("docker", "rm", "-f", containerID).Run()
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create pool: %v\n", err)
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
	source := "BMKG"
	sourceRefID := "bmkg-test-" + uuid.NewString()

	req := UpsertRequest{
		HazardID:      hazardID1,
		Source:        source,
		SourceRefID:   sourceRefID,
		HazardType:    hazard.HazardTypeSeismic,
		Severity:      hazard.SeverityNormal,
		AreaName:      "Jakarta",
		Latitude:      -6.2,
		Longitude:     106.8,
		OccurredAt:    time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
		Sources:       map[string]interface{}{"bmkg": map[string]interface{}{"event_id": sourceRefID, "value": 1}},
		Attributes:    map[string]interface{}{"confidence_level": "high"},
		CorrelationID: "test-correlation-1",
	}

	returnedID1, err := testRepo.UpsertEvent(ctx, req)
	if err != nil {
		t.Fatalf("first UpsertEvent failed: %v", err)
	}
	if returnedID1 != hazardID1 {
		t.Fatalf("expected hazard_id %s, got %s", hazardID1, returnedID1)
	}

	hazardID2 := uuid.NewString()
	req2 := req
	req2.HazardID = hazardID2
	req2.Sources = map[string]interface{}{"pvmbg": map[string]interface{}{"report_id": "pvmbg-001", "value": 2}}
	req2.Attributes = map[string]interface{}{"confidence_level": "medium", "extra_attr": true}

	returnedID2, err := testRepo.UpsertEvent(ctx, req2)
	if err != nil {
		t.Fatalf("second UpsertEvent failed: %v", err)
	}

	if returnedID2 != hazardID1 {
		t.Fatalf("hazard_id not preserved: expected %s, got %s", hazardID1, returnedID2)
	}

	var storedHazardID string
	var version int64
	err = testPool.QueryRow(ctx,
		"SELECT hazard_id, version FROM canonical_events WHERE source = $1 AND source_ref_id = $2",
		source, sourceRefID,
	).Scan(&storedHazardID, &version)
	if err != nil {
		t.Fatalf("failed to SELECT hazard_id, version: %v", err)
	}
	if storedHazardID != hazardID1 {
		t.Errorf("SELECT hazard_id: expected %s, got %s", hazardID1, storedHazardID)
	}
	if version != 2 {
		t.Errorf("SELECT version: expected 2, got %d", version)
	}

	expectedEventKey := hazardID1 + ":2"
	var eventKey string
	err = testPool.QueryRow(ctx,
		"SELECT event_key FROM outbox WHERE event_key = $1", expectedEventKey,
	).Scan(&eventKey)
	if err != nil {
		t.Fatalf("outbox row with event_key %s not found: %v", expectedEventKey, err)
	}
	if eventKey != expectedEventKey {
		t.Errorf("outbox event_key: expected %s, got %s", expectedEventKey, eventKey)
	}

	var sourcesJSON []byte
	err = testPool.QueryRow(ctx,
		"SELECT sources FROM canonical_events WHERE hazard_id = $1", hazardID1,
	).Scan(&sourcesJSON)
	if err != nil {
		t.Fatalf("failed to SELECT sources: %v", err)
	}
	var sources map[string]interface{}
	if err := json.Unmarshal(sourcesJSON, &sources); err != nil {
		t.Fatalf("failed to unmarshal sources: %v", err)
	}
	if _, ok := sources["bmkg"]; !ok {
		t.Error("sources merge failed: 'bmkg' key missing")
	}
	if _, ok := sources["pvmbg"]; !ok {
		t.Error("sources merge failed: 'pvmbg' key missing")
	}

	var attrsJSON []byte
	err = testPool.QueryRow(ctx,
		"SELECT attributes FROM canonical_events WHERE hazard_id = $1", hazardID1,
	).Scan(&attrsJSON)
	if err != nil {
		t.Fatalf("failed to SELECT attributes: %v", err)
	}
	var attrs map[string]interface{}
	if err := json.Unmarshal(attrsJSON, &attrs); err != nil {
		t.Fatalf("failed to unmarshal attributes: %v", err)
	}
	if attrs["confidence_level"] != "medium" {
		t.Errorf("attributes merge: expected confidence_level=medium, got %v", attrs["confidence_level"])
	}
	if _, ok := attrs["extra_attr"]; !ok {
		t.Error("attributes merge failed: 'extra_attr' key missing")
	}
}

func TestGetEvents_Filters(t *testing.T) {
	truncateTables(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts1 := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 2, 10, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 6, 3, 10, 0, 0, 0, time.UTC)

	hazardIDA := uuid.NewString()
	hazardIDB := uuid.NewString()
	hazardIDC := uuid.NewString()

	events := []UpsertRequest{
		{
			HazardID:    hazardIDA,
			Source:      "BMKG",
			SourceRefID: "seismic-a-" + uuid.NewString(),
			HazardType:  hazard.HazardTypeSeismic,
			Severity:    hazard.SeverityNormal,
			AreaName:    "AreaA",
			Latitude:    -6.0,
			Longitude:   106.0,
			OccurredAt:  ts1,
			Sources:     map[string]interface{}{},
			Attributes:  map[string]interface{}{},
		},
		{
			HazardID:    hazardIDB,
			Source:      "PVMBG",
			SourceRefID: "volcanic-b-" + uuid.NewString(),
			HazardType:  hazard.HazardTypeVolcanic,
			Severity:    hazard.SeverityWaspada,
			AreaName:    "AreaB",
			Latitude:    -8.0,
			Longitude:   112.0,
			OccurredAt:  ts2,
			Sources:     map[string]interface{}{},
			Attributes:  map[string]interface{}{},
		},
		{
			HazardID:    hazardIDC,
			Source:      "BMKG",
			SourceRefID: "seismic-c-" + uuid.NewString(),
			HazardType:  hazard.HazardTypeSeismic,
			Severity:    hazard.SeveritySiaga,
			AreaName:    "AreaC",
			Latitude:    -7.0,
			Longitude:   110.0,
			OccurredAt:  ts3,
			Sources:     map[string]interface{}{},
			Attributes:  map[string]interface{}{},
		},
	}

	for _, e := range events {
		if _, err := testRepo.UpsertEvent(ctx, e); err != nil {
			t.Fatalf("failed to insert event %s: %v", e.HazardID, err)
		}
	}

	t.Run("hazard_type", func(t *testing.T) {
		result, err := testRepo.GetEvents(ctx, EventFilter{HazardType: "SEISMIC"})
		if err != nil {
			t.Fatalf("GetEvents failed: %v", err)
		}
		foundA, foundC := false, false
		for _, e := range result {
			if e.HazardID == hazardIDA {
				foundA = true
			}
			if e.HazardID == hazardIDC {
				foundC = true
			}
			if string(e.HazardType) != "SEISMIC" {
				t.Errorf("hazard_type filter: expected all SEISMIC, got %s", e.HazardType)
			}
		}
		if !foundA {
			t.Error("hazard_type filter: event A (SEISMIC) not found")
		}
		if !foundC {
			t.Error("hazard_type filter: event C (SEISMIC) not found")
		}
	})

	t.Run("hazard_id", func(t *testing.T) {
		result, err := testRepo.GetEvents(ctx, EventFilter{HazardID: hazardIDB})
		if err != nil {
			t.Fatalf("GetEvents failed: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("hazard_id filter: expected 1 event, got %d", len(result))
		}
		if result[0].HazardID != hazardIDB {
			t.Errorf("hazard_id filter: expected %s, got %s", hazardIDB, result[0].HazardID)
		}
	})

	t.Run("since", func(t *testing.T) {
		since := ts2.Add(-1 * time.Hour)
		result, err := testRepo.GetEvents(ctx, EventFilter{Since: &since})
		if err != nil {
			t.Fatalf("GetEvents failed: %v", err)
		}
		foundA, foundB, foundC := false, false, false
		for _, e := range result {
			if e.HazardID == hazardIDA {
				foundA = true
			}
			if e.HazardID == hazardIDB {
				foundB = true
			}
			if e.HazardID == hazardIDC {
				foundC = true
			}
		}
		if foundA {
			t.Error("since filter: event A (before since) should not be included")
		}
		if !foundB {
			t.Error("since filter: event B should be included")
		}
		if !foundC {
			t.Error("since filter: event C should be included")
		}
	})

	t.Run("limit", func(t *testing.T) {
		result, err := testRepo.GetEvents(ctx, EventFilter{Limit: 2})
		if err != nil {
			t.Fatalf("GetEvents failed: %v", err)
		}
		if len(result) > 2 {
			t.Errorf("limit filter: expected at most 2 events, got %d", len(result))
		}
	})

	t.Run("order_desc", func(t *testing.T) {
		result, err := testRepo.GetEvents(ctx, EventFilter{})
		if err != nil {
			t.Fatalf("GetEvents failed: %v", err)
		}
		posA, posB, posC := -1, -1, -1
		for i, e := range result {
			if e.HazardID == hazardIDA {
				posA = i
			}
			if e.HazardID == hazardIDB {
				posB = i
			}
			if e.HazardID == hazardIDC {
				posC = i
			}
		}
		if posA == -1 || posB == -1 || posC == -1 {
			t.Fatalf("order test: not all events found (A=%d, B=%d, C=%d)", posA, posB, posC)
		}
		if posC > posB {
			t.Errorf("ORDER BY occurred_at DESC: C (latest) should come before B, but C at %d, B at %d", posC, posB)
		}
		if posB > posA {
			t.Errorf("ORDER BY occurred_at DESC: B should come before A, but B at %d, A at %d", posB, posA)
		}
	})
}

func TestGetVolcanoReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	refs, err := testRepo.GetVolcanoReferences(ctx)
	if err != nil {
		t.Fatalf("GetVolcanoReferences failed: %v", err)
	}

	if len(refs) != 5 {
		t.Fatalf("expected 5 volcano references, got %d", len(refs))
	}

	expectedIDs := []string{"GNG-001", "GNG-002", "GNG-003", "GNG-004", "GNG-005"}
	for _, id := range expectedIDs {
		ref, ok := refs[id]
		if !ok {
			t.Errorf("missing volcano reference: %s", id)
			continue
		}
		if ref.VolcanoID != id {
			t.Errorf("volcano %s: VolcanoID mismatch (%s)", id, ref.VolcanoID)
		}
		if ref.Name == "" {
			t.Errorf("volcano %s: empty name", id)
		}
		if ref.Latitude == 0 && ref.Longitude == 0 {
			t.Errorf("volcano %s: zero coordinates", id)
		}
	}
}
