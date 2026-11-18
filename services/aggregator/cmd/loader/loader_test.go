package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeBatchInserter struct {
	mu          sync.Mutex
	seen        map[string]store.UpsertRequest
	withOutbox  []bool
	outboxCount int64
}

func newFakeBatchInserter() *fakeBatchInserter {
	return &fakeBatchInserter{
		seen: make(map[string]store.UpsertRequest),
	}
}

func (f *fakeBatchInserter) InsertBatch(_ context.Context, batch []store.UpsertRequest, withOutbox bool) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.withOutbox = append(f.withOutbox, withOutbox)
	var ins, skip int64
	for _, rec := range batch {
		if _, exists := f.seen[rec.HazardID]; exists {
			skip++
			continue
		}
		f.seen[rec.HazardID] = rec
		ins++
		if withOutbox {
			f.outboxCount++
		}
	}
	return ins, skip, nil
}

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}

func TestLoaderCSV_USGS_And_Idempotency(t *testing.T) {
	csvData := `time,latitude,longitude,depth,mag,place,id,magType
2024-05-01T10:15:30Z,-1.42,120.15,10.5,6.2,Sulawesi Tengah,us7000abc1,mww
2024-05-02T11:00:00Z,-7.80,110.36,15.0,4.5,Yogyakarta,us7000abc2,mb
`
	path := writeTempFile(t, "usgs.csv", csvData)
	inserter := newFakeBatchInserter()
	cfg := loaderConfig{
		File:        path,
		Concurrency: 2,
		BatchSize:   1,
	}

	sum1, err := runLoader(context.Background(), cfg, inserter, zerolog.Nop())
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if sum1.Read != 2 || sum1.Inserted != 2 || sum1.Skipped != 0 || sum1.Invalid != 0 {
		t.Fatalf("unexpected summary 1: %+v", sum1)
	}

	ev1 := inserter.seen[hazard.ID(hazard.SourceBMKG, "us7000abc1")]
	if ev1.Source != "BMKG" || ev1.HazardType != hazard.HazardTypeSeismic {
		t.Fatalf("unexpected source/type: %+v", ev1)
	}
	if ev1.Severity != hazard.SeverityWaspada {
		t.Fatalf("expected WASPADA for mag 6.2, got %s", ev1.Severity)
	}
	if ev1.AreaName != "Sulawesi Tengah" {
		t.Fatalf("expected area Sulawesi Tengah, got %q", ev1.AreaName)
	}
	if ev1.Attributes["dataset"] != "historical_catalog" || ev1.Attributes["magType"] != "mww" {
		t.Fatalf("unexpected attributes: %+v", ev1.Attributes)
	}
	if inserter.outboxCount != 0 {
		t.Fatalf("expected 0 outbox rows by default, got %d", inserter.outboxCount)
	}

	// Re-run must insert 0 new rows.
	sum2, err := runLoader(context.Background(), cfg, inserter, zerolog.Nop())
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if sum2.Inserted != 0 || sum2.Skipped != 2 {
		t.Fatalf("expected 0 inserted / 2 skipped on re-run, got %+v", sum2)
	}
}

func TestLoaderCSV_BMKG_FallbackID_And_BadRows(t *testing.T) {
	csvData := `tgl,ot,lat,lon,depth,mag,remark,strike
2024-06-10,08:30:00,-0.90,119.87,12,6.8,Palu Sulawesi Tengah,145
2024-06-11,09:00:00,99.5,119.87,10,5.0,Bad Lat,
bad-date,09:00:00,-1.0,119.87,10,5.0,Bad Date,
2024-06-12,10:00:00,-2.0,120.0,10,not-a-num,Bad Mag,
2024-06-13,11:00:00
`
	path := writeTempFile(t, "bmkg.csv", csvData)
	inserter := newFakeBatchInserter()
	cfg := loaderConfig{
		File:        path,
		Concurrency: 2,
		BatchSize:   10,
		WithOutbox:  true,
	}

	sum, err := runLoader(context.Background(), cfg, inserter, zerolog.Nop())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if sum.Read != 5 || sum.Inserted != 1 || sum.Invalid != 4 {
		t.Fatalf("unexpected summary: %+v", sum)
	}
	if inserter.outboxCount != 1 {
		t.Fatalf("expected 1 outbox row when --with-outbox=true, got %d", inserter.outboxCount)
	}

	var only store.UpsertRequest
	for _, v := range inserter.seen {
		only = v
	}
	if !strings.HasPrefix(only.SourceRefID, "hist-") || len(only.SourceRefID) != len("hist-")+16 {
		t.Fatalf("expected stable hist-<16hex> fallback id, got %q", only.SourceRefID)
	}
	if only.Severity != hazard.SeveritySiaga {
		t.Fatalf("expected SIAGA for mag 6.8, got %s", only.Severity)
	}
	if only.Attributes["strike"] != "145" {
		t.Fatalf("expected unmapped column strike=145, got %+v", only.Attributes)
	}

	// Verify fallback ID is deterministic on re-run.
	sum2, err := runLoader(context.Background(), cfg, inserter, zerolog.Nop())
	if err != nil {
		t.Fatalf("re-run failed: %v", err)
	}
	if sum2.Inserted != 0 || sum2.Skipped != 1 {
		t.Fatalf("expected deterministic fallback ID to skip on re-run, got %+v", sum2)
	}
}

func TestLoaderJSON_And_NDJSON_CustomColumns(t *testing.T) {
	jsonData := `[
		{"ev_id":"j1","ts":"15/07/2024 12:00:00","y":-1.2,"x":120.5,"d":8,"m":4.2,"wil":""},
		{"ev_id":"j2","ts":"16/07/2024 13:00:00","y":-1.3,"x":120.6,"d":15,"m":5.5,"wil":"Poso"}
	]`
	jsonPath := writeTempFile(t, "events.json", jsonData)
	inserter := newFakeBatchInserter()

	sum, err := runLoader(context.Background(), loaderConfig{
		File:        jsonPath,
		Concurrency: 2,
		BatchSize:   10,
		Cols: ColumnMap{
			ID:         "ev_id",
			Time:       "ts",
			Lat:        "y",
			Lon:        "x",
			Depth:      "d",
			Mag:        "m",
			Region:     "wil",
			TimeLayout: "02/01/2006 15:04:05",
		},
	}, inserter, zerolog.Nop())
	if err != nil {
		t.Fatalf("json run failed: %v", err)
	}
	if sum.Inserted != 2 || sum.Invalid != 0 {
		t.Fatalf("unexpected json summary: %+v", sum)
	}
	ev1 := inserter.seen[hazard.ID(hazard.SourceBMKG, "j1")]
	if ev1.AreaName != "Unknown" {
		t.Fatalf("expected empty region to default to Unknown, got %q", ev1.AreaName)
	}

	ndjsonData := `{"id":"n1","time":"2024-08-01T00:00:00Z","latitude":-0.5,"longitude":121.0,"depth":10,"mag":5.1,"place":"Gorontalo"}
{"id":"n2","time":"2024-08-01T01:00:00Z","latitude":200,"longitude":121.0,"depth":10,"mag":5.1,"place":"Invalid"}
`
	ndjsonPath := writeTempFile(t, "events.ndjson", ndjsonData)
	sumND, err := runLoader(context.Background(), loaderConfig{
		File:        ndjsonPath,
		Concurrency: 1,
		BatchSize:   10,
		DryRun:      true,
	}, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("ndjson dry-run failed: %v", err)
	}
	if sumND.Read != 2 || sumND.Inserted != 1 || sumND.Invalid != 1 {
		t.Fatalf("unexpected ndjson dry-run summary: %+v", sumND)
	}
}

func TestBuildInsertSQL(t *testing.T) {
	sqlNoRet := buildInsertSQL(2, false)
	if !strings.Contains(sqlNoRet, "ON CONFLICT (shard, hazard_id) DO NOTHING") {
		t.Fatalf("missing conflict clause: %s", sqlNoRet)
	}
	if strings.Contains(sqlNoRet, "RETURNING") {
		t.Fatalf("unexpected RETURNING in no-outbox SQL: %s", sqlNoRet)
	}
	sqlRet := buildInsertSQL(2, true)
	if !strings.Contains(sqlRet, "RETURNING hazard_id, version, ingested_at") {
		t.Fatalf("missing RETURNING in outbox SQL: %s", sqlRet)
	}
}
