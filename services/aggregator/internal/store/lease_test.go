package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func resetLeaseState(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_, err := testPool.Exec(ctx, `
		UPDATE leader_leases
		   SET holder = '', fencing_token = 0, expires_at = '1970-01-01'
		 WHERE name = $1
	`, DefaultLeaseName)
	if err != nil {
		t.Fatalf("failed to reset leader_leases: %v", err)
	}
	if _, err := testPool.Exec(ctx, `TRUNCATE TABLE poller_watermarks`); err != nil {
		t.Fatalf("failed to truncate poller_watermarks: %v", err)
	}
}

func TestLease_AcquireRenewTakeoverRelease(t *testing.T) {
	resetLeaseState(t)
	ctx := context.Background()

	// 1. First acquire by agg-1 gets token 1
	tok1, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-1", 10*time.Second)
	if err != nil || !ok || tok1 != 1 {
		t.Fatalf("expected agg-1 to acquire token 1, got tok=%d ok=%v err=%v", tok1, ok, err)
	}

	// 2. agg-2 cannot acquire while lease is active
	tok2, ok2, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-2", 10*time.Second)
	if err != nil || ok2 || tok2 != 0 {
		t.Fatalf("expected agg-2 to fail acquire, got tok=%d ok=%v err=%v", tok2, ok2, err)
	}

	// 3. agg-1 renews -> preserves token 1
	tok1Renew, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-1", 10*time.Second)
	if err != nil || !ok || tok1Renew != 1 {
		t.Fatalf("expected agg-1 renew to keep token 1, got tok=%d ok=%v err=%v", tok1Renew, ok, err)
	}

	// 4. GetLease reports agg-1 as active holder
	info, err := testRepo.GetLease(ctx, DefaultLeaseName)
	if err != nil {
		t.Fatalf("GetLease failed: %v", err)
	}
	if info.Holder != "agg-1" || info.FencingToken != 1 || !info.IsActive {
		t.Fatalf("unexpected GetLease info: %+v", info)
	}

	// 5. ReleaseLease expires immediately so agg-2 can take over with token 2
	if err := testRepo.ReleaseLease(ctx, DefaultLeaseName, "agg-1"); err != nil {
		t.Fatalf("ReleaseLease failed: %v", err)
	}
	tok2Takeover, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-2", 10*time.Second)
	if err != nil || !ok || tok2Takeover != 2 {
		t.Fatalf("expected agg-2 takeover to get token 2, got tok=%d ok=%v err=%v", tok2Takeover, ok, err)
	}
}

func TestUpsertFencedBatch_ValidAndStaleToken(t *testing.T) {
	resetLeaseState(t)
	ctx := context.Background()

	if _, err := testPool.Exec(ctx, `TRUNCATE TABLE canonical_events, outbox`); err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}

	tok1, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-1", 15*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: ok=%v err=%v", ok, err)
	}

	wm1 := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	rec1 := UpsertRequest{
		HazardID:      hazard.ID(hazard.SourceBMKG, "eq-fenced-1"),
		Source:        string(hazard.SourceBMKG),
		SourceRefID:   "eq-fenced-1",
		HazardType:    hazard.HazardTypeSeismic,
		Severity:      hazard.SeverityWaspada,
		AreaName:      "Bali",
		Latitude:      -8.5,
		Longitude:     115.2,
		OccurredAt:    wm1,
		CorrelationID: "corr-1",
	}

	// 1. Valid fenced batch succeeds and records written_by, fencing_token, outbox, watermark
	err = testRepo.UpsertFencedBatch(ctx, Fence{
		LeaseName: DefaultLeaseName,
		Holder:    "agg-1",
		Token:     tok1,
	}, string(hazard.SourceBMKG), wm1, []UpsertRequest{rec1})
	if err != nil {
		t.Fatalf("UpsertFencedBatch with valid token failed: %v", err)
	}

	var writtenBy string
	var fencingTok int64
	err = testPool.QueryRow(ctx, `
		SELECT written_by, fencing_token FROM canonical_events WHERE hazard_id = $1
	`, rec1.HazardID).Scan(&writtenBy, &fencingTok)
	if err != nil {
		t.Fatalf("query canonical_events failed: %v", err)
	}
	if writtenBy != "agg-1" || fencingTok != tok1 {
		t.Errorf("expected written_by=agg-1 fencing_token=%d, got %q %d", tok1, writtenBy, fencingTok)
	}

	gotWM, err := testRepo.GetPollerWatermark(ctx, string(hazard.SourceBMKG))
	if err != nil {
		t.Fatalf("GetPollerWatermark failed: %v", err)
	}
	if !gotWM.Equal(wm1) {
		t.Errorf("expected watermark %v, got %v", wm1, gotWM)
	}

	// 2. Takeover by agg-2 invalidates agg-1's token
	_ = testRepo.ReleaseLease(ctx, DefaultLeaseName, "agg-1")
	tok2, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-2", 15*time.Second)
	if err != nil || !ok || tok2 <= tok1 {
		t.Fatalf("agg-2 takeover failed: tok=%d ok=%v err=%v", tok2, ok, err)
	}

	wm2 := wm1.Add(1 * time.Minute)
	rec2 := UpsertRequest{
		HazardID:      hazard.ID(hazard.SourceBMKG, "eq-fenced-stale"),
		Source:        string(hazard.SourceBMKG),
		SourceRefID:   "eq-fenced-stale",
		HazardType:    hazard.HazardTypeSeismic,
		Severity:      hazard.SeveritySiaga,
		AreaName:      "Lombok",
		Latitude:      -8.6,
		Longitude:     116.1,
		OccurredAt:    wm2,
		CorrelationID: "corr-stale",
	}

	// Stale write by agg-1 must return ErrFenced and write nothing
	err = testRepo.UpsertFencedBatch(ctx, Fence{
		LeaseName: DefaultLeaseName,
		Holder:    "agg-1",
		Token:     tok1,
	}, string(hazard.SourceBMKG), wm2, []UpsertRequest{rec2})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced for stale token, got %v", err)
	}

	var count int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM canonical_events WHERE hazard_id = $1`, rec2.HazardID).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows written by fenced zombie, got %d", count)
	}

	// Watermark must still be wm1
	gotWM, err = testRepo.GetPollerWatermark(ctx, string(hazard.SourceBMKG))
	if err != nil {
		t.Fatalf("GetPollerWatermark failed: %v", err)
	}
	if !gotWM.Equal(wm1) {
		t.Errorf("expected watermark unchanged at %v, got %v", wm1, gotWM)
	}
}

func TestUpsertFencedBatch_ChunkedMultiRowAndIdempotency(t *testing.T) {
	resetLeaseState(t)
	ctx := context.Background()

	if _, err := testPool.Exec(ctx, `TRUNCATE TABLE canonical_events, outbox`); err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}

	tok, ok, err := testRepo.AcquireOrRenewLease(ctx, DefaultLeaseName, "agg-chunk", 15*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: ok=%v err=%v", ok, err)
	}
	fence := Fence{LeaseName: DefaultLeaseName, Holder: "agg-chunk", Token: tok}

	base := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	// Build 120 events (> 2 chunks of 50) plus a duplicate HazardID with updated severity in the same batch.
	recs := make([]UpsertRequest, 0, 121)
	for i := 0; i < 120; i++ {
		ref := "eq-chunk-" + time.Duration(i).String()
		recs = append(recs, UpsertRequest{
			HazardID:    hazard.ID(hazard.SourceBMKG, ref),
			Source:      string(hazard.SourceBMKG),
			SourceRefID: ref,
			HazardType:  hazard.HazardTypeSeismic,
			Severity:    hazard.SeverityWaspada,
			AreaName:    "Jawa Barat",
			Latitude:    -6.9,
			Longitude:   107.6,
			OccurredAt:  base.Add(time.Duration(i) * time.Second),
		})
	}
	updated0 := recs[0]
	updated0.Severity = hazard.SeverityAwas
	recs = append(recs, updated0)

	wm := base.Add(120 * time.Second)
	if err := testRepo.UpsertFencedBatch(ctx, fence, string(hazard.SourceBMKG), wm, recs); err != nil {
		t.Fatalf("UpsertFencedBatch (pass 1) failed: %v", err)
	}

	var totalEvents, totalOutbox int
	var ver0 int64
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM canonical_events`).Scan(&totalEvents)
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&totalOutbox)
	_ = testPool.QueryRow(ctx, `SELECT version FROM canonical_events WHERE shard = $1 AND hazard_id = $2`, ShardForHazardID(recs[0].HazardID), recs[0].HazardID).Scan(&ver0)

	if totalEvents != 120 {
		t.Fatalf("expected 120 unique events, got %d", totalEvents)
	}
	if ver0 != 2 {
		t.Fatalf("expected duplicated-then-updated event version 2, got %d", ver0)
	}
	if totalOutbox != 121 {
		t.Fatalf("expected 121 outbox rows (120 inserts + 1 update), got %d", totalOutbox)
	}

	// Re-running the unchanged 120-row batch (with updated0's current state) must be a complete no-op on version & outbox.
	recs[0] = updated0
	if err := testRepo.UpsertFencedBatch(ctx, fence, string(hazard.SourceBMKG), wm, recs[:120]); err != nil {
		t.Fatalf("UpsertFencedBatch (re-poll) failed: %v", err)
	}
	var outboxAfter int
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outboxAfter)
	if outboxAfter != totalOutbox {
		t.Fatalf("expected unchanged re-poll to add 0 outbox rows, got %d -> %d", totalOutbox, outboxAfter)
	}
}
