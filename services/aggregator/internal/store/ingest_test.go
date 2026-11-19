package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func TestApplyIngest_UpsertFullReplaceAndVersionGuard(t *testing.T) {
	truncateTables(t)
	ctx := context.Background()

	v1Time := "2026-11-18T10:00:00Z"
	v1 := event.IngestMessage{
		MessageID:     "msg-1",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSatelit,
		SourceRefID:   "wf-001",
		SourceVersion: 1,
		HazardType:    hazard.HazardTypeWildfire,
		Severity:      hazard.SeverityWaspada,
		AreaName:      "Riau",
		Latitude:      0.5,
		Longitude:     101.4,
		OccurredAt:    v1Time,
		Attributes: map[string]any{
			"detections": 5,
			"legacy":     "keep-check",
		},
		TraceID: "trace-v1",
	}

	applied, err := testRepo.ApplyIngest(ctx, v1)
	if err != nil {
		t.Fatalf("ApplyIngest(v1) error = %v", err)
	}
	if !applied {
		t.Fatal("ApplyIngest(v1) applied = false, want true")
	}

	expectedID := hazard.ID(hazard.SourceSatelit, "wf-001")
	events, err := testRepo.GetEvents(ctx, EventFilter{HazardType: string(hazard.HazardTypeWildfire)})
	if err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].HazardID != expectedID {
		t.Fatalf("GetEvents() = %+v, want 1 event with ID %s", events, expectedID)
	}
	firstIngestedAt := events[0].IngestedAt

	// v2: full replace with fewer detections and no "legacy" attribute.
	v2Time := "2026-11-18T10:05:00Z"
	v2 := event.IngestMessage{
		MessageID:     "msg-2",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSatelit,
		SourceRefID:   "wf-001",
		SourceVersion: 2,
		HazardType:    hazard.HazardTypeWildfire,
		Severity:      hazard.SeveritySiaga,
		AreaName:      "Riau Selatan",
		Latitude:      0.4,
		Longitude:     101.5,
		OccurredAt:    v2Time,
		Attributes: map[string]any{
			"detections": 2,
		},
		TraceID: "trace-v2",
	}

	applied, err = testRepo.ApplyIngest(ctx, v2)
	if err != nil {
		t.Fatalf("ApplyIngest(v2) error = %v", err)
	}
	if !applied {
		t.Fatal("ApplyIngest(v2) applied = false, want true")
	}

	events, err = testRepo.GetEvents(ctx, EventFilter{HazardID: expectedID})
	if err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	got := events[0]
	if got.Severity != hazard.SeveritySiaga || got.AreaName != "Riau Selatan" {
		t.Fatalf("got severity=%s area=%s, want SIAGA / Riau Selatan", got.Severity, got.AreaName)
	}
	if got.IngestedAt != firstIngestedAt {
		t.Fatalf("ingested_at changed from %s to %s; want preserved", firstIngestedAt, got.IngestedAt)
	}
	if _, hasLegacy := got.Attributes["legacy"]; hasLegacy {
		t.Fatalf("attributes still contains 'legacy' (%v); want full replace", got.Attributes)
	}
	if got.Attributes["detections"] != float64(2) {
		t.Fatalf("attributes.detections = %v, want 2", got.Attributes["detections"])
	}

	// Republish v1 (stale) and v2 (duplicate): both must be ignored.
	for _, stale := range []event.IngestMessage{v1, v2} {
		applied, err = testRepo.ApplyIngest(ctx, stale)
		if err != nil {
			t.Fatalf("ApplyIngest(stale v%d) error = %v", stale.SourceVersion, err)
		}
		if applied {
			t.Fatalf("ApplyIngest(stale v%d) applied = true, want false", stale.SourceVersion)
		}
	}

	var outboxCount int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox error = %v", err)
	}
	if outboxCount != 2 {
		t.Fatalf("outbox count = %d, want 2 (v1 and v2 only)", outboxCount)
	}
}

func TestApplyIngest_SoftDeleteAndTombstone(t *testing.T) {
	truncateTables(t)
	ctx := context.Background()

	// 1. Insert v1, then soft-delete at v2.
	up1 := event.IngestMessage{
		MessageID:     "msg-up-1",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSatelit,
		SourceRefID:   "wf-del-1",
		SourceVersion: 1,
		HazardType:    hazard.HazardTypeWildfire,
		Severity:      hazard.SeverityWaspada,
		AreaName:      "Kalimantan",
		Latitude:      -1.2,
		Longitude:     113.9,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if applied, err := testRepo.ApplyIngest(ctx, up1); err != nil || !applied {
		t.Fatalf("ApplyIngest(up1) = (%v, %v), want (true, nil)", applied, err)
	}

	del2 := event.IngestMessage{
		MessageID:     "msg-del-2",
		Op:            event.IngestOpDelete,
		Source:        hazard.SourceSatelit,
		SourceRefID:   "wf-del-1",
		SourceVersion: 2,
		TraceID:       "trace-del-2",
	}
	if applied, err := testRepo.ApplyIngest(ctx, del2); err != nil || !applied {
		t.Fatalf("ApplyIngest(del2) = (%v, %v), want (true, nil)", applied, err)
	}

	expectedID := hazard.ID(hazard.SourceSatelit, "wf-del-1")
	events, err := testRepo.GetEvents(ctx, EventFilter{HazardID: expectedID})
	if err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("GetEvents() returned %d events after delete, want 0", len(events))
	}

	// Verify outbox delete envelope has attributes.deleted = true.
	var payload []byte
	if err := testPool.QueryRow(ctx, `SELECT payload FROM outbox WHERE event_key = $1`, expectedID+":2").Scan(&payload); err != nil {
		t.Fatalf("query delete outbox error = %v", err)
	}
	var env event.Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal outbox envelope error = %v", err)
	}
	if env.Data.Attributes["deleted"] != true {
		t.Fatalf("delete outbox attributes.deleted = %v, want true", env.Data.Attributes["deleted"])
	}

	// Late older upsert v1 must not resurrect deleted row.
	if applied, err := testRepo.ApplyIngest(ctx, up1); err != nil || applied {
		t.Fatalf("ApplyIngest(late up1) = (%v, %v), want (false, nil)", applied, err)
	}

	// 2. Delete arriving before any upsert inserts a tombstone row.
	tombDel := event.IngestMessage{
		MessageID:     "msg-tomb-5",
		Op:            event.IngestOpDelete,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   "sp-tomb-1",
		SourceVersion: 5,
	}
	if applied, err := testRepo.ApplyIngest(ctx, tombDel); err != nil || !applied {
		t.Fatalf("ApplyIngest(tombDel) = (%v, %v), want (true, nil)", applied, err)
	}

	// Older upsert v3 cannot resurrect tombstone.
	oldUpsert := event.IngestMessage{
		MessageID:     "msg-old-3",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   "sp-tomb-1",
		SourceVersion: 3,
		HazardType:    hazard.HazardTypeCoastal,
		Severity:      hazard.SeveritySiaga,
		AreaName:      "Banten",
		Latitude:      -6.1,
		Longitude:     105.8,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if applied, err := testRepo.ApplyIngest(ctx, oldUpsert); err != nil || applied {
		t.Fatalf("ApplyIngest(oldUpsert) = (%v, %v), want (false, nil)", applied, err)
	}

	// Newer upsert v6 clears deleted_at and makes the event visible again.
	newUpsert := oldUpsert
	newUpsert.MessageID = "msg-new-6"
	newUpsert.SourceVersion = 6
	if applied, err := testRepo.ApplyIngest(ctx, newUpsert); err != nil || !applied {
		t.Fatalf("ApplyIngest(newUpsert) = (%v, %v), want (true, nil)", applied, err)
	}

	tombID := hazard.ID(hazard.SourceSensorPantai, "sp-tomb-1")
	events, err = testRepo.GetEvents(ctx, EventFilter{HazardID: tombID})
	if err != nil {
		t.Fatalf("GetEvents(tombID) error = %v", err)
	}
	if len(events) != 1 || events[0].HazardID != tombID {
		t.Fatalf("GetEvents(tombID) = %+v, want 1 resurrected event", events)
	}
}
