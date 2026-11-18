package store

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeShardQuerier struct {
	mu       sync.Mutex
	shards   map[int][]shardEvent
	errs     map[int]error
	delays   map[int]time.Duration
	queried  []int
}

func (f *fakeShardQuerier) queryShard(
	ctx context.Context,
	shard int,
	filter ListFilter,
	cursor *decodedCursor,
	limit int,
) ([]shardEvent, error) {
	f.mu.Lock()
	f.queried = append(f.queried, shard)
	delay := f.delays[shard]
	err := f.errs[shard]
	rows := append([]shardEvent(nil), f.shards[shard]...)
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}

	var filtered []shardEvent
	for _, r := range rows {
		if filter.HazardType != "" && string(r.event.HazardType) != filter.HazardType {
			continue
		}
		if filter.HazardID != "" && r.event.HazardID != filter.HazardID {
			continue
		}
		if filter.Source != "" && string(r.event.Source) != filter.Source {
			continue
		}
		if filter.Since != nil && r.occurredAt.Before(*filter.Since) {
			continue
		}
		if cursor != nil {
			if r.occurredAt.After(cursor.occurredAt) {
				continue
			}
			if r.occurredAt.Equal(cursor.occurredAt) && r.event.HazardID >= cursor.hazardID {
				continue
			}
		}
		filtered = append(filtered, r)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if !filtered[i].occurredAt.Equal(filtered[j].occurredAt) {
			return filtered[i].occurredAt.After(filtered[j].occurredAt)
		}
		return filtered[i].event.HazardID > filtered[j].event.HazardID
	})

	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func makeTestEvent(id string, src hazard.Source, htype hazard.HazardType, occurredAt time.Time) shardEvent {
	return shardEvent{
		occurredAt: occurredAt.UTC(),
		event: hazard.HazardEvent{
			HazardID:   id,
			Source:     src,
			HazardType: htype,
			OccurredAt: occurredAt.UTC().Format(time.RFC3339),
			Attributes: map[string]interface{}{},
		},
	}
}

func TestListEvents_OrderingAndCursorPagination(t *testing.T) {
	base := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

	// Spread events across shards with ties on occurred_at and sub-second differences.
	all := []shardEvent{
		makeTestEvent("hz-01", hazard.SourceBMKG, hazard.HazardTypeSeismic, base.Add(5*time.Second)),
		makeTestEvent("hz-02", hazard.SourceBMKG, hazard.HazardTypeSeismic, base.Add(4*time.Second)),
		makeTestEvent("hz-03", hazard.SourcePVMBG, hazard.HazardTypeVolcanic, base.Add(4*time.Second)),               // tie with hz-02
		makeTestEvent("hz-04", hazard.SourceBMKG, hazard.HazardTypeSeismic, base.Add(4*time.Second)),               // tie with hz-02, hz-03
		makeTestEvent("hz-05", hazard.SourceSatelit, hazard.HazardTypeWildfire, base.Add(3*time.Second+500*time.Millisecond)),
		makeTestEvent("hz-06", hazard.SourceSatelit, hazard.HazardTypeWildfire, base.Add(3*time.Second+100*time.Millisecond)),
		makeTestEvent("hz-07", hazard.SourceSensorPantai, hazard.HazardTypeCoastal, base.Add(2*time.Second)),
		makeTestEvent("hz-08", hazard.SourceBMKG, hazard.HazardTypeSeismic, base.Add(1*time.Second)),
		makeTestEvent("hz-09", hazard.SourcePVMBG, hazard.HazardTypeVolcanic, base),
	}

	shards := make(map[int][]shardEvent)
	for _, ev := range all {
		s := ShardForHazardID(ev.event.HazardID)
		shards[s] = append(shards[s], ev)
	}
	q := &fakeShardQuerier{shards: shards}

	// Page through 2 items at a time.
	var seen []string
	var cursor string
	pages := 0
	for {
		pages++
		res, err := listEventsWithQuerier(context.Background(), q, 500*time.Millisecond, zerolog.Nop(), ListFilter{
			Limit:  2,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("page %d failed: %v", pages, err)
		}
		if res.Partial || len(res.UnavailableUnits) != 0 {
			t.Fatalf("expected non-partial result, got partial=%v units=%v", res.Partial, res.UnavailableUnits)
		}
		for _, ev := range res.Events {
			seen = append(seen, ev.HazardID)
		}
		if res.NextCursor == nil {
			break
		}
		cursor = *res.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	// Expected strict order: occurred_at DESC, hazard_id DESC.
	want := []string{"hz-01", "hz-04", "hz-03", "hz-02", "hz-05", "hz-06", "hz-07", "hz-08", "hz-09"}
	if len(seen) != len(want) {
		t.Fatalf("got %d items %v, want %d %v", len(seen), seen, len(want), want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Errorf("index %d: got %s, want %s (full: %v)", i, seen[i], want[i], seen)
		}
	}
}

func TestListEvents_InvalidCursor(t *testing.T) {
	q := &fakeShardQuerier{shards: map[int][]shardEvent{}}
	for _, bad := range []string{"not-base64!!!", "e30", "eyJ0IjoiYmFkIiwiaWQiOiJ4In0"} {
		_, err := listEventsWithQuerier(context.Background(), q, 500*time.Millisecond, zerolog.Nop(), ListFilter{
			Limit:  10,
			Cursor: bad,
		})
		if !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q: expected ErrInvalidCursor, got %v", bad, err)
		}
	}
}

func TestListEvents_PartialOnTimeoutAndNoGoroutineLeak(t *testing.T) {
	base := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	shards := map[int][]shardEvent{
		0: {makeTestEvent("hz-a", hazard.SourceBMKG, hazard.HazardTypeSeismic, base)},
		3: {makeTestEvent("hz-slow", hazard.SourceBMKG, hazard.HazardTypeSeismic, base.Add(time.Second))},
		5: {makeTestEvent("hz-b", hazard.SourcePVMBG, hazard.HazardTypeVolcanic, base.Add(-time.Second))},
	}
	q := &fakeShardQuerier{
		shards: shards,
		delays: map[int]time.Duration{3: 2 * time.Second},
		errs:   map[int]error{6: errors.New("connection refused")},
	}

	// Warm up runtime goroutine count baseline.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	start := time.Now()
	res, err := listEventsWithQuerier(context.Background(), q, 50*time.Millisecond, zerolog.Nop(), ListFilter{Limit: 10})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error on partial failure, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("scatter took too long (%v), expected ~50ms", elapsed)
	}
	if !res.Partial {
		t.Error("expected Partial=true")
	}
	if len(res.UnavailableUnits) != 2 || res.UnavailableUnits[0] != "shard-3" || res.UnavailableUnits[1] != "shard-6" {
		t.Errorf("unexpected UnavailableUnits: %v", res.UnavailableUnits)
	}
	if len(res.Events) != 2 || res.Events[0].HazardID != "hz-a" || res.Events[1].HazardID != "hz-b" {
		t.Errorf("unexpected Events: %+v", res.Events)
	}

	// Allow scheduler to reap exited shard goroutines.
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	if after := runtime.NumGoroutine(); after > baseline+2 {
		t.Errorf("potential goroutine leak: baseline=%d after=%d", baseline, after)
	}
}

func TestListEvents_AllShardsDown(t *testing.T) {
	errs := make(map[int]error, NumShards)
	for i := 0; i < NumShards; i++ {
		errs[i] = errors.New("down")
	}
	q := &fakeShardQuerier{errs: errs}

	res, err := listEventsWithQuerier(context.Background(), q, 100*time.Millisecond, zerolog.Nop(), ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("expected nil error when all shards down, got %v", err)
	}
	if !res.Partial {
		t.Error("expected Partial=true")
	}
	if len(res.UnavailableUnits) != NumShards {
		t.Errorf("expected %d unavailable units, got %v", NumShards, res.UnavailableUnits)
	}
	if res.Events == nil || len(res.Events) != 0 {
		t.Errorf("expected empty non-nil Events slice, got %v", res.Events)
	}
	if res.NextCursor != nil {
		t.Errorf("expected nil NextCursor, got %v", *res.NextCursor)
	}
}

func TestListEvents_HazardIDTargetsSingleShard(t *testing.T) {
	targetID := "hz-target-123"
	targetShard := ShardForHazardID(targetID)
	base := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

	q := &fakeShardQuerier{
		shards: map[int][]shardEvent{
			targetShard: {makeTestEvent(targetID, hazard.SourceBMKG, hazard.HazardTypeSeismic, base)},
		},
	}

	res, err := listEventsWithQuerier(context.Background(), q, 100*time.Millisecond, zerolog.Nop(), ListFilter{
		HazardID: targetID,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Events) != 1 || res.Events[0].HazardID != targetID {
		t.Fatalf("unexpected events: %+v", res.Events)
	}
	if len(q.queried) != 1 || q.queried[0] != targetShard {
		t.Errorf("expected only shard %d to be queried, got %v", targetShard, q.queried)
	}
}

func TestListEvents_CockroachDB(t *testing.T) {
	if testRepo == nil || testPool == nil {
		t.Skip("CockroachDB testRepo not initialized")
	}
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `TRUNCATE TABLE canonical_events, outbox`); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	base := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	ids := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		src := hazard.SourceBMKG
		ht := hazard.HazardTypeSeismic
		if i%3 == 0 {
			src = hazard.SourcePVMBG
			ht = hazard.HazardTypeVolcanic
		}
		ref := "scatter-ref-" + string(rune('a'+i))
		id := hazard.ID(src, ref)
		ids = append(ids, id)
		// Create ties on occurred_at every 3 events and sub-second offsets on others.
		occ := base.Add(time.Duration(i/3)*time.Second + time.Duration(i%2)*250*time.Millisecond)
		if _, err := testRepo.UpsertEvent(ctx, UpsertRequest{
			HazardID:    id,
			Source:      string(src),
			SourceRefID: ref,
			HazardType:  ht,
			Severity:    hazard.SeverityWaspada,
			AreaName:    "Test Area",
			Latitude:    -7.5,
			Longitude:   110.4,
			OccurredAt:  occ,
		}); err != nil {
			t.Fatalf("upsert %d failed: %v", i, err)
		}

		// Verify Go ShardForHazardID matches CockroachDB's computed shard column.
		var dbShard int
		if err := testPool.QueryRow(ctx, `SELECT shard FROM canonical_events WHERE hazard_id = $1`, id).Scan(&dbShard); err != nil {
			t.Fatalf("select shard failed: %v", err)
		}
		if goShard := ShardForHazardID(id); goShard != dbShard {
			t.Fatalf("shard mismatch for %s: Go=%d, DB=%d", id, goShard, dbShard)
		}
	}

	// Paginate 4 items at a time across all 8 shards.
	seen := make(map[string]bool)
	var cursor string
	for page := 1; ; page++ {
		res, err := testRepo.ListEvents(ctx, ListFilter{Limit: 4, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d failed: %v", page, err)
		}
		if res.Partial {
			t.Fatalf("unexpected partial result: %v", res.UnavailableUnits)
		}
		for _, ev := range res.Events {
			if seen[ev.HazardID] {
				t.Fatalf("duplicate event %s on page %d", ev.HazardID, page)
			}
			seen[ev.HazardID] = true
		}
		if res.NextCursor == nil {
			break
		}
		cursor = *res.NextCursor
	}
	if len(seen) != len(ids) {
		t.Fatalf("visited %d events, want %d", len(seen), len(ids))
	}

	// Source filter + HazardID single-shard lookup.
	resBMKG, err := testRepo.ListEvents(ctx, ListFilter{Source: string(hazard.SourceBMKG), Limit: 20})
	if err != nil {
		t.Fatalf("source filter failed: %v", err)
	}
	if len(resBMKG.Events) != 8 {
		t.Errorf("expected 8 BMKG events, got %d", len(resBMKG.Events))
	}

	resOne, err := testRepo.ListEvents(ctx, ListFilter{HazardID: ids[0], Limit: 10})
	if err != nil || len(resOne.Events) != 1 || resOne.Events[0].HazardID != ids[0] {
		t.Errorf("single hazard_id lookup failed: err=%v len=%d", err, len(resOne.Events))
	}
}
