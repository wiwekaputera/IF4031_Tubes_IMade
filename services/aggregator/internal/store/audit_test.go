package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeAuditShardQuerier struct {
	shards map[int][]auditShardEvent
	errs   map[int]error
}

func (f *fakeAuditShardQuerier) queryAuditShard(
	_ context.Context,
	shard int,
	filter AuditEventsFilter,
	cursor *decodedCursor,
	limit int,
) ([]auditShardEvent, error) {
	if err := f.errs[shard]; err != nil {
		return nil, err
	}
	var out []auditShardEvent
	for _, r := range f.shards[shard] {
		if filter.Source != "" && string(r.event.Source) != filter.Source {
			continue
		}
		if filter.Since != nil && r.ingestedAt.Before(*filter.Since) {
			continue
		}
		if cursor != nil {
			if r.ingestedAt.Before(cursor.occurredAt) {
				continue
			}
			if r.ingestedAt.Equal(cursor.occurredAt) && r.event.HazardID <= cursor.hazardID {
				continue
			}
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ingestedAt.Equal(out[j].ingestedAt) {
			return out[i].ingestedAt.Before(out[j].ingestedAt)
		}
		return out[i].event.HazardID < out[j].event.HazardID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func TestListAuditEvents_AscendingOrderAndSubsecondCursorPagination(t *testing.T) {
	base := time.Date(2026, 11, 12, 2, 51, 8, 0, time.UTC)
	events := []auditShardEvent{
		{
			ingestedAt: base.Add(100 * time.Microsecond),
			event:      hazard.HazardEvent{HazardID: "hz-b", Source: hazard.SourceBMKG, IngestedAt: base.Format(time.RFC3339)},
		},
		{
			ingestedAt: base.Add(200 * time.Microsecond),
			event:      hazard.HazardEvent{HazardID: "hz-a", Source: hazard.SourceBMKG, IngestedAt: base.Format(time.RFC3339)},
		},
		{
			ingestedAt: base.Add(200 * time.Microsecond),
			event:      hazard.HazardEvent{HazardID: "hz-c", Source: hazard.SourcePVMBG, IngestedAt: base.Format(time.RFC3339)},
		},
		{
			ingestedAt: base.Add(1 * time.Second),
			event:      hazard.HazardEvent{HazardID: "hz-d", Source: hazard.SourceSatelit, IngestedAt: base.Add(1 * time.Second).Format(time.RFC3339)},
		},
	}

	shards := make(map[int][]auditShardEvent)
	for _, ev := range events {
		s := ShardForHazardID(ev.event.HazardID)
		shards[s] = append(shards[s], ev)
	}

	querier := &fakeAuditShardQuerier{shards: shards}
	var gotIDs []string
	var cursor string

	for {
		page, err := listAuditEventsWithQuerier(context.Background(), querier, time.Second, zerolog.Nop(), AuditEventsFilter{
			Limit:  2,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.Partial {
			t.Fatalf("expected Partial=false, got true (unavailable: %v)", page.UnavailableUnits)
		}
		if page.UnavailableUnits == nil {
			t.Fatal("expected non-nil UnavailableUnits")
		}
		for _, item := range page.Items {
			gotIDs = append(gotIDs, item.HazardID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}

	wantIDs := []string{"hz-b", "hz-a", "hz-c", "hz-d"}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("got %v, want %v", gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("index %d: got %s, want %s (full: %v)", i, gotIDs[i], wantIDs[i], gotIDs)
		}
	}
}

func TestListAuditEvents_PartialOnShardFailure(t *testing.T) {
	querier := &fakeAuditShardQuerier{
		shards: map[int][]auditShardEvent{
			0: {{ingestedAt: time.Now().UTC(), event: hazard.HazardEvent{HazardID: "hz-1", Source: hazard.SourceBMKG}}},
		},
		errs: map[int]error{
			3: errors.New("shard 3 down"),
		},
	}

	page, err := listAuditEventsWithQuerier(context.Background(), querier, time.Second, zerolog.Nop(), AuditEventsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !page.Partial {
		t.Fatal("expected Partial=true")
	}
	if len(page.UnavailableUnits) != 1 || page.UnavailableUnits[0] != "shard-3" {
		t.Fatalf("expected unavailable_units=[shard-3], got %v", page.UnavailableUnits)
	}
	if len(page.Items) != 1 || page.Items[0].HazardID != "hz-1" {
		t.Fatalf("expected 1 event from healthy shard, got %v", page.Items)
	}
}

func TestListAuditEvents_InvalidCursor(t *testing.T) {
	querier := &fakeAuditShardQuerier{shards: map[int][]auditShardEvent{}}
	_, err := listAuditEventsWithQuerier(context.Background(), querier, time.Second, zerolog.Nop(), AuditEventsFilter{Cursor: "not-valid-base64!"})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

type fakeChecksumShardQuerier struct {
	shards map[int][]string
	errs   map[int]error
}

func (f *fakeChecksumShardQuerier) querySourceRefIDs(_ context.Context, shard int, _ string) ([]string, error) {
	if err := f.errs[shard]; err != nil {
		return nil, err
	}
	return f.shards[shard], nil
}

func TestComputeChecksum_KnownVector(t *testing.T) {
	querier := &fakeChecksumShardQuerier{
		shards: map[int][]string{
			0: {"b"},
			2: {"a"},
			5: {"c"},
		},
	}

	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	cs, err := computeChecksumWithQuerier(context.Background(), querier, time.Second, zerolog.Nop(), "BMKG", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sum := sha256.Sum256([]byte("a\nb\nc"))
	wantHex := hex.EncodeToString(sum[:])

	if cs.Checksum != wantHex {
		t.Errorf("got checksum %s, want %s", cs.Checksum, wantHex)
	}
	if cs.Count != 3 {
		t.Errorf("got count %d, want 3", cs.Count)
	}
	if cs.Algorithm != "sha256" {
		t.Errorf("got algorithm %q, want sha256", cs.Algorithm)
	}
	if cs.Source == nil || *cs.Source != "BMKG" {
		t.Errorf("got source %v, want BMKG", cs.Source)
	}
	if cs.Partial {
		t.Errorf("expected Partial=false")
	}
	if cs.UnavailableUnits == nil || len(cs.UnavailableUnits) != 0 {
		t.Errorf("expected empty non-nil UnavailableUnits, got %v", cs.UnavailableUnits)
	}
}

func TestComputeChecksum_AllSourcesAndPartial(t *testing.T) {
	querier := &fakeChecksumShardQuerier{
		shards: map[int][]string{
			1: {"id-1"},
		},
		errs: map[int]error{
			6: errors.New("timeout"),
		},
	}

	cs, err := computeChecksumWithQuerier(context.Background(), querier, time.Second, zerolog.Nop(), "", time.Now().UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cs.Source != nil {
		t.Errorf("expected nil Source when source omitted, got %v", *cs.Source)
	}
	if !cs.Partial {
		t.Errorf("expected Partial=true")
	}
	if len(cs.UnavailableUnits) != 1 || cs.UnavailableUnits[0] != "shard-6" {
		t.Errorf("expected [shard-6], got %v", cs.UnavailableUnits)
	}
}

type fakeLocationQuerier struct {
	exists    bool
	existsErr error
	meta      rangeMetadata
	rangeErr  error
}

func (f *fakeLocationQuerier) eventExists(context.Context, int, string) (bool, error) {
	return f.exists, f.existsErr
}

func (f *fakeLocationQuerier) showRange(context.Context, int, string) (rangeMetadata, error) {
	return f.meta, f.rangeErr
}

func TestLocateEvent(t *testing.T) {
	hazardID := "hz-1234567890abcdef"
	expectedUnit := UnitName(ShardForHazardID(hazardID))

	t.Run("not found", func(t *testing.T) {
		_, err := locateEventWithQuerier(context.Background(), &fakeLocationQuerier{exists: false}, time.Second, zerolog.Nop(), hazardID)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("primary and replicas", func(t *testing.T) {
		q := &fakeLocationQuerier{
			exists: true,
			meta: rangeMetadata{
				leaseHolder:       2,
				replicas:          []int64{1, 2, 3},
				replicaLocalities: []string{"node=node-a", "region=id,node=node-b", "node=node-c"},
			},
		}
		resp, err := locateEventWithQuerier(context.Background(), q, time.Second, zerolog.Nop(), hazardID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Partial {
			t.Fatalf("expected Partial=false")
		}
		if len(resp.Locations) != 3 {
			t.Fatalf("expected 3 locations, got %d", len(resp.Locations))
		}
		if resp.Locations[0].Role != "primary" || resp.Locations[0].Node != "node-b" || resp.Locations[0].Unit != expectedUnit {
			t.Errorf("unexpected primary: %+v", resp.Locations[0])
		}
		if resp.Locations[1].Role != "replica" || resp.Locations[1].Node != "node-a" || resp.Locations[1].Unit != expectedUnit+"-r1" {
			t.Errorf("unexpected replica 1: %+v", resp.Locations[1])
		}
		if resp.Locations[2].Role != "replica" || resp.Locations[2].Node != "node-c" || resp.Locations[2].Unit != expectedUnit+"-r2" {
			t.Errorf("unexpected replica 2: %+v", resp.Locations[2])
		}
	})

	t.Run("partial when showRange fails", func(t *testing.T) {
		q := &fakeLocationQuerier{
			exists:   true,
			rangeErr: errors.New("range metadata timeout"),
		}
		resp, err := locateEventWithQuerier(context.Background(), q, time.Second, zerolog.Nop(), hazardID)
		if err != nil {
			t.Fatalf("expected nil error on range timeout, got %v", err)
		}
		if !resp.Partial {
			t.Errorf("expected Partial=true")
		}
		if resp.Locations == nil || len(resp.Locations) != 0 {
			t.Errorf("expected empty non-nil Locations, got %v", resp.Locations)
		}
	})
}
