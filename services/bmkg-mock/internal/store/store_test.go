package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

func makeSeismicEvent(id string, occurredAt time.Time) model.SeismicEvent {
	return model.SeismicEvent{
		EventID:          id,
		Magnitude:        5.5,
		DepthKm:          10.0,
		EpicenterLat:     -7.54,
		EpicenterLon:     110.44,
		RegionName:       "TestRegion",
		OccurredAt:       occurredAt.Format(time.RFC3339),
		PotentialTsunami: false,
	}
}

func makeTsunamiWarning(id, relatedEventID string, occurredAt time.Time) model.TsunamiWarning {
	return model.TsunamiWarning{
		WarningID:        id,
		RelatedEventID:   relatedEventID,
		ThreatLevel:      model.ThreatWaspada,
		AffectedZones:    []string{"zone-a", "zone-b"},
		EstimatedArrival: occurredAt.Add(20 * time.Minute).Format(time.RFC3339),
	}
}

func TestMemoryStoreSeismicRetentionBoundsCapacity(t *testing.T) {
	const maxCapacity = 5
	s := store.NewMemoryStore(maxCapacity)

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddSeismicEvent(makeSeismicEvent(fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}

	seismicCount, tsunamiCount := s.Count()
	if seismicCount != maxCapacity {
		t.Fatalf("seismic count: expected %d, got %d", maxCapacity, seismicCount)
	}
	if tsunamiCount != 0 {
		t.Fatalf("tsunami count: expected 0, got %d", tsunamiCount)
	}

	got := s.GetSeismicEvents(nil, maxCapacity+10)
	if len(got) != maxCapacity {
		t.Fatalf("GetSeismicEvents: expected %d, got %d", maxCapacity, len(got))
	}

	// The 5 newest events (indices 5..9) must survive; the 5 oldest (0..4) evicted.
	for j, evt := range got {
		wantIdx := 5 + j
		wantID := fmt.Sprintf("evt-%02d", wantIdx)
		if evt.EventID != wantID {
			t.Errorf("position %d: expected event %q, got %q", j, wantID, evt.EventID)
		}

		tm, err := time.Parse(time.RFC3339, evt.OccurredAt)
		if err != nil {
			t.Fatalf("failed to parse occurred_at %q: %v", evt.OccurredAt, err)
		}
		wantTS := base.Add(time.Duration(wantIdx) * time.Second)
		if !tm.Equal(wantTS) {
			t.Errorf("position %d: expected ts %v, got %v", j, wantTS, tm)
		}

		// Results must remain in chronological (ascending) order.
		if j > 0 {
			prev, _ := time.Parse(time.RFC3339, got[j-1].OccurredAt)
			if tm.Before(prev) {
				t.Errorf("events not chronologically ascending at position %d", j)
			}
		}
	}
}

func TestMemoryStoreTsunamiRetentionBoundsCapacity(t *testing.T) {
	const maxCapacity = 5
	s := store.NewMemoryStore(maxCapacity)

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddTsunamiWarning(
			makeTsunamiWarning(fmt.Sprintf("warn-%02d", i), fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)),
			base.Add(time.Duration(i)*time.Second),
		)
	}

	seismicCount, tsunamiCount := s.Count()
	if tsunamiCount != maxCapacity {
		t.Fatalf("tsunami count: expected %d, got %d", maxCapacity, tsunamiCount)
	}
	if seismicCount != 0 {
		t.Fatalf("seismic count: expected 0, got %d", seismicCount)
	}

	got := s.GetTsunamiWarnings(nil, maxCapacity+10)
	if len(got) != maxCapacity {
		t.Fatalf("GetTsunamiWarnings: expected %d, got %d", maxCapacity, len(got))
	}

	// The 5 newest warnings (indices 5..9) must survive; the 5 oldest evicted.
	for j, w := range got {
		wantIdx := 5 + j
		wantID := fmt.Sprintf("warn-%02d", wantIdx)
		if w.WarningID != wantID {
			t.Errorf("position %d: expected warning %q, got %q", j, wantID, w.WarningID)
		}

		// Referenced sub-slices must still be present on the surviving records.
		if len(w.AffectedZones) == 0 {
			t.Errorf("position %d: evicted warning lost its AffectedZones sub-slice", j)
		}
	}
}

func TestMemoryStoreRetentionDoesNotEvictUnderCapacity(t *testing.T) {
	s := store.NewMemoryStore(5000)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		s.AddSeismicEvent(makeSeismicEvent(fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}

	seismicCount, _ := s.Count()
	if seismicCount != 20 {
		t.Fatalf("expected 20 seismic events (under capacity), got %d", seismicCount)
	}

	got := s.GetSeismicEvents(nil, 100)
	if len(got) != 20 {
		t.Fatalf("expected all 20 events retrievable, got %d", len(got))
	}
}

func TestNewMemoryStoreClampsInvalidCapacity(t *testing.T) {
	s := store.NewMemoryStore(0)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	s.AddSeismicEvent(makeSeismicEvent("evt-0", base))
	s.AddSeismicEvent(makeSeismicEvent("evt-1", base.Add(time.Second)))

	sc, _ := s.Count()
	if sc != 1 {
		t.Fatalf("expected capacity clamped to 1 (oldest evicted), got %d", sc)
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	s := store.NewMemoryStore(500)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.AddSeismicEvent(makeSeismicEvent(fmt.Sprintf("evt-%03d", i), base.Add(time.Duration(i)*time.Millisecond)))
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.GetSeismicEvents(nil, 50)
			s.Count()
		}()
	}
	wg.Wait()

	sc, _ := s.Count()
	if sc > 500 {
		t.Fatalf("seismic count exceeds capacity under concurrency: got %d", sc)
	}
}
