package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

func makeSeismic(id string, t time.Time) model.SeismicEvent {
	return model.SeismicEvent{EventID: id, Magnitude: 5.5, OccurredAt: t.Format(time.RFC3339)}
}

func makeWarning(id, relatedID string, t time.Time) model.TsunamiWarning {
	return model.TsunamiWarning{
		WarningID:        id,
		RelatedEventID:   relatedID,
		ThreatLevel:      model.ThreatWaspada,
		AffectedZones:    []string{"zone-a"},
		EstimatedArrival: t.Add(20 * time.Minute).Format(time.RFC3339),
	}
}

func TestMemoryStoreSeismicRetentionBoundsCapacity(t *testing.T) {
	s := store.NewMemoryStore(5)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddSeismicEvent(makeSeismic(fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}
	sc, tc := s.Count()
	if sc != 5 || tc != 0 {
		t.Fatalf("expected count 5 seismic, 0 tsunami; got %d, %d", sc, tc)
	}
	got := s.GetSeismicEvents(nil, 15)
	if len(got) != 5 || got[0].EventID != "evt-05" || got[4].EventID != "evt-09" {
		t.Fatalf("expected newest 5 events (evt-05..evt-09), got %+v", got)
	}
}

func TestMemoryStoreTsunamiRetentionBoundsCapacity(t *testing.T) {
	s := store.NewMemoryStore(5)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddTsunamiWarning(makeWarning(fmt.Sprintf("warn-%02d", i), fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)), base.Add(time.Duration(i)*time.Second))
	}
	sc, tc := s.Count()
	if sc != 0 || tc != 5 {
		t.Fatalf("expected count 0 seismic, 5 tsunami; got %d, %d", sc, tc)
	}
	got := s.GetTsunamiWarnings(nil, 15)
	if len(got) != 5 || got[0].WarningID != "warn-05" || len(got[0].AffectedZones) == 0 {
		t.Fatalf("expected newest 5 warnings with zones, got %+v", got)
	}
}

func TestMemoryStoreCapacityEdgeCases(t *testing.T) {
	// Under capacity retains all
	s := store.NewMemoryStore(5000)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		s.AddSeismicEvent(makeSeismic(fmt.Sprintf("evt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}
	if sc, _ := s.Count(); sc != 20 || len(s.GetSeismicEvents(nil, 100)) != 20 {
		t.Fatalf("expected 20 events retained under capacity")
	}

	// Clamps 0 capacity to 1
	s0 := store.NewMemoryStore(0)
	s0.AddSeismicEvent(makeSeismic("evt-0", base))
	s0.AddSeismicEvent(makeSeismic("evt-1", base.Add(time.Second)))
	if sc, _ := s0.Count(); sc != 1 {
		t.Fatalf("expected capacity clamped to 1, got %d", sc)
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
			s.AddSeismicEvent(makeSeismic(fmt.Sprintf("evt-%03d", i), base.Add(time.Duration(i)*time.Millisecond)))
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

	if sc, _ := s.Count(); sc > 500 {
		t.Fatalf("seismic count exceeds capacity under concurrency: %d", sc)
	}
}
