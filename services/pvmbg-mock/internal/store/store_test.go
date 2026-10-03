package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

func makeReport(id string, t time.Time) model.VolcanicReport {
	return model.VolcanicReport{
		ReportID:   id,
		VolcanoID:  "GNG-001",
		AlertLevel: model.AlertNormal,
		ReportedAt: t.Format(time.RFC3339),
	}
}

func TestMemoryStoreVolcanicRetentionBoundsCapacity(t *testing.T) {
	s := store.NewMemoryStore(5)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddVolcanicReport(makeReport(fmt.Sprintf("rpt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}
	if c := s.Count(); c != 5 {
		t.Fatalf("expected 5 reports, got %d", c)
	}
	got := s.GetVolcanicReports(nil, 15)
	if len(got) != 5 || got[0].ReportID != "rpt-05" || got[4].ReportID != "rpt-09" {
		t.Fatalf("expected newest 5 reports (rpt-05..rpt-09), got %+v", got)
	}
}

func TestMemoryStoreCapacityEdgeCases(t *testing.T) {
	s := store.NewMemoryStore(5000)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		s.AddVolcanicReport(makeReport(fmt.Sprintf("rpt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}
	if c := s.Count(); c != 20 || len(s.GetVolcanicReports(nil, 100)) != 20 {
		t.Fatalf("expected 20 reports retained under capacity")
	}

	s0 := store.NewMemoryStore(0)
	s0.AddVolcanicReport(makeReport("rpt-0", base))
	s0.AddVolcanicReport(makeReport("rpt-1", base.Add(time.Second)))
	if c := s0.Count(); c != 1 {
		t.Fatalf("expected capacity clamped to 1, got %d", c)
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
			s.AddVolcanicReport(makeReport(fmt.Sprintf("rpt-%03d", i), base.Add(time.Duration(i)*time.Millisecond)))
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.GetVolcanicReports(nil, 50)
			s.Count()
		}()
	}
	wg.Wait()

	if c := s.Count(); c > 500 {
		t.Fatalf("report count exceeds capacity under concurrency: %d", c)
	}
}
