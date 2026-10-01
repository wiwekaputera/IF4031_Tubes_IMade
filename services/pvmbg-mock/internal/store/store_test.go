package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

func makeVolcanicReport(id string, reportedAt time.Time) model.VolcanicReport {
	return model.VolcanicReport{
		ReportID:         id,
		VolcanoID:        "GNG-001",
		AlertLevel:       model.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       reportedAt.Format(time.RFC3339),
	}
}

func TestMemoryStoreVolcanicRetentionBoundsCapacity(t *testing.T) {
	const maxCapacity = 5
	s := store.NewMemoryStore(maxCapacity)

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		s.AddVolcanicReport(makeVolcanicReport(fmt.Sprintf("rpt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}

	if c := s.Count(); c != maxCapacity {
		t.Fatalf("report count: expected %d, got %d", maxCapacity, c)
	}

	got := s.GetVolcanicReports(nil, maxCapacity+10)
	if len(got) != maxCapacity {
		t.Fatalf("GetVolcanicReports: expected %d, got %d", maxCapacity, len(got))
	}

	// The 5 newest reports (indices 5..9) must survive; the 5 oldest evicted.
	for j, rpt := range got {
		wantIdx := 5 + j
		wantID := fmt.Sprintf("rpt-%02d", wantIdx)
		if rpt.ReportID != wantID {
			t.Errorf("position %d: expected report %q, got %q", j, wantID, rpt.ReportID)
		}

		tm, err := time.Parse(time.RFC3339, rpt.ReportedAt)
		if err != nil {
			t.Fatalf("failed to parse reported_at %q: %v", rpt.ReportedAt, err)
		}
		wantTS := base.Add(time.Duration(wantIdx) * time.Second)
		if !tm.Equal(wantTS) {
			t.Errorf("position %d: expected ts %v, got %v", j, wantTS, tm)
		}

		if j > 0 {
			prev, _ := time.Parse(time.RFC3339, got[j-1].ReportedAt)
			if tm.Before(prev) {
				t.Errorf("reports not chronologically ascending at position %d", j)
			}
		}
	}
}

func TestMemoryStoreRetentionDoesNotEvictUnderCapacity(t *testing.T) {
	s := store.NewMemoryStore(5000)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		s.AddVolcanicReport(makeVolcanicReport(fmt.Sprintf("rpt-%02d", i), base.Add(time.Duration(i)*time.Second)))
	}

	if c := s.Count(); c != 20 {
		t.Fatalf("expected 20 reports (under capacity), got %d", c)
	}

	got := s.GetVolcanicReports(nil, 100)
	if len(got) != 20 {
		t.Fatalf("expected all 20 reports retrievable, got %d", len(got))
	}
}

func TestNewMemoryStoreClampsInvalidCapacity(t *testing.T) {
	s := store.NewMemoryStore(0)
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	s.AddVolcanicReport(makeVolcanicReport("rpt-0", base))
	s.AddVolcanicReport(makeVolcanicReport("rpt-1", base.Add(time.Second)))

	if c := s.Count(); c != 1 {
		t.Fatalf("expected capacity clamped to 1 (oldest evicted), got %d", c)
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
			s.AddVolcanicReport(makeVolcanicReport(fmt.Sprintf("rpt-%03d", i), base.Add(time.Duration(i)*time.Millisecond)))
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
		t.Fatalf("report count exceeds capacity under concurrency: got %d", c)
	}
}
