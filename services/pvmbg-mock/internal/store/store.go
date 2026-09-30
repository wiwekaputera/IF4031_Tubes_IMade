package store

import (
	"sort"
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
)

// Store defines the interface for managing volcanic reports in memory.
type Store interface {
	AddVolcanicReport(rpt model.VolcanicReport)
	GetVolcanicReports(since *time.Time, limit int) []model.VolcanicReport
	Count() int
}

// MemoryStore is a thread-safe in-memory store for volcanic reports.
type MemoryStore struct {
	mu              sync.RWMutex
	volcanicReports []model.VolcanicReport
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		volcanicReports: make([]model.VolcanicReport, 0),
	}
}

// AddVolcanicReport adds a volcanic report to the store in a thread-safe manner.
func (s *MemoryStore) AddVolcanicReport(rpt model.VolcanicReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.volcanicReports = append(s.volcanicReports, rpt)
}

// GetVolcanicReports retrieves volcanic reports reported on or after 'since'.
// If 'since' is nil, all reports are returned up to 'limit'.
// Results are sorted chronologically by ReportedAt ascending.
func (s *MemoryStore) GetVolcanicReports(since *time.Time, limit int) []model.VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sorted := make([]model.VolcanicReport, len(s.volcanicReports))
	copy(sorted, s.volcanicReports)
	sort.Slice(sorted, func(i, j int) bool {
		ti, _ := parseTimestamp(sorted[i].ReportedAt)
		tj, _ := parseTimestamp(sorted[j].ReportedAt)
		return ti.Before(tj)
	})

	result := make([]model.VolcanicReport, 0, min(limit, len(sorted)))
	for _, rpt := range sorted {
		if since != nil {
			t, err := parseTimestamp(rpt.ReportedAt)
			if err == nil && t.Before(*since) {
				continue
			}
		}
		result = append(result, rpt)
		if len(result) >= limit {
			break
		}
	}
	return result
}

// Count returns the number of volcanic reports currently stored.
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.volcanicReports)
}

// parseTimestamp parses an ISO 8601 timestamp string, trying RFC3339 then RFC3339Nano.
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return t, err
}
