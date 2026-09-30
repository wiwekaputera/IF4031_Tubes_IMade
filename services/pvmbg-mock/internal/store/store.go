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

// storedVolcanicReport wraps a volcanic report together with its pre-parsed
// report timestamp, so time.Parse is never invoked on the read path.
type storedVolcanicReport struct {
	report     model.VolcanicReport
	reportedAt time.Time
}

// MemoryStore is a thread-safe in-memory store for volcanic reports. Reports are
// kept in chronological (ascending reportedAt) order, so read paths can locate
// records with a binary search and never need to sort a copy or re-parse
// timestamps.
type MemoryStore struct {
	mu              sync.RWMutex
	volcanicReports []storedVolcanicReport
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		volcanicReports: make([]storedVolcanicReport, 0),
	}
}

// AddVolcanicReport adds a volcanic report to the store in chronological order.
// The reported_at timestamp is parsed exactly once, at insertion time.
func (s *MemoryStore) AddVolcanicReport(rpt model.VolcanicReport) {
	reportedAt, _ := parseTimestamp(rpt.ReportedAt)

	s.mu.Lock()
	defer s.mu.Unlock()

	idx := sort.Search(len(s.volcanicReports), func(i int) bool {
		return !s.volcanicReports[i].reportedAt.Before(reportedAt)
	})
	s.volcanicReports = append(s.volcanicReports, storedVolcanicReport{})
	copy(s.volcanicReports[idx+1:], s.volcanicReports[idx:])
	s.volcanicReports[idx] = storedVolcanicReport{report: rpt, reportedAt: reportedAt}
}

// GetVolcanicReports retrieves volcanic reports reported on or after 'since'.
// If 'since' is nil, all reports are returned up to 'limit'.
// Results are returned in chronological (ascending reportedAt) order. No sorting
// or timestamp parsing is performed on the read path.
func (s *MemoryStore) GetVolcanicReports(since *time.Time, limit int) []model.VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()

	start := 0
	if since != nil {
		start = sort.Search(len(s.volcanicReports), func(i int) bool {
			return !s.volcanicReports[i].reportedAt.Before(*since)
		})
	}

	remaining := len(s.volcanicReports) - start
	if limit > remaining {
		limit = remaining
	}

	result := make([]model.VolcanicReport, 0, limit)
	for i := start; i < start+limit; i++ {
		result = append(result, s.volcanicReports[i].report)
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
