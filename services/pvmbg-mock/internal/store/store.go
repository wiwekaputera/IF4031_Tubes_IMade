package store

import (
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
)

// Store defines the interface for managing volcanic reports in memory.
type Store interface {
	AddVolcanicReport(rpt model.VolcanicReport)
	GetVolcanicReports(since *time.Time) []model.VolcanicReport
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
// If 'since' is nil, all reports are returned.
func (s *MemoryStore) GetVolcanicReports(since *time.Time) []model.VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if since == nil {
		result := make([]model.VolcanicReport, len(s.volcanicReports))
		copy(result, s.volcanicReports)
		return result
	}

	result := make([]model.VolcanicReport, 0)
	for _, rpt := range s.volcanicReports {
		t, err := time.Parse(time.RFC3339, rpt.ReportedAt)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, rpt.ReportedAt)
		}

		if err == nil {
			if !t.Before(*since) {
				result = append(result, rpt)
			}
		} else {
			result = append(result, rpt)
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
