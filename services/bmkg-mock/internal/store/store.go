package store

import (
	"sort"
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
)

// Store defines the interface for managing seismic events and tsunami warnings in memory.
type Store interface {
	AddSeismicEvent(evt model.SeismicEvent)
	GetSeismicEvents(since *time.Time, limit int) []model.SeismicEvent
	AddTsunamiWarning(warn model.TsunamiWarning, occurredAt time.Time)
	GetTsunamiWarnings(since *time.Time, limit int) []model.TsunamiWarning
	Count() (seismicCount int, tsunamiCount int)
}

type warningRecord struct {
	warning    model.TsunamiWarning
	occurredAt time.Time
	arrivalAt  time.Time
}

// MemoryStore is a thread-safe in-memory store for seismic events and tsunami warnings.
type MemoryStore struct {
	mu             sync.RWMutex
	seismicEvents  []model.SeismicEvent
	tsunamiRecords []warningRecord
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		seismicEvents:  make([]model.SeismicEvent, 0),
		tsunamiRecords: make([]warningRecord, 0),
	}
}

// AddSeismicEvent adds a seismic event to the store in a thread-safe manner.
func (s *MemoryStore) AddSeismicEvent(evt model.SeismicEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seismicEvents = append(s.seismicEvents, evt)
}

// GetSeismicEvents retrieves seismic events occurred on or after 'since'.
// If 'since' is nil, all events are returned up to 'limit'.
// Results are sorted chronologically by OccurredAt ascending.
func (s *MemoryStore) GetSeismicEvents(since *time.Time, limit int) []model.SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sorted := make([]model.SeismicEvent, len(s.seismicEvents))
	copy(sorted, s.seismicEvents)
	sort.Slice(sorted, func(i, j int) bool {
		ti, _ := parseTimestamp(sorted[i].OccurredAt)
		tj, _ := parseTimestamp(sorted[j].OccurredAt)
		return ti.Before(tj)
	})

	result := make([]model.SeismicEvent, 0, min(limit, len(sorted)))
	for _, evt := range sorted {
		if since != nil {
			t, err := parseTimestamp(evt.OccurredAt)
			if err == nil && t.Before(*since) {
				continue
			}
		}
		result = append(result, evt)
		if len(result) >= limit {
			break
		}
	}
	return result
}

// AddTsunamiWarning adds a tsunami warning to the store in a thread-safe manner.
func (s *MemoryStore) AddTsunamiWarning(warn model.TsunamiWarning, occurredAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var arrivalAt time.Time
	if t, err := time.Parse(time.RFC3339, warn.EstimatedArrival); err == nil {
		arrivalAt = t
	} else if t, err := time.Parse(time.RFC3339Nano, warn.EstimatedArrival); err == nil {
		arrivalAt = t
	} else {
		arrivalAt = occurredAt
	}

	s.tsunamiRecords = append(s.tsunamiRecords, warningRecord{
		warning:    warn,
		occurredAt: occurredAt,
		arrivalAt:  arrivalAt,
	})
}

// GetTsunamiWarnings retrieves tsunami warnings matching the 'since' filter.
// If 'since' is nil, all warnings are returned up to 'limit'.
// Results are sorted chronologically by occurredAt ascending.
func (s *MemoryStore) GetTsunamiWarnings(since *time.Time, limit int) []model.TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sorted := make([]warningRecord, len(s.tsunamiRecords))
	copy(sorted, s.tsunamiRecords)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].occurredAt.Before(sorted[j].occurredAt)
	})

	result := make([]model.TsunamiWarning, 0, min(limit, len(sorted)))
	for _, rec := range sorted {
		if since == nil || !rec.occurredAt.Before(*since) || !rec.arrivalAt.Before(*since) {
			result = append(result, rec.warning)
			if len(result) >= limit {
				break
			}
		}
	}
	return result
}

// Count returns the number of seismic events and tsunami warnings currently stored.
func (s *MemoryStore) Count() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.seismicEvents), len(s.tsunamiRecords)
}

// parseTimestamp parses an ISO 8601 timestamp string, trying RFC3339 then RFC3339Nano.
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return t, err
}
