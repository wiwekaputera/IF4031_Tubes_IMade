package store

import (
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
)

// Store defines the interface for managing seismic events and tsunami warnings in memory.
type Store interface {
	AddSeismicEvent(evt agency.SeismicEvent)
	GetSeismicEvents(since *time.Time) []agency.SeismicEvent
	AddTsunamiWarning(warn agency.TsunamiWarning, occurredAt time.Time)
	GetTsunamiWarnings(since *time.Time) []agency.TsunamiWarning
	Count() (seismicCount int, tsunamiCount int)
}

type warningRecord struct {
	warning    agency.TsunamiWarning
	occurredAt time.Time
	arrivalAt  time.Time
}

// MemoryStore is a thread-safe in-memory store for seismic events and tsunami warnings.
type MemoryStore struct {
	mu             sync.RWMutex
	seismicEvents  []agency.SeismicEvent
	tsunamiRecords []warningRecord
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		seismicEvents:  make([]agency.SeismicEvent, 0),
		tsunamiRecords: make([]warningRecord, 0),
	}
}

// AddSeismicEvent adds a seismic event to the store in a thread-safe manner.
func (s *MemoryStore) AddSeismicEvent(evt agency.SeismicEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seismicEvents = append(s.seismicEvents, evt)
}

// GetSeismicEvents retrieves seismic events occurred on or after 'since'.
// If 'since' is nil, all events are returned.
func (s *MemoryStore) GetSeismicEvents(since *time.Time) []agency.SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if since == nil {
		result := make([]agency.SeismicEvent, len(s.seismicEvents))
		copy(result, s.seismicEvents)
		return result
	}

	result := make([]agency.SeismicEvent, 0)
	for _, evt := range s.seismicEvents {
		t, err := time.Parse(time.RFC3339, evt.OccurredAt)
		if err != nil {
			// Fallback to RFC3339Nano if nanoseconds were included
			t, err = time.Parse(time.RFC3339Nano, evt.OccurredAt)
		}

		if err == nil {
			if !t.Before(*since) {
				result = append(result, evt)
			}
		} else {
			// If parsing fails for any reason, conservatively include it
			result = append(result, evt)
		}
	}
	return result
}

// AddTsunamiWarning adds a tsunami warning to the store in a thread-safe manner.
func (s *MemoryStore) AddTsunamiWarning(warn agency.TsunamiWarning, occurredAt time.Time) {
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
// If 'since' is nil, all warnings are returned.
func (s *MemoryStore) GetTsunamiWarnings(since *time.Time) []agency.TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if since == nil {
		result := make([]agency.TsunamiWarning, len(s.tsunamiRecords))
		for i, rec := range s.tsunamiRecords {
			result[i] = rec.warning
		}
		return result
	}

	result := make([]agency.TsunamiWarning, 0)
	for _, rec := range s.tsunamiRecords {
		// Include if event occurred at/after since, or arrival is at/after since
		if !rec.occurredAt.Before(*since) || !rec.arrivalAt.Before(*since) {
			result = append(result, rec.warning)
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
