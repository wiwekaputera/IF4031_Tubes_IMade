// Package store provides an in-memory, thread-safe store for seismic events and tsunami warnings.
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

// storedSeismicEvent stores a seismic event with its pre-parsed timestamp.
type storedSeismicEvent struct {
	event      model.SeismicEvent
	occurredAt time.Time
}

// warningRecord stores a tsunami warning with its pre-parsed timestamps.
type warningRecord struct {
	warning    model.TsunamiWarning
	occurredAt time.Time
	arrivalAt  time.Time
}

// MemoryStore is a thread-safe in-memory store for seismic events and tsunami warnings.
// Records are stored in chronological order and bounded by a sliding-window capacity.
type MemoryStore struct {
	mu             sync.RWMutex
	maxCapacity    int
	seismicEvents  []storedSeismicEvent
	tsunamiRecords []warningRecord
}

// NewMemoryStore initializes a MemoryStore bounded to maxCapacity records.
func NewMemoryStore(maxCapacity int) *MemoryStore {
	if maxCapacity < 1 {
		maxCapacity = 1
	}
	return &MemoryStore{
		maxCapacity:    maxCapacity,
		seismicEvents:  make([]storedSeismicEvent, 0),
		tsunamiRecords: make([]warningRecord, 0),
	}
}

// AddSeismicEvent inserts a seismic event into the store in chronological order.
func (s *MemoryStore) AddSeismicEvent(evt model.SeismicEvent) {
	occurredAt, _ := parseTimestamp(evt.OccurredAt)

	s.mu.Lock()
	defer s.mu.Unlock()

	idx := sort.Search(len(s.seismicEvents), func(i int) bool {
		return !s.seismicEvents[i].occurredAt.Before(occurredAt)
	})
	s.seismicEvents = append(s.seismicEvents, storedSeismicEvent{})
	copy(s.seismicEvents[idx+1:], s.seismicEvents[idx:])
	s.seismicEvents[idx] = storedSeismicEvent{event: evt, occurredAt: occurredAt}

	s.seismicEvents = evictOldest(s.seismicEvents, s.maxCapacity)
}

// GetSeismicEvents retrieves seismic events occurred on or after 'since'.
// If 'since' is nil, events are returned in chronological order up to 'limit'.
func (s *MemoryStore) GetSeismicEvents(since *time.Time, limit int) []model.SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	start := 0
	if since != nil {
		start = sort.Search(len(s.seismicEvents), func(i int) bool {
			return !s.seismicEvents[i].occurredAt.Before(*since)
		})
	}

	remaining := len(s.seismicEvents) - start
	if limit > remaining {
		limit = remaining
	}

	result := make([]model.SeismicEvent, 0, limit)
	for i := start; i < start+limit; i++ {
		result = append(result, s.seismicEvents[i].event)
	}
	return result
}

// AddTsunamiWarning inserts a tsunami warning into the store in chronological order.
func (s *MemoryStore) AddTsunamiWarning(warn model.TsunamiWarning, occurredAt time.Time) {
	var arrivalAt time.Time
	if t, err := time.Parse(time.RFC3339, warn.EstimatedArrival); err == nil {
		arrivalAt = t
	} else if t, err := time.Parse(time.RFC3339Nano, warn.EstimatedArrival); err == nil {
		arrivalAt = t
	} else {
		arrivalAt = occurredAt
	}

	rec := warningRecord{warning: warn, occurredAt: occurredAt, arrivalAt: arrivalAt}

	s.mu.Lock()
	defer s.mu.Unlock()

	idx := sort.Search(len(s.tsunamiRecords), func(i int) bool {
		return !s.tsunamiRecords[i].occurredAt.Before(rec.occurredAt)
	})
	s.tsunamiRecords = append(s.tsunamiRecords, warningRecord{})
	copy(s.tsunamiRecords[idx+1:], s.tsunamiRecords[idx:])
	s.tsunamiRecords[idx] = rec

	s.tsunamiRecords = evictOldest(s.tsunamiRecords, s.maxCapacity)
}

// GetTsunamiWarnings retrieves tsunami warnings occurring or arriving on or after 'since'.
// If 'since' is nil, warnings are returned in chronological order up to 'limit'.
func (s *MemoryStore) GetTsunamiWarnings(since *time.Time, limit int) []model.TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	records := s.tsunamiRecords
	if limit > len(records) {
		limit = len(records)
	}

	result := make([]model.TsunamiWarning, 0, limit)
	for i := 0; i < len(records); i++ {
		if len(result) >= limit {
			break
		}
		rec := &records[i]
		if since == nil || !rec.occurredAt.Before(*since) || !rec.arrivalAt.Before(*since) {
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

// parseTimestamp parses an RFC3339 or RFC3339Nano timestamp string.
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return t, err
}

// evictOldest caps slice length to maxCapacity, discarding the oldest elements.
func evictOldest[T any](slice []T, maxCapacity int) []T {
	if len(slice) <= maxCapacity {
		return slice
	}
	drop := len(slice) - maxCapacity
	for i := 0; i < drop; i++ {
		var zero T
		slice[i] = zero
	}
	return slice[drop:]
}
