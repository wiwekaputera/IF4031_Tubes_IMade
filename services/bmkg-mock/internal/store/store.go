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

// storedSeismicEvent wraps a seismic event together with its pre-parsed
// occurrence timestamp, so time.Parse is never invoked on the read path.
type storedSeismicEvent struct {
	event      model.SeismicEvent
	occurredAt time.Time
}

type warningRecord struct {
	warning    model.TsunamiWarning
	occurredAt time.Time
	arrivalAt  time.Time
}

// MemoryStore is a thread-safe in-memory store for seismic events and tsunami
// warnings. Both collections are kept in chronological (ascending occurredAt)
// order, so read paths can locate records with a binary search and never need to
// sort a copy or re-parse timestamps.
type MemoryStore struct {
	mu             sync.RWMutex
	seismicEvents  []storedSeismicEvent
	tsunamiRecords []warningRecord
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		seismicEvents:  make([]storedSeismicEvent, 0),
		tsunamiRecords: make([]warningRecord, 0),
	}
}

// AddSeismicEvent adds a seismic event to the store in chronological order.
// The occurred_at timestamp is parsed exactly once, at insertion time.
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
}

// GetSeismicEvents retrieves seismic events occurred on or after 'since'.
// If 'since' is nil, all events are returned up to 'limit'.
// Results are returned in chronological (ascending occurredAt) order. No sorting
// or timestamp parsing is performed on the read path.
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

// AddTsunamiWarning adds a tsunami warning to the store in chronological order.
// The occurrence timestamp is supplied pre-parsed; the estimated arrival is parsed
// exactly once, at insertion time.
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
}

// GetTsunamiWarnings retrieves tsunami warnings matching the 'since' filter.
// If 'since' is nil, all warnings are returned up to 'limit'.
// A warning is included when it either occurred on or after 'since', or is still
// estimated to arrive on or after 'since'. Because arrival times are not strictly
// monotonic with occurrence times, the matching set is not a contiguous suffix, so a
// linear scan (no copy, no sort, no parse) is used to preserve the exact filter
// semantics. Results are returned in chronological (ascending occurredAt) order.
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

// parseTimestamp parses an ISO 8601 timestamp string, trying RFC3339 then RFC3339Nano.
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return t, err
}
