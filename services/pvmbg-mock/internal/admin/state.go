package admin

import "sync/atomic"

// OutageState holds the atomic outage flag for the PVMBG service.
// When outage is ON, /volcanic-reports returns 503 immediately without latency.
type OutageState struct {
	active atomic.Bool
}

// NewOutageState creates a new OutageState with outage OFF.
func NewOutageState() *OutageState {
	return &OutageState{}
}

// Toggle flips the outage state and returns the new value.
func (s *OutageState) Toggle() bool {
	for {
		cur := s.active.Load()
		if s.active.CompareAndSwap(cur, !cur) {
			return !cur
		}
	}
}

// IsActive returns whether outage is currently active.
func (s *OutageState) IsActive() bool {
	return s.active.Load()
}

// SchemaVersionState holds the atomic flag controlling whether
// confidence_level appears in volcanic report responses.
type SchemaVersionState struct {
	enabled atomic.Bool
}

// NewSchemaVersionState creates a new SchemaVersionState with the toggle OFF.
func NewSchemaVersionState() *SchemaVersionState {
	return &SchemaVersionState{}
}

// Toggle flips the schema version state and returns the new value.
func (s *SchemaVersionState) Toggle() bool {
	for {
		cur := s.enabled.Load()
		if s.enabled.CompareAndSwap(cur, !cur) {
			return !cur
		}
	}
}

// IsEnabled returns whether the schema version toggle is currently enabled.
func (s *SchemaVersionState) IsEnabled() bool {
	return s.enabled.Load()
}
