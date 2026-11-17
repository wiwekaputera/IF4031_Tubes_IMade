package event

import (
	"errors"
	"fmt"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// JetStream names for the M2 ingest pipeline (non-aggregator sources -> aggregator).
const (
	IngestStreamName    = "HAZARD_INGEST"
	IngestSubjectPrefix = "hazard.ingest."
	IngestSubjectFilter = "hazard.ingest.>"
)

// IngestSubject returns the JetStream subject for ingesting events from source s.
func IngestSubject(s hazard.Source) string {
	return IngestSubjectPrefix + string(s)
}

// IngestOp identifies the mutation kind carried by an IngestMessage.
type IngestOp string

const (
	IngestOpUpsert IngestOp = "upsert"
	IngestOpDelete IngestOp = "delete"
)

// IngestMessage is published to hazard.ingest.<source> by source pollers/ingestors
// that do not have direct database access.
//
// Semantics:
//   - Full replace: an upsert replaces all mutable fields of the canonical event.
//   - Version guard: applied only when source_version > stored source_version;
//     stale or duplicate deliveries are no-ops.
//   - Soft delete: op="delete" marks the canonical event as deleted (deleted_at)
//     when source_version > stored source_version.
type IngestMessage struct {
	MessageID     string            `json:"message_id"`
	Op            IngestOp          `json:"op"`
	Source        hazard.Source     `json:"source"`
	SourceRefID   string            `json:"source_ref_id"`
	SourceVersion int64             `json:"source_version"`
	HazardType    hazard.HazardType `json:"hazard_type,omitempty"`
	Severity      hazard.Severity   `json:"severity,omitempty"`
	AreaName      string            `json:"area_name,omitempty"`
	Latitude      float64           `json:"latitude,omitempty"`
	Longitude     float64           `json:"longitude,omitempty"`
	OccurredAt    string            `json:"occurred_at,omitempty"`
	Attributes    map[string]any    `json:"attributes,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
}

// Validate checks that the IngestMessage has all required fields and valid values.
// For op=delete, only source, source_ref_id, and source_version are required.
func (m IngestMessage) Validate() error {
	if !m.Source.Valid() {
		return fmt.Errorf("invalid source %q", m.Source)
	}
	if m.SourceRefID == "" {
		return errors.New("source_ref_id is required")
	}
	if m.SourceVersion <= 0 {
		return errors.New("source_version must be > 0")
	}

	switch m.Op {
	case IngestOpDelete:
		return nil
	case IngestOpUpsert:
		if m.MessageID == "" {
			return errors.New("message_id is required")
		}
		if !m.HazardType.Valid() {
			return fmt.Errorf("invalid hazard_type %q", m.HazardType)
		}
		if !m.Severity.Valid() {
			return fmt.Errorf("invalid severity %q", m.Severity)
		}
		if m.AreaName == "" {
			return errors.New("area_name is required")
		}
		if m.Latitude < -90 || m.Latitude > 90 {
			return fmt.Errorf("latitude %v out of range [-90, 90]", m.Latitude)
		}
		if m.Longitude < -180 || m.Longitude > 180 {
			return fmt.Errorf("longitude %v out of range [-180, 180]", m.Longitude)
		}
		if _, err := time.Parse(time.RFC3339, m.OccurredAt); err != nil {
			return fmt.Errorf("invalid occurred_at %q: %w", m.OccurredAt, err)
		}
		return nil
	default:
		return fmt.Errorf("invalid op %q", m.Op)
	}
}
