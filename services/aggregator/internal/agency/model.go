// Package agency defines the aggregator's internal ingestion models for
// upstream agency responses (BMKG seismic events and PVMBG volcanic reports).
//
// These models use the tolerant reader pattern: known fields are decoded into
// typed struct fields, while unknown/additive fields (e.g. future schema
// extensions like confidence_level or any new field introduced by an agency)
// are captured dynamically into the Extra map and routed into HazardEvent
// attributes without crashing or losing data.
//
// These structs are local to the aggregator and are NOT shared with the
// upstream mock services, preserving service independence per
// Specification Ketentuan #1.
package agency

import "encoding/json"

// SeismicEvent is the ingestion model for BMKG seismic events.
type SeismicEvent struct {
	EventID          string  `json:"event_id"`
	Magnitude        float64 `json:"magnitude"`
	DepthKm          float64 `json:"depth_km"`
	EpicenterLat     float64 `json:"epicenter_lat"`
	EpicenterLon     float64 `json:"epicenter_lon"`
	RegionName       string  `json:"region_name"`
	OccurredAt       string  `json:"occurred_at"` // ISO 8601
	PotentialTsunami bool    `json:"potential_tsunami"`
}

// TsunamiThreatLevel is the threat severity enum from BMKG.
type TsunamiThreatLevel string

const (
	ThreatNormal  TsunamiThreatLevel = "Normal"
	ThreatWaspada TsunamiThreatLevel = "Waspada"
	ThreatSiaga   TsunamiThreatLevel = "Siaga"
	ThreatAwas    TsunamiThreatLevel = "Awas"
)

// TsunamiWarning is the ingestion model for BMKG tsunami warnings.
type TsunamiWarning struct {
	WarningID        string             `json:"warning_id"`
	RelatedEventID   string             `json:"related_event_id"`
	ThreatLevel      TsunamiThreatLevel `json:"threat_level"`
	AffectedZones    []string           `json:"affected_zones"`
	EstimatedArrival string             `json:"estimated_arrival"` // ISO 8601
}

// VolcanicAlertLevel is the activity level enum from PVMBG.
type VolcanicAlertLevel string

const (
	AlertNormal  VolcanicAlertLevel = "Normal"
	AlertWaspada VolcanicAlertLevel = "Waspada"
	AlertSiaga   VolcanicAlertLevel = "Siaga"
	AlertAwas    VolcanicAlertLevel = "Awas"
)

// VolcanicReport is the ingestion model for PVMBG volcanic reports.
// It uses a tolerant reader pattern: known fields are decoded into typed
// fields, while unknown/additive fields are captured into Extra as raw JSON
// so they can be merged into HazardEvent.Attributes without crashing or
// losing data. confidence_level is a pointer because it may be absent before
// schema-version is toggled.
type VolcanicReport struct {
	ReportID         string             `json:"report_id"`
	VolcanoID        string             `json:"volcano_id"`
	AlertLevel       VolcanicAlertLevel `json:"alert_level"`
	EruptionCount24h int                `json:"eruption_count_24h"`
	AshColumnHeightM float64            `json:"ash_column_height_m"`
	ReportedAt       string             `json:"reported_at"` // ISO 8601
	ConfidenceLevel  *float64           `json:"confidence_level,omitempty"`

	// Extra holds any unknown/additive fields decoded from the upstream JSON
	// that are not part of the typed struct above. These are merged into
	// HazardEvent.Attributes by the normalizer.
	Extra map[string]json.RawMessage `json:"-"`
}

// knownVolcanicFields lists JSON keys that are explicitly typed fields on
// VolcanicReport. Any key not in this set is treated as additive and stored
// in Extra.
var knownVolcanicFields = map[string]struct{}{
	"report_id":           {},
	"volcano_id":          {},
	"alert_level":         {},
	"eruption_count_24h":  {},
	"ash_column_height_m": {},
	"reported_at":         {},
	"confidence_level":    {},
}

// UnmarshalJSON implements the tolerant reader pattern for VolcanicReport.
// Known fields are decoded into typed struct fields; unknown/additive fields
// are captured into Extra as raw JSON so they can be forwarded into
// attributes without crashing or losing data.
func (r *VolcanicReport) UnmarshalJSON(data []byte) error {
	type alias VolcanicReport
	aux := alias{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*r = VolcanicReport(aux)

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for k := range knownVolcanicFields {
		delete(raw, k)
	}
	if len(raw) > 0 {
		r.Extra = raw
	}
	return nil
}
