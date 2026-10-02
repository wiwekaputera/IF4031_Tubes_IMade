// Package agency defines ingestion models and unmarshaling logic for
// upstream agency payloads from BMKG (seismic/tsunami) and PVMBG (volcanic).
// Unrecognized attributes are captured dynamically for forward compatibility.
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

// VolcanicReport represents an upstream volcanic activity report from PVMBG.
// Unknown or schema-extended fields are captured into Extra during unmarshaling.
type VolcanicReport struct {
	ReportID         string             `json:"report_id"`
	VolcanoID        string             `json:"volcano_id"`
	AlertLevel       VolcanicAlertLevel `json:"alert_level"`
	EruptionCount24h int                `json:"eruption_count_24h"`
	AshColumnHeightM float64            `json:"ash_column_height_m"`
	ReportedAt       string             `json:"reported_at"` // ISO 8601
	ConfidenceLevel  *float64           `json:"confidence_level,omitempty"`

	// Extra holds unrecognized fields for forward compatibility.
	Extra map[string]json.RawMessage `json:"-"`
}

// knownVolcanicFields tracks recognized fields to identify additive attributes.
var knownVolcanicFields = map[string]struct{}{
	"report_id":           {},
	"volcano_id":          {},
	"alert_level":         {},
	"eruption_count_24h":  {},
	"ash_column_height_m": {},
	"reported_at":         {},
	"confidence_level":    {},
}

// UnmarshalJSON decodes known fields into the struct and captures remaining fields in Extra.
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
