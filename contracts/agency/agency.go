// Package agency defines the upstream agency data models (BMKG and PVMBG).
// This package contains ONLY struct definitions and JSON tags — no business logic.
package agency

// SeismicEvent is the raw data model returned by the BMKG mock.
type SeismicEvent struct {
	EventID         string  `json:"event_id"`
	Magnitude       float64 `json:"magnitude"`
	DepthKm         float64 `json:"depth_km"`
	EpicenterLat    float64 `json:"epicenter_lat"`
	EpicenterLon    float64 `json:"epicenter_lon"`
	RegionName      string  `json:"region_name"`
	OccurredAt      string  `json:"occurred_at"` // ISO 8601
	PotentialTsunami bool   `json:"potential_tsunami"`
}

// TsunamiThreatLevel is the threat severity enum from BMKG.
type TsunamiThreatLevel string

const (
	ThreatWaspada TsunamiThreatLevel = "Waspada"
	ThreatSiaga   TsunamiThreatLevel = "Siaga"
	ThreatAwas    TsunamiThreatLevel = "Awas"
)

// TsunamiWarning is the raw data model for tsunami alerts from BMKG.
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
	AlertNormal   VolcanicAlertLevel = "Normal"
	AlertWaspada  VolcanicAlertLevel = "Waspada"
	AlertSiaga    VolcanicAlertLevel = "Siaga"
	AlertAwas     VolcanicAlertLevel = "Awas"
)

// VolcanicReport is the raw data model returned by the PVMBG mock.
// ConfidenceLevel is a pointer because it may be absent before schema-version is toggled.
type VolcanicReport struct {
	ReportID           string             `json:"report_id"`
	VolcanoID          string             `json:"volcano_id"`
	AlertLevel         VolcanicAlertLevel `json:"alert_level"`
	EruptionCount24h   int                `json:"eruption_count_24h"`
	AshColumnHeightM   float64            `json:"ash_column_height_m"`
	ReportedAt         string             `json:"reported_at"` // ISO 8601
	ConfidenceLevel    *float64           `json:"confidence_level,omitempty"`
}
