// Package model defines the internal data models for the bmkg-mock service.
package model

// SeismicEvent is the raw data model returned by the BMKG mock.
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

// TsunamiWarning is the raw data model for tsunami alerts from BMKG.
type TsunamiWarning struct {
	WarningID        string             `json:"warning_id"`
	RelatedEventID   string             `json:"related_event_id"`
	ThreatLevel      TsunamiThreatLevel `json:"threat_level"`
	AffectedZones    []string           `json:"affected_zones"`
	EstimatedArrival string             `json:"estimated_arrival"` // ISO 8601
}
