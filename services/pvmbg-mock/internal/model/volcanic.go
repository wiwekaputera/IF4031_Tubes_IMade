// Package model defines the internal data models for the pvmbg-mock service.
package model

// VolcanicAlertLevel is the activity level enum from PVMBG.
type VolcanicAlertLevel string

const (
	AlertNormal  VolcanicAlertLevel = "Normal"
	AlertWaspada VolcanicAlertLevel = "Waspada"
	AlertSiaga   VolcanicAlertLevel = "Siaga"
	AlertAwas    VolcanicAlertLevel = "Awas"
)

// VolcanicReport is the raw data model returned by the PVMBG mock.
// ConfidenceLevel is a pointer because it may be absent before schema-version is toggled.
type VolcanicReport struct {
	ReportID         string             `json:"report_id"`
	VolcanoID        string             `json:"volcano_id"`
	AlertLevel       VolcanicAlertLevel `json:"alert_level"`
	EruptionCount24h int                `json:"eruption_count_24h"`
	AshColumnHeightM float64            `json:"ash_column_height_m"`
	ReportedAt       string             `json:"reported_at"` // ISO 8601
	ConfidenceLevel  *float64           `json:"confidence_level,omitempty"`
}
