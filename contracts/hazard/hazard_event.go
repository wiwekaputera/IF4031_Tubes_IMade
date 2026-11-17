package hazard

// Source identifies the upstream agency that produced the event.
type Source string

const (
	SourceBMKG         Source = "BMKG"
	SourcePVMBG        Source = "PVMBG"
	SourceSatelit      Source = "SATELIT"
	SourceSensorPantai Source = "SENSOR_PANTAI"
)

// Valid reports whether s is a known upstream source.
func (s Source) Valid() bool {
	switch s {
	case SourceBMKG, SourcePVMBG, SourceSatelit, SourceSensorPantai:
		return true
	default:
		return false
	}
}

// HazardType classifies the kind of disaster.
type HazardType string

const (
	HazardTypeSeismic  HazardType = "SEISMIC"
	HazardTypeVolcanic HazardType = "VOLCANIC"
	HazardTypeWildfire HazardType = "WILDFIRE"
	HazardTypeCoastal  HazardType = "COASTAL"
)

// Valid reports whether t is a known hazard type.
func (t HazardType) Valid() bool {
	switch t {
	case HazardTypeSeismic, HazardTypeVolcanic, HazardTypeWildfire, HazardTypeCoastal:
		return true
	default:
		return false
	}
}

// Severity is the unified threat level used across the canonical format.
type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// Valid reports whether s is a known severity level.
func (s Severity) Valid() bool {
	switch s {
	case SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas:
		return true
	default:
		return false
	}
}

// HazardEvent is the canonical representation of a disaster event stored by BNPB.
// Fields classified as "Ringkasan" are visible to all client scopes.
// Fields classified as "Mentah" are restricted to field_team and internal_ops scopes.
type HazardEvent struct {
	// Ringkasan fields
	HazardID   string     `json:"hazard_id"`
	Source     Source     `json:"source"`
	HazardType HazardType `json:"hazard_type"`
	Severity   Severity   `json:"severity"`
	AreaName   string     `json:"area_name"`
	OccurredAt string     `json:"occurred_at"`
	IngestedAt string     `json:"ingested_at"`

	// Mentah fields (omitted for media scope)
	SourceRefID string                 `json:"source_ref_id,omitempty"`
	Latitude    float64                `json:"latitude"`
	Longitude   float64                `json:"longitude"`
	Attributes  map[string]interface{} `json:"attributes,omitempty"`
}
