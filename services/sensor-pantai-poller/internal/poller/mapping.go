package poller

import (
	"math"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

// ObservationKey identifies one observation: its ID together with its time.
// A sensor that restarts may reuse IDs for different observations; the time
// keeps them apart, while an identical re-send still has the same key.
func ObservationKey(o sensorpantai.Observation) string {
	return o.ObservationID + "@" + o.ObservedAt.UTC().Format(time.RFC3339)
}

// Thresholds turn measured values into a severity. A critical flag from the
// sensor is always AWAS.
type Thresholds struct {
	SiagaAnomalyM   float64
	SiagaWaveM      float64
	WaspadaAnomalyM float64
	WaspadaWaveM    float64
}

// DefaultThresholds: tsunami-like sea-level anomalies and storm waves.
func DefaultThresholds() Thresholds {
	return Thresholds{SiagaAnomalyM: 1.0, SiagaWaveM: 4.0, WaspadaAnomalyM: 0.5, WaspadaWaveM: 2.5}
}

// Severity of one valid observation.
func (t Thresholds) Severity(o sensorpantai.Observation) hazard.Severity {
	a := math.Abs(o.SeaLevelAnomalyM)
	switch {
	case o.Critical:
		return hazard.SeverityAwas
	case a >= t.SiagaAnomalyM || o.WaveHeightM >= t.SiagaWaveM:
		return hazard.SeveritySiaga
	case a >= t.WaspadaAnomalyM || o.WaveHeightM >= t.WaspadaWaveM:
		return hazard.SeverityWaspada
	default:
		return hazard.SeverityNormal
	}
}

func areaName(st sensorpantai.Station) string {
	if st.Province == "" {
		return st.Name
	}
	return st.Name + ", " + st.Province
}

func attributes(o sensorpantai.Observation, generatedAt, detectedAt time.Time) map[string]any {
	return map[string]any{
		"observation_id":      o.ObservationID,
		"station_id":          o.StationID,
		"sea_level_anomaly_m": o.SeaLevelAnomalyM,
		"wave_height_m":       o.WaveHeightM,
		"quality":             o.Quality,
		"critical":            o.Critical,
		"sensor_generated_at": generatedAt.UTC().Format(time.RFC3339),
		event.AttrDetectedAt:  detectedAt.UTC().Format(time.RFC3339Nano),
	}
}

// StationIngest is the per-station coastal condition: one HazardEvent per
// station (source_ref_id "station:<id>"), replaced by each newer observation.
// source_version is observed_at in milliseconds, so the aggregator's version
// guard ignores late or repeated observations.
func StationIngest(o sensorpantai.Observation, st sensorpantai.Station, sev hazard.Severity, generatedAt, detectedAt time.Time, traceID string) event.IngestMessage {
	return event.IngestMessage{
		MessageID:     "sp:" + ObservationKey(o) + ":station",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   "station:" + o.StationID,
		SourceVersion: o.ObservedAt.UnixMilli(),
		HazardType:    hazard.HazardTypeCoastal,
		Severity:      sev,
		AreaName:      areaName(st),
		Latitude:      st.Latitude,
		Longitude:     st.Longitude,
		OccurredAt:    o.ObservedAt.UTC().Format(time.RFC3339),
		Attributes:    attributes(o, generatedAt, detectedAt),
		TraceID:       traceID,
	}
}

// CriticalIngest is the HazardEvent of one critical observation; it is kept
// even after the station's condition returns to normal.
func CriticalIngest(o sensorpantai.Observation, st sensorpantai.Station, generatedAt, detectedAt time.Time, traceID string) event.IngestMessage {
	return event.IngestMessage{
		MessageID:     "sp:" + ObservationKey(o) + ":critical",
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSensorPantai,
		SourceRefID:   ObservationKey(o),
		SourceVersion: 1,
		HazardType:    hazard.HazardTypeCoastal,
		Severity:      hazard.SeverityAwas,
		AreaName:      areaName(st),
		Latitude:      st.Latitude,
		Longitude:     st.Longitude,
		OccurredAt:    o.ObservedAt.UTC().Format(time.RFC3339),
		Attributes:    attributes(o, generatedAt, detectedAt),
		TraceID:       traceID,
	}
}

// CriticalAlert is the priority notification for a critical observation. Its
// EventID equals the hazard_id of CriticalIngest's HazardEvent with version 1,
// so the notification consumer delivers it once whichever path arrives first.
func CriticalAlert(o sensorpantai.Observation, st sensorpantai.Station, generatedAt, detectedAt time.Time, traceID string) event.Envelope {
	id := hazard.ID(hazard.SourceSensorPantai, ObservationKey(o))
	occurred := o.ObservedAt.UTC().Format(time.RFC3339)
	return event.Envelope{
		EventID:    id,
		Version:    1,
		EventType:  hazard.HazardTypeCoastal,
		OccurredAt: occurred,
		TraceID:    traceID,
		Data: hazard.HazardEvent{
			HazardID:    id,
			Source:      hazard.SourceSensorPantai,
			HazardType:  hazard.HazardTypeCoastal,
			Severity:    hazard.SeverityAwas,
			AreaName:    areaName(st),
			OccurredAt:  occurred,
			IngestedAt:  detectedAt.UTC().Format(time.RFC3339),
			SourceRefID: ObservationKey(o),
			Latitude:    st.Latitude,
			Longitude:   st.Longitude,
			Attributes:  attributes(o, generatedAt, detectedAt),
		},
	}
}
