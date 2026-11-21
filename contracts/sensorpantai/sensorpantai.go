// Package sensorpantai holds the coastal sensor's API types and the status the
// BNPB sensor poller publishes for other services.
package sensorpantai

import "time"

// Station is one item of GET /v1/stations.
type Station struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// StationsResponse is the body of GET /v1/stations.
type StationsResponse struct {
	Items []Station `json:"items"`
}

// Observation is one well-formed item of GET /v1/observations, after the
// poller checked its fields and types.
type Observation struct {
	ObservationID    string    `json:"observation_id"`
	StationID        string    `json:"station_id"`
	ObservedAt       time.Time `json:"observed_at"`
	SeaLevelAnomalyM float64   `json:"sea_level_anomaly_m"`
	WaveHeightM      float64   `json:"wave_height_m"`
	Quality          string    `json:"quality"`
	Critical         bool      `json:"critical"`
}

// JetStream key-value bucket where the poller keeps its state.
const (
	StateBucket = "SENSOR_PANTAI_STATE"
	KeyCursor   = "cursor"
	KeyStatus   = "status"
)

// Source status values, aligned with the BMKG/PVMBG ones in query responses.
const (
	StatusUp       = "UP"
	StatusDegraded = "DEGRADED"
	StatusOutage   = "OUTAGE"
	StatusDown     = "DOWN"
)

// Status is published by the poller after every poll attempt.
type Status struct {
	Status string `json:"status"`
	// Breaker is "closed", "open" or "half_open".
	Breaker string `json:"breaker"`
	// LastSyncedAt is the wall-clock time of the last successful poll.
	LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
	// StaleSince is set while the sensor is unavailable or serves stale data.
	StaleSince *time.Time `json:"stale_since,omitempty"`
	// SensorTime is the sensor's own generated_at of the last good response.
	SensorTime *time.Time `json:"sensor_time,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
}
