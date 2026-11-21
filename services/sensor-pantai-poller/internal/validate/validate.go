// Package validate checks coastal sensor observations against the base
// contract before anything is stored. Each rule is generic (field types,
// known stations, time consistency with the response's generated_at, physical
// ranges, quality flag); none depends on a particular ID, seed or order.
package validate

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

// Limits used by Check.
type Limits struct {
	// MaxAge: an observation older than the response's generated_at by more
	// than this is expired.
	MaxAge time.Duration
	// FutureSkew: an observation newer than generated_at by more than this is
	// rejected as dated in the future.
	FutureSkew time.Duration
	// Physical bounds for the measured values.
	MaxAbsSeaLevelAnomalyM float64
	MaxWaveHeightM         float64
}

// DefaultLimits are the values used unless configured otherwise.
func DefaultLimits() Limits {
	return Limits{
		MaxAge:                 2 * time.Hour,
		FutureSkew:             time.Minute,
		MaxAbsSeaLevelAnomalyM: 15,
		MaxWaveHeightM:         30,
	}
}

// Rejection explains why an item was set aside.
type Rejection struct {
	// Code is a short stable category, used as a metric label.
	Code string
	// Detail is the human-readable explanation stored with the item.
	Detail string
	// ObservationID is set when the item had a usable string ID.
	ObservationID string
}

func (r *Rejection) Error() string { return r.Code + ": " + r.Detail }

func reject(id, code, format string, args ...any) *Rejection {
	return &Rejection{Code: code, Detail: fmt.Sprintf(format, args...), ObservationID: id}
}

var requiredFields = []string{
	"observation_id", "station_id", "observed_at",
	"sea_level_anomaly_m", "wave_height_m", "quality", "critical",
}

// Check decodes one raw item of GET /v1/observations and applies every rule.
// generatedAt is the sensor's own clock from the same response; times are
// compared with it, not with BNPB's clock, because the sensor runs on its own
// (possibly accelerated) time.
func Check(raw json.RawMessage, stations map[string]sensorpantai.Station, generatedAt time.Time, lim Limits) (sensorpantai.Observation, *Rejection) {
	var obs sensorpantai.Observation
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return obs, reject("", "malformed", "item is not a JSON object")
	}

	var id string
	if v, ok := fields["observation_id"]; ok {
		_ = json.Unmarshal(v, &id)
	}
	for _, f := range requiredFields {
		v, ok := fields[f]
		if !ok || string(v) == "null" {
			return obs, reject(id, "missing_field", "required field %q is missing or null", f)
		}
	}

	str := func(name string) (string, *Rejection) {
		var s string
		if err := json.Unmarshal(fields[name], &s); err != nil {
			return "", reject(id, "wrong_type", "%s must be a string, got %s", name, truncate(fields[name]))
		}
		return s, nil
	}
	num := func(name string) (float64, *Rejection) {
		var f float64
		if err := json.Unmarshal(fields[name], &f); err != nil {
			return 0, reject(id, "wrong_type", "%s must be a number, got %s", name, truncate(fields[name]))
		}
		return f, nil
	}

	var rej *Rejection
	if obs.ObservationID, rej = str("observation_id"); rej != nil {
		return obs, rej
	}
	if strings.TrimSpace(obs.ObservationID) == "" {
		return obs, reject("", "missing_field", "observation_id is empty")
	}
	if obs.StationID, rej = str("station_id"); rej != nil {
		return obs, rej
	}
	observedAt, rej := str("observed_at")
	if rej != nil {
		return obs, rej
	}
	if obs.SeaLevelAnomalyM, rej = num("sea_level_anomaly_m"); rej != nil {
		return obs, rej
	}
	if obs.WaveHeightM, rej = num("wave_height_m"); rej != nil {
		return obs, rej
	}
	if obs.Quality, rej = str("quality"); rej != nil {
		return obs, rej
	}
	if err := json.Unmarshal(fields["critical"], &obs.Critical); err != nil {
		return obs, reject(id, "wrong_type", "critical must be a boolean, got %s", truncate(fields["critical"]))
	}

	t, err := time.Parse(time.RFC3339, observedAt)
	if err != nil {
		return obs, reject(id, "invalid_time", "observed_at %q is not RFC 3339", observedAt)
	}
	obs.ObservedAt = t.UTC()

	if _, ok := stations[obs.StationID]; !ok {
		return obs, reject(id, "unknown_station", "station_id %q is not in /v1/stations", obs.StationID)
	}
	if !generatedAt.IsZero() {
		if age := generatedAt.Sub(obs.ObservedAt); age > lim.MaxAge {
			return obs, reject(id, "expired", "observed_at %s is %s older than the sensor's generated_at %s (limit %s)",
				observedAt, age.Round(time.Second), generatedAt.UTC().Format(time.RFC3339), lim.MaxAge)
		}
		if ahead := obs.ObservedAt.Sub(generatedAt); ahead > lim.FutureSkew {
			return obs, reject(id, "future_time", "observed_at %s is %s after the sensor's generated_at %s",
				observedAt, ahead.Round(time.Second), generatedAt.UTC().Format(time.RFC3339))
		}
	}
	if bad(obs.SeaLevelAnomalyM) || math.Abs(obs.SeaLevelAnomalyM) > lim.MaxAbsSeaLevelAnomalyM {
		return obs, reject(id, "out_of_range", "sea_level_anomaly_m %v outside ±%v m", obs.SeaLevelAnomalyM, lim.MaxAbsSeaLevelAnomalyM)
	}
	if bad(obs.WaveHeightM) || obs.WaveHeightM < 0 || obs.WaveHeightM > lim.MaxWaveHeightM {
		return obs, reject(id, "out_of_range", "wave_height_m %v outside [0, %v] m", obs.WaveHeightM, lim.MaxWaveHeightM)
	}
	if obs.Quality != "ok" {
		return obs, reject(id, "quality_not_ok", "quality is %q; only \"ok\" observations are trusted", obs.Quality)
	}
	return obs, nil
}

func bad(f float64) bool { return math.IsNaN(f) || math.IsInf(f, 0) }

func truncate(b json.RawMessage) string {
	s := string(b)
	if len(s) > 40 {
		s = s[:40] + "…"
	}
	return s
}
