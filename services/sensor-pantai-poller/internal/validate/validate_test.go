package validate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

var (
	gen      = time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	stations = map[string]sensorpantai.Station{"stn-padang-01": {StationID: "stn-padang-01"}}
)

func item(over map[string]any, drop ...string) json.RawMessage {
	m := map[string]any{
		"observation_id":      "obs-1",
		"station_id":          "stn-padang-01",
		"observed_at":         "2026-11-12T02:59:45Z",
		"sea_level_anomaly_m": 0.42,
		"wave_height_m":       1.8,
		"quality":             "ok",
		"critical":            false,
	}
	for k, v := range over {
		m[k] = v
	}
	for _, k := range drop {
		delete(m, k)
	}
	b, _ := json.Marshal(m)
	return b
}

func TestValidObservation(t *testing.T) {
	obs, rej := Check(item(nil), stations, gen, DefaultLimits())
	if rej != nil {
		t.Fatalf("unexpected rejection %v", rej)
	}
	if obs.ObservationID != "obs-1" || obs.WaveHeightM != 1.8 || obs.ObservedAt.Minute() != 59 {
		t.Fatalf("decoded %+v", obs)
	}
}

func TestRejections(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
		code string
	}{
		{"not an object", json.RawMessage(`[1,2]`), "malformed"},
		{"string item", json.RawMessage(`"obs"`), "malformed"},
		{"missing field", item(nil, "wave_height_m"), "missing_field"},
		{"null field", item(map[string]any{"quality": nil}), "missing_field"},
		{"empty id", item(map[string]any{"observation_id": " "}), "missing_field"},
		{"number as string", item(map[string]any{"sea_level_anomaly_m": "0.4"}), "wrong_type"},
		{"critical as string", item(map[string]any{"critical": "true"}), "wrong_type"},
		{"id as number", item(map[string]any{"observation_id": 7}), "wrong_type"},
		{"bad time", item(map[string]any{"observed_at": "12/11/2026"}), "invalid_time"},
		{"unknown station", item(map[string]any{"station_id": "stn-x"}), "unknown_station"},
		{"expired", item(map[string]any{"observed_at": "2026-11-12T00:30:00Z"}), "expired"},
		{"future", item(map[string]any{"observed_at": "2026-11-12T03:05:00Z"}), "future_time"},
		{"anomaly range", item(map[string]any{"sea_level_anomaly_m": 99.0}), "out_of_range"},
		{"negative wave", item(map[string]any{"wave_height_m": -1.0}), "out_of_range"},
		{"suspect", item(map[string]any{"quality": "suspect"}), "quality_not_ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rej := Check(tc.raw, stations, gen, DefaultLimits())
			if rej == nil || rej.Code != tc.code {
				t.Fatalf("got %v, want code %s", rej, tc.code)
			}
			if rej.Detail == "" {
				t.Fatal("a rejection must explain itself")
			}
		})
	}
}

func TestRejectionKeepsIDWhenKnown(t *testing.T) {
	_, rej := Check(item(map[string]any{"quality": "suspect"}), stations, gen, DefaultLimits())
	if rej.ObservationID != "obs-1" || !strings.HasPrefix(rej.Error(), "quality_not_ok: ") {
		t.Fatalf("rejection %+v", rej)
	}
}

func TestTimesComparedWithSensorClockNotLocalClock(t *testing.T) {
	// The sensor's clock may be days away from BNPB's; only generated_at matters.
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	raw := item(map[string]any{"observed_at": "2019-12-31T23:59:00Z"})
	if _, rej := Check(raw, stations, old, DefaultLimits()); rej != nil {
		t.Fatalf("unexpected rejection %v", rej)
	}
}
