// Package fire turns VIIRS hotspot detections from the satellite feed into fires
// (M2 P4). It is pure Go with no I/O so the Definisi Kebakaran rules can be tested directly.
package fire

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// Detection is one hotspot row from the satellite feed.
// Latitude and longitude keep the feed's raw text so detection keys are not rounded.
type Detection struct {
	Satellite  string      `json:"satellite"`
	AcqDate    string      `json:"acq_date"` // YYYY-MM-DD
	AcqTime    json.Number `json:"acq_time"` // HHMM, UTC; the feed sends a number, e.g. 2310 or 45
	Latitude   json.Number `json:"latitude"`
	Longitude  json.Number `json:"longitude"`
	Confidence string      `json:"confidence"`
	FRP        float64     `json:"frp"` // megawatts
}

// Key identifies a detection: satellite|acq_date|acq_time|latitude|longitude.
func (d Detection) Key() string {
	return strings.Join([]string{d.Satellite, d.AcqDate, d.AcqTime.String(), d.Latitude.String(), d.Longitude.String()}, "|")
}

// Time is the detection time: acq_date and acq_time combined in UTC.
// It is the satellite overpass time, not the time the row reached BNPB.
func (d Detection) Time() (time.Time, error) {
	t, err := time.Parse("2006-01-02 1504", d.AcqDate+" "+padTime(d.AcqTime.String()))
	if err != nil {
		return time.Time{}, fmt.Errorf("bad acquisition time %q %q: %w", d.AcqDate, d.AcqTime.String(), err)
	}
	return t.UTC(), nil
}

// Lat and Lon return the coordinates as floats.
func (d Detection) Lat() (float64, error) { return d.Latitude.Float64() }
func (d Detection) Lon() (float64, error) { return d.Longitude.Float64() }

// IsLowConfidence reports whether the detection must be excluded (confidence low).
// VIIRS codes are l, n, h; the word "low" is accepted as well.
func (d Detection) IsLowConfidence() bool {
	c := strings.ToLower(strings.TrimSpace(d.Confidence))
	return c == "l" || c == "low"
}

// padTime left-pads HHMM to four digits: "530" becomes "0530", "45" becomes "0045".
func padTime(s string) string {
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

// Validate checks the fields the rules depend on.
func (d Detection) Validate() error {
	if _, err := d.Time(); err != nil {
		return err
	}
	lat, err := d.Lat()
	if err != nil || math.IsNaN(lat) || lat < -90 || lat > 90 {
		return fmt.Errorf("bad latitude %q", d.Latitude)
	}
	lon, err := d.Lon()
	if err != nil || math.IsNaN(lon) || lon < -180 || lon > 180 {
		return fmt.Errorf("bad longitude %q", d.Longitude)
	}
	if d.Satellite == "" {
		return fmt.Errorf("missing satellite")
	}
	if d.FRP < 0 || math.IsNaN(d.FRP) {
		return fmt.Errorf("bad frp %v", d.FRP)
	}
	return nil
}
