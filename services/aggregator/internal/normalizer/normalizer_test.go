package normalizer

import (
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func newTestNormalizer() *Normalizer {
	return New(zerolog.Nop(), map[string]store.VolcanoReference{
		"GNG-001": {VolcanoID: "GNG-001", Name: "Gunung Merapi", Latitude: -7.5407, Longitude: 110.4457},
	})
}

func TestSeverityCalculations(t *testing.T) {
	seismicTests := []struct {
		mag      float64
		tsunami  bool
		threat   agency.TsunamiThreatLevel
		expected hazard.Severity
	}{
		{7.0, true, agency.ThreatAwas, hazard.SeverityAwas},
		{6.5, true, agency.ThreatSiaga, hazard.SeveritySiaga},
		{6.0, true, agency.ThreatWaspada, hazard.SeverityWaspada},
		{4.5, false, "", hazard.SeverityNormal},
		{5.5, false, "", hazard.SeverityWaspada},
		{7.0, false, "", hazard.SeveritySiaga},
	}
	for _, tc := range seismicTests {
		if got := SeismicSeverity(tc.mag, tc.tsunami, tc.threat); got != tc.expected {
			t.Errorf("SeismicSeverity(%v, %v, %v) = %s, want %s", tc.mag, tc.tsunami, tc.threat, got, tc.expected)
		}
	}

	volcanicTests := []struct {
		level    agency.VolcanicAlertLevel
		expected hazard.Severity
	}{
		{agency.AlertNormal, hazard.SeverityNormal},
		{agency.AlertWaspada, hazard.SeverityWaspada},
		{agency.AlertSiaga, hazard.SeveritySiaga},
		{agency.AlertAwas, hazard.SeverityAwas},
	}
	for _, tc := range volcanicTests {
		if got := VolcanicSeverity(tc.level); got != tc.expected {
			t.Errorf("VolcanicSeverity(%s) = %s, want %s", tc.level, got, tc.expected)
		}
	}
}

func TestNormalizeSeismic(t *testing.T) {
	n := newTestNormalizer()
	evt := agency.SeismicEvent{
		EventID:      "evt-001",
		Magnitude:    7.5,
		DepthKm:      10.0,
		EpicenterLat: -0.8988,
		EpicenterLon: 119.8474,
		RegionName:   "Palu",
		OccurredAt:   "2018-09-28T10:02:43Z",
	}
	warning := &agency.TsunamiWarning{
		WarningID:   "warn-001",
		ThreatLevel: agency.ThreatSiaga,
	}

	// With tsunami warning
	rec := n.NormalizeSeismic(evt, warning)
	if rec.Source != string(hazard.SourceBMKG) || rec.HazardType != hazard.HazardTypeSeismic || rec.Severity != hazard.SeveritySiaga {
		t.Fatalf("unexpected seismic record metadata: %+v", rec)
	}
	if rec.Attributes["magnitude"] != 7.5 || rec.Attributes["tsunami_warning"] == nil {
		t.Errorf("expected magnitude 7.5 and tsunami warning in attributes")
	}

	// Without tsunami warning
	evtNoTsunami := evt
	evtNoTsunami.Magnitude = 4.5
	recNoWarn := n.NormalizeSeismic(evtNoTsunami, nil)
	if recNoWarn.Severity != hazard.SeverityNormal || recNoWarn.Attributes["tsunami_warning"] != nil {
		t.Errorf("expected Normal severity and nil tsunami warning")
	}
}

func TestNormalizeVolcanic(t *testing.T) {
	n := newTestNormalizer()
	cl := 0.85
	report := agency.VolcanicReport{
		ReportID:         "rep-001",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 3,
		AshColumnHeightM: 5000.0,
		ReportedAt:       "2026-09-21T22:40:00Z",
		ConfidenceLevel:  &cl,
		Extra: map[string]json.RawMessage{
			"sensor_status": json.RawMessage(`"CALIBRATED"`),
			"wind_dir":      json.RawMessage(`"NE"`),
		},
	}

	rec := n.NormalizeVolcanic(report)
	if rec.Source != string(hazard.SourcePVMBG) || rec.HazardType != hazard.HazardTypeVolcanic || rec.Severity != hazard.SeveritySiaga {
		t.Fatalf("unexpected volcanic metadata: %+v", rec)
	}
	if rec.AreaName != "Gunung Merapi" || rec.Latitude != -7.5407 || rec.Longitude != 110.4457 {
		t.Errorf("reference resolution failed: %s (%f, %f)", rec.AreaName, rec.Latitude, rec.Longitude)
	}
	// Verify confidence level and unknown fields in attributes
	if rec.Attributes["confidence_level"] != 0.85 {
		t.Errorf("expected confidence_level 0.85, got %v", rec.Attributes["confidence_level"])
	}
	if rec.Attributes["sensor_status"] != "CALIBRATED" || rec.Attributes["wind_dir"] != "NE" {
		t.Errorf("expected extra unknown fields preserved in attributes, got %+v", rec.Attributes)
	}

	// Fallback for unknown volcano
	reportUnknown := report
	reportUnknown.VolcanoID = "GNG-UNKNOWN"
	recUnknown := n.NormalizeVolcanic(reportUnknown)
	if recUnknown.AreaName != "Unknown (volcano_id: GNG-UNKNOWN)" || recUnknown.Latitude != 0.0 {
		t.Errorf("expected fallback for unknown volcano, got %s", recUnknown.AreaName)
	}
}
