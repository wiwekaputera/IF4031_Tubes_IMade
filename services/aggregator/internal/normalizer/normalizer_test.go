package normalizer

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func newTestNormalizer() *Normalizer {
	refs := map[string]store.VolcanoReference{
		"GNG-001": {VolcanoID: "GNG-001", Name: "Gunung Merapi", Latitude: -7.5407, Longitude: 110.4457},
		"GNG-002": {VolcanoID: "GNG-002", Name: "Gunung Semeru", Latitude: -8.1080, Longitude: 112.9220},
	}
	return New(zerolog.Nop(), refs)
}

func TestSeismicSeverity(t *testing.T) {
	tests := []struct {
		name       string
		magnitude  float64
		hasTsunami bool
		threat     agency.TsunamiThreatLevel
		expected   hazard.Severity
	}{
		{"tsunami awas", 7.0, true, agency.ThreatAwas, hazard.SeverityAwas},
		{"tsunami siaga", 6.5, true, agency.ThreatSiaga, hazard.SeveritySiaga},
		{"tsunami waspada", 6.0, true, agency.ThreatWaspada, hazard.SeverityWaspada},
		{"no tsunami mag<5", 4.5, false, "", hazard.SeverityNormal},
		{"no tsunami mag 5-6.5", 5.5, false, "", hazard.SeverityWaspada},
		{"no tsunami mag>=6.5", 7.0, false, "", hazard.SeveritySiaga},
		{"boundary mag=5.0", 5.0, false, "", hazard.SeverityWaspada},
		{"boundary mag=6.5", 6.5, false, "", hazard.SeveritySiaga},
		{"boundary mag=4.9", 4.9, false, "", hazard.SeverityNormal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SeismicSeverity(tc.magnitude, tc.hasTsunami, tc.threat)
			if got != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestVolcanicSeverity(t *testing.T) {
	tests := []struct {
		level    agency.VolcanicAlertLevel
		expected hazard.Severity
	}{
		{agency.AlertNormal, hazard.SeverityNormal},
		{agency.AlertWaspada, hazard.SeverityWaspada},
		{agency.AlertSiaga, hazard.SeveritySiaga},
		{agency.AlertAwas, hazard.SeverityAwas},
	}

	for _, tc := range tests {
		t.Run(string(tc.level), func(t *testing.T) {
			got := VolcanicSeverity(tc.level)
			if got != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestNormalizeSeismic(t *testing.T) {
	n := newTestNormalizer()

	evt := agency.SeismicEvent{
		EventID:          "evt-test-001",
		Magnitude:        7.5,
		DepthKm:          10.0,
		EpicenterLat:     -0.8988,
		EpicenterLon:     119.8474,
		RegionName:       "Sulawesi Tengah (Palu)",
		OccurredAt:       "2018-09-28T10:02:43Z",
		PotentialTsunami: true,
	}
	warning := &agency.TsunamiWarning{
		WarningID:        "warn-test-001",
		RelatedEventID:   "evt-test-001",
		ThreatLevel:      agency.ThreatSiaga,
		AffectedZones:    []string{"Palu", "Donggala"},
		EstimatedArrival: "2018-09-28T10:25:00Z",
	}

	rec := n.NormalizeSeismic(evt, warning)

	if rec.Source != string(hazard.SourceBMKG) {
		t.Errorf("expected source BMKG, got %s", rec.Source)
	}
	if rec.SourceRefID != "evt-test-001" {
		t.Errorf("expected source_ref_id evt-test-001, got %s", rec.SourceRefID)
	}
	if rec.HazardType != hazard.HazardTypeSeismic {
		t.Errorf("expected hazard_type SEISMIC, got %s", rec.HazardType)
	}
	if rec.Severity != hazard.SeveritySiaga {
		t.Errorf("expected severity SIAGA, got %s", rec.Severity)
	}
	if rec.AreaName != "Sulawesi Tengah (Palu)" {
		t.Errorf("expected area_name, got %s", rec.AreaName)
	}
	if rec.Latitude != -0.8988 {
		t.Errorf("expected latitude -0.8988, got %f", rec.Latitude)
	}
	if rec.Longitude != 119.8474 {
		t.Errorf("expected longitude 119.8474, got %f", rec.Longitude)
	}
	if rec.HazardID == "" {
		t.Error("expected non-empty hazard_id")
	}

	attrs := rec.Attributes
	if attrs["magnitude"].(float64) != 7.5 {
		t.Errorf("expected magnitude 7.5, got %v", attrs["magnitude"])
	}
	if attrs["depth_km"].(float64) != 10.0 {
		t.Errorf("expected depth_km 10.0, got %v", attrs["depth_km"])
	}
	if attrs["potential_tsunami"].(bool) != true {
		t.Errorf("expected potential_tsunami true, got %v", attrs["potential_tsunami"])
	}
	tw, ok := attrs["tsunami_warning"]
	if !ok {
		t.Error("expected tsunami_warning in attributes")
	}
	_ = tw
}

func TestNormalizeSeismicNoTsunami(t *testing.T) {
	n := newTestNormalizer()

	evt := agency.SeismicEvent{
		EventID:          "evt-test-002",
		Magnitude:        6.7,
		DepthKm:          15.0,
		EpicenterLat:     -1.4,
		EpicenterLon:     119.8,
		RegionName:       "Sulawesi Tengah",
		OccurredAt:       "2026-09-21T20:00:00Z",
		PotentialTsunami: false,
	}

	rec := n.NormalizeSeismic(evt, nil)

	if rec.Severity != hazard.SeveritySiaga {
		t.Errorf("expected SIAGA for mag>=6.5 no tsunami, got %s", rec.Severity)
	}

	attrs := rec.Attributes
	if _, ok := attrs["tsunami_warning"]; ok {
		t.Error("expected no tsunami_warning in attributes")
	}
}

func TestNormalizeVolcanic(t *testing.T) {
	n := newTestNormalizer()

	cl := 0.85
	report := agency.VolcanicReport{
		ReportID:         "rep-test-001",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 3,
		AshColumnHeightM: 5000.0,
		ReportedAt:       "2026-09-21T22:40:00Z",
		ConfidenceLevel:  &cl,
	}

	rec := n.NormalizeVolcanic(report)

	if rec.Source != string(hazard.SourcePVMBG) {
		t.Errorf("expected source PVMBG, got %s", rec.Source)
	}
	if rec.SourceRefID != "rep-test-001" {
		t.Errorf("expected source_ref_id rep-test-001, got %s", rec.SourceRefID)
	}
	if rec.HazardType != hazard.HazardTypeVolcanic {
		t.Errorf("expected hazard_type VOLCANIC, got %s", rec.HazardType)
	}
	if rec.Severity != hazard.SeveritySiaga {
		t.Errorf("expected severity SIAGA, got %s", rec.Severity)
	}
	if rec.AreaName != "Gunung Merapi" {
		t.Errorf("expected area_name Gunungs Merapi, got %s", rec.AreaName)
	}
	if rec.Latitude != -7.5407 {
		t.Errorf("expected latitude -7.5407, got %f", rec.Latitude)
	}
	if rec.Longitude != 110.4457 {
		t.Errorf("expected longitude 110.4457, got %f", rec.Longitude)
	}

	attrs := rec.Attributes
	if attrs["eruption_count_24h"].(int) != 3 {
		t.Errorf("expected eruption_count_24h 3, got %v", attrs["eruption_count_24h"])
	}
	if attrs["ash_column_height_m"].(float64) != 5000.0 {
		t.Errorf("expected ash_column_height_m 5000.0, got %v", attrs["ash_column_height_m"])
	}
	if attrs["confidence_level"].(float64) != 0.85 {
		t.Errorf("expected confidence_level 0.85, got %v", attrs["confidence_level"])
	}
}

func TestNormalizeVolcanicUnknownVolcano(t *testing.T) {
	n := newTestNormalizer()

	report := agency.VolcanicReport{
		ReportID:         "rep-test-002",
		VolcanoID:        "GNG-999",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0.0,
		ReportedAt:       "2026-09-21T22:40:00Z",
		ConfidenceLevel:  nil,
	}

	rec := n.NormalizeVolcanic(report)

	if rec.AreaName != "Unknown (volcano_id: GNG-999)" {
		t.Errorf("expected Unknown area name, got %s", rec.AreaName)
	}
	if rec.Latitude != 0.0 {
		t.Errorf("expected latitude 0.0 for unknown volcano, got %f", rec.Latitude)
	}
	if rec.Longitude != 0.0 {
		t.Errorf("expected longitude 0.0 for unknown volcano, got %f", rec.Longitude)
	}
	if rec.Severity != hazard.SeverityNormal {
		t.Errorf("expected NORMAL severity, got %s", rec.Severity)
	}

	if _, ok := rec.Attributes["confidence_level"]; ok {
		t.Error("expected no confidence_level in attributes when nil")
	}
}
