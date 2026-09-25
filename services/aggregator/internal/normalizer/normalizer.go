package normalizer

import (
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type Normalizer struct {
	logger      zerolog.Logger
	volcanoRefs map[string]store.VolcanoReference
}

func New(logger zerolog.Logger, volcanoRefs map[string]store.VolcanoReference) *Normalizer {
	return &Normalizer{
		logger:      logger.With().Str("component", "normalizer").Logger(),
		volcanoRefs: volcanoRefs,
	}
}

func (n *Normalizer) UpdateVolcanoRefs(refs map[string]store.VolcanoReference) {
	n.volcanoRefs = refs
}

func (n *Normalizer) NormalizeSeismic(event agency.SeismicEvent, warning *agency.TsunamiWarning) store.UpsertRequest {
	attributes := map[string]interface{}{
		"magnitude":          event.Magnitude,
		"depth_km":           event.DepthKm,
		"potential_tsunami":  event.PotentialTsunami,
	}
	if warning != nil {
		attributes["tsunami_warning"] = warning
	}

	occurredAt, _ := time.Parse(time.RFC3339, event.OccurredAt)
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	var sev hazard.Severity
	if warning != nil {
		sev = SeismicSeverity(event.Magnitude, true, warning.ThreatLevel)
	} else {
		sev = SeismicSeverity(event.Magnitude, false, "")
	}

	return store.UpsertRequest{
		HazardID:    uuid.New().String(),
		Source:      string(hazard.SourceBMKG),
		SourceRefID: event.EventID,
		HazardType:  hazard.HazardTypeSeismic,
		Severity:    sev,
		AreaName:    event.RegionName,
		Latitude:    event.EpicenterLat,
		Longitude:   event.EpicenterLon,
		OccurredAt:  occurredAt,
		Sources: map[string]interface{}{
			"bmkg": event,
		},
		Attributes: attributes,
	}
}

func (n *Normalizer) NormalizeVolcanic(report agency.VolcanicReport) store.UpsertRequest {
	volcanoID := report.VolcanoID
	ref, ok := n.volcanoRefs[volcanoID]

	var areaName string
	var lat, lon float64
	if ok {
		areaName = ref.Name
		lat = ref.Latitude
		lon = ref.Longitude
	} else {
		areaName = "Unknown (volcano_id: " + volcanoID + ")"
		lat = 0.0
		lon = 0.0
		n.logger.Warn().Str("volcano_id", volcanoID).Msg("volcano_id not found in reference table")
	}

	attributes := map[string]interface{}{
		"eruption_count_24h":   report.EruptionCount24h,
		"ash_column_height_m":  report.AshColumnHeightM,
	}
	if report.ConfidenceLevel != nil {
		attributes["confidence_level"] = *report.ConfidenceLevel
	}

	occurredAt, _ := time.Parse(time.RFC3339, report.ReportedAt)
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	return store.UpsertRequest{
		HazardID:    uuid.New().String(),
		Source:      string(hazard.SourcePVMBG),
		SourceRefID: report.ReportID,
		HazardType:  hazard.HazardTypeVolcanic,
		Severity:    VolcanicSeverity(report.AlertLevel),
		AreaName:    areaName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  occurredAt,
		Sources: map[string]interface{}{
			"pvmbg": report,
		},
		Attributes: attributes,
	}
}
