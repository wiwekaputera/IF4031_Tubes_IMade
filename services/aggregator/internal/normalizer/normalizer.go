package normalizer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type Normalizer struct {
	logger      zerolog.Logger
	mu          sync.RWMutex
	volcanoRefs map[string]store.VolcanoReference
}

func New(logger zerolog.Logger, volcanoRefs map[string]store.VolcanoReference) *Normalizer {
	return &Normalizer{
		logger:      logger.With().Str("component", "normalizer").Logger(),
		volcanoRefs: volcanoRefs,
	}
}

func (n *Normalizer) UpdateVolcanoRefs(refs map[string]store.VolcanoReference) {
	n.mu.Lock()
	n.volcanoRefs = refs
	n.mu.Unlock()
}

func (n *Normalizer) NormalizeSeismic(event agency.SeismicEvent, warning *agency.TsunamiWarning) store.UpsertRequest {
	attributes := map[string]interface{}{
		"magnitude":         event.Magnitude,
		"depth_km":          event.DepthKm,
		"potential_tsunami": event.PotentialTsunami,
	}
	if warning != nil {
		attributes["tsunami_warning"] = warning
	}

	occurredAt, err := time.Parse(time.RFC3339, event.OccurredAt)
	if err != nil {
		n.logger.Warn().Err(err).Str("event_id", event.EventID).Msg("failed to parse occurred_at, falling back to now")
		occurredAt = time.Time{}
	}
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

	n.mu.RLock()
	ref, ok := n.volcanoRefs[volcanoID]
	n.mu.RUnlock()

	var areaName string
	var lat, lon float64
	if ok {
		areaName = ref.Name
		lat = ref.Latitude
		lon = ref.Longitude
	} else {
		areaName = fmt.Sprintf("Unknown (volcano_id: %s)", volcanoID)
		lat = 0.0
		lon = 0.0
		n.logger.Warn().Str("volcano_id", volcanoID).Msg("volcano_id not found in reference table")
	}

	attributes := map[string]interface{}{
		"eruption_count_24h":  report.EruptionCount24h,
		"ash_column_height_m": report.AshColumnHeightM,
	}
	if report.ConfidenceLevel != nil {
		attributes["confidence_level"] = *report.ConfidenceLevel
	}

	// Tolerant reader: capture any unknown/additive fields (stored as raw JSON
	// in Extra during unmarshaling) into attributes so they are preserved
	// without crashing or losing data.
	for k, raw := range report.Extra {
		var v interface{}
		if err := json.Unmarshal(raw, &v); err == nil {
			attributes[k] = v
		}
	}

	occurredAt, err := time.Parse(time.RFC3339, report.ReportedAt)
	if err != nil {
		n.logger.Warn().Err(err).Str("report_id", report.ReportID).Msg("failed to parse reported_at, falling back to now")
		occurredAt = time.Time{}
	}
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
