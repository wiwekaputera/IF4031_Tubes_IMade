package generator

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
)

// Generator dynamically produces realistic seismic events and tsunami warnings.
type Generator struct {
	store    store.Store
	interval time.Duration
	logger   zerolog.Logger
	rng      *rand.Rand
	counter  uint64
}

// NewGenerator creates a new dynamic event generator.
func NewGenerator(s store.Store, intervalS int, logger zerolog.Logger) *Generator {
	source := rand.NewSource(time.Now().UnixNano())
	return &Generator{
		store:    s,
		interval: time.Duration(intervalS) * time.Second,
		logger:   logger.With().Str("component", "generator").Logger(),
		rng:      rand.New(source),
	}
}

// Start runs the generator loop until the context is cancelled.
func (g *Generator) Start(ctx context.Context) {
	g.logger.Info().Dur("interval", g.interval).Msg("Starting dynamic seismic event generator")
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			g.logger.Info().Msg("Stopping dynamic seismic event generator")
			return
		case <-ticker.C:
			g.generateEvent()
		}
	}
}

func (g *Generator) generateEvent() {
	now := time.Now().UTC()
	seq := atomic.AddUint64(&g.counter, 1)
	// Example: seq=1 -> "evt-bmkg-dyn-0001"
	eventID := fmt.Sprintf("evt-bmkg-dyn-%s-%04d", now.Format("20060102T150405"), seq)

	// Pick random region
	reg := regionTemplates[g.rng.Intn(len(regionTemplates))]

	// Slight coordinate perturbation (+- 0.15 deg)
	latJitter := (g.rng.Float64() - 0.5) * 0.3
	lonJitter := (g.rng.Float64() - 0.5) * 0.3
	lat := math.Round((reg.baseLat+latJitter)*10000) / 10000
	lon := math.Round((reg.baseLon+lonJitter)*10000) / 10000

	// Magnitude between 4.0 and 7.8
	mag := math.Round((4.0+g.rng.Float64()*3.8)*10) / 10
	// Depth between 10.0 and 120.0 km
	depth := math.Round((10.0+g.rng.Float64()*110.0)*10) / 10

	// Tsunami condition: shallow (< 60km) AND (high magnitude >= 6.8 OR 20% probabilistic chance for mag >= 6.0)
	potentialTsunami := false
	if depth <= 60.0 {
		if mag >= 6.8 {
			potentialTsunami = true
		} else if mag >= 6.0 && g.rng.Float64() < 0.25 {
			potentialTsunami = true
		}
	}

	event := model.SeismicEvent{
		EventID:          eventID,
		Magnitude:        mag,
		DepthKm:          depth,
		EpicenterLat:     lat,
		EpicenterLon:     lon,
		RegionName:       reg.name,
		OccurredAt:       now.Format(time.RFC3339),
		PotentialTsunami: potentialTsunami,
	}

	g.store.AddSeismicEvent(event)
	g.logger.Info().
		Str("event_id", event.EventID).
		Float64("magnitude", event.Magnitude).
		Str("region", event.RegionName).
		Bool("potential_tsunami", event.PotentialTsunami).
		Msg("Generated new seismic event")

	if potentialTsunami {
		var threat model.TsunamiThreatLevel
		if mag >= 7.5 {
			threat = model.ThreatAwas
		} else if mag >= 7.0 {
			threat = model.ThreatSiaga
		} else {
			threat = model.ThreatWaspada
		}

		estMinutes := 15 + g.rng.Intn(30)
		arrival := now.Add(time.Duration(estMinutes) * time.Minute).Format(time.RFC3339)

		warningID := fmt.Sprintf("warn-bmkg-dyn-%s-%04d", now.Format("20060102T150405"), seq)
		warning := model.TsunamiWarning{
			WarningID:        warningID,
			RelatedEventID:   eventID,
			ThreatLevel:      threat,
			AffectedZones:    reg.coastalZones,
			EstimatedArrival: arrival,
		}

		g.store.AddTsunamiWarning(warning, now)
		g.logger.Warn().
			Str("warning_id", warning.WarningID).
			Str("related_event_id", warning.RelatedEventID).
			Str("threat_level", string(warning.ThreatLevel)).
			Strs("affected_zones", warning.AffectedZones).
			Str("estimated_arrival", warning.EstimatedArrival).
			Msg("Generated matching tsunami warning")
	}
}
