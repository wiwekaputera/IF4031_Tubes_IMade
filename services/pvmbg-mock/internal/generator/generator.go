package generator

import (
	"context"
	"fmt"
	"math"
	mathrand "math/rand"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

type volcanoTemplate struct {
	id     string
	name   string
	lat    float64
	lon    float64
	alerts []model.VolcanicAlertLevel
}

var volcanoTemplates = []volcanoTemplate{
	{id: "GNG-001", name: "Gunung Merapi", lat: -7.5407, lon: 110.4457, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga, model.AlertAwas}},
	{id: "GNG-002", name: "Gunung Semeru", lat: -8.1080, lon: 112.9220, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga}},
	{id: "GNG-003", name: "Gunung Krakatau", lat: -6.1020, lon: 105.4230, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada}},
	{id: "GNG-004", name: "Gunung Rinjani", lat: -8.4167, lon: 116.4667, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga}},
	{id: "GNG-005", name: "Gunung Sinabung", lat: 3.1700, lon: 98.3920, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga, model.AlertAwas}},
}

// SchemaVersionChecker returns true when confidence_level should be included.
type SchemaVersionChecker interface {
	IsEnabled() bool
}

// Generator dynamically produces realistic volcanic reports.
type Generator struct {
	store         store.Store
	interval      time.Duration
	logger        zerolog.Logger
	rng           *mathrand.Rand
	counter       uint64
	schemaChecker SchemaVersionChecker
}

// NewGenerator creates a new dynamic event generator.
func NewGenerator(s store.Store, intervalS int, schemaChecker SchemaVersionChecker, logger zerolog.Logger) *Generator {
	source := mathrand.NewSource(time.Now().UnixNano())
	return &Generator{
		store:         s,
		interval:      time.Duration(intervalS) * time.Second,
		logger:        logger.With().Str("component", "generator").Logger(),
		rng:           mathrand.New(source),
		schemaChecker: schemaChecker,
	}
}

// Start runs the generator loop until the context is cancelled.
func (g *Generator) Start(ctx context.Context) {
	g.logger.Info().Dur("interval", g.interval).Msg("Starting dynamic volcanic report generator")
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			g.logger.Info().Msg("Stopping dynamic volcanic report generator")
			return
		case <-ticker.C:
			g.generateReport()
		}
	}
}

func (g *Generator) generateReport() {
	now := time.Now().UTC()
	seq := atomic.AddUint64(&g.counter, 1)
	reportID := fmt.Sprintf("rpt-pvmbg-dyn-%04d", seq)

	vt := volcanoTemplates[g.rng.Intn(len(volcanoTemplates))]
	alertLevel := vt.alerts[g.rng.Intn(len(vt.alerts))]

	var eruptionCount int
	var ashHeight float64
	switch alertLevel {
	case model.AlertNormal:
		eruptionCount = 0
		ashHeight = 0
	case model.AlertWaspada:
		eruptionCount = 1 + g.rng.Intn(2)
		ashHeight = math.Round((200+g.rng.Float64()*600)*10) / 10
	case model.AlertSiaga:
		eruptionCount = 2 + g.rng.Intn(3)
		ashHeight = math.Round((800+g.rng.Float64()*800)*10) / 10
	case model.AlertAwas:
		eruptionCount = 4 + g.rng.Intn(4)
		ashHeight = math.Round((3000+g.rng.Float64()*2000)*10) / 10
	}

	report := model.VolcanicReport{
		ReportID:         reportID,
		VolcanoID:        vt.id,
		AlertLevel:       alertLevel,
		EruptionCount24h: eruptionCount,
		AshColumnHeightM: ashHeight,
		ReportedAt:       now.Format(time.RFC3339),
		ConfidenceLevel:  nil,
	}

	if g.schemaChecker.IsEnabled() {
		conf := math.Round((0.5+g.rng.Float64()*0.5)*1000) / 1000
		report.ConfidenceLevel = &conf
	}

	g.store.AddVolcanicReport(report)
	g.logger.Info().
		Str("report_id", report.ReportID).
		Str("volcano_id", report.VolcanoID).
		Str("volcano_name", vt.name).
		Str("alert_level", string(report.AlertLevel)).
		Int("eruption_count_24h", report.EruptionCount24h).
		Float64("ash_column_height_m", report.AshColumnHeightM).
		Bool("confidence_level_present", report.ConfidenceLevel != nil).
		Msg("Generated new volcanic report")
}
