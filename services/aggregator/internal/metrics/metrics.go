// Package metrics holds the aggregator's own Prometheus metrics: the state of
// each upstream source (which shows the circuit breaker), the dead-letter
// queue, the outbox backlog, and whether this instance is the leader.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

var (
	sourceStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aggregator_source_status",
		Help: "1 for the current status of each upstream source (UP, DOWN, OUTAGE). OUTAGE means the circuit breaker is open.",
	}, []string{"source", "status"})

	sourceSyncAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aggregator_source_last_sync_age_seconds",
		Help: "Seconds since the newest event stored from each source.",
	}, []string{"source"})

	deadLetters = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aggregator_dead_letters",
		Help: "Rows in the dead-letter queue by status (retrying, parked, resolved).",
	}, []string{"status"})

	outboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aggregator_outbox_backlog",
		Help: "Committed events not yet acknowledged by JetStream.",
	})

	// Leader is 1 while this instance holds the polling lease. It stays 1 for
	// a single instance; the leader-election code sets it once it exists.
	Leader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aggregator_leader",
		Help: "1 while this instance is the leader that polls the agencies, else 0.",
	})
)

var statuses = []string{"UP", "DOWN", "OUTAGE"}

// Source is one upstream's health as the poller reports it.
type Source struct {
	Name       string
	Status     string
	LastSynced time.Time
}

// Snapshot is what the monitor reads each round.
type Snapshot struct {
	Sources       func() []Source
	DeadLetters   func(context.Context) (map[string]int64, error)
	OutboxBacklog func(context.Context) (int64, error)
}

// Run refreshes the gauges every interval until ctx ends.
func Run(ctx context.Context, s Snapshot, interval time.Duration, logger zerolog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		refresh(ctx, s, logger)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func refresh(ctx context.Context, s Snapshot, logger zerolog.Logger) {
	for _, src := range s.Sources() {
		for _, st := range statuses {
			v := 0.0
			if st == src.Status {
				v = 1
			}
			sourceStatus.WithLabelValues(src.Name, st).Set(v)
		}
		if !src.LastSynced.IsZero() {
			sourceSyncAge.WithLabelValues(src.Name).Set(time.Since(src.LastSynced).Seconds())
		}
	}
	if counts, err := s.DeadLetters(ctx); err == nil {
		for _, st := range []string{"retrying", "parked", "resolved"} {
			deadLetters.WithLabelValues(st).Set(float64(counts[st]))
		}
	} else if ctx.Err() == nil {
		logger.Warn().Err(err).Msg("metrics: dead-letter count failed")
	}
	if n, err := s.OutboxBacklog(ctx); err == nil {
		outboxBacklog.Set(float64(n))
	} else if ctx.Err() == nil {
		logger.Warn().Err(err).Msg("metrics: outbox backlog failed")
	}
}
