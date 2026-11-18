// Package metrics exposes the satellite ingestor's Prometheus metrics: how
// fast feed messages are committed, how far behind the feed the ingestor is,
// and how many messages were rejected, late, or are waiting to be published.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
)

var (
	messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_feed_messages_total",
		Help: "Feed messages committed, by event type and outcome.",
	}, []string{"type", "outcome"})

	latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satellite_ingest_latency_seconds",
		Help:    "Time from reading a feed event off the stream to committing its result.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})

	nextOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_next_offset",
		Help: "Next feed offset the ingestor will read; everything before it is committed.",
	})

	offsetLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_lag_offsets",
		Help: "Feed events published by the feed but not yet committed.",
	})

	eventTimeLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_event_time_lag_seconds",
		Help: "Feed virtual time minus the newest detection time committed.",
	})

	durable = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "satellite_durable_count",
		Help: "Running totals kept in the database, so they survive restarts.",
	}, []string{"kind"})

	outboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_outbox_backlog",
		Help: "Hazard messages committed but not yet acknowledged by JetStream.",
	})

	deadLetters = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_dead_letters",
		Help: "Parked feed events that could not be processed.",
	})
)

// Observe records one committed message. before and after are the processor
// state around the commit; the counter that moved tells the outcome.
func Observe(m feed.Message, before, after ingest.State) {
	outcome := "processed"
	switch {
	case after.Invalid > before.Invalid:
		outcome = "dead_lettered"
	case after.LowConf > before.LowConf:
		outcome = "low_confidence"
	case after.Late > before.Late:
		outcome = "late"
	case after.Unknown > before.Unknown:
		outcome = "unknown_key"
	case after.Duplicate > before.Duplicate:
		outcome = "duplicate"
	}
	messages.WithLabelValues(m.Type, outcome).Inc()
	if !m.ReceivedAt.IsZero() {
		latency.Observe(time.Since(m.ReceivedAt).Seconds())
	}
	SetState(after)
}

// SetState publishes the durable counters and the committed offset.
func SetState(st ingest.State) {
	nextOffset.Set(float64(st.NextOffset))
	durable.WithLabelValues("invalid").Set(float64(st.Invalid))
	durable.WithLabelValues("low_confidence").Set(float64(st.LowConf))
	durable.WithLabelValues("late").Set(float64(st.Late))
	durable.WithLabelValues("unknown_key").Set(float64(st.Unknown))
	durable.WithLabelValues("duplicate").Set(float64(st.Duplicate))
}

// SetLag records how far the ingestor is behind the feed. latestOffset is the
// newest offset the feed reports; virtualNow is the feed's clock.
func SetLag(latestOffset, next int64, virtualNow, watermark time.Time) {
	lag := latestOffset + 1 - next
	if lag < 0 {
		lag = 0
	}
	offsetLag.Set(float64(lag))
	if !virtualNow.IsZero() && !watermark.IsZero() {
		eventTimeLag.Set(virtualNow.Sub(watermark).Seconds())
	}
}

// SetBacklog records the outbox and dead-letter sizes.
func SetBacklog(outbox, dead int64) {
	outboxBacklog.Set(float64(outbox))
	deadLetters.Set(float64(dead))
}
