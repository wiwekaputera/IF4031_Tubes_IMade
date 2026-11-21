package poller

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_pantai_requests_total",
		Help: "Requests to the coastal sensor by outcome (ok, timeout, network, http_5xx, http_429, http_4xx, bad_response, stalled) plus short_circuited: requests not sent because the breaker was open.",
	}, []string{"outcome"})

	requestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sensor_pantai_request_duration_seconds",
		Help:    "Duration of requests to the coastal sensor.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2, 5},
	})

	breakerState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_pantai_breaker_state",
		Help: "Circuit breaker in front of the coastal sensor: 0 closed, 1 open, 2 half-open.",
	})

	breakerTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_pantai_breaker_transitions_total",
		Help: "Circuit breaker transitions by new state.",
	}, []string{"to"})

	observations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_pantai_observations_total",
		Help: "Observations read from the sensor: accepted, rejected (contract violation) or duplicate (already handled).",
	}, []string{"result"})

	rejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sensor_pantai_rejected_total",
		Help: "Observations set aside, by reason.",
	}, []string{"reason"})

	criticalPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sensor_pantai_critical_published_total",
		Help: "Critical observations published on the priority alert path.",
	})

	criticalPublishLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sensor_pantai_critical_publish_seconds",
		Help:    "Time from reading a critical observation to the alert being acknowledged by JetStream.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})

	backlogPages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sensor_pantai_backlog_cycles_total",
		Help: "Poll cycles that hit the page limit and left a backlog for the next cycle.",
	})

	dataLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_pantai_data_lag_seconds",
		Help: "Sensor generated_at minus the newest observed_at of the last page (sensor clock).",
	})

	sensorTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_pantai_sensor_time_seconds",
		Help: "Sensor generated_at of the last good response (Unix seconds, sensor clock).",
	})

	lastSuccessTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_pantai_last_success_timestamp_seconds",
		Help: "Wall-clock time of the last successful poll.",
	})

	stationUpdates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sensor_pantai_station_updates_total",
		Help: "Station condition HazardEvent updates published (severity change or periodic refresh).",
	})

	sensorResets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sensor_pantai_sensor_resets_total",
		Help: "Times the sensor clock went back (sensor restarted) and reading restarted from its beginning.",
	})

	stationsKnown = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sensor_pantai_stations",
		Help: "Stations known from /v1/stations.",
	})
)
