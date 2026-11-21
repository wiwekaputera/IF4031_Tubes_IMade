// Package poller reads the coastal sensor and turns its observations into
// BNPB events without letting a sick sensor hurt anything else.
//
// One poll cycle:
//  1. Ask the breaker. While it is open no request is sent at all.
//  2. GET /v1/observations from the saved cursor, following next_cursor while
//     pages are full (bounded per cycle, so a burst is drained in steps).
//  3. Check every item against the contract. Rejected items go to the
//     aggregator as op=reject with the reason (never a HazardEvent).
//  4. Critical observations first: the alert goes to BNPB_ALERTS (priority
//     path to the field team), then their HazardEvent; then the per-station
//     condition for every valid observation.
//  5. Only when every publish of the page is acknowledged is the cursor saved,
//     so a crash re-reads the page and message IDs drop the duplicates.
package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/breaker"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/sensor"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/validate"
)

// Sensor is the coastal sensor API.
type Sensor interface {
	Stations(ctx context.Context) ([]sensorpantai.Station, error)
	Observations(ctx context.Context, cursor string, limit int) (sensor.Page, error)
}

// Publisher stores messages durably and returns only after the broker's ack.
type Publisher interface {
	// PublishAlert reports duplicate=true when the broker already held this alert.
	PublishAlert(ctx context.Context, env event.Envelope) (duplicate bool, err error)
	PublishIngest(ctx context.Context, msg event.IngestMessage) error
}

// Cursor is the read position, saved with the sensor clock it was read at so
// a restarted sensor (clock back at its start) is recognised even after this
// poller restarts.
type Cursor struct {
	Value      string    `json:"cursor"`
	SensorTime time.Time `json:"sensor_time"`
}

// State persists the cursor and the published status.
type State interface {
	LoadCursor(ctx context.Context) (Cursor, error)
	SaveCursor(ctx context.Context, c Cursor) error
	PutStatus(ctx context.Context, st sensorpantai.Status) error
}

// Config of the poller.
type Config struct {
	PollInterval   time.Duration
	PageLimit      int
	MaxPagesPerRun int
	RequestTimeout time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
	// StallAfter: a sensor answering 200 whose generated_at has not moved for
	// this long is serving stale data and is treated as failing.
	StallAfter time.Duration
	// SilenceAfter: when the newest observation is older than the sensor's
	// own clock by more than this, the sensor is "silent" (answering but not
	// observing). Status becomes DEGRADED; the breaker is not involved.
	SilenceAfter time.Duration
	// StationUpdateEvery: a station's condition is republished when its
	// severity changes, otherwise at most this often (sensor clock). Every
	// observation is still checked; critical ones are always published.
	StationUpdateEvery time.Duration
	StationsRefresh    time.Duration
	Limits             validate.Limits
	Thresholds         Thresholds
}

// Poller is the coastal sensor poller.
type Poller struct {
	cfg     Config
	sensor  Sensor
	pub     Publisher
	state   State
	breaker *breaker.Breaker
	log     zerolog.Logger
	now     func() time.Time
	rnd     func() float64

	mu                 sync.Mutex
	stations           map[string]sensorpantai.Station
	stationsAt         time.Time
	cursor             string
	cursorLoaded       bool
	failures           int
	lastSuccess        time.Time
	lastGenerated      time.Time
	lastGeneratedMoved time.Time
	staleSince         time.Time
	lastObserved       time.Time
	seen               *recentIDs
	stationPublished   map[string]stationState
}

// New builds a poller.
func New(cfg Config, s Sensor, pub Publisher, st State, b *breaker.Breaker, log zerolog.Logger) *Poller {
	p := &Poller{
		cfg: cfg, sensor: s, pub: pub, state: st, breaker: b, log: log,
		now: time.Now, rnd: rand.Float64, seen: newRecentIDs(50000),
		stationPublished: map[string]stationState{},
	}
	b.OnChange(func(from, to breaker.State) {
		breakerState.Set(float64(to))
		breakerTransitions.WithLabelValues(to.String()).Inc()
		log.Warn().Str("from", from.String()).Str("to", to.String()).Msg("sensor circuit breaker changed state")
	})
	return p
}

// Run polls until ctx ends. The delay between cycles is the poll interval
// when healthy, an exponential back-off with jitter after failures, and the
// remaining open period while the breaker is open.
func (p *Poller) Run(ctx context.Context) {
	for ctx.Err() == nil {
		delay := p.Cycle(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
	}
}

// Cycle runs one poll cycle and returns how long to wait before the next.
func (p *Poller) Cycle(ctx context.Context) time.Duration {
	if !p.cursorLoaded {
		cur, err := p.state.LoadCursor(ctx)
		if err != nil {
			p.log.Warn().Err(err).Msg("cannot load cursor yet; retrying")
			return p.cfg.PollInterval
		}
		p.cursor, p.lastGenerated, p.cursorLoaded = cur.Value, cur.SensorTime, true
		p.log.Info().Str("cursor", cur.Value).Time("sensor_time", cur.SensorTime).Msg("resuming from saved cursor")
	}

	if !p.breaker.Allow() {
		requests.WithLabelValues("short_circuited").Inc()
		p.publishStatus(ctx, "breaker open")
		wait := p.breaker.RetryIn()
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		return wait
	}

	err := p.pollOnce(ctx)
	var local *localError
	if errors.As(err, &local) {
		// The sensor answered; BNPB's own broker or state store failed. Do not
		// blame the sensor (the breaker is untouched) and retry the same page.
		p.breaker.Success()
		p.failures++
		p.publishStatus(ctx, "")
		p.log.Error().Err(err).Msg("cannot store sensor data; the page will be read again")
		return breaker.Backoff(p.failures, p.cfg.RetryBase, p.cfg.RetryMax, p.rnd)
	}
	if err != nil {
		p.breaker.Failure()
		p.failures++
		p.markStale()
		p.publishStatus(ctx, err.Error())
		p.log.Warn().Err(err).Int("consecutive_failures", p.failures).Str("breaker", p.breaker.State().String()).
			Msg("sensor poll failed")
		if p.breaker.State() == breaker.Open {
			return p.breaker.RetryIn()
		}
		return breaker.Backoff(p.failures, p.cfg.RetryBase, p.cfg.RetryMax, p.rnd)
	}
	p.breaker.Success()
	p.failures = 0
	reason := p.silence()
	if reason != "" {
		p.markStale()
	}
	p.publishStatus(ctx, reason)
	return p.cfg.PollInterval
}

var errStalled = errors.New("sensor answers but its generated_at has stopped advancing")

// resetTolerance: a sensor clock that goes back by more than this means the
// sensor restarted.
const resetTolerance = time.Minute

// localError is a failure on BNPB's side (JetStream publish, state store).
type localError struct{ err error }

func (e *localError) Error() string { return e.err.Error() }
func (e *localError) Unwrap() error { return e.err }

func local(format string, args ...any) error { return &localError{fmt.Errorf(format, args...)} }

func (p *Poller) pollOnce(ctx context.Context) error {
	if err := p.refreshStations(ctx, false); err != nil {
		return err
	}
	for page := 0; page < p.cfg.MaxPagesPerRun; page++ {
		reqCtx, cancel := context.WithTimeout(ctx, p.cfg.RequestTimeout)
		started := p.now()
		pg, err := p.sensor.Observations(reqCtx, p.cursor, p.cfg.PageLimit)
		cancel()
		requestDuration.Observe(p.now().Sub(started).Seconds())
		if err != nil {
			requests.WithLabelValues(kindOf(err)).Inc()
			var se *sensor.Error
			if errors.As(err, &se) && se.Kind == "http_4xx" && p.cursor != "" {
				// The sensor no longer accepts our position: read from its start.
				p.log.Warn().Err(err).Str("cursor", p.cursor).Msg("sensor rejected the cursor; reading from its start")
				p.cursor = ""
			}
			return err
		}
		requests.WithLabelValues("ok").Inc()

		if !p.lastGenerated.IsZero() && pg.GeneratedAt.Before(p.lastGenerated.Add(-resetTolerance)) {
			// The sensor clock went back: the sensor restarted from the beginning
			// of its data, so our cursor points past its end. Read again from
			// the start; observation keys drop what was already handled.
			sensorResets.Inc()
			p.log.Warn().Time("sensor_time", pg.GeneratedAt).Time("previous_sensor_time", p.lastGenerated).
				Msg("sensor clock went back: the sensor restarted; reading from its start")
			p.lastGenerated, p.lastGeneratedMoved, p.lastObserved = time.Time{}, time.Time{}, time.Time{}
			p.stationPublished = map[string]stationState{}
			if p.cursor != "" {
				p.cursor = ""
				if err := p.state.SaveCursor(ctx, Cursor{SensorTime: pg.GeneratedAt}); err != nil {
					return local("save cursor: %w", err)
				}
				continue
			}
		}

		if err := p.checkClock(pg.GeneratedAt); err != nil {
			requests.WithLabelValues("stalled").Inc()
			return err
		}
		if err := p.handlePage(ctx, pg); err != nil {
			return err
		}
		if pg.NextCursor != "" && pg.NextCursor != p.cursor {
			if err := p.state.SaveCursor(ctx, Cursor{Value: pg.NextCursor, SensorTime: pg.GeneratedAt}); err != nil {
				return local("save cursor: %w", err)
			}
			p.cursor = pg.NextCursor
		}
		p.lastSuccess = p.now()
		if len(pg.Items) < p.cfg.PageLimit || pg.NextCursor == "" {
			return nil
		}
	}
	backlogPages.Inc()
	return nil
}

// silence describes a sensor that answers but has stopped producing
// observations, measured on the sensor's own clock.
func (p *Poller) silence() string {
	if p.lastObserved.IsZero() || p.lastGenerated.IsZero() || p.cfg.SilenceAfter <= 0 {
		return ""
	}
	gap := p.lastGenerated.Sub(p.lastObserved)
	dataLag.Set(gap.Seconds())
	if gap <= p.cfg.SilenceAfter {
		return ""
	}
	return fmt.Sprintf("sensor silent: newest observation %s is %s behind the sensor clock",
		p.lastObserved.Format(time.RFC3339), gap.Round(time.Second))
}

// checkClock detects a sensor that keeps answering 200 with a frozen clock.
func (p *Poller) checkClock(gen time.Time) error {
	now := p.now()
	if gen.After(p.lastGenerated) {
		p.lastGenerated = gen
		p.lastGeneratedMoved = now
		sensorTime.Set(float64(gen.Unix()))
		return nil
	}
	if p.lastGeneratedMoved.IsZero() {
		p.lastGeneratedMoved = now
	}
	if now.Sub(p.lastGeneratedMoved) > p.cfg.StallAfter {
		return fmt.Errorf("%w (generated_at %s for %s)", errStalled,
			p.lastGenerated.Format(time.RFC3339), now.Sub(p.lastGeneratedMoved).Round(time.Second))
	}
	return nil
}

// handlePage validates and publishes every item of a page.
func (p *Poller) handlePage(ctx context.Context, pg sensor.Page) error {
	detectedAt := p.now()
	var valid []sensorpantai.Observation
	inPage := make(map[string]bool, len(pg.Items))
	for _, raw := range pg.Items {
		obs, rej := validate.Check(raw, p.stations, pg.GeneratedAt, p.cfg.Limits)
		if rej != nil && rej.Code == "unknown_station" {
			// A station may have been added since the last refresh: check once.
			if err := p.refreshStations(ctx, true); err == nil {
				obs, rej = validate.Check(raw, p.stations, pg.GeneratedAt, p.cfg.Limits)
			}
		}
		if rej != nil {
			if err := p.publishReject(ctx, raw, rej); err != nil {
				return err
			}
			continue
		}
		key := ObservationKey(obs)
		if p.seen.has(key) || inPage[key] {
			observations.WithLabelValues("duplicate").Inc()
			continue
		}
		inPage[key] = true
		valid = append(valid, obs)
	}

	// Priority: critical alerts before anything else on the page.
	for _, o := range valid {
		if !o.Critical {
			continue
		}
		st := p.stations[o.StationID]
		alert := CriticalAlert(o, st, pg.GeneratedAt, detectedAt, "")
		dup, err := p.pub.PublishAlert(ctx, alert)
		if err != nil {
			return local("publish critical alert %s: %w", o.ObservationID, err)
		}
		if !dup {
			// Counted once per alert: the SLO denominator.
			criticalPublished.Inc()
		}
		criticalPublishLatency.Observe(p.now().Sub(detectedAt).Seconds())
		p.log.Warn().Str("observation_id", o.ObservationID).Str("station_id", o.StationID).
			Float64("sea_level_anomaly_m", o.SeaLevelAnomalyM).Float64("wave_height_m", o.WaveHeightM).
			Str("hazard_id", alert.EventID).Msg("critical coastal observation published on the priority path")
		if err := p.pub.PublishIngest(ctx, CriticalIngest(o, st, pg.GeneratedAt, detectedAt, "")); err != nil {
			return local("publish critical event %s: %w", o.ObservationID, err)
		}
	}
	for _, o := range valid {
		st := p.stations[o.StationID]
		sev := p.cfg.Thresholds.Severity(o)
		last, seenBefore := p.stationPublished[o.StationID]
		if !seenBefore || sev != last.severity || o.ObservedAt.Sub(last.observedAt) >= p.cfg.StationUpdateEvery {
			msg := StationIngest(o, st, sev, pg.GeneratedAt, detectedAt, "")
			if err := p.pub.PublishIngest(ctx, msg); err != nil {
				return local("publish observation %s: %w", o.ObservationID, err)
			}
			p.stationPublished[o.StationID] = stationState{severity: sev, observedAt: o.ObservedAt}
			stationUpdates.Inc()
		}
		observations.WithLabelValues("accepted").Inc()
		p.seen.add(ObservationKey(o))
		if o.ObservedAt.After(p.lastObserved) {
			p.lastObserved = o.ObservedAt
		}
	}
	if n := len(pg.Items); n > 0 {
		var newest struct {
			ObservedAt string `json:"observed_at"`
		}
		if json.Unmarshal(pg.Items[n-1], &newest) == nil {
			if t, err := time.Parse(time.RFC3339, newest.ObservedAt); err == nil {
				dataLag.Set(pg.GeneratedAt.Sub(t).Seconds())
			}
		}
	}
	return nil
}

func (p *Poller) publishReject(ctx context.Context, raw json.RawMessage, rej *validate.Rejection) error {
	ref := rej.ObservationID
	msgID := "sp-reject:" + ref
	if ref == "" {
		msgID = "sp-reject:" + hazard.ID(hazard.SourceSensorPantai, string(raw))
	}
	msg := event.IngestMessage{
		MessageID:   msgID,
		Op:          event.IngestOpReject,
		Source:      hazard.SourceSensorPantai,
		SourceRefID: ref,
		Reason:      rej.Error(),
		Raw:         raw,
	}
	if err := p.pub.PublishIngest(ctx, msg); err != nil {
		return local("publish rejected item: %w", err)
	}
	observations.WithLabelValues("rejected").Inc()
	rejected.WithLabelValues(rej.Code).Inc()
	p.log.Info().Str("observation_id", ref).Str("reason", rej.Error()).Msg("observation set aside: breaks the sensor contract")
	return nil
}

func (p *Poller) refreshStations(ctx context.Context, force bool) error {
	if !force && p.stations != nil && p.now().Sub(p.stationsAt) < p.cfg.StationsRefresh {
		return nil
	}
	if force && p.now().Sub(p.stationsAt) < 30*time.Second {
		return errors.New("stations refreshed recently")
	}
	reqCtx, cancel := context.WithTimeout(ctx, p.cfg.RequestTimeout)
	defer cancel()
	list, err := p.sensor.Stations(reqCtx)
	if err != nil {
		requests.WithLabelValues(kindOf(err)).Inc()
		if p.stations != nil && !force {
			// Keep the known list; the observations request decides health.
			p.log.Warn().Err(err).Msg("station refresh failed; keeping the known list")
			return nil
		}
		return err
	}
	m := make(map[string]sensorpantai.Station, len(list))
	for _, s := range list {
		if s.StationID != "" {
			m[s.StationID] = s
		}
	}
	p.stations, p.stationsAt = m, p.now()
	stationsKnown.Set(float64(len(m)))
	return nil
}

func (p *Poller) markStale() {
	if p.staleSince.IsZero() {
		p.staleSince = p.now()
	}
}

func (p *Poller) publishStatus(ctx context.Context, reason string) {
	now := p.now()
	st := sensorpantai.Status{Breaker: p.breaker.State().String(), UpdatedAt: now.UTC(), Reason: reason}
	switch {
	case reason == "":
		st.Status = sensorpantai.StatusUp
		p.staleSince = time.Time{}
	case p.breaker.State() == breaker.Closed || p.breaker.State() == breaker.HalfOpen:
		st.Status = sensorpantai.StatusDegraded
	default:
		st.Status = sensorpantai.StatusOutage
	}
	if !p.lastSuccess.IsZero() {
		t := p.lastSuccess.UTC()
		st.LastSyncedAt = &t
	}
	if !p.staleSince.IsZero() {
		t := p.staleSince.UTC()
		st.StaleSince = &t
	}
	if !p.lastGenerated.IsZero() {
		t := p.lastGenerated
		st.SensorTime = &t
	}
	if st.Status == sensorpantai.StatusUp {
		lastSuccessTime.Set(float64(now.Unix()))
	}
	if err := p.state.PutStatus(ctx, st); err != nil {
		p.log.Debug().Err(err).Msg("cannot publish sensor status")
	}
}

func kindOf(err error) string {
	var se *sensor.Error
	if errors.As(err, &se) {
		return se.Kind
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "error"
}

// stationState is the last condition published for a station.
type stationState struct {
	severity   hazard.Severity
	observedAt time.Time
}

// recentIDs remembers the last n observation IDs (FIFO eviction), enough to
// drop the repeats the sensor sends within and across pages.
type recentIDs struct {
	n     int
	set   map[string]struct{}
	order []string
}

func newRecentIDs(n int) *recentIDs {
	return &recentIDs{n: n, set: make(map[string]struct{}, n)}
}

func (r *recentIDs) has(id string) bool { _, ok := r.set[id]; return ok }

func (r *recentIDs) add(id string) {
	if _, ok := r.set[id]; ok {
		return
	}
	r.set[id] = struct{}{}
	r.order = append(r.order, id)
	if len(r.order) > r.n {
		delete(r.set, r.order[0])
		r.order = r.order[1:]
	}
}
