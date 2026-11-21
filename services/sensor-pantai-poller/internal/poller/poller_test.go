package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/breaker"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/sensor"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/validate"
)

type fakeSensor struct {
	pages []sensor.Page
	errs  []error
	calls int
}

func (f *fakeSensor) Stations(context.Context) ([]sensorpantai.Station, error) {
	return []sensorpantai.Station{{StationID: "stn-a", Name: "Padang", Province: "Sumatera Barat", Latitude: -0.9, Longitude: 100.3}}, nil
}

func (f *fakeSensor) Observations(_ context.Context, cursor string, _ int) (sensor.Page, error) {
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return sensor.Page{}, f.errs[i]
	}
	if i < len(f.pages) {
		return f.pages[i], nil
	}
	return sensor.Page{GeneratedAt: time.Date(2026, 11, 12, 5, 0, i, 0, time.UTC), NextCursor: cursor}, nil
}

type fakePub struct {
	log     []string
	ingest  []event.IngestMessage
	failAll bool
}

func (f *fakePub) PublishAlert(_ context.Context, env event.Envelope) (bool, error) {
	if f.failAll {
		return false, errors.New("nats down")
	}
	f.log = append(f.log, "alert:"+env.Data.SourceRefID)
	return false, nil
}

func (f *fakePub) PublishIngest(_ context.Context, m event.IngestMessage) error {
	if f.failAll {
		return errors.New("nats down")
	}
	f.log = append(f.log, string(m.Op)+":"+m.SourceRefID)
	f.ingest = append(f.ingest, m)
	return nil
}

type fakeState struct {
	cursor     string
	sensorTime time.Time
	status     []sensorpantai.Status
}

func (f *fakeState) LoadCursor(context.Context) (Cursor, error) {
	return Cursor{Value: f.cursor, SensorTime: f.sensorTime}, nil
}
func (f *fakeState) SaveCursor(_ context.Context, c Cursor) error {
	f.cursor, f.sensorTime = c.Value, c.SensorTime
	return nil
}
func (f *fakeState) PutStatus(_ context.Context, s sensorpantai.Status) error {
	f.status = append(f.status, s)
	return nil
}

func obs(id string, at string, critical bool, quality string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"observation_id": id, "station_id": "stn-a", "observed_at": at,
		"sea_level_anomaly_m": 0.2, "wave_height_m": 1.0, "quality": quality, "critical": critical,
	})
	return b
}

func testConfig() Config {
	return Config{
		PollInterval: time.Second, PageLimit: 10, MaxPagesPerRun: 5, RequestTimeout: time.Second,
		RetryBase: 500 * time.Millisecond, RetryMax: 5 * time.Second, StallAfter: 10 * time.Second, SilenceAfter: 10 * time.Minute, StationUpdateEvery: 5 * time.Minute,
		StationsRefresh: time.Hour, Limits: validate.DefaultLimits(), Thresholds: DefaultThresholds(),
	}
}

type harness struct {
	p     *Poller
	s     *fakeSensor
	pub   *fakePub
	state *fakeState
	clock *time.Time
}

func newHarness(s *fakeSensor) *harness {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	h := &harness{s: s, pub: &fakePub{}, state: &fakeState{cursor: "c0"}, clock: &now}
	b := breaker.New(breaker.Config{FailureThreshold: 3, BaseOpen: 10 * time.Second, MaxOpen: time.Minute})
	h.p = New(testConfig(), s, h.pub, h.state, b, zerolog.Nop())
	h.p.now = func() time.Time { return *h.clock }
	h.p.rnd = func() float64 { return 1 }
	b.SetClock(h.p.now, func() float64 { return 0.5 })
	return h
}

func (h *harness) advance(d time.Duration) { *h.clock = h.clock.Add(d) }

func TestPageOrderRejectsAndDuplicates(t *testing.T) {
	gen := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	s := &fakeSensor{pages: []sensor.Page{{
		GeneratedAt: gen,
		NextCursor:  "c1",
		Items: []json.RawMessage{
			obs("obs-1", "2026-11-12T00:59:00Z", false, "ok"),
			obs("obs-2", "2026-11-12T00:59:15Z", false, "suspect"),
			obs("obs-3", "2026-11-12T00:59:30Z", true, "ok"),
			obs("obs-1", "2026-11-12T00:59:00Z", false, "ok"), // repeated by the sensor
			json.RawMessage(`{"observation_id":"obs-4"}`),
		},
	}}}
	h := newHarness(s)
	if d := h.p.Cycle(context.Background()); d != time.Second {
		t.Fatalf("delay %v", d)
	}

	want := []string{
		"reject:obs-2", "reject:obs-4", // rejects published while scanning
		"alert:obs-3@2026-11-12T00:59:30Z", "upsert:obs-3@2026-11-12T00:59:30Z", // critical first
		"upsert:station:stn-a", "upsert:station:stn-a", // obs-1 (first), obs-3 (severity changed to AWAS); duplicate obs-1 dropped
	}
	if strings.Join(h.pub.log, ",") != strings.Join(want, ",") {
		t.Fatalf("publish order\n got %v\nwant %v", h.pub.log, want)
	}
	if h.state.cursor != "c1" {
		t.Fatalf("cursor %q not saved", h.state.cursor)
	}
	var rejectReasons []string
	for _, m := range h.pub.ingest {
		if m.Op == event.IngestOpReject {
			rejectReasons = append(rejectReasons, m.Reason)
		}
	}
	if !strings.HasPrefix(rejectReasons[0], "quality_not_ok") || !strings.HasPrefix(rejectReasons[1], "missing_field") {
		t.Fatalf("reasons %v", rejectReasons)
	}
	last := h.state.status[len(h.state.status)-1]
	if last.Status != sensorpantai.StatusUp || last.LastSyncedAt == nil {
		t.Fatalf("status %+v", last)
	}
}

func TestCriticalAlertMatchesCanonicalEventID(t *testing.T) {
	o := sensorpantai.Observation{ObservationID: "obs-9", StationID: "stn-a", Critical: true, ObservedAt: time.Now()}
	st := sensorpantai.Station{StationID: "stn-a", Name: "Padang"}
	alert := CriticalAlert(o, st, time.Now(), time.Now(), "")
	ingest := CriticalIngest(o, st, time.Now(), time.Now(), "")
	if alert.EventID != hazard.ID(ingest.Source, ingest.SourceRefID) || alert.Version != 1 || ingest.Severity != hazard.SeverityAwas {
		t.Fatalf("alert %s v%d does not match ingest %s", alert.EventID, alert.Version, hazard.ID(ingest.Source, ingest.SourceRefID))
	}
	if err := ingest.Validate(); err != nil {
		t.Fatalf("critical ingest invalid: %v", err)
	}
}

func TestBreakerStopsRequestsWhileSensorIsDown(t *testing.T) {
	down := &sensor.Error{Kind: "http_5xx", Status: 503, Err: errors.New("unavailable")}
	s := &fakeSensor{errs: []error{down, down, down, down}}
	h := newHarness(s)

	for i := 0; i < 3; i++ {
		h.p.Cycle(context.Background())
		h.advance(time.Second)
	}
	if h.p.breaker.State() != breaker.Open {
		t.Fatal("breaker should be open after 3 failures")
	}
	calls := s.calls
	// While open, 50 cycles do not reach the sensor.
	for i := 0; i < 50; i++ {
		h.p.Cycle(context.Background())
		h.advance(100 * time.Millisecond)
	}
	if s.calls != calls {
		t.Fatalf("sensor called %d times while the breaker was open", s.calls-calls)
	}
	if st := h.state.status[len(h.state.status)-1]; st.Status != sensorpantai.StatusOutage || st.StaleSince == nil {
		t.Fatalf("status during outage %+v", st)
	}
	// After the open period one probe goes out; it fails, so the breaker re-opens for longer.
	h.advance(10 * time.Second)
	h.p.Cycle(context.Background())
	if s.calls != calls+1 || h.p.breaker.State() != breaker.Open {
		t.Fatalf("calls=%d state=%v", s.calls-calls, h.p.breaker.State())
	}
	// The sensor recovers: the next probe closes the breaker without intervention.
	h.advance(h.p.breaker.RetryIn())
	h.p.Cycle(context.Background())
	if h.p.breaker.State() != breaker.Closed {
		t.Fatal("breaker should close after the sensor recovered")
	}
	if st := h.state.status[len(h.state.status)-1]; st.Status != sensorpantai.StatusUp || st.StaleSince != nil {
		t.Fatalf("status after recovery %+v", st)
	}
}

func TestBrokerFailureDoesNotBlameSensorAndKeepsCursor(t *testing.T) {
	page := sensor.Page{
		GeneratedAt: time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC), NextCursor: "c1",
		Items: []json.RawMessage{obs("obs-1", "2026-11-12T00:59:00Z", true, "ok")},
	}
	s := &fakeSensor{pages: []sensor.Page{page, page, page, page, page, page}}
	h := newHarness(s)
	h.pub.failAll = true
	for i := 0; i < 5; i++ {
		h.p.Cycle(context.Background())
	}
	if h.p.breaker.State() != breaker.Closed {
		t.Fatal("a broker failure must not open the sensor breaker")
	}
	if h.state.cursor != "c0" {
		t.Fatal("cursor must not move past an unpublished page")
	}
	h.pub.failAll = false
	h.p.Cycle(context.Background())
	if h.state.cursor != "c1" || h.pub.log[0] != "alert:obs-1@2026-11-12T00:59:00Z" {
		t.Fatalf("after recovery cursor=%s log=%v", h.state.cursor, h.pub.log)
	}
}

func TestFrozenSensorClockCountsAsFailure(t *testing.T) {
	gen := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	var pages []sensor.Page
	for i := 0; i < 20; i++ {
		pages = append(pages, sensor.Page{GeneratedAt: gen, NextCursor: "c0"})
	}
	s := &fakeSensor{pages: pages}
	h := newHarness(s)
	for i := 0; i < 15; i++ {
		h.p.Cycle(context.Background())
		h.advance(time.Second)
	}
	if h.p.breaker.State() == breaker.Closed {
		t.Fatal("a sensor whose clock stopped must trip the breaker")
	}
	if !strings.Contains(h.state.status[len(h.state.status)-1].Reason, "stopped advancing") &&
		!strings.Contains(h.state.status[len(h.state.status)-1].Reason, "breaker open") {
		t.Fatalf("reason %q", h.state.status[len(h.state.status)-1].Reason)
	}
}

func TestSeverityThresholds(t *testing.T) {
	th := DefaultThresholds()
	cases := []struct {
		a, w float64
		crit bool
		want hazard.Severity
	}{
		{0.1, 1, false, hazard.SeverityNormal},
		{0.6, 1, false, hazard.SeverityWaspada},
		{-1.2, 1, false, hazard.SeveritySiaga},
		{0.1, 4.5, false, hazard.SeveritySiaga},
		{0.0, 0.5, true, hazard.SeverityAwas},
	}
	for _, c := range cases {
		got := th.Severity(sensorpantai.Observation{SeaLevelAnomalyM: c.a, WaveHeightM: c.w, Critical: c.crit})
		if got != c.want {
			t.Errorf("%v/%v/%v -> %s, want %s", c.a, c.w, c.crit, got, c.want)
		}
	}
}

func TestRecentIDsEvictsOldest(t *testing.T) {
	r := newRecentIDs(2)
	for i := 0; i < 3; i++ {
		r.add(fmt.Sprint(i))
	}
	if r.has("0") || !r.has("1") || !r.has("2") {
		t.Fatal("expected FIFO eviction")
	}
}

func TestSilentSensorIsDegradedNotBroken(t *testing.T) {
	gen := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	pages := []sensor.Page{{GeneratedAt: gen, NextCursor: "c1", Items: []json.RawMessage{obs("obs-1", "2026-11-12T00:59:50Z", false, "ok")}}}
	for i := 1; i <= 30; i++ { // answers every time, clock advances, but no data
		pages = append(pages, sensor.Page{GeneratedAt: gen.Add(time.Duration(i) * time.Minute), NextCursor: "c1"})
	}
	h := newHarness(&fakeSensor{pages: pages})
	for i := 0; i < 31; i++ {
		h.p.Cycle(context.Background())
		h.advance(time.Second)
	}
	last := h.state.status[len(h.state.status)-1]
	if last.Status != sensorpantai.StatusDegraded || !strings.Contains(last.Reason, "silent") || last.StaleSince == nil {
		t.Fatalf("status %+v", last)
	}
	if h.p.breaker.State() != breaker.Closed {
		t.Fatal("silence must not open the breaker: the sensor still answers")
	}
}

// recordingSensor answers by cursor and records the cursors it was asked for.
type recordingSensor struct {
	fakeSensor
	asked []string
	byCur map[string]sensor.Page
}

func (r *recordingSensor) Observations(_ context.Context, cursor string, _ int) (sensor.Page, error) {
	r.asked = append(r.asked, cursor)
	return r.byCur[cursor], nil
}

func TestSensorRestartRereadsFromStartAndKeepsNewCriticals(t *testing.T) {
	// Before: the sensor was at 05:00 and we saved cursor "far".
	// Now: the sensor restarted; its clock is back at 00:31 and "far" is past its end.
	restarted := time.Date(2026, 11, 12, 0, 31, 0, 0, time.UTC)
	s := &recordingSensor{byCur: map[string]sensor.Page{
		"far": {GeneratedAt: restarted, NextCursor: "far"},
		"": {GeneratedAt: restarted, NextCursor: "n1", Items: []json.RawMessage{
			// Same ID as an observation of the previous run, different time: a new event.
			obs("obs-1", "2026-11-12T00:30:00Z", true, "ok"),
		}},
	}}
	h := newHarness(&s.fakeSensor)
	h.p.sensor = s
	h.state.cursor = "far"
	h.state.sensorTime = time.Date(2026, 11, 12, 5, 0, 0, 0, time.UTC)
	h.p.seen.add("obs-1@2026-11-12T04:00:00Z") // handled in the previous run

	h.p.Cycle(context.Background())

	if strings.Join(s.asked, ",") != "far," {
		t.Fatalf("asked cursors %q, want the old one then the start", s.asked)
	}
	if h.state.cursor != "n1" {
		t.Fatalf("cursor %q", h.state.cursor)
	}
	if len(h.pub.log) == 0 || h.pub.log[0] != "alert:obs-1@2026-11-12T00:30:00Z" {
		t.Fatalf("the new critical observation must be alerted: %v", h.pub.log)
	}
}

func TestStationConditionThrottledButSeverityChangesPublished(t *testing.T) {
	gen := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	var items []json.RawMessage
	for i := 0; i < 20; i++ { // 20 normal observations, 15 s apart (5 min total)
		items = append(items, obs(fmt.Sprintf("n-%d", i), gen.Add(time.Duration(i-20)*15*time.Second).Format(time.RFC3339), false, "ok"))
	}
	big, _ := json.Marshal(map[string]any{"observation_id": "big", "station_id": "stn-a", "observed_at": gen.Format(time.RFC3339),
		"sea_level_anomaly_m": 0.7, "wave_height_m": 1.0, "quality": "ok", "critical": false})
	items = append(items, big)
	h := newHarness(&fakeSensor{pages: []sensor.Page{{GeneratedAt: gen, NextCursor: "c1", Items: items}}})
	h.p.cfg.PageLimit = 100
	h.p.Cycle(context.Background())
	var station []string
	for _, m := range h.pub.ingest {
		if m.SourceRefID == "station:stn-a" {
			station = append(station, string(m.Severity))
		}
	}
	// first observation, one periodic refresh after 5 min, and the WASPADA change
	if strings.Join(station, ",") != "NORMAL,NORMAL,WASPADA" && strings.Join(station, ",") != "NORMAL,WASPADA" {
		t.Fatalf("station updates %v", station)
	}
}
