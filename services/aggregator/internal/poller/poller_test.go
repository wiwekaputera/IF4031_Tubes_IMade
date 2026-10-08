package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
)

func TestPoll_BMKGSucceedWhilePVMBGFails(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32
	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pvmbgCalled.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker(bmkgSrv.URL, pvmbgSrv.URL, 5, 30*time.Second)
	p.Poll(context.Background(), "test-bmkg-up-pvmbg-err", time.Time{})

	if bmkgCalled.Load() == 0 || pvmbgCalled.Load() == 0 {
		t.Fatal("expected both services to be polled")
	}
	hs := p.Health()
	if hs.BMKG.Status != "UP" || hs.BMKG.LastSynced.IsZero() {
		t.Errorf("expected BMKG status UP with LastSynced, got %+v", hs.BMKG)
	}
	if hs.PVMBG.Status != "ERROR" {
		t.Errorf("expected PVMBG status ERROR, got %s", hs.PVMBG.Status)
	}
}

func TestPoll_CircuitBreakerSkipPath(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32
	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	var failing atomic.Bool
	pvmbgSrv := toggleablePVMBGServer(t, &failing, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker(bmkgSrv.URL, pvmbgSrv.URL, 2, 30*time.Second)

	// 1. Initial success sets LastSynced
	p.Poll(context.Background(), "succ", time.Time{})
	hs := p.Health()
	if hs.PVMBG.Status != "UP" || hs.PVMBG.LastSynced.IsZero() {
		t.Fatalf("expected initial PVMBG UP, got %+v", hs.PVMBG)
	}
	lastSynced := hs.PVMBG.LastSynced

	// 2. Fail twice to trip circuit breaker
	failing.Store(true)
	p.Poll(context.Background(), "fail-1", time.Time{})
	p.Poll(context.Background(), "fail-2", time.Time{})

	if !p.breaker.IsOpen() {
		t.Fatal("expected circuit breaker to be open after 2 failures")
	}

	// 3. Skip path on open breaker marks OUTAGE and preserves lastSynced
	p.Poll(context.Background(), "skip-open", time.Time{})
	hs = p.Health()
	if hs.PVMBG.Status != "OUTAGE" {
		t.Errorf("expected OUTAGE on skip path, got %s", hs.PVMBG.Status)
	}
	if hs.PVMBG.StaleSince == nil || !hs.PVMBG.StaleSince.Equal(lastSynced) {
		t.Errorf("expected StaleSince equal to lastSynced %v, got %v", lastSynced, hs.PVMBG.StaleSince)
	}
	if !hs.PVMBG.LastSynced.Equal(lastSynced) {
		t.Errorf("expected LastSynced unchanged, got %v", hs.PVMBG.LastSynced)
	}
}

func TestPoller_HydrateWatermarks(t *testing.T) {
	bmkgTime := time.Date(2026, 10, 8, 1, 0, 0, 0, time.UTC)
	pvmbgTime := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	repo := &fakeRepo{
		latestTimes: map[string]time.Time{
			"BMKG":  bmkgTime,
			"PVMBG": pvmbgTime,
		},
	}
	cfg := &config.Config{
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
	}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), repo, zerolog.Nop())

	if err := p.HydrateWatermarks(context.Background()); err != nil {
		t.Fatalf("unexpected hydrate error: %v", err)
	}

	hs := p.Health()
	if !hs.BMKG.LastSynced.Equal(bmkgTime) {
		t.Errorf("expected BMKG LastSynced %v, got %v", bmkgTime, hs.BMKG.LastSynced)
	}
	if !hs.PVMBG.LastSynced.Equal(pvmbgTime) {
		t.Errorf("expected PVMBG LastSynced %v, got %v", pvmbgTime, hs.PVMBG.LastSynced)
	}
}

func TestPoller_WatermarkAdvancesToEventTimestamp(t *testing.T) {
	eventTime := time.Date(2026, 10, 8, 2, 16, 54, 0, time.UTC)
	eventTimeStr := eventTime.Format(time.RFC3339)

	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{{
			ReportID:   "rpt-1",
			VolcanoID:  "GNG-001",
			AlertLevel: agency.AlertNormal,
			ReportedAt: eventTimeStr,
		}})
	}))
	defer pvmbgSrv.Close()

	bmkgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.SeismicEvent{})
	}))
	defer bmkgSrv.Close()

	p := newPollerWithBreaker(bmkgSrv.URL, pvmbgSrv.URL, 5, 30*time.Second)
	p.Poll(context.Background(), "test-watermark", time.Time{})

	hs := p.Health()
	if !hs.PVMBG.LastSynced.Equal(eventTime) {
		t.Errorf("expected PVMBG LastSynced to equal event time %v, got %v", eventTime, hs.PVMBG.LastSynced)
	}
}

func TestPoller_BacklogDrainingPagination(t *testing.T) {
	var pageCalls atomic.Int32
	t1 := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)

	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := pageCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if call == 1 {
			// First page returns full batch of 250 records
			reports := make([]agency.VolcanicReport, 250)
			for i := 0; i < 250; i++ {
				reports[i] = agency.VolcanicReport{
					ReportID:   fmt.Sprintf("rpt-%04d", i),
					VolcanoID:  "GNG-001",
					AlertLevel: agency.AlertNormal,
					ReportedAt: t1.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
				}
			}
			json.NewEncoder(w).Encode(reports)
			return
		}
		// Second page returns remaining 50 records
		reports := make([]agency.VolcanicReport, 50)
		for i := 0; i < 50; i++ {
			reports[i] = agency.VolcanicReport{
				ReportID:   fmt.Sprintf("rpt-p2-%04d", i),
				VolcanoID:  "GNG-001",
				AlertLevel: agency.AlertNormal,
				ReportedAt: t1.Add(250*time.Second + time.Duration(i)*time.Second).Format(time.RFC3339),
			}
		}
		json.NewEncoder(w).Encode(reports)
	}))
	defer pvmbgSrv.Close()

	bmkgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.SeismicEvent{})
	}))
	defer bmkgSrv.Close()

	repo := &fakeRepo{}
	cfg := &config.Config{
		BMKGBaseURL:          bmkgSrv.URL,
		PVMBGBaseURL:         pvmbgSrv.URL,
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         1500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
	}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), repo, zerolog.Nop())

	p.Poll(context.Background(), "test-drain", time.Time{})

	if pageCalls.Load() != 2 {
		t.Fatalf("expected 2 page calls for backlog draining, got %d", pageCalls.Load())
	}
	if repo.upserts != 300 {
		t.Fatalf("expected 300 upserts across two pages, got %d", repo.upserts)
	}

	expectedLast := t1.Add(299 * time.Second)
	hs := p.Health()
	if !hs.PVMBG.LastSynced.Equal(expectedLast) {
		t.Errorf("expected LastSynced %v, got %v", expectedLast, hs.PVMBG.LastSynced)
	}
}
