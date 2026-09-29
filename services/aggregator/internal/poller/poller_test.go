package poller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
)

func failingPVMBGServer(t *testing.T, called *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/volcanic-reports", func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	return httptest.NewServer(mux)
}

func toggleablePVMBGServer(t *testing.T, failing *atomic.Bool, called *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/volcanic-reports", func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{
			{ReportID: "VR-1", VolcanoID: "GNG-001", AlertLevel: agency.AlertNormal, ReportedAt: time.Now().Format(time.RFC3339)},
		})
	})
	return httptest.NewServer(mux)
}

func newPollerWithBreaker(bmkgURL, pvmbgURL string, threshold int, probeInterval time.Duration) *Poller {
	cfg := &config.Config{
		BMKGBaseURL:          bmkgURL,
		PVMBGBaseURL:         pvmbgURL,
		BMKGTimeout:          1 * time.Second,
		PVMBGTimeout:         4 * time.Second,
		PollInterval:         3 * time.Second,
		PVMBGCbThreshold:     threshold,
		PVMBGCbProbeInterval: probeInterval,
	}
	return New(cfg, normalizer.New(zerolog.Nop(), nil), &fakeRepo{}, zerolog.Nop())
}

func TestPoll_BMKGSucceedWhilePVMBGFails(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32

	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	pvmbgSrv := failingPVMBGServer(t, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newTestPoller(t, bmkgSrv.URL, pvmbgSrv.URL)

	p.Poll(context.Background(), "test-bmkg-while-pvmbg-fails", time.Time{})

	if bmkgCalled.Load() == 0 {
		t.Error("BMKG was never polled")
	}
	if pvmbgCalled.Load() == 0 {
		t.Error("PVMBG was never polled")
	}

	hs := p.Health()
	if hs.BMKG.Status != "UP" {
		t.Errorf("BMKG status = %q, want UP", hs.BMKG.Status)
	}
	if hs.BMKG.LastSynced.IsZero() {
		t.Error("BMKG LastSynced should be set after successful poll")
	}
	if hs.PVMBG.Status != "ERROR" {
		t.Errorf("PVMBG status = %q, want ERROR", hs.PVMBG.Status)
	}
}

func TestPoll_BMKGDataRefreshesIndependentlyOfSlowPVMBG(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32

	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	pvmbgSrv := slowPVMBGServer(t, 4*time.Second, &pvmbgCalled)
	defer pvmbgSrv.Close()

	repo := &fakeRepo{}
	cfg := &config.Config{
		BMKGBaseURL:          bmkgSrv.URL,
		PVMBGBaseURL:         pvmbgSrv.URL,
		BMKGTimeout:          1 * time.Second,
		PVMBGTimeout:         4 * time.Second,
		PollInterval:         3 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
	}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), repo, zerolog.Nop())

	start := time.Now()
	p.Poll(context.Background(), "test-bmkg-refresh", time.Time{})
	elapsed := time.Since(start)

	if bmkgCalled.Load() == 0 {
		t.Error("BMKG was never polled")
	}
	if pvmbgCalled.Load() == 0 {
		t.Error("PVMBG was never polled")
	}
	if repo.upserts == 0 {
		t.Error("expected BMKG events to be upserted despite PVMBG delay")
	}

	hs := p.Health()
	if hs.BMKG.Status != "UP" {
		t.Errorf("BMKG status = %q, want UP", hs.BMKG.Status)
	}
	if elapsed > 4500*time.Millisecond {
		t.Errorf("Poll took %v; expected BMKG to complete within ~1s and not block on PVMBG", elapsed)
	}
}

func TestPoll_ConcurrentHealthSafeWithFailingPVMBG(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32

	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	pvmbgSrv := failingPVMBGServer(t, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newTestPoller(t, bmkgSrv.URL, pvmbgSrv.URL)

	var wg sync.WaitGroup
	var stop atomic.Bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			p.Poll(context.Background(), "concurrent-race-failing", time.Time{})
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				hs := p.Health()
				if hs.BMKG.Status != "UP" && hs.BMKG.Status != "ERROR" && hs.BMKG.Status != "DOWN" {
					t.Errorf("unexpected BMKG status: %q", hs.BMKG.Status)
				}
				if hs.PVMBG.Status != "UP" && hs.PVMBG.Status != "ERROR" && hs.PVMBG.Status != "OUTAGE" && hs.PVMBG.Status != "DOWN" {
					t.Errorf("unexpected PVMBG status: %q", hs.PVMBG.Status)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	hs := p.Health()
	if hs.BMKG.Status != "UP" {
		t.Errorf("BMKG status = %q, want UP", hs.BMKG.Status)
	}
	if bmkgCalled.Load() == 0 {
		t.Error("BMKG was never polled during concurrent test")
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

	failing.Store(false)
	p.Poll(context.Background(), "circuit-skip-success", time.Time{})

	hs := p.Health()
	if hs.PVMBG.Status != "UP" {
		t.Fatalf("after successful PVMBG poll: expected UP, got %q", hs.PVMBG.Status)
	}
	lastSynced := hs.PVMBG.LastSynced
	if lastSynced.IsZero() {
		t.Fatal("PVMBG LastSynced should be set after successful poll")
	}
	if hs.PVMBG.StaleSince != nil {
		t.Error("PVMBG StaleSince should be nil after successful poll")
	}

	failing.Store(true)
	p.Poll(context.Background(), "circuit-skip-fail-1", time.Time{})
	p.Poll(context.Background(), "circuit-skip-fail-2", time.Time{})

	if !p.breaker.IsOpen() {
		t.Fatal("expected circuit breaker to be open after 2 consecutive PVMBG failures")
	}

	p.Poll(context.Background(), "circuit-skip-open", time.Time{})

	hs = p.Health()
	if hs.PVMBG.Status != "OUTAGE" {
		t.Errorf("after skip path: PVMBG status = %q, want OUTAGE", hs.PVMBG.Status)
	}
	if hs.PVMBG.StaleSince == nil {
		t.Fatal("after skip path: PVMBG StaleSince should be set")
	}
	if !hs.PVMBG.StaleSince.Equal(lastSynced) {
		t.Errorf("StaleSince = %v, want %v (LastSynced from successful poll)", *hs.PVMBG.StaleSince, lastSynced)
	}
	if !hs.PVMBG.LastSynced.Equal(lastSynced) {
		t.Errorf("LastSynced = %v, want %v (should not change in skip path)", hs.PVMBG.LastSynced, lastSynced)
	}
}
