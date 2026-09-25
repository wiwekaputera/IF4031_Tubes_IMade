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
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeRepo struct {
	mu      sync.Mutex
	upserts int
}

func (f *fakeRepo) HealthCheck(context.Context) error { return nil }
func (f *fakeRepo) UpsertEvent(_ context.Context, rec store.UpsertRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserts++
	return "id-" + rec.HazardID, nil
}
func (f *fakeRepo) GetEvents(context.Context, store.EventFilter) ([]hazard.HazardEvent, error) {
	return nil, nil
}
func (f *fakeRepo) GetVolcanoReferences(context.Context) (map[string]store.VolcanoReference, error) {
	return nil, nil
}

func fastBMKGServer(t *testing.T, called *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/seismic-events", func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.SeismicEvent{
			{EventID: "E-1", Magnitude: 5.5, DepthKm: 10, EpicenterLat: -7.5, EpicenterLon: 110.4, RegionName: "Jawa", OccurredAt: time.Now().Format(time.RFC3339), PotentialTsunami: false},
		})
	})
	mux.HandleFunc("/tsunami-warnings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.TsunamiWarning{})
	})
	return httptest.NewServer(mux)
}

func slowPVMBGServer(t *testing.T, delay time.Duration, called *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/volcanic-reports", func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{
			{ReportID: "VR-1", VolcanoID: "GNG-001", AlertLevel: agency.AlertNormal, ReportedAt: time.Now().Format(time.RFC3339)},
		})
	})
	return httptest.NewServer(mux)
}

func newTestPoller(t *testing.T, bmkgURL, pvmbgURL string) *Poller {
	t.Helper()
	cfg := &config.Config{
		BMKGBaseURL:          bmkgURL,
		PVMBGBaseURL:         pvmbgURL,
		BMKGTimeout:          1 * time.Second,
		PVMBGTimeout:         4 * time.Second,
		PollInterval:         3 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
	}
	norm := normalizer.New(zerolog.Nop(), nil)
	repo := &fakeRepo{}
	return New(cfg, norm, repo, zerolog.Nop())
}

func TestPollConcurrent_BMKGNotBlockedBySlowPVMBG(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32

	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	// PVMBG server delays 4s — BMKG should still complete in ~1s.
	pvmbgSrv := slowPVMBGServer(t, 4*time.Second, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newTestPoller(t, bmkgSrv.URL, pvmbgSrv.URL)

	start := time.Now()
	p.Poll(context.Background(), "test-concurrent", time.Time{})
	elapsed := time.Since(start)

	// BMKG has a 1s timeout; PVMBG has a 4s timeout. Both run concurrently,
	// so total wall time should be ~4s (the max of the two), not 5s (sum).
	// More importantly, BMKG should have completed within its own 1s budget.
	if elapsed > 4500*time.Millisecond {
		t.Errorf("Poll took %v; expected ~4s max (concurrent), not 5s+ (sequential)", elapsed)
	}

	if bmkgCalled.Load() == 0 {
		t.Error("BMKG was never polled")
	}
	if pvmbgCalled.Load() == 0 {
		t.Error("PVMBG was never polled")
	}

	// BMKG should be UP (it completed quickly).
	hs := p.Health()
	if hs.BMKG.Status != "UP" {
		t.Errorf("BMKG status = %q, want %q", hs.BMKG.Status, "UP")
	}
}

func TestPollConcurrent_BMKGCompletesWithinTimeout(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32

	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()

	// PVMBG delays far beyond its 4s timeout.
	pvmbgSrv := slowPVMBGServer(t, 6*time.Second, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newTestPoller(t, bmkgSrv.URL, pvmbgSrv.URL)

	p.Poll(context.Background(), "test-timeout", time.Time{})

	// BMKG should have succeeded despite PVMBG hanging.
	hs := p.Health()
	if hs.BMKG.Status != "UP" {
		t.Errorf("BMKG status = %q, want %q", hs.BMKG.Status, "UP")
	}
	// PVMBG should have timed out (status ERROR) because it took 30s with a 4s ctx.
	if hs.PVMBG.Status != "ERROR" {
		t.Errorf("PVMBG status = %q, want %q", hs.PVMBG.Status, "ERROR")
	}
}

func TestHealthConcurrentSafe(t *testing.T) {
	bmkgSrv := fastBMKGServer(t, new(atomic.Int32))
	defer bmkgSrv.Close()

	// PVMBG completes instantly; we're only testing races in Health().
	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{
			{ReportID: "VR-1", VolcanoID: "GNG-001", AlertLevel: agency.AlertNormal, ReportedAt: time.Now().Format(time.RFC3339)},
		})
	}))
	defer pvmbgSrv.Close()

	p := newTestPoller(t, bmkgSrv.URL, pvmbgSrv.URL)

	var wg sync.WaitGroup
	var stop atomic.Bool

	// Writer: poll loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			p.Poll(context.Background(), "race-test", time.Time{})
		}
	}()

	// Readers: concurrent Health() calls.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = p.Health()
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
