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
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
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
	mux := http.NewServeMux()
	mux.HandleFunc("/seismic-events", func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.SeismicEvent{{EventID: "E-1", Magnitude: 5.5, OccurredAt: time.Now().Format(time.RFC3339)}})
	})
	mux.HandleFunc("/tsunami-warnings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.TsunamiWarning{})
	})
	return httptest.NewServer(mux)
}

func slowPVMBGServer(t *testing.T, delay time.Duration, called *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{{ReportID: "VR-1", AlertLevel: agency.AlertNormal, ReportedAt: time.Now().Format(time.RFC3339)}})
	}))
}

func toggleablePVMBGServer(t *testing.T, failing *atomic.Bool, called *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{{ReportID: "VR-1", AlertLevel: agency.AlertNormal, ReportedAt: time.Now().Format(time.RFC3339)}})
	}))
}

func newPollerWithBreaker(bmkgURL, pvmbgURL string, threshold int, probeInterval time.Duration) *Poller {
	cfg := &config.Config{
		BMKGBaseURL:          bmkgURL,
		PVMBGBaseURL:         pvmbgURL,
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         1500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     threshold,
		PVMBGCbProbeInterval: probeInterval,
	}
	return New(cfg, normalizer.New(zerolog.Nop(), nil), &fakeRepo{}, zerolog.Nop())
}

func TestPollConcurrent_BMKGNotBlockedBySlowPVMBG(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32
	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()
	pvmbgSrv := slowPVMBGServer(t, 600*time.Millisecond, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker(bmkgSrv.URL, pvmbgSrv.URL, 5, 30*time.Second)
	start := time.Now()
	p.Poll(context.Background(), "test-concurrent", time.Time{})
	elapsed := time.Since(start)

	if bmkgCalled.Load() == 0 || pvmbgCalled.Load() == 0 {
		t.Fatal("expected both agencies to be polled concurrently")
	}
	if elapsed > 1200*time.Millisecond {
		t.Errorf("poll took %v, expected concurrent execution within ~600ms", elapsed)
	}
	if hs := p.Health(); hs.BMKG.Status != "UP" {
		t.Errorf("expected BMKG UP, got %s", hs.BMKG.Status)
	}
}

func TestHealthConcurrentSafe(t *testing.T) {
	bmkgSrv := fastBMKGServer(t, new(atomic.Int32))
	defer bmkgSrv.Close()
	pvmbgSrv := slowPVMBGServer(t, 0, new(atomic.Int32))
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker(bmkgSrv.URL, pvmbgSrv.URL, 5, 30*time.Second)
	var wg sync.WaitGroup
	var stop atomic.Bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			p.Poll(context.Background(), "race-test", time.Time{})
		}
	}()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = p.Health()
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
