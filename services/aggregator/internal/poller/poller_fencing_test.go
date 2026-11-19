package poller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

type fakeElector struct {
	token    int64
	ok       bool
	demoted  atomic.Int32
	renewCnt atomic.Int32
}

func (f *fakeElector) RenewNow(context.Context) (int64, bool) {
	f.renewCnt.Add(1)
	return f.token, f.ok
}

func (f *fakeElector) IsLeader() (int64, bool) {
	return f.token, f.ok
}

func (f *fakeElector) Demote() {
	f.demoted.Add(1)
	f.ok = false
}

func TestPoller_DoesNotCallUpstreamWhenRenewFails(t *testing.T) {
	var bmkgCalled, pvmbgCalled atomic.Int32
	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()
	pvmbgSrv := toggleablePVMBGServer(t, &atomic.Bool{}, &pvmbgCalled)
	defer pvmbgSrv.Close()

	cfg := &config.Config{
		BMKGBaseURL:          bmkgSrv.URL,
		PVMBGBaseURL:         pvmbgSrv.URL,
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
		InstanceID:           "agg-1",
	}
	elector := &fakeElector{token: 0, ok: false}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), &fakeRepo{}, zerolog.Nop()).WithElector(elector)

	p.Poll(context.Background(), "no-lease", time.Time{})

	if elector.renewCnt.Load() != 1 {
		t.Fatalf("expected RenewNow called once, got %d", elector.renewCnt.Load())
	}
	if bmkgCalled.Load() != 0 || pvmbgCalled.Load() != 0 {
		t.Fatalf("expected 0 upstream calls when renew fails, got bmkg=%d pvmbg=%d", bmkgCalled.Load(), pvmbgCalled.Load())
	}
}

func TestPoller_ReloadsWatermarkOnPromotionWithOverlap(t *testing.T) {
	wm := time.Date(2026, 11, 18, 10, 0, 10, 0, time.UTC)
	var seenSince atomic.Value

	bmkgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/seismic-events" {
			seenSince.Store(r.URL.Query().Get("since"))
			if r.Header.Get("X-Instance-ID") != "agg-2" {
				t.Errorf("expected X-Instance-ID agg-2, got %q", r.Header.Get("X-Instance-ID"))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.SeismicEvent{})
	}))
	defer bmkgSrv.Close()

	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{})
	}))
	defer pvmbgSrv.Close()

	repo := &fakeRepo{
		watermarks: map[string]time.Time{
			"BMKG":  wm,
			"PVMBG": wm,
		},
	}
	cfg := &config.Config{
		BMKGBaseURL:          bmkgSrv.URL,
		PVMBGBaseURL:         pvmbgSrv.URL,
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
		InstanceID:           "agg-2",
	}
	elector := &fakeElector{token: 3, ok: true}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), repo, zerolog.Nop()).WithElector(elector)

	p.Poll(context.Background(), "promoted", time.Time{})

	expectedSince := wm.Add(-watermarkOverlap).UTC().Format(time.RFC3339)
	if got, _ := seenSince.Load().(string); got != expectedSince {
		t.Fatalf("expected since=%q (watermark - 1s overlap), got %q", expectedSince, got)
	}
}

func TestPoller_DemotesOnErrFenced(t *testing.T) {
	var bmkgCalled atomic.Int32
	bmkgSrv := fastBMKGServer(t, &bmkgCalled)
	defer bmkgSrv.Close()
	pvmbgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]agency.VolcanicReport{})
	}))
	defer pvmbgSrv.Close()

	repo := &fakeRepo{fencedErr: store.ErrFenced}
	cfg := &config.Config{
		BMKGBaseURL:          bmkgSrv.URL,
		PVMBGBaseURL:         pvmbgSrv.URL,
		BMKGTimeout:          500 * time.Millisecond,
		PVMBGTimeout:         500 * time.Millisecond,
		PollInterval:         1 * time.Second,
		PVMBGCbThreshold:     5,
		PVMBGCbProbeInterval: 30 * time.Second,
		InstanceID:           "agg-zombie",
	}
	elector := &fakeElector{token: 1, ok: true}
	p := New(cfg, normalizer.New(zerolog.Nop(), nil), repo, zerolog.Nop()).WithElector(elector)

	p.Poll(context.Background(), "fenced-write", time.Time{})

	if elector.demoted.Load() != 1 {
		t.Fatalf("expected elector.Demote() called once on ErrFenced, got %d", elector.demoted.Load())
	}
	if _, ok := elector.IsLeader(); ok {
		t.Fatal("expected elector to no longer be leader after Demote")
	}
}
