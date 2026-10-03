package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
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
