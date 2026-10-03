package poller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/circuitbreaker"
)

func TestPVMBG_CircuitTransitions(t *testing.T) {
	var pvmbgCalled atomic.Int32
	var failing atomic.Bool
	failing.Store(true)

	pvmbgSrv := toggleablePVMBGServer(t, &failing, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker("", pvmbgSrv.URL, 2, 40*time.Millisecond)
	ctx := context.Background()

	// 1. Fail twice to trip breaker to StateOpen
	p.pollPVMBG(ctx, "open-1", time.Time{})
	p.pollPVMBG(ctx, "open-2", time.Time{})
	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen after 2 failures, got %d", p.breaker.State())
	}

	// 2. Poll while open takes skip path and sets OUTAGE
	p.pollPVMBG(ctx, "skip", time.Time{})
	if hs := p.Health(); hs.PVMBG.Status != "OUTAGE" {
		t.Fatalf("expected OUTAGE while breaker open, got %s", hs.PVMBG.Status)
	}

	// 3. Wait for probe interval -> half-open allows probe
	time.Sleep(50 * time.Millisecond)
	if !p.breaker.Allow() || p.breaker.State() != circuitbreaker.StateHalfOpen {
		t.Fatal("expected StateHalfOpen after probe interval")
	}

	// 4. Successful recovery closes breaker and restores UP
	failing.Store(false)
	p.pollPVMBG(ctx, "recover", time.Time{})
	if p.breaker.State() != circuitbreaker.StateClosed {
		t.Errorf("expected StateClosed after recovery, got %d", p.breaker.State())
	}
	if hs := p.Health(); hs.PVMBG.Status != "UP" || hs.PVMBG.StaleSince != nil {
		t.Errorf("expected UP with nil StaleSince, got %+v", hs.PVMBG)
	}

	// 5. Half-open failure returns to StateOpen
	failing.Store(true)
	p.pollPVMBG(ctx, "trip-1", time.Time{})
	p.pollPVMBG(ctx, "trip-2", time.Time{})
	time.Sleep(50 * time.Millisecond)
	p.breaker.Allow() // half-open
	p.pollPVMBG(ctx, "reopen-fail", time.Time{})
	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("expected StateOpen after half-open failure, got %d", p.breaker.State())
	}
	if hs := p.Health(); hs.PVMBG.Status != "ERROR" {
		t.Errorf("expected ERROR status on half-open failure, got %s", hs.PVMBG.Status)
	}
}
