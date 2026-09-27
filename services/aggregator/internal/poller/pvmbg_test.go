package poller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/circuitbreaker"
)

func TestPVMBG_CircuitOpensAfterRepeatedFailures(t *testing.T) {
	var pvmbgCalled atomic.Int32
	var failing atomic.Bool
	failing.Store(true)

	pvmbgSrv := toggleablePVMBGServer(t, &failing, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker("", pvmbgSrv.URL, 2, 30*time.Second)

	ctx := context.Background()
	p.pollPVMBG(ctx, "circuit-open-1", time.Time{})
	p.pollPVMBG(ctx, "circuit-open-2", time.Time{})

	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("expected StateOpen after 2 failures, got %d", p.breaker.State())
	}

	p.pollPVMBG(ctx, "circuit-open-skip", time.Time{})

	hs := p.Health()
	if hs.PVMBG.Status != "OUTAGE" {
		t.Errorf("expected OUTAGE on skip path, got %q", hs.PVMBG.Status)
	}
}

func TestPVMBG_CircuitOpensToHalfOpenToClose(t *testing.T) {
	var pvmbgCalled atomic.Int32
	var failing atomic.Bool
	failing.Store(true)

	pvmbgSrv := toggleablePVMBGServer(t, &failing, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker("", pvmbgSrv.URL, 2, 50*time.Millisecond)

	ctx := context.Background()

	p.pollPVMBG(ctx, "trans-open-1", time.Time{})
	p.pollPVMBG(ctx, "trans-open-2", time.Time{})

	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen after 2 failures, got %d", p.breaker.State())
	}

	p.pollPVMBG(ctx, "trans-skip", time.Time{})

	hs := p.Health()
	if hs.PVMBG.Status != "OUTAGE" {
		t.Fatalf("expected OUTAGE while breaker is open, got %q", hs.PVMBG.Status)
	}

	time.Sleep(100 * time.Millisecond)

	if p.breaker.Allow() != true {
		t.Fatal("expected Allow() to return true after probe interval (half-open)")
	}
	if p.breaker.State() != circuitbreaker.StateHalfOpen {
		t.Errorf("expected StateHalfOpen, got %d", p.breaker.State())
	}

	failing.Store(false)
	p.pollPVMBG(ctx, "trans-recover", time.Time{})

	if p.breaker.State() != circuitbreaker.StateClosed {
		t.Errorf("expected StateClosed after successful recovery, got %d", p.breaker.State())
	}

	hs = p.Health()
	if hs.PVMBG.Status != "UP" {
		t.Errorf("expected PVMBG UP after recovery, got %q", hs.PVMBG.Status)
	}
	if hs.PVMBG.StaleSince != nil {
		t.Error("expected StaleSince to be nil after recovery")
	}
	if hs.PVMBG.LastSynced.IsZero() {
		t.Error("expected LastSynced to be set after recovery")
	}
}

func TestPVMBG_CircuitHalfOpenReopensOnFailure(t *testing.T) {
	var pvmbgCalled atomic.Int32
	var failing atomic.Bool
	failing.Store(true)

	pvmbgSrv := toggleablePVMBGServer(t, &failing, &pvmbgCalled)
	defer pvmbgSrv.Close()

	p := newPollerWithBreaker("", pvmbgSrv.URL, 2, 50*time.Millisecond)

	ctx := context.Background()

	p.pollPVMBG(ctx, "reopen-1", time.Time{})
	p.pollPVMBG(ctx, "reopen-2", time.Time{})

	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %d", p.breaker.State())
	}

	time.Sleep(80 * time.Millisecond)

	p.breaker.Allow()

	if p.breaker.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %d", p.breaker.State())
	}

	failing.Store(true)
	p.pollPVMBG(ctx, "reopen-fail", time.Time{})

	if p.breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("expected StateOpen after half-open failure, got %d", p.breaker.State())
	}

	hs := p.Health()
	if hs.PVMBG.Status != "ERROR" {
		t.Errorf("expected ERROR after half-open failure, got %q", hs.PVMBG.Status)
	}
}
