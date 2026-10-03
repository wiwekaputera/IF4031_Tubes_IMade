package circuitbreaker

import (
	"testing"
	"time"
)

func TestBreaker_StateTransitions(t *testing.T) {
	b := New(2, 20*time.Millisecond)

	// 1. Initial StateClosed
	if b.State() != StateClosed || !b.Allow() {
		t.Fatal("expected closed and allowed initially")
	}

	// 2. Success resets failure count
	b.RecordFailure()
	b.RecordSuccess()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatal("expected closed after reset")
	}

	// 3. Trips to StateOpen at threshold
	b.RecordFailure()
	if b.State() != StateOpen || b.Allow() {
		t.Fatal("expected open and rejected at threshold")
	}

	// 4. Transitions to StateHalfOpen after probe interval
	time.Sleep(30 * time.Millisecond)
	if !b.Allow() || b.State() != StateHalfOpen {
		t.Fatal("expected half-open after probe interval")
	}

	// 5. Success in half-open closes breaker
	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Fatal("expected closed after half-open success")
	}

	// 6. Half-open failure reopens breaker
	b.RecordFailure()
	b.RecordFailure() // open
	time.Sleep(30 * time.Millisecond)
	b.Allow() // half-open
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatal("expected open after half-open failure")
	}
}
