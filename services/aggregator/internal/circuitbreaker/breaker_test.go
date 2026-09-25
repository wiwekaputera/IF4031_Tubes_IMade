package circuitbreaker

import (
	"testing"
	"time"
)

func TestClosedStateInitial(t *testing.T) {
	b := New(3, 30*time.Second)
	if b.State() != StateClosed {
		t.Errorf("expected StateClosed, got %d", b.State())
	}
	if !b.Allow() {
		t.Error("expected Allow to return true in closed state")
	}
}

func TestTripsToOpenAfterThreshold(t *testing.T) {
	b := New(3, 30*time.Second)

	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Errorf("expected StateClosed after 2 failures, got %d", b.State())
	}
	if !b.Allow() {
		t.Error("expected Allow to return true in closed state")
	}

	b.RecordFailure()
	if b.State() != StateOpen {
		t.Errorf("expected StateOpen after 3 failures, got %d", b.State())
	}
	if b.Allow() {
		t.Error("expected Allow to return false in open state")
	}
}

func TestHalfOpenAfterProbeInterval(t *testing.T) {
	b := New(3, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	if b.State() != StateOpen {
		t.Fatal("expected StateOpen")
	}

	time.Sleep(60 * time.Millisecond)

	if !b.Allow() {
		t.Error("expected Allow to return true after probe interval (half-open)")
	}
	if b.State() != StateHalfOpen {
		t.Errorf("expected StateHalfOpen, got %d", b.State())
	}
}

func TestHalfOpenSuccessCloses(t *testing.T) {
	b := New(3, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	time.Sleep(60 * time.Millisecond)
	b.Allow()

	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Errorf("expected StateClosed after half-open success, got %d", b.State())
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	b := New(3, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	time.Sleep(60 * time.Millisecond)
	b.Allow()

	b.RecordFailure()
	if b.State() != StateOpen {
		t.Errorf("expected StateOpen after half-open failure, got %d", b.State())
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b := New(3, 30*time.Second)

	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess()

	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Errorf("expected StateClosed (count reset), got %d", b.State())
	}
}
