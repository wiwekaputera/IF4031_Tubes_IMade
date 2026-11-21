package breaker

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTest(c *clock) *Breaker {
	b := New(Config{FailureThreshold: 3, BaseOpen: 10 * time.Second, MaxOpen: 40 * time.Second})
	b.now = c.now
	b.rand = func() float64 { return 0.5 } // jitter factor exactly 1.0
	return b
}

func TestOpensAfterThresholdAndBlocks(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTest(c)
	for i := 0; i < 2; i++ {
		b.Failure()
		if b.State() != Closed {
			t.Fatalf("opened after %d failures", i+1)
		}
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("expected open and no request allowed")
	}
	if got := b.RetryIn(); got != 10*time.Second {
		t.Fatalf("RetryIn = %v", got)
	}
}

func TestHalfOpenAllowsOneProbe(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTest(c)
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	c.t = c.t.Add(10 * time.Second)
	if !b.Allow() || b.State() != HalfOpen {
		t.Fatal("expected one probe in half-open")
	}
	if b.Allow() {
		t.Fatal("a second request must wait for the probe result")
	}
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatal("successful probe must close the breaker")
	}
}

func TestFailedProbeDoublesOpenPeriodUpToMax(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTest(c)
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	want := []time.Duration{20 * time.Second, 40 * time.Second, 40 * time.Second}
	for _, w := range want {
		c.t = c.t.Add(b.RetryIn())
		if !b.Allow() {
			t.Fatal("probe expected")
		}
		b.Failure()
		if got := b.RetryIn(); got != w {
			t.Fatalf("open period = %v, want %v", got, w)
		}
	}
	// Recovery resets the back-off.
	c.t = c.t.Add(b.RetryIn())
	b.Allow()
	b.Success()
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	if got := b.RetryIn(); got != 10*time.Second {
		t.Fatalf("after recovery open period = %v, want base", got)
	}
}

func TestOpenPeriodJitterStaysWithin20Percent(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	for _, r := range []float64{0, 1} {
		b := newTest(c)
		b.rand = func() float64 { return r }
		for i := 0; i < 3; i++ {
			b.Failure()
		}
		got := b.RetryIn()
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("rand=%v open period %v outside ±20%%", r, got)
		}
	}
}

func TestBackoff(t *testing.T) {
	one := func() float64 { return 1 }
	cases := map[int]time.Duration{0: 0, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 10: 8 * time.Second}
	for n, w := range cases {
		if got := Backoff(n, time.Second, 8*time.Second, one); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", n, got, w)
		}
	}
	if got := Backoff(3, time.Second, 8*time.Second, func() float64 { return 0 }); got != 2*time.Second {
		t.Errorf("minimum jitter = %v, want half", got)
	}
}

func TestOnChangeReportsTransitions(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newTest(c)
	var seen []string
	b.OnChange(func(from, to State) { seen = append(seen, from.String()+">"+to.String()) })
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	c.t = c.t.Add(10 * time.Second)
	b.Allow()
	b.Success()
	want := []string{"closed>open", "open>half_open", "half_open>closed"}
	if len(seen) != len(want) {
		t.Fatalf("transitions %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("transitions %v, want %v", seen, want)
		}
	}
}
