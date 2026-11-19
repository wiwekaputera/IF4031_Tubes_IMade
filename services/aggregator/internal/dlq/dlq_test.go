package dlq

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeStore struct {
	due    []Row
	parked []Row
	calls  []string
}

func (f *fakeStore) DueRetries(context.Context, time.Time, int) ([]Row, error) { return f.due, nil }
func (f *fakeStore) Parked(context.Context, int) ([]Row, error)                { return f.parked, nil }
func (f *fakeStore) MarkResolved(_ context.Context, id int64) error {
	f.calls = append(f.calls, "resolved")
	return nil
}
func (f *fakeStore) Reschedule(_ context.Context, id int64, attempts int, next time.Time, _ string) error {
	f.calls = append(f.calls, "rescheduled")
	return nil
}
func (f *fakeStore) MarkParked(_ context.Context, id int64, attempts int, _ string) error {
	f.calls = append(f.calls, "parked")
	return nil
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	if Backoff(1) != 5*time.Second || Backoff(2) != 10*time.Second || Backoff(3) != 20*time.Second {
		t.Fatalf("unexpected early backoff: %v %v %v", Backoff(1), Backoff(2), Backoff(3))
	}
	if Backoff(50) != maxBackoff {
		t.Fatalf("Backoff(50) = %v, want cap %v", Backoff(50), maxBackoff)
	}
}

func TestRunRetriesOutcomes(t *testing.T) {
	now := time.Now()
	ok := func(context.Context, []byte) error { return nil }
	transient := func(context.Context, []byte) error { return errors.New("db timeout") }
	permanent := func(context.Context, []byte) error { return &PermanentError{Reason: "type_mismatch:x"} }

	tests := []struct {
		name string
		row  Row
		h    Handler
		want string
	}{
		{"success resolves", Row{ID: 1}, ok, "resolved"},
		{"transient reschedules", Row{ID: 2, Attempts: 1}, transient, "rescheduled"},
		{"permanent parks at once", Row{ID: 3}, permanent, "parked"},
		{"last attempt parks", Row{ID: 4, Attempts: MaxAttempts - 1}, transient, "parked"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &fakeStore{due: []Row{tt.row}}
			res, err := RunRetries(context.Background(), s, tt.h, now, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(s.calls) != 1 || s.calls[0] != tt.want {
				t.Fatalf("calls = %v, want [%s]", s.calls, tt.want)
			}
			if res.Resolved+res.Rescheduled+res.Parked != 1 {
				t.Fatalf("result = %+v, want exactly one outcome", res)
			}
		})
	}
}

func TestReplayKeepsPerKeyOrder(t *testing.T) {
	rows := []Row{
		{ID: 1, Source: "PVMBG", SourceRefID: "R-1", Raw: []byte("a")},
		{ID: 2, Source: "PVMBG", SourceRefID: "R-1", Raw: []byte("b")}, // same key, must wait for row 1
		{ID: 3, Source: "PVMBG", SourceRefID: "R-2", Raw: []byte("c")}, // different key, independent
	}
	s := &fakeStore{parked: rows}
	h := func(_ context.Context, raw []byte) error {
		if string(raw) == "a" {
			return errors.New("still broken")
		}
		return nil
	}
	res, err := Replay(context.Background(), s, h, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"parked", "resolved"} // row 2 is skipped without a write
	if len(s.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", s.calls, want)
	}
	for i := range want {
		if s.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", s.calls, want)
		}
	}
	if res.Resolved != 1 || res.Parked != 2 {
		t.Fatalf("result = %+v, want Resolved=1 Parked=2", res)
	}
}

func (f *fakeStore) List(context.Context, string, int) ([]Row, error) { return nil, nil }
