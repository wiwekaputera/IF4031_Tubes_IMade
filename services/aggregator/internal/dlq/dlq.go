// Package dlq retries transient failures and replays parked messages.
// It depends only on the standard library so the policy can be tested in isolation.
package dlq

import (
	"context"
	"errors"
	"time"
)

const (
	// MaxAttempts is how many times a transient failure is retried before it is parked.
	MaxAttempts = 5
	baseBackoff = 5 * time.Second
	maxBackoff  = 10 * time.Minute
)

// Row is one dead-letter entry as the worker sees it.
type Row struct {
	ID          int64     `json:"id"`
	Source      string    `json:"source"`
	SourceRefID string    `json:"source_ref_id"`
	Raw         []byte    `json:"-"`
	Reason      string    `json:"reason"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error"`
	ReceivedAt  time.Time `json:"received_at"`
}

// Store is the persistence the worker needs. The PostgreSQL implementation lives in the store package.
type Store interface {
	// DueRetries returns status='retrying' rows whose next_attempt_at <= now, oldest first.
	DueRetries(ctx context.Context, now time.Time, limit int) ([]Row, error)
	// Parked returns status='parked' rows, oldest first (received_at, id).
	Parked(ctx context.Context, limit int) ([]Row, error)
	// List returns rows with the given status, oldest first, for inspection.
	List(ctx context.Context, status string, limit int) ([]Row, error)
	MarkResolved(ctx context.Context, id int64) error
	Reschedule(ctx context.Context, id int64, attempts int, next time.Time, lastErr string) error
	MarkParked(ctx context.Context, id int64, attempts int, lastErr string) error
}

// Handler reprocesses one raw message: classify, normalize, upsert. It must be idempotent.
// Return a *PermanentError for failures retrying cannot fix; any other error is transient.
type Handler func(ctx context.Context, raw []byte) error

// PermanentError marks a failure that retrying cannot fix.
type PermanentError struct{ Reason string }

func (e *PermanentError) Error() string { return e.Reason }

func isPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// Backoff returns the delay before retry number attempts (1-based): 5s, 10s, 20s, ... capped at 10 min.
func Backoff(attempts int) time.Duration {
	d := baseBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	return d
}

// Result counts what one pass did.
type Result struct {
	Resolved    int
	Rescheduled int
	Parked      int
}

// RunRetries processes due transient failures once.
func RunRetries(ctx context.Context, s Store, h Handler, now time.Time, limit int) (Result, error) {
	var res Result
	rows, err := s.DueRetries(ctx, now, limit)
	if err != nil {
		return res, err
	}
	for _, row := range rows {
		attempts := row.Attempts + 1
		herr := h(WithReceivedAt(ctx, row.ReceivedAt), row.Raw)
		switch {
		case herr == nil:
			if err := s.MarkResolved(ctx, row.ID); err != nil {
				return res, err
			}
			res.Resolved++
		case isPermanent(herr) || attempts >= MaxAttempts:
			if err := s.MarkParked(ctx, row.ID, attempts, herr.Error()); err != nil {
				return res, err
			}
			res.Parked++
		default:
			if err := s.Reschedule(ctx, row.ID, attempts, now.Add(Backoff(attempts)), herr.Error()); err != nil {
				return res, err
			}
			res.Rescheduled++
		}
	}
	return res, nil
}

// Replay reprocesses parked messages after the normalizer has been fixed.
// Rows are handled oldest first. Once one row for a source_ref_id fails, later rows
// for that same key are left parked, so per-key order is preserved.
func Replay(ctx context.Context, s Store, h Handler, limit int) (Result, error) {
	var res Result
	rows, err := s.Parked(ctx, limit)
	if err != nil {
		return res, err
	}
	blocked := map[string]bool{}
	for _, row := range rows {
		key := row.Source + "|" + row.SourceRefID
		if blocked[key] {
			res.Parked++
			continue
		}
		if herr := h(WithReceivedAt(ctx, row.ReceivedAt), row.Raw); herr != nil {
			blocked[key] = true
			if err := s.MarkParked(ctx, row.ID, row.Attempts+1, herr.Error()); err != nil {
				return res, err
			}
			res.Parked++
			continue
		}
		if err := s.MarkResolved(ctx, row.ID); err != nil {
			return res, err
		}
		res.Resolved++
	}
	return res, nil
}

type receivedAtKey struct{}

// WithReceivedAt returns ctx carrying the time a dead-lettered message was
// first received. Retry and replay set it so a handler can tell when the
// message actually arrived, not when it is being reprocessed.
func WithReceivedAt(ctx context.Context, t time.Time) context.Context {
	if t.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, receivedAtKey{}, t)
}

// ReceivedAt returns the time set by WithReceivedAt, or fallback.
func ReceivedAt(ctx context.Context, fallback time.Time) time.Time {
	if t, ok := ctx.Value(receivedAtKey{}).(time.Time); ok {
		return t
	}
	return fallback
}
