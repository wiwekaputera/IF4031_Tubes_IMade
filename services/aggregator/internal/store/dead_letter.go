package store

import (
	"context"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/dlq"
)

// DeadLetter is a message that could not be accepted, kept with its raw bytes.
// Retryable=true means a transient failure (status "retrying"); false means a
// permanent schema or format problem (status "parked").
type DeadLetter struct {
	Source      string
	SourceRefID string
	Raw         []byte
	Reason      string
	Retryable   bool
}

// retryBackoff is the delay before the first retry of a transient failure.
const retryBackoff = 5 * time.Second

// InsertDeadLetter stores a rejected or failed message in the dead_letters table.
func (r *PostgresRepo) InsertDeadLetter(ctx context.Context, dl DeadLetter) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	status := "parked"
	var nextAttempt *time.Time
	if dl.Retryable {
		status = "retrying"
		t := time.Now().UTC().Add(retryBackoff)
		nextAttempt = &t
	}

	_, err := r.db.Exec(ctx, `
		INSERT INTO dead_letters
			(source, source_ref_id, raw_payload, reason, retryable, attempts, status, next_attempt_at, last_error)
		VALUES ($1, NULLIF($2, ''), $3, $4, $5, 0, $6, $7, $4)`,
		dl.Source, dl.SourceRefID, dl.Raw, dl.Reason, dl.Retryable, status, nextAttempt,
	)
	return err
}

const deadLetterColumns = `id, source, COALESCE(source_ref_id, ''), raw_payload, reason, status,
	attempts, received_at, COALESCE(last_error, '')`

// DueRetries returns retrying rows whose next attempt time has passed, oldest first.
func (r *PostgresRepo) DueRetries(ctx context.Context, now time.Time, limit int) ([]dlq.Row, error) {
	return r.queryDeadLetters(ctx,
		`SELECT `+deadLetterColumns+` FROM dead_letters
		 WHERE status = 'retrying' AND next_attempt_at <= $1
		 ORDER BY received_at, id LIMIT $2`, now, limit)
}

// Parked returns parked rows, oldest first.
func (r *PostgresRepo) Parked(ctx context.Context, limit int) ([]dlq.Row, error) {
	return r.queryDeadLetters(ctx,
		`SELECT `+deadLetterColumns+` FROM dead_letters
		 WHERE status = 'parked' ORDER BY received_at, id LIMIT $1`, limit)
}

// List returns rows with the given status, oldest first.
func (r *PostgresRepo) List(ctx context.Context, status string, limit int) ([]dlq.Row, error) {
	return r.queryDeadLetters(ctx,
		`SELECT `+deadLetterColumns+` FROM dead_letters
		 WHERE status = $1 ORDER BY received_at, id LIMIT $2`, status, limit)
}

// MarkResolved records that a dead-letter row was successfully reprocessed.
func (r *PostgresRepo) MarkResolved(ctx context.Context, id int64) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	_, err := r.db.Exec(ctx,
		`UPDATE dead_letters SET status = 'resolved', next_attempt_at = NULL, last_error = NULL WHERE id = $1`, id)
	return err
}

// Reschedule keeps a row retrying and sets its next attempt time.
func (r *PostgresRepo) Reschedule(ctx context.Context, id int64, attempts int, next time.Time, lastErr string) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	_, err := r.db.Exec(ctx,
		`UPDATE dead_letters SET attempts = $2, next_attempt_at = $3, last_error = $4 WHERE id = $1`,
		id, attempts, next, lastErr)
	return err
}

// MarkParked moves a row to parked, where it waits for an explicit replay.
func (r *PostgresRepo) MarkParked(ctx context.Context, id int64, attempts int, lastErr string) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	_, err := r.db.Exec(ctx,
		`UPDATE dead_letters SET status = 'parked', attempts = $2, next_attempt_at = NULL, last_error = $3 WHERE id = $1`,
		id, attempts, lastErr)
	return err
}

func (r *PostgresRepo) queryDeadLetters(ctx context.Context, sql string, args ...any) ([]dlq.Row, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []dlq.Row
	for rows.Next() {
		var row dlq.Row
		if err := rows.Scan(&row.ID, &row.Source, &row.SourceRefID, &row.Raw, &row.Reason,
			&row.Status, &row.Attempts, &row.ReceivedAt, &row.LastError); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
