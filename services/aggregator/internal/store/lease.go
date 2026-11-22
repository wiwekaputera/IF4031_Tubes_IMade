package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrFenced is returned when a fenced write detects that the caller's lease
// has expired or been superseded by a newer fencing token.
var ErrFenced = errors.New("fencing token stale or lease not held")

// DefaultLeaseName is the lease row name used by the ingest poller.
const DefaultLeaseName = "ingest-poller"

// Fence identifies the active leader lease and fencing token for a write.
type Fence struct {
	LeaseName string
	Holder    string
	Token     int64
}

// LeaseInfo describes the current state of a leader lease row in the DB.
type LeaseInfo struct {
	Name         string
	Holder       string
	FencingToken int64
	ExpiresAt    time.Time
	IsActive     bool
}

// AcquireOrRenewLease attempts to acquire an expired lease or renew an existing
// lease held by holder, using CockroachDB's now() as the sole time source.
// Takeover increments fencing_token; renewal preserves it.
func (r *PostgresRepo) AcquireOrRenewLease(ctx context.Context, leaseName, holder string, ttl time.Duration) (int64, bool, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	interval := fmt.Sprintf("%d milliseconds", ttl.Milliseconds())
	var token int64
	var acquired bool

	err := withRetry40001(ctx, func() error {
		acquired = false
		token = 0
		err := r.db.QueryRow(ctx, `
			UPDATE leader_leases
			   SET fencing_token = CASE WHEN holder = $2 AND expires_at > now() THEN fencing_token ELSE fencing_token + 1 END,
			       holder = $2,
			       expires_at = now() + $3::INTERVAL
			 WHERE name = $1 AND (expires_at <= now() OR holder = $2)
			RETURNING fencing_token
		`, leaseName, holder, interval).Scan(&token)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		acquired = true
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return token, acquired, nil
}

// ReleaseLease expires the lease immediately if currently held by holder.
func (r *PostgresRepo) ReleaseLease(ctx context.Context, leaseName, holder string) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	return withRetry40001(ctx, func() error {
		_, err := r.db.Exec(ctx, `
			UPDATE leader_leases
			   SET expires_at = now()
			 WHERE name = $1 AND holder = $2
		`, leaseName, holder)
		return err
	})
}

// GetLease returns the current lease row and whether it is unexpired according to DB now().
func (r *PostgresRepo) GetLease(ctx context.Context, leaseName string) (LeaseInfo, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var info LeaseInfo
	err := r.db.QueryRow(ctx, `
		SELECT name, holder, fencing_token, expires_at, (expires_at > now()) AS is_active
		FROM leader_leases
		WHERE name = $1
	`, leaseName).Scan(&info.Name, &info.Holder, &info.FencingToken, &info.ExpiresAt, &info.IsActive)
	if err != nil {
		return LeaseInfo{}, err
	}
	return info, nil
}

// GetPollerWatermark returns the durable watermark for source, or zero time if none exists.
func (r *PostgresRepo) GetPollerWatermark(ctx context.Context, source string) (time.Time, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var wm time.Time
	err := r.db.QueryRow(ctx, `
		SELECT watermark
		FROM poller_watermarks
		WHERE source = $1
	`, source).Scan(&wm)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return wm.UTC(), nil
}

func verifyLeaseTx(ctx context.Context, tx pgx.Tx, fence Fence) error {
	var ok int
	err := tx.QueryRow(ctx, `
		SELECT 1
		FROM leader_leases
		WHERE name = $1 AND holder = $2 AND fencing_token = $3 AND expires_at > now()
	`, fence.LeaseName, fence.Holder, fence.Token).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFenced
	}
	return err
}

const fencedBatchChunkSize = 50

// UpsertFencedBatch verifies the leader lease, upserts all events (with outbox),
// and advances poller_watermarks in short chunked SERIALIZABLE transactions.
// Each chunk re-verifies the lease inside the same transaction so a stale leader
// can never write even a single row; the watermark advances only in the final chunk.
// Returns ErrFenced (without retrying) if the lease is no longer valid.
func (r *PostgresRepo) UpsertFencedBatch(ctx context.Context, fence Fence, source string, watermark time.Time, recs []UpsertRequest) error {
	ctx, cancel := withDefaultTimeout(ctx, 10*time.Second)
	defer cancel()

	chunks := chunkUniqueByHazardID(recs, fencedBatchChunkSize)
	if len(chunks) == 0 {
		return withRetry40001(ctx, func() error {
			tx, err := r.db.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			if err := verifyLeaseTx(ctx, tx, fence); err != nil {
				return err
			}
			if !watermark.IsZero() {
				if err := advanceWatermarkTx(ctx, tx, source, watermark, fence.Token); err != nil {
					return err
				}
			}
			return tx.Commit(ctx)
		})
	}

	for i, chunk := range chunks {
		isLast := i == len(chunks)-1
		err := withRetry40001(ctx, func() error {
			tx, err := r.db.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			if err := verifyLeaseTx(ctx, tx, fence); err != nil {
				return err
			}
			if err := upsertChunkInTx(ctx, tx, chunk, &fence.Holder, &fence.Token); err != nil {
				return err
			}
			if isLast && !watermark.IsZero() {
				if err := advanceWatermarkTx(ctx, tx, source, watermark, fence.Token); err != nil {
					return err
				}
			}
			return tx.Commit(ctx)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func advanceWatermarkTx(ctx context.Context, tx pgx.Tx, source string, watermark time.Time, token int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO poller_watermarks (source, watermark, fencing_token, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (source) DO UPDATE SET
			watermark     = GREATEST(poller_watermarks.watermark, EXCLUDED.watermark),
			fencing_token = EXCLUDED.fencing_token,
			updated_at    = now()
	`, source, watermark.UTC(), token)
	return err
}
