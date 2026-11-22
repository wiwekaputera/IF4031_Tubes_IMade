package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/audit"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type VolcanoReference struct {
	VolcanoID string
	Name      string
	Latitude  float64
	Longitude float64
}

type CanonicalRepo interface {
	HealthCheck(ctx context.Context) error
	UpsertEvent(ctx context.Context, rec UpsertRequest) (string, error)
	UpsertFencedBatch(ctx context.Context, fence Fence, source string, watermark time.Time, recs []UpsertRequest) error
	GetEvents(ctx context.Context, filter EventFilter) ([]hazard.HazardEvent, error)
	ListEvents(ctx context.Context, filter ListFilter) (ListResult, error)
	ListAuditEvents(ctx context.Context, filter AuditEventsFilter) (audit.EventsPage, error)
	ListUnprocessed(ctx context.Context, filter UnprocessedFilter) (audit.UnprocessedPage, error)
	ComputeChecksum(ctx context.Context, source string) (audit.Checksum, error)
	LocateEvent(ctx context.Context, hazardID string) (audit.LocationResponse, error)
	GetDistribution(ctx context.Context) (DistributionReport, error)
	GetAnalyticsSummary(ctx context.Context, filter AnalyticsFilter) (AnalyticsSummary, error)
	GetVolcanoReferences(ctx context.Context) (map[string]VolcanoReference, error)
	GetLatestEventTime(ctx context.Context, source string) (time.Time, error)
	GetPollerWatermark(ctx context.Context, source string) (time.Time, error)
	GetLease(ctx context.Context, leaseName string) (LeaseInfo, error)
	InsertDeadLetter(ctx context.Context, dl DeadLetter) error
}

type UpsertRequest struct {
	HazardID      string
	Source        string
	SourceRefID   string
	HazardType    hazard.HazardType
	Severity      hazard.Severity
	AreaName      string
	Latitude      float64
	Longitude     float64
	OccurredAt    time.Time
	Sources       map[string]interface{}
	Attributes    map[string]interface{}
	CorrelationID string
	Fence         *Fence
}

const (
	defaultQueryLimit   = 1000
	defaultQueryTimeout = 5 * time.Second
)

type EventFilter struct {
	HazardType string
	HazardID   string
	Since      *time.Time
	Limit      int
}

// OutboxRecord is a committed canonical event waiting to be published.
type OutboxRecord struct {
	ID       int64
	EventKey string
	Payload  []byte
}

type PostgresRepo struct {
	db                *pgxpool.Pool
	logger            zerolog.Logger
	shardQueryTimeout time.Duration
}

func NewPostgresRepo(db *pgxpool.Pool, logger zerolog.Logger) *PostgresRepo {
	return &PostgresRepo{
		db:     db,
		logger: logger.With().Str("component", "canonical-repo").Logger(),
	}
}

// withDefaultTimeout ensures queries never hang indefinitely if the parent context has no deadline.
func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (r *PostgresRepo) HealthCheck(ctx context.Context) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	return r.db.Ping(ctx)
}

func (r *PostgresRepo) GetVolcanoReferences(ctx context.Context) (map[string]VolcanoReference, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	rows, err := r.db.Query(ctx, `SELECT volcano_id, name, latitude, longitude FROM volcano_reference`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]VolcanoReference)
	for rows.Next() {
		var ref VolcanoReference
		if err := rows.Scan(&ref.VolcanoID, &ref.Name, &ref.Latitude, &ref.Longitude); err != nil {
			return nil, err
		}
		result[ref.VolcanoID] = ref
	}
	return result, rows.Err()
}

func (r *PostgresRepo) GetLatestEventTime(ctx context.Context, source string) (time.Time, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var maxTime *time.Time
	err := r.db.QueryRow(ctx, `
		SELECT MAX(occurred_at)
		FROM canonical_events
		WHERE source = $1 AND deleted_at IS NULL
	`, source).Scan(&maxTime)
	if err != nil {
		return time.Time{}, err
	}
	if maxTime == nil {
		return time.Time{}, nil
	}
	return maxTime.UTC(), nil
}

// withRetry40001 retries fn when CockroachDB returns SQLSTATE 40001 (serialization failure).
func withRetry40001(ctx context.Context, fn func() error) error {
	const maxRetries = 5
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "40001" || attempt >= maxRetries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
}

func (r *PostgresRepo) UpsertEvent(ctx context.Context, rec UpsertRequest) (string, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var storedHazardID string
	err := withRetry40001(ctx, func() error {
		tx, err := r.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		var writtenBy *string
		var fencingToken *int64
		if rec.Fence != nil {
			if err := verifyLeaseTx(ctx, tx, *rec.Fence); err != nil {
				return err
			}
			writtenBy = &rec.Fence.Holder
			fencingToken = &rec.Fence.Token
		}

		storedHazardID, err = upsertEventInTx(ctx, tx, rec, writtenBy, fencingToken)
		if err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
	if err != nil {
		return "", err
	}
	return storedHazardID, nil
}

// chunkUniqueByHazardID splits recs into chunks of at most maxChunk elements
// while guaranteeing every HazardID is unique within each chunk (required by
// multi-row INSERT ... ON CONFLICT DO UPDATE) and preserving sequential order.
func chunkUniqueByHazardID(recs []UpsertRequest, maxChunk int) [][]UpsertRequest {
	if len(recs) == 0 {
		return nil
	}
	if maxChunk < 1 {
		maxChunk = 1
	}
	var chunks [][]UpsertRequest
	cur := make([]UpsertRequest, 0, min(len(recs), maxChunk))
	seen := make(map[string]struct{}, min(len(recs), maxChunk))
	for _, rec := range recs {
		if len(cur) >= maxChunk {
			chunks = append(chunks, cur)
			cur = make([]UpsertRequest, 0, maxChunk)
			clear(seen)
		} else if _, dup := seen[rec.HazardID]; dup {
			chunks = append(chunks, cur)
			cur = make([]UpsertRequest, 0, maxChunk)
			clear(seen)
		}
		seen[rec.HazardID] = struct{}{}
		cur = append(cur, rec)
	}
	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}
	return chunks
}

func upsertEventInTx(ctx context.Context, tx pgx.Tx, rec UpsertRequest, writtenBy *string, fencingToken *int64) (string, error) {
	if err := upsertChunkInTx(ctx, tx, []UpsertRequest{rec}, writtenBy, fencingToken); err != nil {
		return "", err
	}
	return rec.HazardID, nil
}

// upsertChunkInTx upserts a slice of records with distinct HazardIDs in a single
// multi-row SQL statement (no full-table scan, no-op on unchanged rows), then
// inserts outbox envelopes for newly created or modified rows in one batch SQL.
func upsertChunkInTx(ctx context.Context, tx pgx.Tx, recs []UpsertRequest, writtenBy *string, fencingToken *int64) error {
	if len(recs) == 0 {
		return nil
	}

	const colsPerRow = 13
	var q strings.Builder
	q.Grow(512 + len(recs)*64)
	q.WriteString(`
		INSERT INTO canonical_events
			(hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			 latitude, longitude, occurred_at, ingested_at, sources, attributes,
			 written_by, fencing_token)
		VALUES `)

	args := make([]any, 0, len(recs)*colsPerRow)
	byID := make(map[string]UpsertRequest, len(recs))

	for i, rec := range recs {
		if rec.Sources == nil {
			rec.Sources = make(map[string]interface{})
		}
		if rec.Attributes == nil {
			rec.Attributes = make(map[string]interface{})
		}
		sourcesJSON, err := json.Marshal(rec.Sources)
		if err != nil {
			return err
		}
		attrsJSON, err := json.Marshal(rec.Attributes)
		if err != nil {
			return err
		}

		if i > 0 {
			q.WriteString(", ")
		}
		base := i*colsPerRow + 1
		fmt.Fprintf(&q, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, now(), $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12)

		args = append(args,
			rec.HazardID, rec.Source, rec.SourceRefID, string(rec.HazardType), string(rec.Severity),
			rec.AreaName, rec.Latitude, rec.Longitude, rec.OccurredAt,
			sourcesJSON, attrsJSON, writtenBy, fencingToken,
		)
		byID[rec.HazardID] = rec
	}

	q.WriteString(`
		ON CONFLICT (shard, hazard_id) DO UPDATE SET
			severity      = EXCLUDED.severity,
			area_name     = EXCLUDED.area_name,
			sources       = canonical_events.sources || EXCLUDED.sources,
			attributes    = canonical_events.attributes || EXCLUDED.attributes,
			written_by    = COALESCE(EXCLUDED.written_by, canonical_events.written_by),
			fencing_token = COALESCE(EXCLUDED.fencing_token, canonical_events.fencing_token),
			version       = canonical_events.version + 1,
			updated_at    = now()
		WHERE canonical_events.severity IS DISTINCT FROM EXCLUDED.severity
		   OR canonical_events.area_name IS DISTINCT FROM EXCLUDED.area_name
		   OR canonical_events.sources IS DISTINCT FROM (canonical_events.sources || EXCLUDED.sources)
		   OR canonical_events.attributes IS DISTINCT FROM (canonical_events.attributes || EXCLUDED.attributes)
		RETURNING hazard_id, version, ingested_at
	`)

	rows, err := tx.Query(ctx, q.String(), args...)
	if err != nil {
		return err
	}

	type changedRow struct {
		hazardID   string
		version    int64
		ingestedAt time.Time
	}
	changed := make([]changedRow, 0, len(recs))
	for rows.Next() {
		var cr changedRow
		if err := rows.Scan(&cr.hazardID, &cr.version, &cr.ingestedAt); err != nil {
			rows.Close()
			return err
		}
		changed = append(changed, cr)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if len(changed) == 0 {
		return nil
	}

	var ob strings.Builder
	ob.Grow(128 + len(changed)*16)
	ob.WriteString(`INSERT INTO outbox (event_key, payload) VALUES `)
	obArgs := make([]any, 0, len(changed)*2)

	for i, cr := range changed {
		rec := byID[cr.hazardID]
		he := hazard.HazardEvent{
			HazardID:    cr.hazardID,
			Source:      hazard.Source(rec.Source),
			SourceRefID: rec.SourceRefID,
			HazardType:  rec.HazardType,
			Severity:    rec.Severity,
			AreaName:    rec.AreaName,
			Latitude:    rec.Latitude,
			Longitude:   rec.Longitude,
			OccurredAt:  rec.OccurredAt.Format(time.RFC3339),
			IngestedAt:  cr.ingestedAt.Format(time.RFC3339),
			Attributes:  rec.Attributes,
		}
		env := event.Envelope{
			EventID:    cr.hazardID,
			Version:    cr.version,
			EventType:  rec.HazardType,
			OccurredAt: rec.OccurredAt.Format(time.RFC3339),
			TraceID:    rec.CorrelationID,
			Data:       he,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if i > 0 {
			ob.WriteString(", ")
		}
		base := i*2 + 1
		fmt.Fprintf(&ob, "($%d, $%d)", base, base+1)
		obArgs = append(obArgs, cr.hazardID+":"+strconv.FormatInt(cr.version, 10), payload)
	}
	ob.WriteString(` ON CONFLICT (event_key) DO NOTHING`)

	_, err = tx.Exec(ctx, ob.String(), obArgs...)
	return err
}

// GetUnpublishedOutbox returns the oldest committed events that have not yet
// been acknowledged by the broker. The relay is intentionally single-threaded
// per Aggregator process; broker message IDs make retry after a crash safe.
func (r *PostgresRepo) GetUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(ctx, `
		SELECT id, event_key, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]OutboxRecord, 0, limit)
	for rows.Next() {
		var record OutboxRecord
		if err := rows.Scan(&record.ID, &record.EventKey, &record.Payload); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// MarkOutboxPublished records successful broker acknowledgement. If the
// process crashes before this update, JetStream deduplicates the retry using
// the row's event key.
func (r *PostgresRepo) MarkOutboxPublished(ctx context.Context, id int64) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	return withRetry40001(ctx, func() error {
		_, err := r.db.Exec(ctx, `
			UPDATE outbox
			SET published_at = now()
			WHERE id = $1 AND published_at IS NULL
		`, id)
		return err
	})
}

// MarkOutboxPublishedBatch marks a batch of outbox rows published in a single
// SQL statement, reducing DB round-trips and lock contention during catch-up.
func (r *PostgresRepo) MarkOutboxPublishedBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	return withRetry40001(ctx, func() error {
		_, err := r.db.Exec(ctx, `
			UPDATE outbox
			SET published_at = now()
			WHERE id = ANY($1) AND published_at IS NULL
		`, ids)
		return err
	})
}

func (r *PostgresRepo) GetEvents(ctx context.Context, filter EventFilter) ([]hazard.HazardEvent, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var q strings.Builder
	q.Grow(256)
	q.WriteString(`
		SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, attributes
		FROM canonical_events
		WHERE deleted_at IS NULL
	`)

	args := make([]any, 0, 5)
	argIdx := 1

	if filter.HazardType != "" {
		q.WriteString(" AND hazard_type = $")
		q.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.HazardType)
		argIdx++
	}
	if filter.HazardID != "" {
		q.WriteString(" AND shard = $")
		q.WriteString(strconv.Itoa(argIdx))
		args = append(args, ShardForHazardID(filter.HazardID))
		argIdx++
		q.WriteString(" AND hazard_id = $")
		q.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.HazardID)
		argIdx++
	}
	if filter.Since != nil {
		q.WriteString(" AND occurred_at >= $")
		q.WriteString(strconv.Itoa(argIdx))
		args = append(args, *filter.Since)
		argIdx++
	}
	q.WriteString(" ORDER BY occurred_at DESC")
	if filter.Limit <= 0 {
		filter.Limit = defaultQueryLimit
	}
	q.WriteString(" LIMIT $")
	q.WriteString(strconv.Itoa(argIdx))
	args = append(args, filter.Limit)

	rows, err := r.db.Query(ctx, q.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	capacity := filter.Limit
	if capacity > 128 {
		capacity = 128
	}
	events := make([]hazard.HazardEvent, 0, capacity)

	for rows.Next() {
		var he hazard.HazardEvent
		var occurredAt, ingestedAt time.Time
		if err := rows.Scan(
			&he.HazardID, &he.Source, &he.SourceRefID, &he.HazardType, &he.Severity,
			&he.AreaName, &he.Latitude, &he.Longitude, &occurredAt, &ingestedAt, &he.Attributes,
		); err != nil {
			return nil, err
		}
		he.OccurredAt = occurredAt.Format(time.RFC3339)
		he.IngestedAt = ingestedAt.Format(time.RFC3339)
		if he.Attributes == nil {
			he.Attributes = make(map[string]interface{})
		}
		events = append(events, he)
	}

	return events, rows.Err()
}
