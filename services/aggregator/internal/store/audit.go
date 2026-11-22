package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/audit"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

const (
	defaultAuditLimit = 100
	maxAuditLimit     = 1000
)

// ErrNotFound is returned when a requested hazard event does not exist or is soft-deleted.
var ErrNotFound = errors.New("event not found")

// AuditEventsFilter defines parameters for paginated scatter-gather audit event reads.
type AuditEventsFilter struct {
	Source string
	Since  *time.Time
	Limit  int
	Cursor string
}

type auditShardEvent struct {
	event      hazard.HazardEvent
	ingestedAt time.Time
}

type auditShardQuerier interface {
	queryAuditShard(ctx context.Context, shard int, filter AuditEventsFilter, cursor *decodedCursor, limit int) ([]auditShardEvent, error)
}

type pgAuditShardQuerier struct {
	repo *PostgresRepo
}

func (q *pgAuditShardQuerier) queryAuditShard(
	ctx context.Context,
	shard int,
	filter AuditEventsFilter,
	cursor *decodedCursor,
	limit int,
) ([]auditShardEvent, error) {
	var sql strings.Builder
	sql.Grow(320)
	sql.WriteString(`
		SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, attributes
		FROM canonical_events
		WHERE shard = $1 AND deleted_at IS NULL
	`)

	args := make([]any, 0, 6)
	args = append(args, shard)
	argIdx := 2

	if filter.Source != "" {
		sql.WriteString(" AND source = $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.Source)
		argIdx++
	}
	if filter.Since != nil {
		sql.WriteString(" AND ingested_at >= $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, *filter.Since)
		argIdx++
	}
	if cursor != nil {
		sql.WriteString(" AND (ingested_at, hazard_id) > ($")
		sql.WriteString(strconv.Itoa(argIdx))
		sql.WriteString(", $")
		sql.WriteString(strconv.Itoa(argIdx + 1))
		sql.WriteString(")")
		args = append(args, cursor.occurredAt, cursor.hazardID)
		argIdx += 2
	}

	sql.WriteString(" ORDER BY ingested_at ASC, hazard_id ASC LIMIT $")
	sql.WriteString(strconv.Itoa(argIdx))
	args = append(args, limit)

	rows, err := q.repo.db.Query(ctx, sql.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]auditShardEvent, 0, min(limit, 64))
	for rows.Next() {
		var he hazard.HazardEvent
		var occurredAt, ingestedAt time.Time
		if err := rows.Scan(
			&he.HazardID, &he.Source, &he.SourceRefID, &he.HazardType, &he.Severity,
			&he.AreaName, &he.Latitude, &he.Longitude, &occurredAt, &ingestedAt, &he.Attributes,
		); err != nil {
			return nil, err
		}
		he.OccurredAt = occurredAt.UTC().Format(time.RFC3339)
		he.IngestedAt = ingestedAt.UTC().Format(time.RFC3339)
		if he.Attributes == nil {
			he.Attributes = make(map[string]interface{})
		}
		out = append(out, auditShardEvent{
			event:      he,
			ingestedAt: ingestedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListAuditEvents performs a scatter-gather read ordered by (ingested_at ASC, hazard_id ASC)
// with keyset cursor pagination and partial-result degradation.
func (r *PostgresRepo) ListAuditEvents(ctx context.Context, filter AuditEventsFilter) (audit.EventsPage, error) {
	return listAuditEventsWithQuerier(ctx, &pgAuditShardQuerier{repo: r}, r.shardTimeout(), r.logger, filter)
}

func listAuditEventsWithQuerier(
	ctx context.Context,
	querier auditShardQuerier,
	timeout time.Duration,
	logger zerolog.Logger,
	filter AuditEventsFilter,
) (audit.EventsPage, error) {
	cursor, err := decodeCursor(filter.Cursor)
	if err != nil {
		return audit.EventsPage{}, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = defaultAuditLimit
	}
	if limit > maxAuditLimit {
		limit = maxAuditLimit
	}

	var (
		mu        sync.Mutex
		collected []auditShardEvent
	)

	// Fetch limit+1 per shard so we know whether another page exists.
	unavailable := scatterShards(ctx, timeout, logger, allShards(), func(shardCtx context.Context, shard int) error {
		rows, err := querier.queryAuditShard(shardCtx, shard, filter, cursor, limit+1)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			mu.Lock()
			collected = append(collected, rows...)
			mu.Unlock()
		}
		return nil
	})
	if unavailable == nil {
		unavailable = []string{}
	}

	sort.Slice(collected, func(i, j int) bool {
		if !collected[i].ingestedAt.Equal(collected[j].ingestedAt) {
			return collected[i].ingestedAt.Before(collected[j].ingestedAt)
		}
		return collected[i].event.HazardID < collected[j].event.HazardID
	})

	var nextCursor *string
	if len(collected) > limit {
		collected = collected[:limit]
		last := collected[limit-1]
		c := encodeCursor(last.ingestedAt, last.event.HazardID)
		nextCursor = &c
	}

	items := make([]hazard.HazardEvent, len(collected))
	for i, item := range collected {
		items[i] = item.event
	}

	return audit.EventsPage{
		Items:            items,
		NextCursor:       nextCursor,
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	}, nil
}

type checksumShardQuerier interface {
	querySourceRefIDs(ctx context.Context, shard int, source string) ([]string, error)
}

type pgChecksumShardQuerier struct {
	repo *PostgresRepo
}

func (q *pgChecksumShardQuerier) querySourceRefIDs(ctx context.Context, shard int, source string) ([]string, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if source != "" {
		rows, err = q.repo.db.Query(ctx, `
			SELECT source_ref_id
			FROM canonical_events
			WHERE shard = $1 AND source = $2 AND deleted_at IS NULL
		`, shard, source)
	} else {
		rows, err = q.repo.db.Query(ctx, `
			SELECT source_ref_id
			FROM canonical_events
			WHERE shard = $1 AND deleted_at IS NULL
		`, shard)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// ComputeChecksum gathers all non-deleted source_ref_id values across reachable shards,
// sorts them in byte order, joins with "\n", and returns the lowercase hex SHA-256 digest.
func (r *PostgresRepo) ComputeChecksum(ctx context.Context, source string) (audit.Checksum, error) {
	return computeChecksumWithQuerier(ctx, &pgChecksumShardQuerier{repo: r}, r.shardTimeout(), r.logger, source, time.Now().UTC())
}

func computeChecksumWithQuerier(
	ctx context.Context,
	querier checksumShardQuerier,
	timeout time.Duration,
	logger zerolog.Logger,
	source string,
	now time.Time,
) (audit.Checksum, error) {
	var (
		mu  sync.Mutex
		ids []string
	)

	unavailable := scatterShards(ctx, timeout, logger, allShards(), func(shardCtx context.Context, shard int) error {
		shardIDs, err := querier.querySourceRefIDs(shardCtx, shard, source)
		if err != nil {
			return err
		}
		if len(shardIDs) > 0 {
			mu.Lock()
			ids = append(ids, shardIDs...)
			mu.Unlock()
		}
		return nil
	})
	if unavailable == nil {
		unavailable = []string{}
	}

	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	hexSum := hex.EncodeToString(sum[:])

	var srcPtr *string
	if source != "" {
		s := source
		srcPtr = &s
	}

	return audit.Checksum{
		Source:           srcPtr,
		Count:            int64(len(ids)),
		Checksum:         hexSum,
		Algorithm:        "sha256",
		AsOf:             now.UTC().Format(time.RFC3339),
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	}, nil
}

type rangeMetadata struct {
	leaseHolder       int64
	replicas          []int64
	replicaLocalities []string
}

type locationQuerier interface {
	eventExists(ctx context.Context, shard int, hazardID string) (bool, error)
	showRange(ctx context.Context, shard int, hazardID string) (rangeMetadata, error)
}

type pgLocationQuerier struct {
	repo *PostgresRepo
}

func (q *pgLocationQuerier) eventExists(ctx context.Context, shard int, hazardID string) (bool, error) {
	var dummy int
	err := q.repo.db.QueryRow(ctx, `
		SELECT 1 FROM canonical_events
		WHERE shard = $1 AND hazard_id = $2 AND deleted_at IS NULL
	`, shard, hazardID).Scan(&dummy)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (q *pgLocationQuerier) showRange(ctx context.Context, shard int, hazardID string) (rangeMetadata, error) {
	if strings.ContainsRune(hazardID, '\x00') {
		return rangeMetadata{}, fmt.Errorf("invalid hazard_id")
	}
	escapedID := strings.ReplaceAll(hazardID, "'", "''")
	query := fmt.Sprintf("SHOW RANGE FROM TABLE canonical_events FOR ROW (%d, '%s')", shard, escapedID)

	var (
		startKey            []byte
		endKey              []byte
		rangeID             int64
		leaseHolder         int64
		leaseHolderLocality string
		replicas            []int64
		replicaLocalities   []string
		votingReplicas      []int64
		nonVotingReplicas   []int64
	)

	err := q.repo.db.QueryRow(ctx, query).Scan(
		&startKey,
		&endKey,
		&rangeID,
		&leaseHolder,
		&leaseHolderLocality,
		&replicas,
		&replicaLocalities,
		&votingReplicas,
		&nonVotingReplicas,
	)
	if err != nil {
		return rangeMetadata{}, err
	}

	return rangeMetadata{
		leaseHolder:       leaseHolder,
		replicas:          replicas,
		replicaLocalities: replicaLocalities,
	}, nil
}

// LocateEvent returns the CockroachDB replica placement for a hazard event.
func (r *PostgresRepo) LocateEvent(ctx context.Context, hazardID string) (audit.LocationResponse, error) {
	return locateEventWithQuerier(ctx, &pgLocationQuerier{repo: r}, r.shardTimeout(), r.logger, hazardID)
}

func locateEventWithQuerier(
	ctx context.Context,
	querier locationQuerier,
	timeout time.Duration,
	logger zerolog.Logger,
	hazardID string,
) (audit.LocationResponse, error) {
	if timeout <= 0 {
		timeout = defaultShardQueryTimeout
	}
	shard := ShardForHazardID(hazardID)

	existCtx, existCancel := context.WithTimeout(ctx, timeout)
	defer existCancel()

	exists, err := querier.eventExists(existCtx, shard, hazardID)
	if err != nil {
		logger.Warn().Err(err).Str("hazard_id", hazardID).Int("shard", shard).Msg("audit location existence check failed")
		return audit.LocationResponse{
			HazardID:  hazardID,
			Locations: []audit.Location{},
			Partial:   true,
		}, nil
	}
	if !exists {
		return audit.LocationResponse{}, ErrNotFound
	}

	rangeCtx, rangeCancel := context.WithTimeout(ctx, timeout)
	defer rangeCancel()

	meta, err := querier.showRange(rangeCtx, shard, hazardID)
	if err != nil {
		logger.Warn().Err(err).Str("hazard_id", hazardID).Int("shard", shard).Msg("audit location SHOW RANGE failed")
		return audit.LocationResponse{
			HazardID:  hazardID,
			Locations: []audit.Location{},
			Partial:   true,
		}, nil
	}

	return audit.LocationResponse{
		HazardID:  hazardID,
		Locations: mapRangeLocations(shard, meta),
		Partial:   false,
	}, nil
}

func mapRangeLocations(shard int, meta rangeMetadata) []audit.Location {
	unit := UnitName(shard)
	var primary *audit.Location
	replicas := make([]audit.Location, 0, len(meta.replicas))

	rIdx := 0
	for i, repID := range meta.replicas {
		locality := ""
		if i < len(meta.replicaLocalities) {
			locality = meta.replicaLocalities[i]
		}
		node := extractNodeLocality(locality)
		if repID == meta.leaseHolder && primary == nil {
			loc := audit.Location{
				Node: node,
				Unit: unit,
				Role: "primary",
			}
			primary = &loc
		} else {
			rIdx++
			replicas = append(replicas, audit.Location{
				Node: node,
				Unit: fmt.Sprintf("%s-r%d", unit, rIdx),
				Role: "replica",
			})
		}
	}

	out := make([]audit.Location, 0, len(meta.replicas))
	if primary != nil {
		out = append(out, *primary)
	}
	out = append(out, replicas...)
	return out
}

func extractNodeLocality(locality string) string {
	for _, part := range strings.Split(locality, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "node=") {
			return strings.TrimPrefix(part, "node=")
		}
	}
	if locality != "" {
		return locality
	}
	return "unknown"
}

// UnprocessedFilter defines parameters for paginated DLQ / unprocessed message reads.
type UnprocessedFilter struct {
	Source string
	Limit  int
	Cursor string
}

type unprocessedRow struct {
	item       audit.UnprocessedItem
	receivedAt time.Time
}

type unprocessedQuerier interface {
	queryUnprocessed(ctx context.Context, source string, cursorTime *time.Time, cursorID *int64, limit int) ([]unprocessedRow, error)
}

type pgUnprocessedQuerier struct {
	repo *PostgresRepo
}

func (q *pgUnprocessedQuerier) queryUnprocessed(
	ctx context.Context,
	source string,
	cursorTime *time.Time,
	cursorID *int64,
	limit int,
) ([]unprocessedRow, error) {
	var sql strings.Builder
	sql.Grow(240)
	sql.WriteString(`
		SELECT id, source, received_at, reason, retryable, attempts, status
		FROM dead_letters
		WHERE status IN ('retrying', 'parked')
	`)

	args := make([]any, 0, 4)
	argIdx := 1

	if source != "" {
		sql.WriteString(" AND source = $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, source)
		argIdx++
	}
	if cursorTime != nil && cursorID != nil {
		sql.WriteString(" AND (received_at, id) > ($")
		sql.WriteString(strconv.Itoa(argIdx))
		sql.WriteString(", $")
		sql.WriteString(strconv.Itoa(argIdx + 1))
		sql.WriteString(")")
		args = append(args, *cursorTime, *cursorID)
		argIdx += 2
	}

	sql.WriteString(" ORDER BY received_at ASC, id ASC LIMIT $")
	sql.WriteString(strconv.Itoa(argIdx))
	args = append(args, limit)

	rows, err := q.repo.db.Query(ctx, sql.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]unprocessedRow, 0, min(limit, 64))
	for rows.Next() {
		var (
			id         int64
			src        string
			receivedAt time.Time
			reason     string
			retryable  bool
			attempts   int
			status     string
		)
		if err := rows.Scan(&id, &src, &receivedAt, &reason, &retryable, &attempts, &status); err != nil {
			return nil, err
		}
		recUTC := receivedAt.UTC()
		out = append(out, unprocessedRow{
			item: audit.UnprocessedItem{
				MessageID:  strconv.FormatInt(id, 10),
				Source:     src,
				ReceivedAt: recUTC.Format(time.RFC3339),
				Reason:     reason,
				Retryable:  retryable,
				Attempts:   attempts,
				Status:     audit.UnprocessedStatus(status),
			},
			receivedAt: recUTC,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListUnprocessed returns retrying and parked rows from dead_letters ordered by
// (received_at ASC, id ASC) with keyset cursor pagination.
func (r *PostgresRepo) ListUnprocessed(ctx context.Context, filter UnprocessedFilter) (audit.UnprocessedPage, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	return listUnprocessedWithQuerier(ctx, &pgUnprocessedQuerier{repo: r}, filter)
}

func listUnprocessedWithQuerier(
	ctx context.Context,
	querier unprocessedQuerier,
	filter UnprocessedFilter,
) (audit.UnprocessedPage, error) {
	cursor, err := decodeCursor(filter.Cursor)
	if err != nil {
		return audit.UnprocessedPage{}, err
	}

	var (
		cursorTime *time.Time
		cursorID   *int64
	)
	if cursor != nil {
		id, err := strconv.ParseInt(cursor.hazardID, 10, 64)
		if err != nil {
			return audit.UnprocessedPage{}, fmt.Errorf("%w: invalid id: %v", ErrInvalidCursor, err)
		}
		cursorTime = &cursor.occurredAt
		cursorID = &id
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = defaultAuditLimit
	}
	if limit > maxAuditLimit {
		limit = maxAuditLimit
	}

	rows, err := querier.queryUnprocessed(ctx, filter.Source, cursorTime, cursorID, limit+1)
	if err != nil {
		return audit.UnprocessedPage{}, err
	}

	var nextCursor *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[limit-1]
		c := encodeCursor(last.receivedAt, last.item.MessageID)
		nextCursor = &c
	}

	items := make([]audit.UnprocessedItem, len(rows))
	for i, row := range rows {
		items[i] = row.item
	}

	return audit.UnprocessedPage{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

// RecordUnprocessed inserts a non-retryable parked message into dead_letters,
// skipping exact duplicates that are already active (parked/retrying).
func (r *PostgresRepo) RecordUnprocessed(ctx context.Context, source, sourceRefID string, raw []byte, reason string) error {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	return withRetry40001(ctx, func() error {
		_, err := r.db.Exec(ctx, `
			INSERT INTO dead_letters
				(source, source_ref_id, raw_payload, reason, retryable, attempts, status, last_error)
			SELECT $1::STRING, NULLIF($2::STRING, ''), $3::BYTES, $4::TEXT, false, 0, 'parked', $4::TEXT
			WHERE NOT EXISTS (
				SELECT 1
				FROM dead_letters
				WHERE source = $1::STRING
				  AND status IN ('retrying', 'parked')
				  AND source_ref_id IS NOT DISTINCT FROM NULLIF($2::STRING, '')
				  AND raw_payload = $3::BYTES
			)
		`, source, sourceRefID, raw, reason)
		return err
	})
}
