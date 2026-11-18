package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// NumShards is the number of logical shards in canonical_events.
// Must match mod(crc32ieee(hazard_id), 8) in the database schema.
const NumShards = 8

const defaultShardQueryTimeout = 800 * time.Millisecond

// ErrInvalidCursor is returned when a pagination cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid cursor")

type scatterCtxKey string

const correlationIDCtxKey scatterCtxKey = "correlation_id"

// WithCorrelationID attaches a correlation ID to ctx for scatter-gather logging.
func WithCorrelationID(ctx context.Context, corrID string) context.Context {
	if corrID == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationIDCtxKey, corrID)
}

func correlationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationIDCtxKey).(string); ok {
		return v
	}
	return ""
}

// ShardForHazardID computes the logical shard (0..NumShards-1) for a hazard_id
// using the same CRC32-IEEE modulo as the CockroachDB computed column.
func ShardForHazardID(hazardID string) int {
	return int(crc32.ChecksumIEEE([]byte(hazardID)) % NumShards)
}

// UnitName returns the canonical storage unit name for a logical shard.
func UnitName(shard int) string {
	return fmt.Sprintf("shard-%d", shard)
}

// WithShardQueryTimeout configures the per-shard timeout for scatter-gather reads.
func (r *PostgresRepo) WithShardQueryTimeout(d time.Duration) *PostgresRepo {
	if d > 0 {
		r.shardQueryTimeout = d
	}
	return r
}

func (r *PostgresRepo) shardTimeout() time.Duration {
	if r.shardQueryTimeout > 0 {
		return r.shardQueryTimeout
	}
	return defaultShardQueryTimeout
}

// ListFilter defines parameters for paginated scatter-gather event reads.
type ListFilter struct {
	HazardType string
	HazardID   string
	Source     string
	Since      *time.Time
	Limit      int
	Cursor     string
}

// ListResult holds the merged events, pagination cursor, and partial-availability metadata.
type ListResult struct {
	Events           []hazard.HazardEvent
	NextCursor       *string
	Partial          bool
	UnavailableUnits []string
}

type eventCursorPayload struct {
	T  string `json:"t"`
	ID string `json:"id"`
}

type decodedCursor struct {
	occurredAt time.Time
	hazardID   string
}

func encodeCursor(occurredAt time.Time, hazardID string) string {
	b, _ := json.Marshal(eventCursorPayload{
		T:  occurredAt.UTC().Format(time.RFC3339Nano),
		ID: hazardID,
	})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(raw string) (*decodedCursor, error) {
	if raw == "" {
		return nil, nil
	}
	trimmed := strings.TrimRight(raw, "=")
	data, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: base64 decode: %v", ErrInvalidCursor, err)
	}
	var payload eventCursorPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("%w: json decode: %v", ErrInvalidCursor, err)
	}
	if payload.T == "" || payload.ID == "" {
		return nil, fmt.Errorf("%w: missing t or id", ErrInvalidCursor)
	}
	t, err := time.Parse(time.RFC3339Nano, payload.T)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timestamp: %v", ErrInvalidCursor, err)
	}
	return &decodedCursor{
		occurredAt: t.UTC(),
		hazardID:   payload.ID,
	}, nil
}

type shardEvent struct {
	event      hazard.HazardEvent
	occurredAt time.Time
}

type shardEventQuerier interface {
	queryShard(ctx context.Context, shard int, filter ListFilter, cursor *decodedCursor, limit int) ([]shardEvent, error)
}

type pgShardQuerier struct {
	repo *PostgresRepo
}

func (q *pgShardQuerier) queryShard(ctx context.Context, shard int, filter ListFilter, cursor *decodedCursor, limit int) ([]shardEvent, error) {
	var sql strings.Builder
	sql.Grow(320)
	sql.WriteString(`
		SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, attributes
		FROM canonical_events
		WHERE shard = $1 AND deleted_at IS NULL
	`)

	args := make([]any, 0, 8)
	args = append(args, shard)
	argIdx := 2

	if filter.HazardType != "" {
		sql.WriteString(" AND hazard_type = $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.HazardType)
		argIdx++
	}
	if filter.HazardID != "" {
		sql.WriteString(" AND hazard_id = $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.HazardID)
		argIdx++
	}
	if filter.Source != "" {
		sql.WriteString(" AND source = $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.Source)
		argIdx++
	}
	if filter.Since != nil {
		sql.WriteString(" AND occurred_at >= $")
		sql.WriteString(strconv.Itoa(argIdx))
		args = append(args, *filter.Since)
		argIdx++
	}
	if cursor != nil {
		sql.WriteString(" AND (occurred_at, hazard_id) < ($")
		sql.WriteString(strconv.Itoa(argIdx))
		sql.WriteString(", $")
		sql.WriteString(strconv.Itoa(argIdx + 1))
		sql.WriteString(")")
		args = append(args, cursor.occurredAt, cursor.hazardID)
		argIdx += 2
	}

	sql.WriteString(" ORDER BY occurred_at DESC, hazard_id DESC LIMIT $")
	sql.WriteString(strconv.Itoa(argIdx))
	args = append(args, limit)

	rows, err := q.repo.db.Query(ctx, sql.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]shardEvent, 0, min(limit, 64))
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
		out = append(out, shardEvent{
			event:      he,
			occurredAt: occurredAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scatter runs fn concurrently across all logical shards, each bounded by its
// own timeout, and returns sorted unit names ("shard-N") for shards that failed.
func (r *PostgresRepo) scatter(ctx context.Context, fn func(ctx context.Context, shard int) error) []string {
	shards := allShards()
	return scatterShards(ctx, r.shardTimeout(), r.logger, shards, fn)
}

func allShards() []int {
	shards := make([]int, NumShards)
	for i := 0; i < NumShards; i++ {
		shards[i] = i
	}
	return shards
}

func scatterShards(
	ctx context.Context,
	timeout time.Duration,
	logger zerolog.Logger,
	shards []int,
	fn func(ctx context.Context, shard int) error,
) []string {
	if timeout <= 0 {
		timeout = defaultShardQueryTimeout
	}
	corrID := correlationIDFromContext(ctx)

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		unavailable []string
	)

	wg.Add(len(shards))
	for _, shard := range shards {
		go func(s int) {
			defer wg.Done()
			shardCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			if err := fn(shardCtx, s); err != nil {
				unit := UnitName(s)
				logger.Warn().
					Err(err).
					Str("correlation_id", corrID).
					Int("shard", s).
					Str("unit", unit).
					Msg("shard query failed")
				mu.Lock()
				unavailable = append(unavailable, unit)
				mu.Unlock()
			}
		}(shard)
	}
	wg.Wait()

	sort.Strings(unavailable)
	return unavailable
}

// ListEvents performs a scatter-gather read across shards with keyset cursor
// pagination and partial-result degradation when individual shards fail or time out.
func (r *PostgresRepo) ListEvents(ctx context.Context, filter ListFilter) (ListResult, error) {
	return listEventsWithQuerier(ctx, &pgShardQuerier{repo: r}, r.shardTimeout(), r.logger, filter)
}

func listEventsWithQuerier(
	ctx context.Context,
	querier shardEventQuerier,
	timeout time.Duration,
	logger zerolog.Logger,
	filter ListFilter,
) (ListResult, error) {
	cursor, err := decodeCursor(filter.Cursor)
	if err != nil {
		return ListResult{}, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	}

	shards := allShards()
	if filter.HazardID != "" {
		shards = []int{ShardForHazardID(filter.HazardID)}
	}

	var (
		mu       sync.Mutex
		collected []shardEvent
	)

	unavailable := scatterShards(ctx, timeout, logger, shards, func(shardCtx context.Context, shard int) error {
		rows, err := querier.queryShard(shardCtx, shard, filter, cursor, limit)
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

	sort.Slice(collected, func(i, j int) bool {
		if !collected[i].occurredAt.Equal(collected[j].occurredAt) {
			return collected[i].occurredAt.After(collected[j].occurredAt)
		}
		return collected[i].event.HazardID > collected[j].event.HazardID
	})

	var nextCursor *string
	if len(collected) >= limit {
		collected = collected[:limit]
		last := collected[limit-1]
		c := encodeCursor(last.occurredAt, last.event.HazardID)
		nextCursor = &c
	}

	events := make([]hazard.HazardEvent, len(collected))
	for i, item := range collected {
		events[i] = item.event
	}

	return ListResult{
		Events:           events,
		NextCursor:       nextCursor,
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	}, nil
}
