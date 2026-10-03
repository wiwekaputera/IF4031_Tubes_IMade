package store

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
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
	GetEvents(ctx context.Context, filter EventFilter) ([]hazard.HazardEvent, error)
	GetVolcanoReferences(ctx context.Context) (map[string]VolcanoReference, error)
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

type PostgresRepo struct {
	db     *pgxpool.Pool
	logger zerolog.Logger
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

func (r *PostgresRepo) UpsertEvent(ctx context.Context, rec UpsertRequest) (string, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	if rec.Sources == nil {
		rec.Sources = make(map[string]interface{})
	}
	if rec.Attributes == nil {
		rec.Attributes = make(map[string]interface{})
	}

	sourcesJSON, err := json.Marshal(rec.Sources)
	if err != nil {
		return "", err
	}
	attrsJSON, err := json.Marshal(rec.Attributes)
	if err != nil {
		return "", err
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var storedHazardID string
	var version int64
	var ingestedAt time.Time
	var isNewOrUpdated bool
	err = tx.QueryRow(ctx, `
		WITH pre AS (
			SELECT version
			FROM canonical_events
			WHERE source = $2 AND source_ref_id = $3
		),
		ins AS (
			INSERT INTO canonical_events
				(hazard_id, source, source_ref_id, hazard_type, severity, area_name,
				 latitude, longitude, occurred_at, ingested_at, sources, attributes)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11)
			ON CONFLICT (source, source_ref_id) DO UPDATE SET
				severity    = EXCLUDED.severity,
				area_name   = EXCLUDED.area_name,
				sources     = canonical_events.sources || EXCLUDED.sources,
				attributes  = canonical_events.attributes || EXCLUDED.attributes,
				version     = CASE
					WHEN canonical_events.severity IS DISTINCT FROM EXCLUDED.severity
					  OR canonical_events.area_name IS DISTINCT FROM EXCLUDED.area_name
					  OR canonical_events.sources IS DISTINCT FROM (canonical_events.sources || EXCLUDED.sources)
					  OR canonical_events.attributes IS DISTINCT FROM (canonical_events.attributes || EXCLUDED.attributes)
					THEN canonical_events.version + 1
					ELSE canonical_events.version
				END,
				updated_at  = CASE
					WHEN canonical_events.severity IS DISTINCT FROM EXCLUDED.severity
					  OR canonical_events.area_name IS DISTINCT FROM EXCLUDED.area_name
					  OR canonical_events.sources IS DISTINCT FROM (canonical_events.sources || EXCLUDED.sources)
					  OR canonical_events.attributes IS DISTINCT FROM (canonical_events.attributes || EXCLUDED.attributes)
					THEN now()
					ELSE canonical_events.updated_at
				END
			RETURNING hazard_id, version, ingested_at
		)
		SELECT
			ins.hazard_id,
			ins.version,
			ins.ingested_at,
			(pre.version IS NULL OR ins.version > pre.version) AS is_new_or_updated
		FROM ins
		LEFT JOIN pre ON true;
	`,
		rec.HazardID, rec.Source, rec.SourceRefID, string(rec.HazardType), string(rec.Severity),
		rec.AreaName, rec.Latitude, rec.Longitude, rec.OccurredAt,
		sourcesJSON, attrsJSON,
	).Scan(&storedHazardID, &version, &ingestedAt, &isNewOrUpdated)
	if err != nil {
		return "", err
	}

	if isNewOrUpdated {
		he := hazard.HazardEvent{
			HazardID:    storedHazardID,
			Source:      hazard.Source(rec.Source),
			SourceRefID: rec.SourceRefID,
			HazardType:  rec.HazardType,
			Severity:    rec.Severity,
			AreaName:    rec.AreaName,
			Latitude:    rec.Latitude,
			Longitude:   rec.Longitude,
			OccurredAt:  rec.OccurredAt.Format(time.RFC3339),
			IngestedAt:  ingestedAt.Format(time.RFC3339),
			Attributes:  rec.Attributes,
		}

		env := event.Envelope{
			EventID:    storedHazardID,
			Version:    version,
			EventType:  rec.HazardType,
			OccurredAt: rec.OccurredAt.Format(time.RFC3339),
			TraceID:    rec.CorrelationID,
			Data:       he,
		}

		payload, err := json.Marshal(env)
		if err != nil {
			return "", err
		}

		eventKey := storedHazardID + ":" + strconv.FormatInt(version, 10)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (event_key, payload)
			VALUES ($1, $2)
			ON CONFLICT (event_key) DO NOTHING
		`, eventKey, payload)
		if err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return storedHazardID, nil
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
		WHERE true
	`)

	args := make([]any, 0, 4)
	argIdx := 1

	if filter.HazardType != "" {
		q.WriteString(" AND hazard_type = $")
		q.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.HazardType)
		argIdx++
	}
	if filter.HazardID != "" {
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
