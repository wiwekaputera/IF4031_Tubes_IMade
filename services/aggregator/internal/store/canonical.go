package store

import (
	"context"
	"encoding/json"
	"strconv"
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

func (r *PostgresRepo) HealthCheck(ctx context.Context) error {
	return r.db.Ping(ctx)
}

func (r *PostgresRepo) GetVolcanoReferences(ctx context.Context) (map[string]VolcanoReference, error) {
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
	err = tx.QueryRow(ctx, `
		INSERT INTO canonical_events
			(hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			 latitude, longitude, occurred_at, ingested_at, sources, attributes)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11)
		ON CONFLICT (source, source_ref_id) DO UPDATE SET
			severity    = EXCLUDED.severity,
			area_name   = EXCLUDED.area_name,
			sources     = canonical_events.sources || EXCLUDED.sources,
			attributes  = canonical_events.attributes || EXCLUDED.attributes,
			version     = canonical_events.version + 1,
			updated_at  = now()
		RETURNING hazard_id, version
	`,
		rec.HazardID, rec.Source, rec.SourceRefID, string(rec.HazardType), string(rec.Severity),
		rec.AreaName, rec.Latitude, rec.Longitude, rec.OccurredAt,
		sourcesJSON, attrsJSON,
	).Scan(&storedHazardID, &version)

	if err != nil {
		return "", err
	}

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
		Attributes:  rec.Attributes,
	}

	env := event.Envelope{
		EventID: storedHazardID,
		Version: version,
		TraceID: rec.CorrelationID,
		Data:    he,
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

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return storedHazardID, nil
}

func (r *PostgresRepo) GetEvents(ctx context.Context, filter EventFilter) ([]hazard.HazardEvent, error) {
	query := `
		SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at
		FROM canonical_events
		WHERE true
	`
	args := []any{}
	argIdx := 1

	if filter.HazardType != "" {
		query += " AND hazard_type = $" + strconv.Itoa(argIdx)
		args = append(args, filter.HazardType)
		argIdx++
	}
	if filter.HazardID != "" {
		query += " AND hazard_id = $" + strconv.Itoa(argIdx)
		args = append(args, filter.HazardID)
		argIdx++
	}
	if filter.Since != nil {
		query += " AND occurred_at >= $" + strconv.Itoa(argIdx)
		args = append(args, *filter.Since)
		argIdx++
	}
	if filter.Limit > 0 {
		query += " LIMIT $" + strconv.Itoa(argIdx)
		args = append(args, filter.Limit)
		argIdx++
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []hazard.HazardEvent
	for rows.Next() {
		var he hazard.HazardEvent
		var occurredAt, ingestedAt string
		if err := rows.Scan(
			&he.HazardID, &he.Source, &he.SourceRefID, &he.HazardType, &he.Severity,
			&he.AreaName, &he.Latitude, &he.Longitude, &occurredAt, &ingestedAt,
		); err != nil {
			return nil, err
		}
		he.OccurredAt = occurredAt
		he.IngestedAt = ingestedAt
		events = append(events, he)
	}
	if len(events) == 0 {
		return []hazard.HazardEvent{}, nil
	}
	return events, rows.Err()
}
