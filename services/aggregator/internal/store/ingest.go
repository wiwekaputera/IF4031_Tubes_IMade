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
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// ApplyIngest applies an IngestMessage (upsert or soft-delete) in a single
// transaction guarded by canonical_events.source_version < EXCLUDED.source_version.
// Returns true if the canonical row was created or updated, false if ignored as
// stale or duplicate.
func (r *PostgresRepo) ApplyIngest(ctx context.Context, msg event.IngestMessage) (bool, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var applied bool
	err := withRetry40001(ctx, func() error {
		applied = false

		tx, err := r.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		var env *event.Envelope
		switch msg.Op {
		case event.IngestOpUpsert:
			env, err = applyIngestUpsertTx(ctx, tx, msg)
		case event.IngestOpDelete:
			env, err = applyIngestDeleteTx(ctx, tx, msg)
		default:
			return fmt.Errorf("unsupported ingest op %q", msg.Op)
		}
		if err != nil {
			return err
		}
		if env == nil {
			return nil
		}

		if err := insertOutboxEnvelopeTx(ctx, tx, *env); err != nil {
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

func applyIngestUpsertTx(ctx context.Context, tx pgx.Tx, msg event.IngestMessage) (*event.Envelope, error) {
	occurredAt, err := time.Parse(time.RFC3339, msg.OccurredAt)
	if err != nil {
		return nil, fmt.Errorf("invalid occurred_at %q: %w", msg.OccurredAt, err)
	}
	occurredAt = occurredAt.UTC()

	attrs := msg.Attributes
	if attrs == nil {
		attrs = make(map[string]any)
	}
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return nil, err
	}

	sourcesJSON, err := json.Marshal(map[string]any{
		strings.ToLower(string(msg.Source)): msg.MessageID,
	})
	if err != nil {
		return nil, err
	}

	hazardID := hazard.ID(msg.Source, msg.SourceRefID)
	var version int64
	var ingestedAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO canonical_events (
			hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, sources, attributes,
			source_version, deleted_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11, $12, NULL)
		ON CONFLICT (shard, hazard_id) DO UPDATE SET
			hazard_type    = EXCLUDED.hazard_type,
			severity       = EXCLUDED.severity,
			area_name      = EXCLUDED.area_name,
			latitude       = EXCLUDED.latitude,
			longitude      = EXCLUDED.longitude,
			occurred_at    = EXCLUDED.occurred_at,
			sources        = EXCLUDED.sources,
			attributes     = EXCLUDED.attributes,
			source_version = EXCLUDED.source_version,
			deleted_at     = NULL,
			version        = canonical_events.version + 1,
			updated_at     = now()
		WHERE canonical_events.source_version < EXCLUDED.source_version
		RETURNING version, ingested_at
	`,
		hazardID, string(msg.Source), msg.SourceRefID, string(msg.HazardType), string(msg.Severity),
		msg.AreaName, msg.Latitude, msg.Longitude, occurredAt,
		sourcesJSON, attrsJSON, msg.SourceVersion,
	).Scan(&version, &ingestedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	he := hazard.HazardEvent{
		HazardID:    hazardID,
		Source:      msg.Source,
		SourceRefID: msg.SourceRefID,
		HazardType:  msg.HazardType,
		Severity:    msg.Severity,
		AreaName:    msg.AreaName,
		Latitude:    msg.Latitude,
		Longitude:   msg.Longitude,
		OccurredAt:  occurredAt.Format(time.RFC3339),
		IngestedAt:  ingestedAt.UTC().Format(time.RFC3339),
		Attributes:  attrs,
	}

	return &event.Envelope{
		EventID:    hazardID,
		Version:    version,
		EventType:  msg.HazardType,
		OccurredAt: occurredAt.Format(time.RFC3339),
		TraceID:    msg.TraceID,
		Data:       he,
	}, nil
}

func applyIngestDeleteTx(ctx context.Context, tx pgx.Tx, msg event.IngestMessage) (*event.Envelope, error) {
	occurredAt := time.Now().UTC()
	if msg.OccurredAt != "" {
		if parsed, err := time.Parse(time.RFC3339, msg.OccurredAt); err == nil {
			occurredAt = parsed.UTC()
		}
	}

	tombstoneAttrsJSON, err := json.Marshal(map[string]any{"deleted": true})
	if err != nil {
		return nil, err
	}
	sourcesJSON, err := json.Marshal(map[string]any{
		strings.ToLower(string(msg.Source)): msg.MessageID,
	})
	if err != nil {
		return nil, err
	}

	hazardID := hazard.ID(msg.Source, msg.SourceRefID)
	var hazardType, severity, areaName string
	var lat, lon float64
	var storedOccurredAt, ingestedAt time.Time
	var rawAttrs []byte
	var version int64

	err = tx.QueryRow(ctx, `
		INSERT INTO canonical_events (
			hazard_id, source, source_ref_id, hazard_type, severity, area_name,
			latitude, longitude, occurred_at, ingested_at, sources, attributes,
			source_version, deleted_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11, $12, now())
		ON CONFLICT (shard, hazard_id) DO UPDATE SET
			deleted_at     = now(),
			source_version = EXCLUDED.source_version,
			version        = canonical_events.version + 1,
			updated_at     = now()
		WHERE canonical_events.source_version < EXCLUDED.source_version
		RETURNING hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes, version
	`,
		hazardID, string(msg.Source), msg.SourceRefID, string(msg.HazardType), string(msg.Severity),
		msg.AreaName, msg.Latitude, msg.Longitude, occurredAt,
		sourcesJSON, tombstoneAttrsJSON, msg.SourceVersion,
	).Scan(&hazardType, &severity, &areaName, &lat, &lon, &storedOccurredAt, &ingestedAt, &rawAttrs, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	attrs := make(map[string]any)
	if len(rawAttrs) > 0 {
		_ = json.Unmarshal(rawAttrs, &attrs)
	}
	attrs["deleted"] = true

	ht := hazard.HazardType(hazardType)
	he := hazard.HazardEvent{
		HazardID:    hazardID,
		Source:      msg.Source,
		SourceRefID: msg.SourceRefID,
		HazardType:  ht,
		Severity:    hazard.Severity(severity),
		AreaName:    areaName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  storedOccurredAt.UTC().Format(time.RFC3339),
		IngestedAt:  ingestedAt.UTC().Format(time.RFC3339),
		Attributes:  attrs,
	}

	return &event.Envelope{
		EventID:    hazardID,
		Version:    version,
		EventType:  ht,
		OccurredAt: storedOccurredAt.UTC().Format(time.RFC3339),
		TraceID:    msg.TraceID,
		Data:       he,
	}, nil
}

func insertOutboxEnvelopeTx(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	eventKey := env.EventID + ":" + strconv.FormatInt(env.Version, 10)
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (event_key, payload)
		VALUES ($1, $2)
		ON CONFLICT (event_key) DO NOTHING
	`, eventKey, payload)
	return err
}
