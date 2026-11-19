package main

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
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// BatchInserter writes batches of canonical events idempotently.
type BatchInserter interface {
	InsertBatch(ctx context.Context, batch []store.UpsertRequest, withOutbox bool) (inserted int64, skipped int64, err error)
}

type pgBatchInserter struct {
	pool *pgxpool.Pool
}

func dedupBatch(batch []store.UpsertRequest) ([]store.UpsertRequest, int64) {
	seen := make(map[string]struct{}, len(batch))
	unique := make([]store.UpsertRequest, 0, len(batch))
	var skipped int64
	for _, rec := range batch {
		if _, ok := seen[rec.HazardID]; ok {
			skipped++
			continue
		}
		seen[rec.HazardID] = struct{}{}
		unique = append(unique, rec)
	}
	return unique, skipped
}

func buildInsertSQL(n int, returning bool) string {
	var b strings.Builder
	b.Grow(256 + n*48)
	b.WriteString(`INSERT INTO canonical_events
		(hazard_id, source, source_ref_id, hazard_type, severity, area_name,
		 latitude, longitude, occurred_at, ingested_at, sources, attributes)
	VALUES `)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		base := i*11 + 1
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, now(), $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10)
	}
	b.WriteString(" ON CONFLICT (shard, hazard_id) DO NOTHING")
	if returning {
		b.WriteString(" RETURNING hazard_id, version, ingested_at")
	}
	return b.String()
}

func buildInsertArgs(batch []store.UpsertRequest) ([]any, error) {
	args := make([]any, 0, len(batch)*11)
	for _, rec := range batch {
		sources := rec.Sources
		if sources == nil {
			sources = map[string]interface{}{}
		}
		attrs := rec.Attributes
		if attrs == nil {
			attrs = map[string]interface{}{}
		}
		sourcesJSON, err := json.Marshal(sources)
		if err != nil {
			return nil, err
		}
		attrsJSON, err := json.Marshal(attrs)
		if err != nil {
			return nil, err
		}
		args = append(args,
			rec.HazardID,
			rec.Source,
			rec.SourceRefID,
			string(rec.HazardType),
			string(rec.Severity),
			rec.AreaName,
			rec.Latitude,
			rec.Longitude,
			rec.OccurredAt,
			sourcesJSON,
			attrsJSON,
		)
	}
	return args, nil
}

func withRetry40001(ctx context.Context, fn func() error) error {
	const maxRetries = 5
	backoff := 15 * time.Millisecond
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

func (p *pgBatchInserter) InsertBatch(ctx context.Context, batch []store.UpsertRequest, withOutbox bool) (int64, int64, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}

	unique, intraSkipped := dedupBatch(batch)
	args, err := buildInsertArgs(unique)
	if err != nil {
		return 0, 0, err
	}

	var inserted int64
	err = withRetry40001(ctx, func() error {
		inserted = 0
		if !withOutbox {
			sql := buildInsertSQL(len(unique), false)
			tag, err := p.pool.Exec(ctx, sql, args...)
			if err != nil {
				return err
			}
			inserted = tag.RowsAffected()
			return nil
		}

		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		sql := buildInsertSQL(len(unique), true)
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			return err
		}

		byID := make(map[string]store.UpsertRequest, len(unique))
		for _, rec := range unique {
			byID[rec.HazardID] = rec
		}

		type insertedMeta struct {
			hazardID   string
			version    int64
			ingestedAt time.Time
		}
		var metas []insertedMeta
		for rows.Next() {
			var m insertedMeta
			if err := rows.Scan(&m.hazardID, &m.version, &m.ingestedAt); err != nil {
				rows.Close()
				return err
			}
			metas = append(metas, m)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		if len(metas) > 0 {
			outboxBatch := &pgx.Batch{}
			for _, m := range metas {
				rec := byID[m.hazardID]
				he := hazard.HazardEvent{
					HazardID:    m.hazardID,
					Source:      hazard.Source(rec.Source),
					SourceRefID: rec.SourceRefID,
					HazardType:  rec.HazardType,
					Severity:    rec.Severity,
					AreaName:    rec.AreaName,
					Latitude:    rec.Latitude,
					Longitude:   rec.Longitude,
					OccurredAt:  rec.OccurredAt.Format(time.RFC3339),
					IngestedAt:  m.ingestedAt.UTC().Format(time.RFC3339),
					Attributes:  rec.Attributes,
				}
				env := event.Envelope{
					EventID:    m.hazardID,
					Version:    m.version,
					EventType:  rec.HazardType,
					OccurredAt: rec.OccurredAt.Format(time.RFC3339),
					Data:       he,
				}
				payload, err := json.Marshal(env)
				if err != nil {
					return err
				}
				eventKey := m.hazardID + ":" + strconv.FormatInt(m.version, 10)
				outboxBatch.Queue(`
					INSERT INTO outbox (event_key, payload)
					VALUES ($1, $2)
					ON CONFLICT (event_key) DO NOTHING
				`, eventKey, payload)
			}
			br := tx.SendBatch(ctx, outboxBatch)
			for range metas {
				if _, err := br.Exec(); err != nil {
					_ = br.Close()
					return err
				}
			}
			if err := br.Close(); err != nil {
				return err
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		inserted = int64(len(metas))
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	skipped := intraSkipped + (int64(len(unique)) - inserted)
	return inserted, skipped, nil
}
