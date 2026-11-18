// Package store implements ingest.Store on Postgres. Every ingest.Commit is a
// single transaction, so the offset and the fire state can never disagree.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
)

// Satellite is the Postgres-backed ingest.Store.
type Satellite struct {
	pool *pgxpool.Pool
}

// NewSatellite wraps an existing pool. The schema must already be applied.
func NewSatellite(pool *pgxpool.Pool) *Satellite {
	return &Satellite{pool: pool}
}

// State reads the single satellite_state row.
func (s *Satellite) State(ctx context.Context) (ingest.State, error) {
	var (
		st ingest.State
		wm *time.Time
	)
	err := s.pool.QueryRow(ctx,
		`SELECT next_offset, watermark, invalid, low_conf, late, unknown FROM satellite_state WHERE id = 1`,
	).Scan(&st.NextOffset, &wm, &st.Invalid, &st.LowConf, &st.Late, &st.Unknown)
	if err != nil {
		return ingest.State{}, fmt.Errorf("read satellite_state: %w", err)
	}
	if wm != nil {
		st.Watermark = *wm
	}
	return st, nil
}

// scanDetection reads one satellite_detections row in the column order used
// by Get and Window. The raw text is kept so the rebuilt key matches the stored one.
func scanDetection(row pgx.Row) (fire.Detection, error) {
	var (
		d                 fire.Detection
		acqTime, lat, lon string
	)
	if err := row.Scan(&d.Satellite, &d.AcqDate, &acqTime, &lat, &lon, &d.Confidence, &d.FRP); err != nil {
		return fire.Detection{}, err
	}
	d.AcqTime = json.Number(acqTime)
	d.Latitude = json.Number(lat)
	d.Longitude = json.Number(lon)
	return d, nil
}

const detectionColumns = `satellite, acq_date::text, acq_time, latitude_text, longitude_text, confidence, frp`

// Get returns one stored detection by its key.
func (s *Satellite) Get(ctx context.Context, key string) (fire.Detection, bool, error) {
	d, err := scanDetection(s.pool.QueryRow(ctx,
		`SELECT `+detectionColumns+` FROM satellite_detections WHERE det_key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return fire.Detection{}, false, nil
	}
	if err != nil {
		return fire.Detection{}, false, fmt.Errorf("get detection: %w", err)
	}
	return d, true, nil
}

// Window returns stored detections acquired at or after since, excluding low
// confidence. Those are kept in the table but do not count toward fires.
func (s *Satellite) Window(ctx context.Context, since time.Time) ([]fire.Detection, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+detectionColumns+` FROM satellite_detections
		 WHERE acquired_at >= $1 AND lower(confidence) NOT IN ('l', 'low')
		 ORDER BY det_key`, since)
	if err != nil {
		return nil, fmt.Errorf("window query: %w", err)
	}
	defer rows.Close()

	var out []fire.Detection
	for rows.Next() {
		d, err := scanDetection(rows)
		if err != nil {
			return nil, fmt.Errorf("scan detection: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Fires returns stored fires whose latest detection is at or after since.
func (s *Satellite) Fires(ctx context.Context, since time.Time) ([]fire.Fire, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT source_ref_id, center_lat, center_lon, occurred_at, last_seen_at, detection_count,
		       satellites, max_confidence, total_frp_mw, detection_keys, severity
		FROM satellite_fires
		WHERE last_seen_at >= $1
		ORDER BY source_ref_id`, since)
	if err != nil {
		return nil, fmt.Errorf("fires query: %w", err)
	}
	defer rows.Close()

	var out []fire.Fire
	for rows.Next() {
		var f fire.Fire
		if err := rows.Scan(&f.SourceRefID, &f.CenterLat, &f.CenterLon, &f.OccurredAt, &f.LastSeenAt,
			&f.DetectionCount, &f.Satellites, &f.MaxConfidence, &f.TotalFRPMW, &f.DetectionKeys, &f.Severity); err != nil {
			return nil, fmt.Errorf("scan fire: %w", err)
		}
		f.OccurredAt = f.OccurredAt.UTC()
		f.LastSeenAt = f.LastSeenAt.UTC()
		out = append(out, f)
	}
	return out, rows.Err()
}

// OutboxRow is one hazard message waiting to be published.
type OutboxRow struct {
	ID        int64
	MessageID string
	Subject   string
	Payload   []byte
}

// PendingOutbox returns unpublished messages in the order they were written.
func (s *Satellite) PendingOutbox(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, message_id, subject, payload::text
		FROM satellite_outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox query: %w", err)
	}
	defer rows.Close()

	var out []OutboxRow
	for rows.Next() {
		var (
			r       OutboxRow
			payload string
		)
		if err := rows.Scan(&r.ID, &r.MessageID, &r.Subject, &payload); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		r.Payload = []byte(payload)
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkPublished records that JetStream acknowledged the message.
func (s *Satellite) MarkPublished(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE satellite_outbox SET published_at = now() WHERE id = $1`, id)
	return err
}

// OutboxBacklog counts unpublished messages, for monitoring.
func (s *Satellite) OutboxBacklog(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM satellite_outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// Commit writes one message's changes in a single transaction.
func (s *Satellite) Commit(ctx context.Context, c ingest.Commit) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once committed

	if d := c.Accepted; d != nil {
		t, err := d.Time()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO satellite_detections
				(det_key, satellite, acq_date, acq_time, latitude_text, longitude_text, confidence, frp, acquired_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (det_key) DO NOTHING`,
			d.Key(), d.Satellite, d.AcqDate, d.AcqTime.String(), d.Latitude.String(), d.Longitude.String(),
			d.Confidence, d.FRP, t,
		); err != nil {
			return fmt.Errorf("insert detection: %w", err)
		}
	}

	for _, e := range c.Edits {
		if e.Retract {
			if _, err := tx.Exec(ctx, `DELETE FROM satellite_detections WHERE det_key = $1`, e.Key); err != nil {
				return fmt.Errorf("retract detection: %w", err)
			}
			continue
		}
		// A nil pointer is sent as NULL, and COALESCE keeps the stored value.
		if _, err := tx.Exec(ctx, `
			UPDATE satellite_detections
			SET frp = COALESCE($2, frp), confidence = COALESCE($3, confidence)
			WHERE det_key = $1`,
			e.Key, e.FRP, e.Confidence,
		); err != nil {
			return fmt.Errorf("update detection: %w", err)
		}
	}

	for _, f := range c.Upserts {
		if _, err := tx.Exec(ctx, `
			INSERT INTO satellite_fires
				(source_ref_id, center_lat, center_lon, occurred_at, last_seen_at, detection_count, satellites,
				 max_confidence, total_frp_mw, detection_keys, severity, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
			ON CONFLICT (source_ref_id) DO UPDATE SET
				center_lat = EXCLUDED.center_lat,
				center_lon = EXCLUDED.center_lon,
				occurred_at = EXCLUDED.occurred_at,
				last_seen_at = EXCLUDED.last_seen_at,
				detection_count = EXCLUDED.detection_count,
				satellites = EXCLUDED.satellites,
				max_confidence = EXCLUDED.max_confidence,
				total_frp_mw = EXCLUDED.total_frp_mw,
				detection_keys = EXCLUDED.detection_keys,
				severity = EXCLUDED.severity,
				updated_at = now()`,
			f.SourceRefID, f.CenterLat, f.CenterLon, f.OccurredAt, f.LastSeenAt, f.DetectionCount, f.Satellites,
			f.MaxConfidence, f.TotalFRPMW, f.DetectionKeys, f.Severity,
		); err != nil {
			return fmt.Errorf("upsert fire %s: %w", f.SourceRefID, err)
		}
	}

	if len(c.Deletes) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM satellite_fires WHERE source_ref_id = ANY($1::text[])`, c.Deletes); err != nil {
			return fmt.Errorf("delete fires: %w", err)
		}
	}

	// A message_id repeats only if this exact commit is replayed, which the
	// offset check prevents; DO NOTHING keeps it harmless anyway.
	for _, o := range c.Outbox {
		if _, err := tx.Exec(ctx, `
			INSERT INTO satellite_outbox (message_id, subject, payload)
			VALUES ($1, $2, $3::jsonb)
			ON CONFLICT (message_id) DO NOTHING`,
			o.MessageID, o.Subject, string(o.Payload),
		); err != nil {
			return fmt.Errorf("insert outbox %s: %w", o.MessageID, err)
		}
	}

	// A zero watermark stays NULL, so a fresh database keeps "no detections yet".
	var wm any
	if !c.Watermark.IsZero() {
		wm = c.Watermark
	}
	if _, err := tx.Exec(ctx, `
		UPDATE satellite_state
		SET next_offset = $1, watermark = $2,
		    invalid = invalid + $3, low_conf = low_conf + $4, late = late + $5, unknown = unknown + $6,
		    updated_at = now()
		WHERE id = 1`,
		c.NextOffset, wm, c.Counts.Invalid, c.Counts.LowConf, c.Counts.Late, c.Counts.Unknown,
	); err != nil {
		return fmt.Errorf("update satellite_state: %w", err)
	}

	return tx.Commit(ctx)
}
