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
		 WHERE acquired_at >= $1 AND lower(confidence) NOT IN ('l', 'low')`, since)
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

	for _, f := range c.Fires {
		if _, err := tx.Exec(ctx, `
			INSERT INTO satellite_fires
				(source_ref_id, center_lat, center_lon, occurred_at, detection_count, satellites,
				 max_confidence, total_frp_mw, detection_keys, severity, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
			ON CONFLICT (source_ref_id) DO UPDATE SET
				center_lat = EXCLUDED.center_lat,
				center_lon = EXCLUDED.center_lon,
				occurred_at = EXCLUDED.occurred_at,
				detection_count = EXCLUDED.detection_count,
				satellites = EXCLUDED.satellites,
				max_confidence = EXCLUDED.max_confidence,
				total_frp_mw = EXCLUDED.total_frp_mw,
				detection_keys = EXCLUDED.detection_keys,
				severity = EXCLUDED.severity,
				updated_at = now()`,
			f.SourceRefID, f.CenterLat, f.CenterLon, f.OccurredAt, f.DetectionCount, f.Satellites,
			f.MaxConfidence, f.TotalFRPMW, f.DetectionKeys, f.Severity,
		); err != nil {
			return fmt.Errorf("upsert fire %s: %w", f.SourceRefID, err)
		}
	}

	if c.PruneSince != nil {
		// Built with make, not a nil slice: a NULL array would make the NOT
		// test NULL and delete nothing.
		keep := make([]string, 0, len(c.Fires))
		for _, f := range c.Fires {
			keep = append(keep, f.SourceRefID)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM satellite_fires
			WHERE occurred_at >= $1 AND NOT (source_ref_id = ANY($2::text[]))`,
			*c.PruneSince, keep,
		); err != nil {
			return fmt.Errorf("prune fires: %w", err)
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
