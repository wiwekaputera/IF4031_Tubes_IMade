package poller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func (p *Poller) pollPVMBG(ctx context.Context, corrID string, since time.Time, fenceToken int64) {
	if !p.breaker.Allow() {
		p.mu.Lock()
		stale := p.health.PVMBG.LastSynced
		p.health.PVMBG.Status = "OUTAGE"
		if p.health.PVMBG.StaleSince == nil {
			t := stale
			p.health.PVMBG.StaleSince = &t
		}
		p.mu.Unlock()
		p.logger.Warn().Msg("PVMBG circuit breaker open, skipping poll")
		return
	}

	currentSince := since
	totalUpserted := 0
	firstPage := true

	for {
		select {
		case <-ctx.Done():
			p.logger.Warn().Msg("PVMBG poll cancelled by context")
			return
		default:
		}

		fetchSince := currentSince
		if firstPage && !fetchSince.IsZero() {
			fetchSince = fetchSince.Add(-watermarkOverlap)
		}
		firstPage = false
		sinceStr := fetchSince.UTC().Format(time.RFC3339)

		reports, err := p.fetchVolcanicReports(ctx, corrID, sinceStr)
		if err != nil {
			p.breaker.RecordFailure()
			p.mu.Lock()
			p.health.PVMBG.Status = "ERROR"
			p.mu.Unlock()
			p.logger.Error().Err(err).Msg("failed to fetch PVMBG volcanic reports")
			return
		}

		p.breaker.RecordSuccess()
		p.mu.Lock()
		p.health.PVMBG.Status = "UP"
		p.health.PVMBG.StaleSince = nil
		p.mu.Unlock()

		if len(reports) == 0 {
			break
		}

		var latestTime time.Time
		recs := make([]store.UpsertRequest, 0, len(reports))
		for _, report := range reports {
			rec := p.normalizer.NormalizeVolcanic(report)
			rec.CorrelationID = corrID
			recs = append(recs, rec)

			t, err := parseAgencyTime(report.ReportedAt)
			if err == nil && t.After(latestTime) {
				latestTime = t
			}
		}

		if p.elector != nil {
			fence := store.Fence{
				LeaseName: p.leaseName,
				Holder:    p.cfg.InstanceID,
				Token:     fenceToken,
			}
			if err := p.repo.UpsertFencedBatch(ctx, fence, string(hazard.SourcePVMBG), latestTime, recs); err != nil {
				if errors.Is(err, store.ErrFenced) {
					p.logger.Warn().
						Int64("fencing_token", fenceToken).
						Str("holder", p.cfg.InstanceID).
						Str("correlation_id", corrID).
						Msg("write rejected: stale fencing token")
					p.elector.Demote()
					return
				}
				p.logger.Error().Err(err).Str("correlation_id", corrID).Msg("failed to upsert PVMBG fenced batch")
				return
			}
			totalUpserted += len(recs)
		} else {
			for _, rec := range recs {
				if _, err := p.repo.UpsertEvent(ctx, rec); err != nil {
					p.logger.Error().Err(err).Str("source_ref_id", rec.SourceRefID).Msg("failed to upsert volcanic report")
					continue
				}
				totalUpserted++
			}
		}

		if !latestTime.IsZero() {
			p.mu.Lock()
			if latestTime.After(p.health.PVMBG.LastSynced) {
				p.health.PVMBG.LastSynced = latestTime
			}
			p.mu.Unlock()
		}

		if len(reports) < defaultBatchPageLimit {
			break
		}

		if !latestTime.After(currentSince) {
			p.logger.Warn().
				Time("watermark", latestTime).
				Time("current_since", currentSince).
				Msg("PVMBG watermark did not advance; breaking pagination loop")
			break
		}

		currentSince = latestTime
	}

	p.logger.Info().Int("reports_upserted", totalUpserted).Msg("PVMBG poll completed")
}

func StartPolling(ctx context.Context, wg *sync.WaitGroup, p *Poller, cfg *config.Config, logger zerolog.Logger) {
	defer wg.Done()

	if p.elector == nil {
		if err := p.HydrateWatermarks(ctx); err != nil {
			logger.Warn().Err(err).Msg("failed to hydrate poller watermarks on startup")
		}
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("poller stopped")
			return
		case t := <-ticker.C:
			corrID := fmt.Sprintf("poll-%d", t.UnixNano())
			p.Poll(ctx, corrID, time.Time{})
		}
	}
}
