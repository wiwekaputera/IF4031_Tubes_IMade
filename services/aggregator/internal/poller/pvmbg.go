package poller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
)

func (p *Poller) pollPVMBG(ctx context.Context, corrID string, since time.Time) {
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

	for {
		select {
		case <-ctx.Done():
			p.logger.Warn().Msg("PVMBG poll cancelled by context")
			return
		default:
		}

		sinceStr := currentSince.UTC().Format(time.RFC3339)
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
		for _, report := range reports {
			rec := p.normalizer.NormalizeVolcanic(report)
			rec.CorrelationID = corrID

			_, err := p.repo.UpsertEvent(ctx, rec)
			if err != nil {
				p.logger.Error().Err(err).Str("report_id", report.ReportID).Msg("failed to upsert volcanic report")
				continue
			}
			totalUpserted++

			t, err := parseAgencyTime(report.ReportedAt)
			if err == nil && t.After(latestTime) {
				latestTime = t
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

	if err := p.HydrateWatermarks(ctx); err != nil {
		logger.Warn().Err(err).Msg("failed to hydrate poller watermarks on startup")
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
