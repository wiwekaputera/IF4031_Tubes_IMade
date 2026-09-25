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

	sinceStr := since.Format(time.RFC3339)
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
	p.health.PVMBG.LastSynced = time.Now().UTC()
	p.health.PVMBG.StaleSince = nil
	p.mu.Unlock()

	count := 0
	for _, report := range reports {
		rec := p.normalizer.NormalizeVolcanic(report)
		rec.CorrelationID = corrID

		_, err := p.repo.UpsertEvent(ctx, rec)
		if err != nil {
			p.logger.Error().Err(err).Str("report_id", report.ReportID).Msg("failed to upsert volcanic report")
			continue
		}
		count++
	}

	p.logger.Info().Int("reports_upserted", count).Msg("PVMBG poll completed")
}

func StartPolling(ctx context.Context, wg *sync.WaitGroup, p *Poller, cfg *config.Config, logger zerolog.Logger) {
	defer wg.Done()

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	lastPoll := time.Time{}

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("poller stopped")
			return
		case t := <-ticker.C:
			corrID := fmt.Sprintf("poll-%d", t.UnixNano())
			p.Poll(ctx, corrID, lastPoll)
			lastPoll = t
		}
	}
}
