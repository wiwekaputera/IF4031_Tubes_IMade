package poller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/dlq"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/schema"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
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
	totalParked := 0

	for {
		select {
		case <-ctx.Done():
			p.logger.Warn().Msg("PVMBG poll cancelled by context")
			return
		default:
		}

		sinceStr := currentSince.UTC().Format(time.RFC3339)
		items, err := p.fetchVolcanicReports(ctx, corrID, sinceStr)
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

		if len(items) == 0 {
			break
		}

		var latestTime time.Time
		for _, raw := range items {
			ts, err := p.processPVMBG(ctx, corrID, raw)
			if err != nil {
				reason, retryable := "upsert_failed: "+err.Error(), true
				var pe *dlq.PermanentError
				if errors.As(err, &pe) {
					reason, retryable = pe.Reason, false
				}
				ref, _ := schema.Peek(raw)
				if perr := p.parkPVMBG(ctx, corrID, raw, ref, reason, retryable); perr != nil {
					// Stop without moving the watermark so this page is read again next poll.
					return
				}
				totalParked++
			} else {
				totalUpserted++
			}
			if !ts.IsZero() && ts.After(latestTime) {
				latestTime = ts
			}
		}

		if !latestTime.IsZero() {
			p.mu.Lock()
			if latestTime.After(p.health.PVMBG.LastSynced) {
				p.health.PVMBG.LastSynced = latestTime
			}
			p.mu.Unlock()
		}

		if len(items) < defaultBatchPageLimit {
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

	p.logger.Info().
		Int("reports_upserted", totalUpserted).
		Int("reports_parked", totalParked).
		Msg("PVMBG poll completed")
}

// parkPVMBG stores a message that could not be accepted in the dead-letter table.
// A non-nil error means the message was NOT stored and the caller must not advance the watermark.
func (p *Poller) parkPVMBG(ctx context.Context, corrID string, raw []byte, sourceRefID, reason string, retryable bool) error {
	err := p.repo.InsertDeadLetter(ctx, store.DeadLetter{
		Source:      string(hazard.SourcePVMBG),
		SourceRefID: sourceRefID,
		Raw:         raw,
		Reason:      reason,
		Retryable:   retryable,
	})
	if err != nil {
		p.logger.Error().Err(err).Str("correlation_id", corrID).Str("reason", reason).Msg("failed to store PVMBG message in DLQ")
		return err
	}
	p.logger.Warn().
		Str("correlation_id", corrID).
		Str("source_ref_id", sourceRefID).
		Str("reason", reason).
		Bool("retryable", retryable).
		Msg("PVMBG message stored in DLQ")
	return nil
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

// PVMBGRetryHandler reprocesses one stored PVMBG message through the same path as the live poll.
func (p *Poller) PVMBGRetryHandler() dlq.Handler {
	return func(ctx context.Context, raw []byte) error {
		_, err := p.processPVMBG(ctx, "dlq-replay", raw)
		return err
	}
}

// processPVMBG classifies, normalizes and stores one PVMBG message.
// It returns the message's reported time (zero if unknown) and:
//   - nil when the message was stored,
//   - *dlq.PermanentError when the message is rejected (schema or format),
//   - any other error when the store failed (transient).
//
// A panic while handling the message is recovered and reported as a permanent
// rejection, so one bad message cannot crash the aggregator.
func (p *Poller) processPVMBG(ctx context.Context, corrID string, raw []byte) (ts time.Time, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts = time.Time{}
			err = &dlq.PermanentError{Reason: fmt.Sprintf("panic: %v", r)}
		}
	}()

	report, rej := schema.ClassifyVolcanicReport(raw)
	if rej != nil {
		_, reportedAt := schema.Peek(raw)
		return reportedAt, &dlq.PermanentError{Reason: rej.Reason}
	}

	ts, _ = parseAgencyTime(report.ReportedAt)
	rec := p.normalizer.NormalizeVolcanic(report)
	rec.CorrelationID = corrID
	if _, err := p.repo.UpsertEvent(ctx, rec); err != nil {
		return ts, err
	}
	return ts, nil
}

// RunDLQWorker retries transient dead-letter rows every interval until ctx is cancelled.
func RunDLQWorker(ctx context.Context, wg *sync.WaitGroup, s dlq.Store, h dlq.Handler, interval time.Duration, logger zerolog.Logger) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("dlq worker stopped")
			return
		case now := <-ticker.C:
			res, err := dlq.RunRetries(ctx, s, h, now.UTC(), 100)
			if err != nil {
				logger.Error().Err(err).Msg("dlq retry pass failed")
				continue
			}
			if res.Resolved+res.Rescheduled+res.Parked > 0 {
				logger.Info().
					Int("resolved", res.Resolved).
					Int("rescheduled", res.Rescheduled).
					Int("parked", res.Parked).
					Msg("dlq retry pass completed")
			}
		}
	}
}
