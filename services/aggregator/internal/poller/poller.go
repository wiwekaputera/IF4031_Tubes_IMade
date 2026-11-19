package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/circuitbreaker"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBatchPageLimit = 250
	watermarkOverlap      = 1 * time.Second
)

// LeaderElector abstracts leader lease checks and demotion for the poller.
type LeaderElector interface {
	RenewNow(ctx context.Context) (token int64, ok bool)
	IsLeader() (token int64, ok bool)
	Demote()
}

func parseAgencyTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	return t, err
}

type SourceStatus struct {
	Status     string
	LastSynced time.Time
	StaleSince *time.Time
}

type SourceHealth struct {
	BMKG  SourceStatus
	PVMBG SourceStatus
}

type Poller struct {
	cfg             *config.Config
	normalizer      *normalizer.Normalizer
	repo            store.CanonicalRepo
	elector         LeaderElector
	leaseName       string
	lastLeaderToken int64
	bmkClient       *http.Client
	pvmbgClient     *http.Client
	breaker         *circuitbreaker.Breaker
	logger          zerolog.Logger
	mu              sync.RWMutex
	health          *SourceHealth
}

func New(cfg *config.Config, norm *normalizer.Normalizer, repo store.CanonicalRepo, logger zerolog.Logger) *Poller {
	return &Poller{
		cfg:         cfg,
		normalizer:  norm,
		repo:        repo,
		leaseName:   store.DefaultLeaseName,
		bmkClient:   &http.Client{Timeout: cfg.BMKGTimeout, Transport: observability.Transport("aggregator", nil)},
		pvmbgClient: &http.Client{Timeout: cfg.PVMBGTimeout, Transport: observability.Transport("aggregator", nil)},
		breaker:     circuitbreaker.New(cfg.PVMBGCbThreshold, cfg.PVMBGCbProbeInterval),
		logger:      logger.With().Str("component", "poller").Logger(),
		health: &SourceHealth{
			BMKG:  SourceStatus{Status: "DOWN"},
			PVMBG: SourceStatus{Status: "DOWN"},
		},
	}
}

// WithElector attaches a leader elector so the poller only polls while holding
// a valid lease and fences every write batch.
func (p *Poller) WithElector(e LeaderElector) *Poller {
	p.elector = e
	return p
}

func (p *Poller) Health() *SourceHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	h := *p.health
	return &h
}

func (p *Poller) HydrateWatermarks(ctx context.Context) error {
	bmkgTime, err := p.repo.GetLatestEventTime(ctx, string(hazard.SourceBMKG))
	if err != nil {
		p.logger.Warn().Err(err).Msg("failed to hydrate BMKG watermark from database")
	} else if !bmkgTime.IsZero() {
		p.mu.Lock()
		p.health.BMKG.LastSynced = bmkgTime
		p.mu.Unlock()
		p.logger.Info().Time("last_synced", bmkgTime).Msg("hydrated BMKG watermark from database")
	}

	pvmbgTime, err := p.repo.GetLatestEventTime(ctx, string(hazard.SourcePVMBG))
	if err != nil {
		p.logger.Warn().Err(err).Msg("failed to hydrate PVMBG watermark from database")
	} else if !pvmbgTime.IsZero() {
		p.mu.Lock()
		p.health.PVMBG.LastSynced = pvmbgTime
		p.mu.Unlock()
		p.logger.Info().Time("last_synced", pvmbgTime).Msg("hydrated PVMBG watermark from database")
	}

	return nil
}

func (p *Poller) loadWatermarks(ctx context.Context) {
	bmkgTime, err := p.repo.GetPollerWatermark(ctx, string(hazard.SourceBMKG))
	if err != nil {
		p.logger.Warn().Err(err).Msg("failed to load BMKG watermark from database")
	} else {
		p.mu.Lock()
		p.health.BMKG.LastSynced = bmkgTime
		p.mu.Unlock()
		if !bmkgTime.IsZero() {
			p.logger.Info().Time("watermark", bmkgTime).Msg("loaded BMKG watermark from database")
		}
	}

	pvmbgTime, err := p.repo.GetPollerWatermark(ctx, string(hazard.SourcePVMBG))
	if err != nil {
		p.logger.Warn().Err(err).Msg("failed to load PVMBG watermark from database")
	} else {
		p.mu.Lock()
		p.health.PVMBG.LastSynced = pvmbgTime
		p.mu.Unlock()
		if !pvmbgTime.IsZero() {
			p.logger.Info().Time("watermark", pvmbgTime).Msg("loaded PVMBG watermark from database")
		}
	}
}

func (p *Poller) Poll(ctx context.Context, corrID string, since time.Time) {
	// If context has no deadline, assign a generous cycle timeout so multi-page
	// backlog draining can complete without hanging indefinitely.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := p.cfg.PollInterval * 10
		if timeout < 30*time.Second {
			timeout = 30 * time.Second
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var fenceToken int64
	if p.elector != nil {
		tok, ok := p.elector.RenewNow(ctx)
		if !ok {
			return
		}
		fenceToken = tok

		p.mu.RLock()
		prevTok := p.lastLeaderToken
		p.mu.RUnlock()
		if tok != prevTok {
			p.loadWatermarks(ctx)
			p.mu.Lock()
			p.lastLeaderToken = tok
			p.mu.Unlock()
		}
	}

	p.mu.RLock()
	bmkgSince := p.health.BMKG.LastSynced
	pvmbgSince := p.health.PVMBG.LastSynced
	p.mu.RUnlock()

	// If explicit non-zero since passed, respect it (for custom queries or tests)
	if !since.IsZero() {
		bmkgSince = since
		pvmbgSince = since
	}

	// Poll upstream sources concurrently with errgroup.
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		p.pollBMKG(egCtx, corrID, bmkgSince, fenceToken)
		return nil
	})

	eg.Go(func() error {
		p.pollPVMBG(egCtx, corrID, pvmbgSince, fenceToken)
		return nil
	})

	_ = eg.Wait()
}

func (p *Poller) pollBMKG(ctx context.Context, corrID string, since time.Time, fenceToken int64) {
	currentSince := since
	totalUpserted := 0
	firstPage := true

	for {
		select {
		case <-ctx.Done():
			p.logger.Warn().Msg("BMKG poll cancelled by context")
			return
		default:
		}

		fetchSince := currentSince
		if firstPage && !fetchSince.IsZero() {
			fetchSince = fetchSince.Add(-watermarkOverlap)
		}
		firstPage = false
		sinceStr := fetchSince.UTC().Format(time.RFC3339)

		events, err := p.fetchSeismicEvents(ctx, corrID, sinceStr)
		if err != nil {
			p.logger.Error().Err(err).Msg("failed to fetch BMKG seismic events")
			p.mu.Lock()
			p.health.BMKG.Status = "ERROR"
			p.mu.Unlock()
			return
		}

		warningsMap := make(map[string]agency.TsunamiWarning)
		warnings, err := p.fetchTsunamiWarnings(ctx, corrID, sinceStr)
		if err != nil {
			p.logger.Warn().Err(err).Msg("failed to fetch BMKG tsunami warnings")
		} else {
			for _, w := range warnings {
				warningsMap[w.RelatedEventID] = w
			}
		}

		p.mu.Lock()
		p.health.BMKG.Status = "UP"
		p.mu.Unlock()

		if len(events) == 0 {
			break
		}

		var latestTime time.Time
		recs := make([]store.UpsertRequest, 0, len(events))
		for _, evt := range events {
			var warning *agency.TsunamiWarning
			if w, ok := warningsMap[evt.EventID]; ok {
				warning = &w
			}

			rec := p.normalizer.NormalizeSeismic(evt, warning)
			rec.CorrelationID = corrID
			recs = append(recs, rec)

			t, err := parseAgencyTime(evt.OccurredAt)
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
			if err := p.repo.UpsertFencedBatch(ctx, fence, string(hazard.SourceBMKG), latestTime, recs); err != nil {
				if errors.Is(err, store.ErrFenced) {
					p.logger.Warn().
						Int64("fencing_token", fenceToken).
						Str("holder", p.cfg.InstanceID).
						Str("correlation_id", corrID).
						Msg("write rejected: stale fencing token")
					p.elector.Demote()
					return
				}
				p.logger.Error().Err(err).Str("correlation_id", corrID).Msg("failed to upsert BMKG fenced batch")
				return
			}
			totalUpserted += len(recs)
		} else {
			for _, rec := range recs {
				if _, err := p.repo.UpsertEvent(ctx, rec); err != nil {
					p.logger.Error().Err(err).Str("source_ref_id", rec.SourceRefID).Msg("failed to upsert seismic event")
					continue
				}
				totalUpserted++
			}
		}

		if !latestTime.IsZero() {
			p.mu.Lock()
			if latestTime.After(p.health.BMKG.LastSynced) {
				p.health.BMKG.LastSynced = latestTime
			}
			p.mu.Unlock()
		}

		if len(events) < defaultBatchPageLimit {
			break
		}

		if !latestTime.After(currentSince) {
			p.logger.Warn().
				Time("watermark", latestTime).
				Time("current_since", currentSince).
				Msg("BMKG watermark did not advance; breaking pagination loop")
			break
		}

		currentSince = latestTime
	}

	p.logger.Info().Int("events_upserted", totalUpserted).Msg("BMKG poll completed")
}

func (p *Poller) fetchSeismicEvents(ctx context.Context, corrID string, since string) ([]agency.SeismicEvent, error) {
	url := fmt.Sprintf("%s/seismic-events?since=%s&limit=%d", p.cfg.BMKGBaseURL, url.QueryEscape(since), defaultBatchPageLimit)
	return fetchAgencyEvents[agency.SeismicEvent](ctx, p.logger, p.bmkClient, url, corrID, p.cfg.InstanceID, p.cfg.BMKGAPIKey, "X-BMKG-Key")
}

func (p *Poller) fetchTsunamiWarnings(ctx context.Context, corrID string, since string) ([]agency.TsunamiWarning, error) {
	url := fmt.Sprintf("%s/tsunami-warnings?since=%s&limit=%d", p.cfg.BMKGBaseURL, url.QueryEscape(since), defaultBatchPageLimit)
	return fetchAgencyEvents[agency.TsunamiWarning](ctx, p.logger, p.bmkClient, url, corrID, p.cfg.InstanceID, p.cfg.BMKGAPIKey, "X-BMKG-Key")
}

func (p *Poller) fetchVolcanicReports(ctx context.Context, corrID string, since string) ([]json.RawMessage, error) {
	url := fmt.Sprintf("%s/volcanic-reports?since=%s&limit=%d", p.cfg.PVMBGBaseURL, url.QueryEscape(since), defaultBatchPageLimit)
	return fetchAgencyEvents[json.RawMessage](ctx, p.logger, p.pvmbgClient, url, corrID, p.cfg.InstanceID, p.cfg.PVMBGBearerToken, "Authorization")
}

func fetchAgencyEvents[T any](ctx context.Context, logger zerolog.Logger, client *http.Client, url, corrID, instanceID, token, authHeader string) ([]T, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Correlation-ID", corrID)
	if instanceID != "" {
		req.Header.Set("X-Instance-ID", instanceID)
	}
	if authHeader == "X-BMKG-Key" {
		req.Header.Set("X-BMKG-Key", token)
	} else {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	start := time.Now()
	resp, err := client.Do(req)
	latencyMs := time.Since(start).Milliseconds()

	if err != nil {
		logger.Error().
			Err(err).
			Str("correlation_id", corrID).
			Str("url", url).
			Int64("latency_ms", latencyMs).
			Msg("outbound request failed")
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	logger.Info().
		Str("correlation_id", corrID).
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Int64("latency_ms", latencyMs).
		Msg("outbound request completed")

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	var result []T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
