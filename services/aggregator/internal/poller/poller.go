package poller

import (
	"context"
	"encoding/json"
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
	"golang.org/x/sync/errgroup"
)

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
	cfg         *config.Config
	normalizer  *normalizer.Normalizer
	repo        store.CanonicalRepo
	bmkClient   *http.Client
	pvmbgClient *http.Client
	breaker     *circuitbreaker.Breaker
	logger      zerolog.Logger
	mu          sync.RWMutex
	health      *SourceHealth
}

func New(cfg *config.Config, norm *normalizer.Normalizer, repo store.CanonicalRepo, logger zerolog.Logger) *Poller {
	return &Poller{
		cfg:         cfg,
		normalizer:  norm,
		repo:        repo,
		bmkClient:   &http.Client{Timeout: cfg.BMKGTimeout},
		pvmbgClient: &http.Client{Timeout: cfg.PVMBGTimeout},
		breaker:     circuitbreaker.New(cfg.PVMBGCbThreshold, cfg.PVMBGCbProbeInterval),
		logger:      logger.With().Str("component", "poller").Logger(),
		health: &SourceHealth{
			BMKG:  SourceStatus{Status: "DOWN"},
			PVMBG: SourceStatus{Status: "DOWN"},
		},
	}
}

func (p *Poller) Health() *SourceHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	h := *p.health
	return &h
}

func (p *Poller) Poll(ctx context.Context, corrID string, since time.Time) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.PollInterval*2)
	defer cancel()

	p.mu.RLock()
	bmkgSince := p.health.BMKG.LastSynced
	pvmbgSince := p.health.PVMBG.LastSynced
	p.mu.RUnlock()

	// If explicit non-zero since passed, respect it (for custom queries or tests)
	if !since.IsZero() {
		bmkgSince = since
		pvmbgSince = since
	}

	// Poll upstream sources concurrently with errgroup, each with its own
	// context timeout so slow or failing sources do not block others.
	eg, ctx := errgroup.WithContext(ctx)

	bmkgCtx, bmkgCancel := context.WithTimeout(ctx, p.cfg.BMKGTimeout)
	defer bmkgCancel()
	eg.Go(func() error {
		p.pollBMKG(bmkgCtx, corrID, bmkgSince)
		return nil
	})

	pvmbgCtx, pvmbgCancel := context.WithTimeout(ctx, p.cfg.PVMBGTimeout)
	defer pvmbgCancel()
	eg.Go(func() error {
		p.pollPVMBG(pvmbgCtx, corrID, pvmbgSince)
		return nil
	})

	_ = eg.Wait()
}

func (p *Poller) pollBMKG(ctx context.Context, corrID string, since time.Time) {
	sinceStr := since.UTC().Format(time.RFC3339)

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

	count := 0
	for _, evt := range events {
		var warning *agency.TsunamiWarning
		if w, ok := warningsMap[evt.EventID]; ok {
			warning = &w
		}

		rec := p.normalizer.NormalizeSeismic(evt, warning)
		rec.CorrelationID = corrID

		id, err := p.repo.UpsertEvent(ctx, rec)
		if err != nil {
			p.logger.Error().Err(err).Str("event_id", evt.EventID).Msg("failed to upsert seismic event")
			continue
		}
		_ = id
		count++
	}

	p.mu.Lock()
	p.health.BMKG.Status = "UP"
	p.health.BMKG.LastSynced = time.Now().UTC()
	p.mu.Unlock()
	p.logger.Info().Int("events_upserted", count).Msg("BMKG poll completed")
}

func (p *Poller) fetchSeismicEvents(ctx context.Context, corrID string, since string) ([]agency.SeismicEvent, error) {
	url := fmt.Sprintf("%s/seismic-events?since=%s", p.cfg.BMKGBaseURL, url.QueryEscape(since))
	return fetchAgencyEvents[agency.SeismicEvent](ctx, p.logger, p.bmkClient, url, corrID, p.cfg.BMKGAPIKey, "X-BMKG-Key")
}

func (p *Poller) fetchTsunamiWarnings(ctx context.Context, corrID string, since string) ([]agency.TsunamiWarning, error) {
	url := fmt.Sprintf("%s/tsunami-warnings?since=%s", p.cfg.BMKGBaseURL, url.QueryEscape(since))
	return fetchAgencyEvents[agency.TsunamiWarning](ctx, p.logger, p.bmkClient, url, corrID, p.cfg.BMKGAPIKey, "X-BMKG-Key")
}

func (p *Poller) fetchVolcanicReports(ctx context.Context, corrID string, since string) ([]agency.VolcanicReport, error) {
	url := fmt.Sprintf("%s/volcanic-reports?since=%s", p.cfg.PVMBGBaseURL, url.QueryEscape(since))
	return fetchAgencyEvents[agency.VolcanicReport](ctx, p.logger, p.pvmbgClient, url, corrID, p.cfg.PVMBGBearerToken, "Authorization")
}

func fetchAgencyEvents[T any](ctx context.Context, logger zerolog.Logger, client *http.Client, url, corrID, token, authHeader string) ([]T, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Correlation-ID", corrID)
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
