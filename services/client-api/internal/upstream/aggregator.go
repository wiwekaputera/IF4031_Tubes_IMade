package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Client defines the interface for interacting with upstream Aggregator.
type Client interface {
	GetEvents(ctx context.Context, rawQuery, correlationID string) (*EventsResponse, error)
}

// EventsResponse represents the payload returned by Aggregator's /internal/events.
type EventsResponse struct {
	Data []map[string]interface{} `json:"data"`
	Meta map[string]interface{}   `json:"meta"`
}

// UpstreamError represents an HTTP or network error encountered when calling Aggregator.
type UpstreamError struct {
	StatusCode int    `json:"status_code"`
	ErrorCode  string `json:"error"`
	Message    string `json:"error_description"`
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream aggregator error [%d %s]: %s", e.StatusCode, e.ErrorCode, e.Message)
}

// Options configures the AggregatorClient.
type Options struct {
	BaseURL    string
	Timeout    time.Duration
	HTTPClient *http.Client
	Logger     zerolog.Logger
}

// AggregatorClient implements Client using http.Client with connection pooling and timeouts.
type AggregatorClient struct {
	baseURL    string
	httpClient *http.Client
	logger     zerolog.Logger
}

// NewClient creates a new AggregatorClient.
func NewClient(opts Options) *AggregatorClient {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   1 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		}
		httpClient = &http.Client{
			Timeout:   timeout,
			Transport: transport,
		}
	}

	baseURL := strings.TrimRight(opts.BaseURL, "/")

	return &AggregatorClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		logger:     opts.Logger,
	}
}

// GetEvents calls GET /internal/events on Aggregator with query parameters and correlation ID propagation.
func (c *AggregatorClient) GetEvents(ctx context.Context, rawQuery, correlationID string) (*EventsResponse, error) {
	reqURL := c.baseURL + "/internal/events"
	if rawQuery != "" {
		reqURL += "?" + rawQuery
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	latencyMs := time.Since(start).Milliseconds()

	if err != nil {
		statusCode := http.StatusServiceUnavailable
		errCode := "upstream_unavailable"
		msg := "aggregator service is unavailable"

		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
			statusCode = http.StatusGatewayTimeout
			errCode = "gateway_timeout"
			msg = "aggregator request timed out"
		}

		c.logger.Error().
			Err(err).
			Str("service", "client-api").
			Str("upstream", "aggregator").
			Str("correlation_id", correlationID).
			Int64("latency_ms", latencyMs).
			Int("upstream_status", statusCode).
			Msg("failed to call upstream aggregator")

		return nil, &UpstreamError{
			StatusCode: statusCode,
			ErrorCode:  errCode,
			Message:    msg,
		}
	}
	defer resp.Body.Close()

	c.logger.Info().
		Str("service", "client-api").
		Str("upstream", "aggregator").
		Str("correlation_id", correlationID).
		Int64("latency_ms", latencyMs).
		Int("upstream_status", resp.StatusCode).
		Msg("upstream aggregator call finished")

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &UpstreamError{
			StatusCode: http.StatusBadGateway,
			ErrorCode:  "bad_gateway",
			Message:    "failed to read aggregator response body",
		}
	}

	if resp.StatusCode != http.StatusOK {
		errCode := "bad_gateway"
		if resp.StatusCode == http.StatusBadRequest {
			errCode = "invalid_request"
		}
		return nil, &UpstreamError{
			StatusCode: resp.StatusCode,
			ErrorCode:  errCode,
			Message:    string(bodyBytes),
		}
	}

	var eventsResp EventsResponse
	if err := json.Unmarshal(bodyBytes, &eventsResp); err != nil {
		c.logger.Error().
			Err(err).
			Str("service", "client-api").
			Str("upstream", "aggregator").
			Str("correlation_id", correlationID).
			Int64("latency_ms", latencyMs).
			Msg("failed to decode JSON response from aggregator")

		return nil, &UpstreamError{
			StatusCode: http.StatusBadGateway,
			ErrorCode:  "bad_gateway",
			Message:    "invalid JSON returned from aggregator",
		}
	}

	return &eventsResp, nil
}
