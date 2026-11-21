// Package sensor is the HTTP client for the coastal sensor API.
package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

const maxBody = 8 << 20

// Page is one decoded GET /v1/observations response. Items stay raw so a
// single malformed item is set aside without losing the rest of the page.
type Page struct {
	GeneratedAt time.Time
	Items       []json.RawMessage
	NextCursor  string
}

// Error classifies a failed request for metrics and the breaker.
type Error struct {
	Kind   string // timeout, network, http_5xx, http_429, http_4xx, bad_response
	Status int
	Err    error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("sensor %s (HTTP %d): %v", e.Kind, e.Status, e.Err)
	}
	return fmt.Sprintf("sensor %s: %v", e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Client talks to one sensor base URL.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// New builds a client with its own connection pool (bulkhead: the sensor can
// hold at most maxConns connections from this process, nothing shared with
// other upstreams) and a per-request timeout.
func New(base, token string, timeout time.Duration, maxConns int, rt http.RoundTripper) *Client {
	if rt == nil {
		rt = &http.Transport{
			MaxConnsPerHost:       maxConns,
			MaxIdleConnsPerHost:   maxConns,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: timeout,
		}
	}
	return &Client{base: base, token: token, http: &http.Client{Timeout: timeout, Transport: rt}}
}

// Stations fetches GET /v1/stations.
func (c *Client) Stations(ctx context.Context) ([]sensorpantai.Station, error) {
	body, err := c.get(ctx, "/v1/stations")
	if err != nil {
		return nil, err
	}
	var resp sensorpantai.StationsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, &Error{Kind: "bad_response", Err: fmt.Errorf("stations: %w", err)}
	}
	return resp.Items, nil
}

// Observations fetches one page after cursor.
func (c *Client) Observations(ctx context.Context, cursor string, limit int) (Page, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	body, err := c.get(ctx, "/v1/observations?"+q.Encode())
	if err != nil {
		return Page{}, err
	}
	var raw struct {
		GeneratedAt string            `json:"generated_at"`
		Items       []json.RawMessage `json:"items"`
		NextCursor  *string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return Page{}, &Error{Kind: "bad_response", Err: fmt.Errorf("observations: %w", err)}
	}
	gen, err := time.Parse(time.RFC3339, raw.GeneratedAt)
	if err != nil {
		return Page{}, &Error{Kind: "bad_response", Err: fmt.Errorf("generated_at %q: %w", raw.GeneratedAt, err)}
	}
	p := Page{GeneratedAt: gen.UTC(), Items: raw.Items}
	if raw.NextCursor != nil {
		p.NextCursor = *raw.NextCursor
	}
	return p, nil
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, &Error{Kind: "network", Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		kind := "network"
		var ne interface{ Timeout() bool }
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			kind = "timeout"
		}
		return nil, &Error{Kind: kind, Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, &Error{Kind: "network", Err: err}
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return body, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &Error{Kind: "http_429", Status: resp.StatusCode, Err: errors.New(snippet(body))}
	case resp.StatusCode >= 500:
		return nil, &Error{Kind: "http_5xx", Status: resp.StatusCode, Err: errors.New(snippet(body))}
	default:
		return nil, &Error{Kind: "http_4xx", Status: resp.StatusCode, Err: errors.New(snippet(body))}
	}
}

func snippet(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}
