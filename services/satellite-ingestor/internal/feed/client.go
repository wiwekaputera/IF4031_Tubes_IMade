package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Message is one event delivered by the stream. Offset comes from the data
// payload; Detection is the raw detection object for processing.
type Message struct {
	Type       string
	Offset     int64
	Detection  json.RawMessage // the "detection" object, for detection events
	Data       json.RawMessage // the whole data object, for update and retract events
	ReceivedAt time.Time       // when the event was read off the stream
	// Bad is set when the event could not be decoded. The offset then comes
	// from the SSE id, Data holds the raw text, and the handler is expected to
	// dead-letter it so one poison event cannot stall the stream.
	Bad string
}

// StatusError is returned when the feed answers with a non-200 status.
type StatusError struct {
	Code       int
	RetryAfter time.Duration
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("feed returned HTTP %d: %s", e.Code, strings.TrimSpace(e.Body))
}

// Terminal reports whether reconnecting cannot help (bad token or request).
func (e *StatusError) Terminal() bool {
	return e.Code == http.StatusBadRequest || e.Code == http.StatusUnauthorized
}

// Client connects to GET /v1/stream with a group Bearer token.
type Client struct {
	BaseURL     string
	Token       string
	HTTP        *http.Client
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	IdleTimeout time.Duration // reconnect if no bytes (including heartbeats) arrive for this long
	Log         *slog.Logger  // reconnects and errors are logged here; nil uses slog.Default
}

// NewClient returns a Client with sane defaults. The HTTP client has no overall
// timeout because the stream is long-lived; idleness is handled by IdleTimeout.
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL:     strings.TrimRight(baseURL, "/"),
		Token:       token,
		HTTP:        &http.Client{},
		MinBackoff:  time.Second,
		MaxBackoff:  time.Minute,
		IdleTimeout: 45 * time.Second,
	}
}

// Run keeps one stream open and reconnects after drops. next is called before
// every connection and must return the offset to resume from, normally the
// last committed offset + 1. handle is called for each message in order and
// must commit its effects (including the offset) before returning nil; an
// error stops Run.
func (c *Client) Run(ctx context.Context, next func(context.Context) (int64, error), handle func(context.Context, Message) error) error {
	backoff := c.MinBackoff
	for {
		from, err := next(ctx)
		if err != nil {
			return err
		}
		delivered, err := c.connect(ctx, from, handle)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var he handlerError
		if errors.As(err, &he) {
			return he.err // our own commit failed: stop, do not skip the message
		}
		if delivered {
			backoff = c.MinBackoff
		}

		wait := backoff
		var se *StatusError
		if errors.As(err, &se) {
			if se.Terminal() {
				return se // bad token or request: reconnecting cannot fix it
			}
			if se.RetryAfter > wait {
				wait = se.RetryAfter
			}
		}

		c.logger().Warn("feed connection ended, reconnecting",
			"err", err, "from", from, "wait", wait.String())
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		backoff = minDuration(backoff*2, c.MaxBackoff)
	}
}

// Session is the feed's view of this group, from GET /v1/session.
type Session struct {
	Profile      string    `json:"profile"`
	Speed        float64   `json:"speed"`
	LatestOffset int64     `json:"latest_offset"`
	VirtualNow   time.Time `json:"virtual_now"`
}

// Session fetches the newest offset and the feed's virtual clock.
func (c *Client) Session(ctx context.Context) (Session, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/session", nil)
	if err != nil {
		return Session{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return Session{}, &StatusError{Code: resp.StatusCode, Body: string(body)}
	}
	var s Session
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&s); err != nil {
		return Session{}, fmt.Errorf("decode session: %w", err)
	}
	return s, nil
}

func (c *Client) logger() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

// handlerError marks errors returned by the caller's handle, so Run stops
// instead of reconnecting.
type handlerError struct{ err error }

func (e handlerError) Error() string { return e.err.Error() }
func (e handlerError) Unwrap() error { return e.err }

// connect opens one stream from offset from and delivers messages until it ends.
// delivered reports whether at least one message was handled.
func (c *Client) connect(ctx context.Context, from int64, handle func(context.Context, Message) error) (delivered bool, err error) {
	u, err := url.Parse(c.BaseURL + "/v1/stream")
	if err != nil {
		return false, err
	}
	q := u.Query()
	q.Set("from", strconv.FormatInt(from, 10))
	u.RawQuery = q.Encode()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, &StatusError{
			Code:       resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			Body:       string(body),
		}
	}

	// Idle watchdog: heartbeats keep the stream alive, so silence means a dead connection.
	idle := time.AfterFunc(c.IdleTimeout, cancel)
	defer idle.Stop()

	expect := from
	body := &watchedReader{r: resp.Body, onRead: func() { idle.Reset(c.IdleTimeout) }}
	err = ReadEvents(body, func(ev Event) error {
		msg, err := decodeMessage(ev)
		if err != nil {
			return err
		}
		if ev.Event == "heartbeat" {
			// A heartbeat takes an offset slot but changes no state. Advance past
			// it, or the next detection looks like a gap and the stream reconnects
			// to the same heartbeat forever.
			if msg.Offset >= expect {
				expect = msg.Offset + 1
			}
			return nil
		}
		if msg.Offset < expect {
			return nil // already committed; skip duplicates after a reconnect
		}
		if msg.Offset > expect {
			return fmt.Errorf("offset gap: expected %d, got %d", expect, msg.Offset)
		}
		if err := handle(ctx, msg); err != nil {
			return handlerError{err}
		}
		expect = msg.Offset + 1
		delivered = true
		return nil
	})
	if err != nil {
		return delivered, err
	}
	return delivered, io.ErrUnexpectedEOF
}

type envelope struct {
	Offset    int64           `json:"offset"`
	Detection json.RawMessage `json:"detection"`
}

// decodeMessage turns an SSE event into a Message. An event whose payload
// cannot be read is still returned, marked Bad, as long as its SSE id gives
// the offset. Only an event with neither a readable payload nor an id is an
// error, because it cannot be placed in the sequence.
func decodeMessage(ev Event) (Message, error) {
	now := time.Now()
	idOffset, idErr := strconv.ParseInt(strings.TrimSpace(ev.ID), 10, 64)
	bad := func(reason string) (Message, error) {
		if idErr != nil {
			return Message{}, fmt.Errorf("%s event without a usable id: %s", ev.Event, reason)
		}
		return Message{Type: ev.Event, Offset: idOffset, Data: json.RawMessage(ev.Data), ReceivedAt: now, Bad: reason}, nil
	}

	if ev.Oversized {
		return bad(fmt.Sprintf("oversized: a line exceeded %d bytes", MaxLineBytes))
	}
	var env envelope
	if err := json.Unmarshal([]byte(ev.Data), &env); err != nil {
		return bad("malformed_json: " + err.Error())
	}
	if idErr == nil && env.Offset != idOffset {
		return bad(fmt.Sprintf("offset_mismatch: id %d, payload %d", idOffset, env.Offset))
	}
	return Message{Type: ev.Event, Offset: env.Offset, Detection: env.Detection, Data: json.RawMessage(ev.Data), ReceivedAt: now}, nil
}

type watchedReader struct {
	r      io.Reader
	onRead func()
}

func (w *watchedReader) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if n > 0 {
		w.onRead()
	}
	return n, err
}

func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
