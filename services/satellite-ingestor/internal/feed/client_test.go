package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReadEventsParsesFieldsAndSkipsComments(t *testing.T) {
	stream := ": keepalive\n\n" +
		"id: 3\nevent: detection\ndata: {\"offset\":3}\n\n" +
		"event: heartbeat\ndata: x\ndata: y\n\n" +
		"id: 9\nevent: detection\ndata: cut off without blank line"

	var got []Event
	if err := ReadEvents(strings.NewReader(stream), func(ev Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 (truncated final event must be dropped): %+v", len(got), got)
	}
	if got[0].ID != "3" || got[0].Event != "detection" || got[0].Data != `{"offset":3}` {
		t.Fatalf("event 0 = %+v", got[0])
	}
	if got[1].Event != "heartbeat" || got[1].Data != "x\ny" {
		t.Fatalf("multi-line data not joined: %+v", got[1])
	}
}

// fakeFeed serves one scripted stream per connection, recording the query and auth on each.
type fakeFeed struct {
	mu      sync.Mutex
	froms   []string
	auths   []string
	scripts []func(w http.ResponseWriter)
}

func (f *fakeFeed) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.froms = append(f.froms, r.URL.Query().Get("from"))
	f.auths = append(f.auths, r.Header.Get("Authorization"))
	i := len(f.froms) - 1
	f.mu.Unlock()
	if i >= len(f.scripts) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	f.scripts[i](w)
}

func sse(w http.ResponseWriter, offsets ...int64) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	for _, o := range offsets {
		fmt.Fprintf(w, "id: %d\nevent: detection\ndata: {\"offset\":%d,\"detection\":{\"satellite\":\"N21\"}}\n\n", o, o)
	}
}

func fastClient(url string) *Client {
	c := NewClient(url, "secret-token")
	c.MinBackoff = time.Millisecond
	c.MaxBackoff = 5 * time.Millisecond
	c.IdleTimeout = time.Second
	return c
}

func TestRunResumesFromCommittedOffsetAfterDrop(t *testing.T) {
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) { sse(w, 0, 1) }, // stream drops after offset 1
		func(w http.ResponseWriter) { sse(w, 2, 3) },
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	committed := int64(0) // next offset to read, as the DB would hold it
	var seen []int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := fastClient(srv.URL).Run(ctx,
		func(context.Context) (int64, error) { return committed, nil },
		func(_ context.Context, m Message) error {
			seen = append(seen, m.Offset)
			committed = m.Offset + 1
			if m.Offset == 3 {
				cancel()
			}
			return nil
		})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	if fmt.Sprint(seen) != "[0 1 2 3]" {
		t.Fatalf("seen offsets = %v, want [0 1 2 3]", seen)
	}
	if fmt.Sprint(f.froms) != "[0 2]" {
		t.Fatalf("from params = %v, want [0 2] (resume must start at committed offset)", f.froms)
	}
	for _, a := range f.auths {
		if a != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", a)
		}
	}
}

func TestRunSkipsDuplicatesAfterReconnect(t *testing.T) {
	// A server that re-sends offset 1 must not cause a second handle call.
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) { sse(w, 0, 1, 1, 2) },
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	var seen []int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = fastClient(srv.URL).Run(ctx,
		func(context.Context) (int64, error) { return 0, nil },
		func(_ context.Context, m Message) error {
			seen = append(seen, m.Offset)
			if m.Offset == 2 {
				cancel()
			}
			return nil
		})
	if fmt.Sprint(seen) != "[0 1 2]" {
		t.Fatalf("seen = %v, want [0 1 2]", seen)
	}
}

func TestRunStopsOnUnauthorized(t *testing.T) {
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) { w.WriteHeader(http.StatusUnauthorized) },
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	err := fastClient(srv.URL).Run(context.Background(),
		func(context.Context) (int64, error) { return 0, nil },
		func(context.Context, Message) error { return nil })
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusUnauthorized {
		t.Fatalf("err = %v, want 401 StatusError", err)
	}
	if len(f.froms) != 1 {
		t.Fatalf("made %d connections, want 1 (401 must not be retried)", len(f.froms))
	}
}

func TestRunRetriesServiceUnavailableThenSucceeds(t *testing.T) {
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) },
		func(w http.ResponseWriter) { sse(w, 0) },
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	var seen []int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = fastClient(srv.URL).Run(ctx,
		func(context.Context) (int64, error) { return 0, nil },
		func(_ context.Context, m Message) error {
			seen = append(seen, m.Offset)
			cancel()
			return nil
		})
	if fmt.Sprint(seen) != "[0]" || len(f.froms) != 2 {
		t.Fatalf("seen=%v connections=%d", seen, len(f.froms))
	}
}

func TestHandlerErrorStopsRunWithoutSkipping(t *testing.T) {
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) { sse(w, 0, 1) },
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	boom := errors.New("db down")
	err := fastClient(srv.URL).Run(context.Background(),
		func(context.Context) (int64, error) { return 0, nil },
		func(context.Context, Message) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want db error", err)
	}
	if len(f.froms) != 1 {
		t.Fatalf("reconnected after handler failure (%d connections)", len(f.froms))
	}
}

func TestDecodeMessageKeepsRawDetection(t *testing.T) {
	m, err := decodeMessage(Event{Event: "detection", Data: `{"offset":7,"detection":{"satellite":"N"}}`})
	if err != nil {
		t.Fatal(err)
	}
	if m.Offset != 7 || m.Type != "detection" {
		t.Fatalf("msg = %+v", m)
	}
	var d map[string]string
	if err := json.Unmarshal(m.Detection, &d); err != nil || d["satellite"] != "N" {
		t.Fatalf("detection raw = %s err=%v", m.Detection, err)
	}
}

// Heartbeats carry an offset and occupy a slot in the sequence. A heartbeat at
// 0 followed by detections 1 and 2 must not look like a gap.
func TestHeartbeatTakesOffsetSlot(t *testing.T) {
	f := &fakeFeed{scripts: []func(http.ResponseWriter){
		func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "id: 0\nevent: heartbeat\ndata: {\"offset\":0}\n\n")
			sse(w, 1, 2)
		},
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	var seen []int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = fastClient(srv.URL).Run(ctx,
		func(context.Context) (int64, error) { return 0, nil },
		func(_ context.Context, m Message) error {
			seen = append(seen, m.Offset)
			if m.Offset == 2 {
				cancel()
			}
			return nil
		})
	if fmt.Sprint(seen) != "[1 2]" || len(f.froms) != 1 {
		t.Fatalf("seen=%v connections=%d, want [1 2] on one connection", seen, len(f.froms))
	}
}
