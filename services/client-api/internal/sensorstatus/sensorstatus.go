// Package sensorstatus reads the coastal sensor status published by
// sensor-pantai-poller, so client-api can tell clients whether coastal data is
// current without ever calling the sensor itself.
package sensorstatus

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

// Getter reads the raw status value.
type Getter func() ([]byte, error)

// Reader caches the status for a short time; reads never block a request for
// longer than one KV lookup per refresh interval.
type Reader struct {
	get      Getter
	refresh  time.Duration
	maxAge   time.Duration
	now      func() time.Time
	mu       sync.Mutex
	cached   *hazard.SourceHealthMeta
	cachedAt time.Time
}

// New builds a reader. maxAge: a status older than this means the poller
// itself is not running, which is reported as DOWN.
func New(get Getter, refresh, maxAge time.Duration) *Reader {
	return &Reader{get: get, refresh: refresh, maxAge: maxAge, now: time.Now}
}

// FromJetStream reads the status key of the poller's KV bucket.
func FromJetStream(js nats.JetStreamContext) Getter {
	var (
		mu sync.Mutex
		kv nats.KeyValue
	)
	return func() ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if kv == nil {
			b, err := js.KeyValue(sensorpantai.StateBucket)
			if err != nil {
				return nil, err
			}
			kv = b
		}
		e, err := kv.Get(sensorpantai.KeyStatus)
		if err != nil {
			return nil, err
		}
		return e.Value(), nil
	}
}

// Meta returns the coastal source status for query responses.
func (r *Reader) Meta() *hazard.SourceHealthMeta {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if r.cached != nil && now.Sub(r.cachedAt) < r.refresh {
		return r.cached
	}
	r.cached, r.cachedAt = r.load(now), now
	return r.cached
}

func (r *Reader) load(now time.Time) *hazard.SourceHealthMeta {
	raw, err := r.get()
	if err != nil {
		return &hazard.SourceHealthMeta{Status: sensorpantai.StatusDown}
	}
	var st sensorpantai.Status
	if err := json.Unmarshal(raw, &st); err != nil {
		return &hazard.SourceHealthMeta{Status: sensorpantai.StatusDown}
	}
	meta := &hazard.SourceHealthMeta{Status: st.Status}
	if st.LastSyncedAt != nil {
		meta.LastSyncedAt = st.LastSyncedAt.UTC().Format(time.RFC3339)
	}
	if st.StaleSince != nil {
		s := st.StaleSince.UTC().Format(time.RFC3339)
		meta.StaleSince = &s
	}
	if now.Sub(st.UpdatedAt) > r.maxAge {
		meta.Status = sensorpantai.StatusDown
		if meta.StaleSince == nil {
			s := st.UpdatedAt.UTC().Format(time.RFC3339)
			meta.StaleSince = &s
		}
	}
	return meta
}
