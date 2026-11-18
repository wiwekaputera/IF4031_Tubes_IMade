// Package ingest turns satellite feed messages into fire events. Each message
// is handled in one commit: the detection changes, the changed fires, the
// outgoing hazard messages, and the next offset are written together, so a
// restart resumes exactly where the last commit ended and never loses or
// repeats a fire change.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
)

// LateWindow is how far behind the newest detection a new detection may
// arrive and still be clustered. Older ones are counted as late.
const LateWindow = 2 * time.Hour

// State is the durable processing state, owned by the store.
type State struct {
	NextOffset int64
	Watermark  time.Time // newest detection time seen so far
	Invalid    int64
	LowConf    int64
	Late       int64
	Unknown    int64 // update or retract for a key we never stored
	Duplicate  int64 // detection re-sent with a key we already have
}

// Counts are the per-message increments applied to State.
type Counts struct {
	Invalid, LowConf, Late, Unknown, Duplicate int64
}

// Edit changes or removes one stored detection, from an update or retract.
type Edit struct {
	Key        string
	Retract    bool
	FRP        *float64
	Confidence *string
}

// DeadLetter keeps a feed event that could not be processed, exactly as it
// arrived, so it can be inspected and replayed after a fix.
type DeadLetter struct {
	Offset int64
	Type   string
	Raw    []byte
	Reason string
}

// Commit is everything written for one message, in a single transaction.
type Commit struct {
	NextOffset int64
	Watermark  time.Time
	DeadLetter *DeadLetter     // set when the message was rejected
	Accepted   *fire.Detection // stored once by key; nil if none
	Edits      []Edit
	Upserts    []fire.Fire // fires that are new or changed
	Deletes    []string    // source_ref_id of fires that no longer exist
	Outbox     []Outgoing  // hazard messages describing Upserts and Deletes
	Counts     Counts
}

// Store is implemented by the Postgres layer (or a fake in tests).
type Store interface {
	State(ctx context.Context) (State, error)
	// Get returns a stored, unretracted detection by key, whether or not it is
	// low confidence.
	Get(ctx context.Context, key string) (fire.Detection, bool, error)
	// Seen reports whether a detection with this key was ever stored,
	// including one that was later retracted.
	Seen(ctx context.Context, key string) (bool, error)
	// Near returns stored detections that are not low confidence and may link
	// to d: within roughly the link distance and time. It may return extra
	// rows; the processor applies the exact rule.
	Near(ctx context.Context, d fire.Detection) ([]fire.Detection, error)
	// FiresContaining returns the stored fires that include any of the keys.
	FiresContaining(ctx context.Context, keys []string) ([]fire.Fire, error)
	// Detections returns the stored detections with the given keys.
	Detections(ctx context.Context, keys []string) ([]fire.Detection, error)
	// Commit applies c atomically.
	Commit(ctx context.Context, c Commit) error
}

// Processor handles feed messages one at a time, in order. State may be read
// from other goroutines, for example by monitoring.
type Processor struct {
	store Store
	mu    sync.RWMutex
	st    State
}

// New loads the durable state from the store.
func New(ctx context.Context, store Store) (*Processor, error) {
	st, err := store.State(ctx)
	if err != nil {
		return nil, err
	}
	return &Processor{store: store, st: st}, nil
}

// State returns the last committed state.
func (p *Processor) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.st
}

// NextOffset is the resume point for feed.Client.Run.
func (p *Processor) NextOffset(context.Context) (int64, error) {
	return p.State().NextOffset, nil
}

// Handle processes one feed message and commits it. A message that cannot be
// processed is dead-lettered and counted, so one bad row cannot stall the
// feed. A panic while handling a message is treated the same way instead of
// crashing the service into a restart loop on the same offset.
func (p *Processor) Handle(ctx context.Context, msg feed.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = p.reject(ctx, msg, fmt.Sprintf("panic: %v", r))
		}
	}()
	if msg.Bad != "" {
		return p.reject(ctx, msg, msg.Bad)
	}
	next := msg.Offset + 1
	switch msg.Type {
	case "detection":
		return p.handleDetection(ctx, msg, next)
	case "update", "retract":
		return p.handleEdit(ctx, msg, next)
	default:
		return p.commit(ctx, p.skip(next, Counts{}))
	}
}

// skip builds a commit that only advances the offset and bumps counters.
func (p *Processor) skip(next int64, c Counts) Commit {
	return Commit{NextOffset: next, Watermark: p.st.Watermark, Counts: c}
}

// reject dead-letters the raw message and advances past it.
func (p *Processor) reject(ctx context.Context, msg feed.Message, reason string) error {
	c := p.skip(msg.Offset+1, Counts{Invalid: 1})
	c.DeadLetter = &DeadLetter{Offset: msg.Offset, Type: msg.Type, Raw: msg.Data, Reason: reason}
	return p.commit(ctx, c)
}

func (p *Processor) handleDetection(ctx context.Context, msg feed.Message, next int64) error {
	var d fire.Detection
	if err := json.Unmarshal(msg.Detection, &d); err != nil {
		return p.reject(ctx, msg, "malformed_detection: "+err.Error())
	}
	if err := d.Validate(); err != nil {
		return p.reject(ctx, msg, "invalid_detection: "+err.Error())
	}
	if d.IsLowConfidence() {
		return p.commit(ctx, p.skip(next, Counts{LowConf: 1}))
	}

	// The feed re-sends detections. A key we already have is a duplicate: the
	// stored row, including any later update or retract, is the truth. Taking
	// the re-sent copy would undo those changes.
	seen, err := p.store.Seen(ctx, d.Key())
	if err != nil {
		return err
	}
	if seen {
		return p.commit(ctx, p.skip(next, Counts{Duplicate: 1}))
	}

	t, _ := d.Time() // Validate passed, so this cannot fail
	if !p.st.Watermark.IsZero() && t.Before(p.st.Watermark.Add(-LateWindow)) {
		return p.commit(ctx, p.skip(next, Counts{Late: 1}))
	}

	c := Commit{NextOffset: next, Watermark: p.st.Watermark}
	if t.After(c.Watermark) {
		c.Watermark = t
	}
	accepted := d
	c.Accepted = &accepted
	if err := p.reconcile(ctx, &c, "", &d); err != nil {
		return err // transient: the offset is not advanced, Run retries
	}
	return p.commit(ctx, c)
}

// editPayload is the part of an update event we use. A retract carries only
// key and reason.
type editPayload struct {
	Key     string `json:"key"`
	Changes struct {
		FRP        *float64 `json:"frp"`
		Confidence *string  `json:"confidence"`
	} `json:"changes"`
}

func (p *Processor) handleEdit(ctx context.Context, msg feed.Message, next int64) error {
	var e editPayload
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		return p.reject(ctx, msg, "malformed_"+msg.Type+": "+err.Error())
	}
	if e.Key == "" {
		return p.reject(ctx, msg, "missing_key")
	}
	d, found, err := p.store.Get(ctx, e.Key)
	if err != nil {
		return err
	}
	if !found {
		// The detection was never stored (low confidence, invalid, or late) or
		// was already retracted.
		return p.commit(ctx, p.skip(next, Counts{Unknown: 1}))
	}

	edit := Edit{Key: e.Key, Retract: msg.Type == "retract"}
	if !edit.Retract {
		edit.FRP = e.Changes.FRP
		edit.Confidence = e.Changes.Confidence
		if edit.FRP != nil {
			d.FRP = *edit.FRP
		}
		if edit.Confidence != nil {
			d.Confidence = *edit.Confidence
		}
	}

	c := Commit{NextOffset: next, Watermark: p.st.Watermark, Edits: []Edit{edit}}
	var after *fire.Detection
	if !edit.Retract && !d.IsLowConfidence() {
		after = &d
	}
	if err := p.reconcile(ctx, &c, e.Key, after); err != nil {
		return err
	}
	return p.commit(ctx, c)
}

// reconcile reclusters only what one detection change can affect, keeps fire
// IDs stable against the stored fires, and fills c with what changed: fires to
// upsert, fires to delete, and one hazard message for each. The message
// version is the feed position, which only grows, so later states always win
// at the aggregator.
//
// removed is the key of a detection leaving its old state (an update or a
// retract); added is the detection's new state when it takes part in fires
// (a new detection, or an update that keeps it eligible). Either may be empty.
//
// The affected set is exact: the fire that held the removed detection, plus
// the added detection, its direct neighbours, and the fires those neighbours
// belong to. A stored fire is a complete connected group, so nothing outside
// this set can link to it.
func (p *Processor) reconcile(ctx context.Context, c *Commit, removed string, added *fire.Detection) error {
	dets := map[string]fire.Detection{}
	var touched []string // keys whose fires must be loaded

	if removed != "" {
		touched = append(touched, removed)
	}
	if added != nil {
		near, err := p.store.Near(ctx, *added)
		if err != nil {
			return err
		}
		for _, n := range near {
			if n.Key() != added.Key() && fire.Linked(*added, n) {
				dets[n.Key()] = n
				touched = append(touched, n.Key())
			}
		}
		touched = append(touched, added.Key())
	}

	stored, err := p.store.FiresContaining(ctx, touched)
	if err != nil {
		return err
	}
	var members []string
	for _, f := range stored {
		members = append(members, f.DetectionKeys...)
	}
	if len(members) > 0 {
		ds, err := p.store.Detections(ctx, members)
		if err != nil {
			return err
		}
		for _, d := range ds {
			dets[d.Key()] = d
		}
	}

	delete(dets, removed)
	if added != nil {
		dets[added.Key()] = *added
	}
	list := make([]fire.Detection, 0, len(dets))
	for _, d := range dets {
		list = append(list, d)
	}

	next := assignIDs(clusterFires(list), stored)
	version := c.NextOffset

	old := make(map[string]fire.Fire, len(stored))
	for _, f := range stored {
		old[f.SourceRefID] = f
	}
	alive := make(map[string]bool, len(next))
	for _, f := range next {
		alive[f.SourceRefID] = true
		if prev, ok := old[f.SourceRefID]; ok && sameFire(prev, f) {
			continue
		}
		out, err := upsertMessage(f, version)
		if err != nil {
			return err
		}
		c.Upserts = append(c.Upserts, f)
		c.Outbox = append(c.Outbox, out)
	}

	var gone []string
	for id := range old {
		if !alive[id] {
			gone = append(gone, id)
		}
	}
	sort.Strings(gone)
	for _, id := range gone {
		out, err := deleteMessage(id, version)
		if err != nil {
			return err
		}
		c.Deletes = append(c.Deletes, id)
		c.Outbox = append(c.Outbox, out)
	}
	return nil
}

// clusterFires runs the fire rule and summarizes each resulting fire.
func clusterFires(ds []fire.Detection) []fire.Fire {
	comps, _ := fire.Cluster(ds)
	out := make([]fire.Fire, 0, len(comps))
	for _, c := range comps {
		out = append(out, fire.Summarize(c))
	}
	return out
}

// sameFire reports whether two summaries describe the same fire state.
func sameFire(a, b fire.Fire) bool {
	return a.SourceRefID == b.SourceRefID &&
		a.CenterLat == b.CenterLat && a.CenterLon == b.CenterLon &&
		a.OccurredAt.Equal(b.OccurredAt) && a.LastSeenAt.Equal(b.LastSeenAt) &&
		a.DetectionCount == b.DetectionCount &&
		a.MaxConfidence == b.MaxConfidence &&
		a.TotalFRPMW == b.TotalFRPMW &&
		a.Severity == b.Severity &&
		slices.Equal(a.Satellites, b.Satellites) &&
		slices.Equal(a.DetectionKeys, b.DetectionKeys)
}

func (p *Processor) commit(ctx context.Context, c Commit) error {
	if err := p.store.Commit(ctx, c); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.NextOffset = c.NextOffset
	p.st.Watermark = c.Watermark
	p.st.Invalid += c.Counts.Invalid
	p.st.LowConf += c.Counts.LowConf
	p.st.Late += c.Counts.Late
	p.st.Unknown += c.Counts.Unknown
	p.st.Duplicate += c.Counts.Duplicate
	return nil
}
