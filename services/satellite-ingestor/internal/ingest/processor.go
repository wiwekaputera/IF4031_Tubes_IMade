// Package ingest turns satellite feed messages into fire events. Each message
// is handled in one commit: the detection changes, the changed fires, the
// outgoing hazard messages, and the next offset are written together, so a
// restart resumes exactly where the last commit ended and never loses or
// repeats a fire change.
package ingest

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
)

const (
	// LateWindow is how far behind the newest detection a new detection may
	// arrive and still be clustered. Older ones are counted as late.
	LateWindow = 2 * time.Hour

	// ContextWindow is how far back from a detection the processor looks for
	// neighbours. A stored fire still active inside this window is always
	// reclustered whole, even if it started earlier.
	ContextWindow = 24 * time.Hour
)

// State is the durable processing state, owned by the store.
type State struct {
	NextOffset int64
	Watermark  time.Time // newest detection time seen so far
	Invalid    int64
	LowConf    int64
	Late       int64
	Unknown    int64 // update or retract for a key we never stored
}

// Counts are the per-message increments applied to State.
type Counts struct {
	Invalid, LowConf, Late, Unknown int64
}

// Edit changes or removes one stored detection, from an update or retract.
type Edit struct {
	Key        string
	Retract    bool
	FRP        *float64
	Confidence *string
}

// Commit is everything written for one message, in a single transaction.
type Commit struct {
	NextOffset int64
	Watermark  time.Time
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
	// Get returns a stored detection by key, whether or not it is low confidence.
	Get(ctx context.Context, key string) (fire.Detection, bool, error)
	// Window returns stored detections at or after since, excluding low confidence.
	Window(ctx context.Context, since time.Time) ([]fire.Detection, error)
	// Fires returns stored fires whose latest detection is at or after since.
	Fires(ctx context.Context, since time.Time) ([]fire.Fire, error)
	// Commit applies c atomically.
	Commit(ctx context.Context, c Commit) error
}

// Processor handles feed messages one at a time, in order.
type Processor struct {
	store Store
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
func (p *Processor) State() State { return p.st }

// NextOffset is the resume point for feed.Client.Run.
func (p *Processor) NextOffset(context.Context) (int64, error) {
	return p.st.NextOffset, nil
}

// Handle processes one feed message and commits it. Bad rows are counted and
// skipped, so one bad row cannot stall the feed.
func (p *Processor) Handle(ctx context.Context, msg feed.Message) error {
	next := msg.Offset + 1
	switch msg.Type {
	case "detection":
		return p.handleDetection(ctx, next, msg.Detection)
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

func (p *Processor) handleDetection(ctx context.Context, next int64, raw json.RawMessage) error {
	var d fire.Detection
	if err := json.Unmarshal(raw, &d); err != nil {
		return p.commit(ctx, p.skip(next, Counts{Invalid: 1}))
	}
	if err := d.Validate(); err != nil {
		return p.commit(ctx, p.skip(next, Counts{Invalid: 1}))
	}
	if d.IsLowConfidence() {
		return p.commit(ctx, p.skip(next, Counts{LowConf: 1}))
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
	withNew := func(window []fire.Detection) []fire.Detection {
		return append(window, d) // Cluster drops the duplicate on a replay
	}
	if err := p.reconcile(ctx, &c, t.Add(-ContextWindow), withNew); err != nil {
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
	if err := json.Unmarshal(msg.Data, &e); err != nil || e.Key == "" {
		return p.commit(ctx, p.skip(next, Counts{Invalid: 1}))
	}
	d, found, err := p.store.Get(ctx, e.Key)
	if err != nil {
		return err
	}
	if !found {
		// The detection was never stored: it was low confidence, invalid, or late.
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

	t, err := d.Time()
	if err != nil {
		return p.commit(ctx, p.skip(next, Counts{Invalid: 1}))
	}
	// Recluster with the edited detection in its new state.
	withEdit := func(window []fire.Detection) []fire.Detection {
		out := make([]fire.Detection, 0, len(window)+1)
		for _, w := range window {
			if w.Key() != e.Key {
				out = append(out, w)
			}
		}
		if !edit.Retract && !d.IsLowConfidence() {
			out = append(out, d)
		}
		return out
	}
	c := Commit{NextOffset: next, Watermark: p.st.Watermark, Edits: []Edit{edit}}
	if err := p.reconcile(ctx, &c, t.Add(-ContextWindow), withEdit); err != nil {
		return err
	}
	return p.commit(ctx, c)
}

// reconcile reclusters the detections around one change, keeps fire IDs
// stable against the stored fires, and fills c with only what changed: fires
// to upsert, fires to delete, and one hazard message for each. The message
// version is the feed position, which only grows, so later states always win
// at the aggregator.
//
// The window starts at since, then widens back to the start of every stored
// fire still active in it, so no fire is ever reclustered from a partial set
// of its detections. change applies the message's detection change to the
// window before clustering.
func (p *Processor) reconcile(ctx context.Context, c *Commit, since time.Time, change func([]fire.Detection) []fire.Detection) error {
	var stored []fire.Fire
	for {
		fs, err := p.store.Fires(ctx, since)
		if err != nil {
			return err
		}
		stored = fs
		earliest := since
		for _, f := range fs {
			if f.OccurredAt.Before(earliest) {
				earliest = f.OccurredAt
			}
		}
		if !earliest.Before(since) {
			break
		}
		since = earliest // a widened window can reach more active fires; look again
	}
	window, err := p.store.Window(ctx, since)
	if err != nil {
		return err
	}
	next := assignIDs(clusterFires(change(window)), stored)
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
	p.st.NextOffset = c.NextOffset
	p.st.Watermark = c.Watermark
	p.st.Invalid += c.Counts.Invalid
	p.st.LowConf += c.Counts.LowConf
	p.st.Late += c.Counts.Late
	p.st.Unknown += c.Counts.Unknown
	return nil
}
