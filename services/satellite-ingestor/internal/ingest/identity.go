package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"

	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
)

// assignIDs gives each recomputed fire a stable source_ref_id by carrying over
// the identity of the stored fire it overlaps with.
//
// A fire that shares at least one detection with a stored fire inherits that
// fire's ID. This keeps the ID when the earliest detection is retracted, when
// a fire grows, or when it shrinks.
//
// When fires merge, the merged fire keeps the ID of the stored fire that
// started first; the other stored fire is deleted. When a fire splits, the
// part that started first keeps the ID and the other part gets a new one.
// A fire with no stored overlap keeps the ID derived from its earliest
// detection key, as fire.Summarize computed it.
//
// The result depends only on the inputs, so replaying the feed gives the same IDs.
func assignIDs(next, stored []fire.Fire) []fire.Fire {
	owner := make(map[string]string, len(stored)*2) // detection key -> stored fire ID
	byID := make(map[string]fire.Fire, len(stored))
	for _, f := range stored {
		byID[f.SourceRefID] = f
		for _, k := range f.DetectionKeys {
			owner[k] = f.SourceRefID
		}
	}

	out := make([]fire.Fire, len(next))
	copy(out, next)
	// Earliest fire claims first, so on a split the older part keeps the ID.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].OccurredAt.Before(out[j].OccurredAt)
		}
		return out[i].SourceRefID < out[j].SourceRefID
	})

	claimed := make(map[string]bool, len(out))
	for i := range out {
		var candidates []fire.Fire
		seen := map[string]bool{}
		for _, k := range out[i].DetectionKeys {
			id, ok := owner[k]
			if !ok || seen[id] || claimed[id] {
				continue
			}
			seen[id] = true
			candidates = append(candidates, byID[id])
		}
		if len(candidates) > 0 {
			sort.Slice(candidates, func(a, b int) bool {
				if !candidates[a].OccurredAt.Equal(candidates[b].OccurredAt) {
					return candidates[a].OccurredAt.Before(candidates[b].OccurredAt)
				}
				return candidates[a].SourceRefID < candidates[b].SourceRefID
			})
			out[i].SourceRefID = candidates[0].SourceRefID
		} else {
			out[i].SourceRefID = freshID(out[i].SourceRefID, claimed)
		}
		claimed[out[i].SourceRefID] = true
	}
	return out
}

// freshID returns id unless another fire in this round already holds it, in
// which case it derives a new one. Collisions need two fires to share an
// earliest detection key, so the loop practically never runs.
func freshID(id string, claimed map[string]bool) string {
	cand := id
	for n := 1; claimed[cand]; n++ {
		sum := sha256.Sum256([]byte(id + "#" + strconv.Itoa(n)))
		cand = hex.EncodeToString(sum[:16]) // same length as fire.Summarize IDs
	}
	return cand
}
