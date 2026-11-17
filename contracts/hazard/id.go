package hazard

import (
	"crypto/sha256"
	"encoding/hex"
)

// ID returns a deterministic canonical hazard_id for (source, sourceRefID).
// Same input always produces the same ID so replay, failover, and catalog loads
// are idempotent by primary key.
func ID(source Source, sourceRefID string) string {
	sum := sha256.Sum256([]byte(string(source) + "|" + sourceRefID))
	return "hz-" + hex.EncodeToString(sum[:])[:16]
}
