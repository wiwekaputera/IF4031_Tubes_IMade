package main

import (
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	bolt "go.etcd.io/bbolt"
)

func TestAuditStoreAppendsOncePerIncreasingEventVersion(t *testing.T) {
	state, err := openAuditStore(t.TempDir() + "/audit.db")
	if err != nil {
		t.Fatal(err)
	}
	defer state.db.Close()

	first := event.Envelope{EventID: "hazard-1", Version: 1, TraceID: "corr-1"}
	if inserted, err := state.appendOnce(first); err != nil || !inserted {
		t.Fatalf("first appendOnce() = (%v, %v), want (true, nil)", inserted, err)
	}
	if inserted, err := state.appendOnce(first); err != nil || inserted {
		t.Fatalf("duplicate appendOnce() = (%v, %v), want (false, nil)", inserted, err)
	}
	newVersion := first
	newVersion.Version = 2
	if inserted, err := state.appendOnce(newVersion); err != nil || !inserted {
		t.Fatalf("new version appendOnce() = (%v, %v), want (true, nil)", inserted, err)
	}

	var historyCount int
	if err := state.db.View(func(tx *bolt.Tx) error {
		historyCount = tx.Bucket(historyBucket).Stats().KeyN
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if historyCount != 2 {
		t.Fatalf("audit history count = %d, want 2", historyCount)
	}
}
