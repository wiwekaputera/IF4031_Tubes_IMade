package main

import "testing"

func TestStateStoreTracksOnlyIncreasingVersions(t *testing.T) {
	state, err := openState(t.TempDir() + "/notification.db")
	if err != nil {
		t.Fatal(err)
	}
	defer state.db.Close()

	if duplicate, err := state.isProcessed("hazard-1", 1); err != nil || duplicate {
		t.Fatalf("initial isProcessed() = (%v, %v), want (false, nil)", duplicate, err)
	}
	if err := state.markProcessed("hazard-1", 2); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int64{1, 2} {
		if duplicate, err := state.isProcessed("hazard-1", version); err != nil || !duplicate {
			t.Fatalf("isProcessed(version=%d) = (%v, %v), want (true, nil)", version, duplicate, err)
		}
	}
	if duplicate, err := state.isProcessed("hazard-1", 3); err != nil || duplicate {
		t.Fatalf("isProcessed(version=3) = (%v, %v), want (false, nil)", duplicate, err)
	}
}
