package metrics

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
)

func TestRefreshSkipsDBQueriesWhenNotLeader(t *testing.T) {
	var dlCalls, obCalls atomic.Int32
	isLeader := false

	snap := Snapshot{
		Sources: func() []Source { return nil },
		DeadLetters: func(context.Context) (map[string]int64, error) {
			dlCalls.Add(1)
			return map[string]int64{"parked": 4}, nil
		},
		OutboxBacklog: func(context.Context) (int64, error) {
			obCalls.Add(1)
			return 7, nil
		},
		IsLeader: func() bool { return isLeader },
	}

	refresh(context.Background(), snap, zerolog.Nop())
	if dlCalls.Load() != 0 || obCalls.Load() != 0 {
		t.Fatalf("expected 0 DB calls when not leader, got dl=%d ob=%d", dlCalls.Load(), obCalls.Load())
	}

	isLeader = true
	refresh(context.Background(), snap, zerolog.Nop())
	if dlCalls.Load() != 1 || obCalls.Load() != 1 {
		t.Fatalf("expected 1 DB call each when leader, got dl=%d ob=%d", dlCalls.Load(), obCalls.Load())
	}
}
