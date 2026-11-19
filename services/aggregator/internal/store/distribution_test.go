package store

import (
	"context"
	"testing"
)

type fakeDistributionQuerier struct {
	counts map[int]int64
	ranges []tableRangeInfo
	stores []storeStatusInfo
}

func (f *fakeDistributionQuerier) queryShardCounts(context.Context) (map[int]int64, error) {
	return f.counts, nil
}

func (f *fakeDistributionQuerier) queryTableRanges(context.Context) ([]tableRangeInfo, error) {
	return f.ranges, nil
}

func (f *fakeDistributionQuerier) queryStoreStatuses(context.Context) ([]storeStatusInfo, error) {
	return f.stores, nil
}

func TestPrimaryRangeShard(t *testing.T) {
	cases := []struct {
		start string
		end   string
		shard int
		ok    bool
	}{
		{"<before:/Table/106>", "…/1/1", 0, true},
		{"…/1/1", "…/1/2", 1, true},
		{"…/1/3", "…/1/4", 3, true},
		{"…/1/3/\"hz-abc\"", "…/1/4", 3, true},
		{"…/1/7", "…/2/1", 7, true},
		{"…/2/1", "…/2/2", 0, false},
		{"…/4/3", "…/4/4", 0, false},
	}
	for _, tc := range cases {
		got, ok := primaryRangeShard(tc.start, tc.end)
		if ok != tc.ok || got != tc.shard {
			t.Errorf("primaryRangeShard(%q, %q) = (%d, %v), want (%d, %v)",
				tc.start, tc.end, got, ok, tc.shard, tc.ok)
		}
	}
}

func TestGetDistributionWithQuerier(t *testing.T) {
	q := &fakeDistributionQuerier{
		counts: map[int]int64{
			0: 6200, 1: 6300, 2: 6150, 3: 6250,
			4: 6300, 5: 6200, 6: 6250, 7: 6350,
		},
		stores: []storeStatusInfo{
			{storeID: 1, nodeID: 1, locality: "node=canonical-store-0", logicalBytes: 4000000},
			{storeID: 2, nodeID: 2, locality: "node=canonical-store-1", logicalBytes: 4100000},
			{storeID: 3, nodeID: 3, locality: "node=canonical-store-2", logicalBytes: 3900000},
			{storeID: 4, nodeID: 4, locality: "node=canonical-store-3", logicalBytes: 4050000},
		},
		ranges: []tableRangeInfo{
			{
				startKey: "<before:/Table/106>", endKey: "…/1/1",
				leaseHolder: 1, leaseHolderLocality: "node=canonical-store-0",
				replicas: []int64{1, 2, 3}, replicaLocalities: []string{"node=canonical-store-0", "node=canonical-store-1", "node=canonical-store-2"},
				rangeSize: 1000,
			},
			{
				startKey: "…/1/1", endKey: "…/1/2",
				leaseHolder: 2, leaseHolderLocality: "node=canonical-store-1",
				replicas: []int64{2, 3, 4}, replicaLocalities: []string{"node=canonical-store-1", "node=canonical-store-2", "node=canonical-store-3"},
				rangeSize: 1000,
			},
			{
				startKey: "…/1/2", endKey: "…/1/3",
				leaseHolder: 3, leaseHolderLocality: "node=canonical-store-2",
				replicas: []int64{1, 3, 4}, replicaLocalities: []string{"node=canonical-store-0", "node=canonical-store-2", "node=canonical-store-3"},
				rangeSize: 1000,
			},
			{
				startKey: "…/1/3", endKey: "…/1/4",
				leaseHolder: 4, leaseHolderLocality: "node=canonical-store-3",
				replicas: []int64{1, 2, 4}, replicaLocalities: []string{"node=canonical-store-0", "node=canonical-store-1", "node=canonical-store-3"},
				rangeSize: 1000,
			},
		},
	}

	rep, err := getDistributionWithQuerier(context.Background(), q)
	if err != nil {
		t.Fatalf("getDistributionWithQuerier failed: %v", err)
	}
	if rep.TotalRows != 50000 {
		t.Fatalf("expected 50000 total rows, got %d", rep.TotalRows)
	}
	if rep.TotalRanges != 4 {
		t.Fatalf("expected 4 total ranges, got %d", rep.TotalRanges)
	}
	if len(rep.Shards) != 8 {
		t.Fatalf("expected 8 shards, got %d", len(rep.Shards))
	}
	if rep.Shards[0].Unit != "shard-0" || rep.Shards[0].RowCount != 6200 || rep.Shards[0].Leaseholder != "canonical-store-0" {
		t.Fatalf("unexpected shard-0: %+v", rep.Shards[0])
	}
	if len(rep.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(rep.Nodes))
	}
	for _, n := range rep.Nodes {
		if n.ReplicaCount != 3 || n.LeaseholderCount != 1 || n.PercentOfRangesOnNode != 75.0 {
			t.Fatalf("unexpected node distribution: %+v", n)
		}
	}
}
