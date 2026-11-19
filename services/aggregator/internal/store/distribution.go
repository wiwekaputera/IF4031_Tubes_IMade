package store

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ShardDistribution describes row count and replica placement for one logical shard.
type ShardDistribution struct {
	Shard       int      `json:"shard"`
	Unit        string   `json:"unit"`
	RowCount    int64    `json:"row_count"`
	Leaseholder string   `json:"leaseholder"`
	Replicas    []string `json:"replicas"`
}

// NodeDistribution summarizes range/replica/byte distribution on one CockroachDB store/node.
type NodeDistribution struct {
	StoreID               int64   `json:"store_id"`
	NodeID                int64   `json:"node_id"`
	Node                  string  `json:"node"`
	ReplicaCount          int     `json:"replica_count"`
	LeaseholderCount      int     `json:"leaseholder_count"`
	ApproximateBytes      int64   `json:"approximate_bytes"`
	StoreLogicalBytes     int64   `json:"store_logical_bytes"`
	PercentOfRangesOnNode float64 `json:"percent_of_ranges_on_node"`
}

// DistributionReport is returned by GET /internal/admin/distribution.
type DistributionReport struct {
	TotalRows   int64               `json:"total_rows"`
	TotalRanges int                 `json:"total_ranges"`
	Shards      []ShardDistribution `json:"shards"`
	Nodes       []NodeDistribution  `json:"nodes"`
}

type tableRangeInfo struct {
	startKey            string
	endKey              string
	leaseHolder         int64
	leaseHolderLocality string
	replicas            []int64
	replicaLocalities   []string
	rangeSize           int64
}

type storeStatusInfo struct {
	storeID      int64
	nodeID       int64
	locality     string
	logicalBytes int64
}

type distributionQuerier interface {
	queryShardCounts(ctx context.Context) (map[int]int64, error)
	queryTableRanges(ctx context.Context) ([]tableRangeInfo, error)
	queryStoreStatuses(ctx context.Context) ([]storeStatusInfo, error)
}

type pgDistributionQuerier struct {
	repo *PostgresRepo
}

func (q *pgDistributionQuerier) queryShardCounts(ctx context.Context) (map[int]int64, error) {
	rows, err := q.repo.db.Query(ctx, `
		SELECT shard, count(*)
		FROM canonical_events
		WHERE deleted_at IS NULL
		GROUP BY shard
		ORDER BY shard
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[int]int64, NumShards)
	for rows.Next() {
		var shard int
		var cnt int64
		if err := rows.Scan(&shard, &cnt); err != nil {
			return nil, err
		}
		counts[shard] = cnt
	}
	return counts, rows.Err()
}

func (q *pgDistributionQuerier) queryTableRanges(ctx context.Context) ([]tableRangeInfo, error) {
	rows, err := q.repo.db.Query(ctx, `SHOW RANGES FROM TABLE canonical_events WITH DETAILS`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []tableRangeInfo
	for rows.Next() {
		var (
			r                   tableRangeInfo
			rangeID             int64
			rangeSizeMB         any
			votingReplicas      []int64
			nonVotingReplicas   []int64
			learnerReplicas     []int64
			splitEnforcedUntil  *time.Time
			spanStats           any
		)
		if err := rows.Scan(
			&r.startKey,
			&r.endKey,
			&rangeID,
			&rangeSizeMB,
			&r.leaseHolder,
			&r.leaseHolderLocality,
			&r.replicas,
			&r.replicaLocalities,
			&votingReplicas,
			&nonVotingReplicas,
			&learnerReplicas,
			&splitEnforcedUntil,
			&r.rangeSize,
			&spanStats,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (q *pgDistributionQuerier) queryStoreStatuses(ctx context.Context) ([]storeStatusInfo, error) {
	rows, err := q.repo.db.Query(ctx, `
		SELECT s.store_id, s.node_id, COALESCE(g.locality, '') AS locality, s.logical_bytes
		FROM crdb_internal.kv_store_status s
		LEFT JOIN crdb_internal.gossip_nodes g ON g.node_id = s.node_id
		ORDER BY s.store_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storeStatusInfo
	for rows.Next() {
		var s storeStatusInfo
		if err := rows.Scan(&s.storeID, &s.nodeID, &s.locality, &s.logicalBytes); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetDistribution queries per-shard row counts, CockroachDB range placement, and per-node storage stats.
func (r *PostgresRepo) GetDistribution(ctx context.Context) (DistributionReport, error) {
	ctx, cancel := withDefaultTimeout(ctx, 10*time.Second)
	defer cancel()
	return getDistributionWithQuerier(ctx, &pgDistributionQuerier{repo: r})
}

func primaryRangeShard(startKey, endKey string) (int, bool) {
	// Primary index ranges in CRDB SHOW RANGES look like:
	//   <before:/Table/106> -> …/1/1  (shard 0)
	//   …/1/1 -> …/1/2                (shard 1)
	//   ...
	//   …/1/7 -> …/2/1                (shard 7)
	// If a shard splits further, startKey or endKey carries …/1/<shard>/...
	const prefix = "…/1/"
	if strings.HasPrefix(startKey, prefix) {
		rest := strings.TrimPrefix(startKey, prefix)
		seg := strings.SplitN(rest, "/", 2)[0]
		if n, err := strconv.Atoi(seg); err == nil && n >= 0 && n < NumShards {
			return n, true
		}
	}
	if strings.HasPrefix(endKey, prefix) {
		rest := strings.TrimPrefix(endKey, prefix)
		seg := strings.SplitN(rest, "/", 2)[0]
		if n, err := strconv.Atoi(seg); err == nil && n >= 1 && n < NumShards {
			// A range ending at …/1/1 with startKey <before:...> belongs to shard 0.
			return n - 1, true
		}
	}
	return 0, false
}

func getDistributionWithQuerier(ctx context.Context, querier distributionQuerier) (DistributionReport, error) {
	counts, err := querier.queryShardCounts(ctx)
	if err != nil {
		return DistributionReport{}, err
	}
	ranges, err := querier.queryTableRanges(ctx)
	if err != nil {
		return DistributionReport{}, err
	}
	stores, err := querier.queryStoreStatuses(ctx)
	if err != nil {
		// Graceful fallback when VIEWCLUSTERMETADATA is not granted:
		// node stats are still derived from SHOW RANGES below.
		stores = nil
	}

	nodeByStore := make(map[int64]*NodeDistribution, len(stores))
	for _, s := range stores {
		nodeName := extractNodeLocality(s.locality)
		if nodeName == "unknown" {
			nodeName = "store-" + strconv.FormatInt(s.storeID, 10)
		}
		nodeByStore[s.storeID] = &NodeDistribution{
			StoreID:           s.storeID,
			NodeID:            s.nodeID,
			Node:              nodeName,
			StoreLogicalBytes: s.logicalBytes,
		}
	}

	shards := make([]ShardDistribution, NumShards)
	var totalRows int64
	for i := 0; i < NumShards; i++ {
		cnt := counts[i]
		totalRows += cnt
		shards[i] = ShardDistribution{
			Shard:    i,
			Unit:     UnitName(i),
			RowCount: cnt,
			Replicas: []string{},
		}
	}

	shardReplicaSets := make([]map[string]struct{}, NumShards)
	for i := range shardReplicaSets {
		shardReplicaSets[i] = make(map[string]struct{})
	}

	for _, r := range ranges {
		// Aggregate per-node range stats across all ranges of canonical_events.
		for i, storeID := range r.replicas {
			nd, ok := nodeByStore[storeID]
			if !ok {
				loc := ""
				if i < len(r.replicaLocalities) {
					loc = r.replicaLocalities[i]
				}
				nd = &NodeDistribution{
					StoreID: storeID,
					NodeID:  storeID,
					Node:    extractNodeLocality(loc),
				}
				nodeByStore[storeID] = nd
			}
			nd.ReplicaCount++
			nd.ApproximateBytes += r.rangeSize
		}
		if nd, ok := nodeByStore[r.leaseHolder]; ok {
			nd.LeaseholderCount++
		}

		// Map primary index ranges to shard-0..7.
		if shardIdx, ok := primaryRangeShard(r.startKey, r.endKey); ok {
			if shards[shardIdx].Leaseholder == "" && r.leaseHolderLocality != "" {
				shards[shardIdx].Leaseholder = extractNodeLocality(r.leaseHolderLocality)
			}
			for _, loc := range r.replicaLocalities {
				node := extractNodeLocality(loc)
				shardReplicaSets[shardIdx][node] = struct{}{}
			}
		}
	}

	for i := 0; i < NumShards; i++ {
		if len(shardReplicaSets[i]) > 0 {
			reps := make([]string, 0, len(shardReplicaSets[i]))
			for n := range shardReplicaSets[i] {
				reps = append(reps, n)
			}
			sort.Strings(reps)
			shards[i].Replicas = reps
		}
	}

	totalRanges := len(ranges)
	nodes := make([]NodeDistribution, 0, len(nodeByStore))
	for _, nd := range nodeByStore {
		if totalRanges > 0 {
			pct := float64(nd.ReplicaCount) * 100.0 / float64(totalRanges)
			nd.PercentOfRangesOnNode = math.Round(pct*100) / 100
		}
		nodes = append(nodes, *nd)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].StoreID < nodes[j].StoreID
	})

	return DistributionReport{
		TotalRows:   totalRows,
		TotalRanges: totalRanges,
		Shards:      shards,
		Nodes:       nodes,
	}, nil
}
