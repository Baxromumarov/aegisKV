package shard

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/baxromumarov/aegisKV/pkg/consistent"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

// shardMap is an immutable map of shards for lock-free reads.
type shardMap map[uint64]*Shard

// Manager manages all shards for a node.
// Uses atomic pointer swap for lock-free shard lookups on hot path.
type Manager struct {
	mu                sync.Mutex // Only for writes
	nodeID            string
	shards            map[uint64]*Shard
	numShards         uint64
	shardMaxBytes     int64
	ring              atomic.Pointer[consistent.HashRing]
	replicationFactor int

	// Immutable snapshot for lock-free reads
	shardSnapshot unsafe.Pointer // *shardMap
}

// NewManager creates a new shard manager.
func NewManager(nodeID string, numShards uint64, shardMaxBytes int64, ring *consistent.HashRing, replicationFactor int) *Manager {
	m := &Manager{
		nodeID:            nodeID,
		shards:            make(map[uint64]*Shard),
		numShards:         numShards,
		shardMaxBytes:     shardMaxBytes,
		replicationFactor: replicationFactor,
	}
	m.ring.Store(ring)
	m.updateSnapshot()
	return m
}

// updateSnapshot creates an immutable copy for lock-free reads.
func (m *Manager) updateSnapshot() {
	snap := make(shardMap, len(m.shards))
	for k, v := range m.shards {
		snap[k] = v
	}
	atomic.StorePointer(&m.shardSnapshot, unsafe.Pointer(&snap))
}

// getSnapshot returns the current immutable shard map.
func (m *Manager) getSnapshot() shardMap {
	ptr := atomic.LoadPointer(&m.shardSnapshot)
	if ptr == nil {
		return nil
	}
	return *(*shardMap)(ptr)
}

// InitializeShards creates all shards this node is responsible for.
func (m *Manager) InitializeShards() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for shardID := uint64(0); shardID < m.numShards; shardID++ {
		start, end := consistent.GetShardRange(shardID, m.numShards)

		sampleKey := make([]byte, 8)
		for i := 0; i < 8; i++ {
			sampleKey[i] = byte(start >> (56 - i*8))
		}

		ring := m.ring.Load()
		if ring == nil {
			continue
		}
		nodes := ring.GetNodes(sampleKey, m.replicationFactor)

		isPrimaryOrFollower := false
		isPrimary := false
		for i, nodeID := range nodes {
			if nodeID == m.nodeID {
				isPrimaryOrFollower = true
				if i == 0 {
					isPrimary = true
				}
				break
			}
		}

		if isPrimaryOrFollower {
			shard := NewShard(shardID, start, end, m.shardMaxBytes)
			if isPrimary {
				shard.SetPrimary(m.nodeID)
				shard.SetState(types.ShardStateActive)
			} else {
				shard.SetPrimary(nodes[0])
				shard.SetState(types.ShardStateActive)
			}
			if len(nodes) > 1 {
				shard.SetFollowers(nodes[1:])
			}
			m.shards[shardID] = shard
		}
	}
	m.updateSnapshot() // Update snapshot after initialization
}

// GetShard returns the shard for a given key.
// Uses lock-free atomic read for maximum performance on hot path.
func (m *Manager) GetShard(key []byte) (*Shard, error) {
	ring := m.ring.Load()
	if ring == nil {
		return nil, fmt.Errorf("ring not initialized")
	}
	hash := ring.GetKeyHash(key)
	shardID := consistent.GetShardID(hash, m.numShards)

	// Lock-free read from snapshot
	snap := m.getSnapshot()
	if snap != nil {
		if shard, ok := snap[shardID]; ok {
			return shard, nil
		}
	}

	return nil, fmt.Errorf("shard %d not found on this node", shardID)
}

// GetShardByID returns a shard by its ID.
// Uses lock-free atomic read for maximum performance.
func (m *Manager) GetShardByID(shardID uint64) (*Shard, error) {
	// Lock-free read from snapshot
	snap := m.getSnapshot()
	if snap != nil {
		if shard, ok := snap[shardID]; ok {
			return shard, nil
		}
	}

	return nil, fmt.Errorf("shard %d not found on this node", shardID)
}

// GetPrimaryForKey returns the primary node for a given key.
func (m *Manager) GetPrimaryForKey(key []byte) string {
	ring := m.ring.Load()
	if ring == nil {
		return ""
	}
	nodes := ring.GetNodes(key, 1)
	if len(nodes) > 0 {
		return nodes[0]
	}
	return ""
}

// GetReplicasForKey returns all replica nodes for a given key.
func (m *Manager) GetReplicasForKey(key []byte) []string {
	ring := m.ring.Load()
	if ring == nil {
		return nil
	}
	return ring.GetNodes(key, m.replicationFactor)
}

// IsPrimaryFor returns true if this node is the primary for the given key.
func (m *Manager) IsPrimaryFor(key []byte) bool {
	return m.GetPrimaryForKey(key) == m.nodeID
}

// OwnedShards returns all shards this node owns.
func (m *Manager) OwnedShards() []*Shard {
	snap := m.getSnapshot()
	if snap == nil {
		return nil
	}

	shards := make([]*Shard, 0, len(snap))
	for _, shard := range snap {
		shards = append(shards, shard)
	}
	return shards
}

// PrimaryShards returns shards where this node is the primary.
func (m *Manager) PrimaryShards() []*Shard {
	snap := m.getSnapshot()
	if snap == nil {
		return nil
	}

	shards := make([]*Shard, 0)
	for _, shard := range snap {
		if shard.Primary == m.nodeID {
			shards = append(shards, shard)
		}
	}
	return shards
}

// FollowerShards returns shards where this node is a follower.
func (m *Manager) FollowerShards() []*Shard {
	snap := m.getSnapshot()
	if snap == nil {
		return nil
	}

	shards := make([]*Shard, 0)
	for _, shard := range snap {
		if shard.Primary != m.nodeID {
			shards = append(shards, shard)
		}
	}
	return shards
}

// AddShard adds a shard to the manager.
func (m *Manager) AddShard(shard *Shard) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[shard.ID] = shard
	m.updateSnapshot()
}

// RemoveShard removes a shard from the manager.
func (m *Manager) RemoveShard(shardID uint64) *Shard {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[shardID]
	if ok {
		delete(m.shards, shardID)
		m.updateSnapshot()
	}
	return shard
}

// Stats returns statistics for all managed shards.
func (m *Manager) Stats() ManagerStats {
	snap := m.getSnapshot()
	if snap == nil {
		return ManagerStats{NodeID: m.nodeID}
	}

	stats := ManagerStats{
		NodeID:      m.nodeID,
		TotalShards: len(snap),
		ShardStats:  make([]ShardStats, 0, len(snap)),
	}

	for _, shard := range snap {
		shardStats := shard.Stats()
		stats.ShardStats = append(stats.ShardStats, shardStats)
		if shard.Primary == m.nodeID {
			stats.PrimaryShards++
		} else {
			stats.FollowerShards++
		}
	}

	return stats
}

// ManagerStats contains statistics for the shard manager.
type ManagerStats struct {
	NodeID         string
	TotalShards    int
	PrimaryShards  int
	FollowerShards int
	ShardStats     []ShardStats
}

// UpdateRing updates the hash ring reference.
func (m *Manager) UpdateRing(ring *consistent.HashRing) {
	m.ring.Store(ring)
}

// RecomputeOwnership recomputes shard ownership after ring changes.
func (m *Manager) RecomputeOwnership() (acquire []uint64, release []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ring := m.ring.Load()
	if ring == nil {
		return nil, nil
	}

	for shardID := uint64(0); shardID < m.numShards; shardID++ {
		start, end := consistent.GetShardRange(shardID, m.numShards)
		sampleKey := make([]byte, 8)
		for i := 0; i < 8; i++ {
			sampleKey[i] = byte(start >> (56 - i*8))
		}

		nodes := ring.GetNodes(sampleKey, m.replicationFactor)

		shouldOwn := false
		for _, nodeID := range nodes {
			if nodeID == m.nodeID {
				shouldOwn = true
				break
			}
		}

		_, hasIt := m.shards[shardID]

		if shouldOwn && !hasIt {
			acquire = append(acquire, shardID)
			shard := NewShard(shardID, start, end, m.shardMaxBytes)
			shard.SetState(types.ShardStateMigratingIn)
			if len(nodes) > 0 {
				shard.SetPrimary(nodes[0])
			}
			if len(nodes) > 1 {
				shard.SetFollowers(nodes[1:])
			}
			m.shards[shardID] = shard
		} else if !shouldOwn && hasIt {
			release = append(release, shardID)
			m.shards[shardID].SetState(types.ShardStateMigratingOut)
		} else if hasIt {
			shard := m.shards[shardID]
			if len(nodes) > 0 && shard.Primary != nodes[0] {
				shard.SetPrimary(nodes[0])
				if nodes[0] == m.nodeID {
					shard.IncrementTerm()
				}
			}
			if len(nodes) > 1 {
				shard.SetFollowers(nodes[1:])
			}
		}
	}

	m.updateSnapshot() // Update snapshot after recomputing
	return acquire, release
}

// Close closes all shards and releases resources.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, shard := range m.shards {
		shard.Close()
	}
}
