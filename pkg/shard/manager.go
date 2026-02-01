package shard

import (
	"fmt"
	"sync"

	"github.com/baxromumarov/aegisKV/pkg/consistent"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

// Manager manages all shards for a node.
type Manager struct {
	mu                sync.RWMutex
	nodeID            string
	shards            map[uint64]*Shard
	numShards         uint64
	shardMaxBytes     int64
	ring              *consistent.HashRing
	replicationFactor int
}

// NewManager creates a new shard manager.
func NewManager(nodeID string, numShards uint64, shardMaxBytes int64, ring *consistent.HashRing, replicationFactor int) *Manager {
	return &Manager{
		nodeID:            nodeID,
		shards:            make(map[uint64]*Shard),
		numShards:         numShards,
		shardMaxBytes:     shardMaxBytes,
		ring:              ring,
		replicationFactor: replicationFactor,
	}
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

		nodes := m.ring.GetNodes(sampleKey, m.replicationFactor)

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
}

// GetShard returns the shard for a given key.
func (m *Manager) GetShard(key []byte) (*Shard, error) {
	hash := m.ring.GetKeyHash(key)
	shardID := consistent.GetShardID(hash, m.numShards)

	m.mu.RLock()
	shard, ok := m.shards[shardID]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("shard %d not found on this node", shardID)
	}
	return shard, nil
}

// GetShardByID returns a shard by its ID.
func (m *Manager) GetShardByID(shardID uint64) (*Shard, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shard, ok := m.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not found on this node", shardID)
	}
	return shard, nil
}

// GetPrimaryForKey returns the primary node for a given key.
func (m *Manager) GetPrimaryForKey(key []byte) string {
	nodes := m.ring.GetNodes(key, 1)
	if len(nodes) > 0 {
		return nodes[0]
	}
	return ""
}

// GetReplicasForKey returns all replica nodes for a given key.
func (m *Manager) GetReplicasForKey(key []byte) []string {
	return m.ring.GetNodes(key, m.replicationFactor)
}

// IsPrimaryFor returns true if this node is the primary for the given key.
func (m *Manager) IsPrimaryFor(key []byte) bool {
	return m.GetPrimaryForKey(key) == m.nodeID
}

// OwnedShards returns all shards this node owns.
func (m *Manager) OwnedShards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shards := make([]*Shard, 0, len(m.shards))
	for _, shard := range m.shards {
		shards = append(shards, shard)
	}
	return shards
}

// PrimaryShards returns shards where this node is the primary.
func (m *Manager) PrimaryShards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shards := make([]*Shard, 0)
	for _, shard := range m.shards {
		if shard.Primary == m.nodeID {
			shards = append(shards, shard)
		}
	}
	return shards
}

// FollowerShards returns shards where this node is a follower.
func (m *Manager) FollowerShards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shards := make([]*Shard, 0)
	for _, shard := range m.shards {
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
}

// RemoveShard removes a shard from the manager.
func (m *Manager) RemoveShard(shardID uint64) *Shard {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[shardID]
	if ok {
		delete(m.shards, shardID)
	}
	return shard
}

// Stats returns statistics for all managed shards.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ManagerStats{
		NodeID:      m.nodeID,
		TotalShards: len(m.shards),
		ShardStats:  make([]ShardStats, 0, len(m.shards)),
	}

	for _, shard := range m.shards {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ring = ring
}

// RecomputeOwnership recomputes shard ownership after ring changes.
func (m *Manager) RecomputeOwnership() (acquire []uint64, release []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for shardID := uint64(0); shardID < m.numShards; shardID++ {
		start, end := consistent.GetShardRange(shardID, m.numShards)
		sampleKey := make([]byte, 8)
		for i := 0; i < 8; i++ {
			sampleKey[i] = byte(start >> (56 - i*8))
		}

		nodes := m.ring.GetNodes(sampleKey, m.replicationFactor)

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
