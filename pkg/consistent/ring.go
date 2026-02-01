// Package consistent implements consistent hashing with virtual nodes.
package consistent

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Hash is a 64-bit hash value.
type Hash uint64

// HashRing implements a consistent hash ring with virtual nodes.
type HashRing struct {
	mu           sync.RWMutex
	ring         []ringEntry
	nodeToVNodes map[string][]Hash
	vnodeToNode  map[Hash]string
	nodeAddrs    map[string]string
	replicas     int
}

type ringEntry struct {
	hash   Hash
	nodeID string
}

// NewHashRing creates a new consistent hash ring.
func NewHashRing(replicas int) *HashRing {
	if replicas < 1 {
		replicas = 100
	}
	return &HashRing{
		ring:         make([]ringEntry, 0),
		nodeToVNodes: make(map[string][]Hash),
		vnodeToNode:  make(map[Hash]string),
		nodeAddrs:    make(map[string]string),
		replicas:     replicas,
	}
}

func hashKey(key []byte) Hash {
	h := sha256.Sum256(key)
	return Hash(binary.BigEndian.Uint64(h[:8]))
}

// AddNode adds a node to the ring with virtual nodes.
func (hr *HashRing) AddNode(nodeID string) {
	hr.AddNodeWithAddr(nodeID, "")
}

// AddNodeWithAddr adds a node with an address to the ring.
func (hr *HashRing) AddNodeWithAddr(nodeID, addr string) {
	hr.AddNodeWithWeight(nodeID, addr, 1)
}

// AddNodeWithWeight adds a node with a weight multiplier for virtual nodes.
func (hr *HashRing) AddNodeWithWeight(nodeID, addr string, weight int) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodeToVNodes[nodeID]; exists {
		return
	}

	if addr != "" {
		hr.nodeAddrs[nodeID] = addr
	}

	numVNodes := hr.replicas * weight
	vnodes := make([]Hash, 0, numVNodes)

	for i := range numVNodes {
		vkey := fmt.Appendf(nil, "%s#%d", nodeID, i)
		hash := hashKey(vkey)
		vnodes = append(vnodes, hash)
		hr.vnodeToNode[hash] = nodeID
		hr.ring = append(hr.ring, ringEntry{hash: hash, nodeID: nodeID})
	}

	hr.nodeToVNodes[nodeID] = vnodes
	hr.sortRing()
}

// RemoveNode removes a node and all its virtual nodes from the ring.
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	vnodes, exists := hr.nodeToVNodes[nodeID]
	if !exists {
		return
	}

	for _, hash := range vnodes {
		delete(hr.vnodeToNode, hash)
	}

	newRing := make([]ringEntry, 0, len(hr.ring)-len(vnodes))
	for _, entry := range hr.ring {
		if entry.nodeID != nodeID {
			newRing = append(newRing, entry)
		}
	}
	hr.ring = newRing
	delete(hr.nodeToVNodes, nodeID)
	delete(hr.nodeAddrs, nodeID)
}

func (hr *HashRing) sortRing() {
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i].hash < hr.ring[j].hash
	})
}

// GetNode returns the node responsible for a given key.
func (hr *HashRing) GetNode(key []byte) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return ""
	}

	hash := hashKey(key)
	idx := hr.search(hash)
	return hr.ring[idx].nodeID
}

// GetNodes returns the primary and R-1 follower nodes for a key.
func (hr *HashRing) GetNodes(key []byte, count int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return nil
	}

	hash := hashKey(key)
	idx := hr.search(hash)

	nodes := make([]string, 0, count)
	seen := make(map[string]bool)

	for i := 0; i < len(hr.ring) && len(nodes) < count; i++ {
		entry := hr.ring[(idx+i)%len(hr.ring)]
		if !seen[entry.nodeID] {
			seen[entry.nodeID] = true
			nodes = append(nodes, entry.nodeID)
		}
	}

	return nodes
}

func (hr *HashRing) search(target Hash) int {
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i].hash >= target
	})
	if idx >= len(hr.ring) {
		idx = 0
	}
	return idx
}

// GetKeyHash returns the hash value for a key.
func (hr *HashRing) GetKeyHash(key []byte) Hash {
	return hashKey(key)
}

// NodeCount returns the number of physical nodes in the ring.
func (hr *HashRing) NodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodeToVNodes)
}

// Nodes returns all node IDs in the ring.
func (hr *HashRing) Nodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]string, 0, len(hr.nodeToVNodes))
	for nodeID := range hr.nodeToVNodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetNodeAddr returns the address for a node ID.
func (hr *HashRing) GetNodeAddr(nodeID string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.nodeAddrs[nodeID]
}

// Size returns the number of physical nodes in the ring.
func (hr *HashRing) Size() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodeToVNodes)
}

// GetShardID returns the shard ID for a given key hash.
func GetShardID(hash Hash, numShards uint64) uint64 {
	return uint64(hash) / (^uint64(0) / numShards)
}

// GetShardRange returns the hash range for a given shard ID.
func GetShardRange(shardID, numShards uint64) (start, end Hash) {
	rangeSize := ^uint64(0) / numShards
	start = Hash(shardID * rangeSize)
	if shardID == numShards-1 {
		end = Hash(^uint64(0))
	} else {
		end = Hash((shardID+1)*rangeSize - 1)
	}
	return
}
