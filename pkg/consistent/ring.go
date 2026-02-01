// Package consistent implements consistent hashing with virtual nodes.
package consistent

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/baxromumarov/aegisKV/pkg/fasthash"
)

// Hash is a 64-bit hash value.
type Hash uint64

// HashRing implements a consistent hash ring with virtual nodes.
// Uses atomic pointer swap for lock-free reads on the hot path.
type HashRing struct {
	mu           sync.RWMutex
	ring         []ringEntry
	nodeToVNodes map[string][]Hash
	vnodeToNode  map[Hash]string
	nodeAddrs    map[string]string
	replicas     int

	// Immutable snapshot for lock-free reads
	snapshot unsafe.Pointer // *ringSnapshot
}

// ringSnapshot is an immutable snapshot for lock-free reads.
type ringSnapshot struct {
	ring        []ringEntry
	vnodeToNode map[Hash]string
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
	hr := &HashRing{
		ring:         make([]ringEntry, 0),
		nodeToVNodes: make(map[string][]Hash),
		vnodeToNode:  make(map[Hash]string),
		nodeAddrs:    make(map[string]string),
		replicas:     replicas,
	}
	hr.updateSnapshot()
	return hr
}

// updateSnapshot creates and stores an immutable snapshot.
func (hr *HashRing) updateSnapshot() {
	snap := &ringSnapshot{
		ring:        hr.ring,
		vnodeToNode: hr.vnodeToNode,
	}
	atomic.StorePointer(&hr.snapshot, unsafe.Pointer(snap))
}

// getSnapshot returns the current immutable snapshot.
func (hr *HashRing) getSnapshot() *ringSnapshot {
	return (*ringSnapshot)(atomic.LoadPointer(&hr.snapshot))
}

// hashKey uses fast xxHash for key hashing.
func hashKey(key []byte) Hash {
	return Hash(fasthash.Sum64(key))
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
	hr.updateSnapshot() // Update immutable snapshot for lock-free reads
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
	hr.updateSnapshot() // Update immutable snapshot
}

func (hr *HashRing) sortRing() {
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i].hash < hr.ring[j].hash
	})
}

// GetNode returns the node responsible for a given key.
// Uses lock-free atomic snapshot read for maximum performance.
func (hr *HashRing) GetNode(key []byte) string {
	snap := hr.getSnapshot()
	if snap == nil || len(snap.ring) == 0 {
		return ""
	}

	hash := hashKey(key)
	idx := searchRing(snap.ring, hash)
	return snap.ring[idx].nodeID
}

// searchRing performs binary search on the ring snapshot.
func searchRing(ring []ringEntry, target Hash) int {
	idx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= target
	})
	if idx >= len(ring) {
		idx = 0
	}
	return idx
}

// GetNodes returns the primary and R-1 follower nodes for a key.
// Uses lock-free snapshot read for the common case.
func (hr *HashRing) GetNodes(key []byte, count int) []string {
	snap := hr.getSnapshot()
	if snap == nil || len(snap.ring) == 0 {
		return nil
	}

	hash := hashKey(key)
	idx := searchRing(snap.ring, hash)

	nodes := make([]string, 0, count)
	seen := make(map[string]bool, count)

	for i := 0; i < len(snap.ring) && len(nodes) < count; i++ {
		entry := snap.ring[(idx+i)%len(snap.ring)]
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

// GetKeyHash returns the hash value for a key (no lock needed - pure function).
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
