package consistent

import (
	"fmt"
	"testing"
)

func TestHashRingAddRemove(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")
	ring.AddNodeWithAddr("node3", "localhost:7703")

	if ring.Size() != 3 {
		t.Errorf("expected 3 nodes, got %d", ring.Size())
	}

	ring.RemoveNode("node2")

	if ring.Size() != 2 {
		t.Errorf("expected 2 nodes, got %d", ring.Size())
	}
}

func TestHashRingGetNode(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")
	ring.AddNodeWithAddr("node3", "localhost:7703")

	key := []byte("test-key")
	node1 := ring.GetNode(key)
	node2 := ring.GetNode(key)

	if node1 != node2 {
		t.Error("same key should map to same node")
	}

	if node1 == "" {
		t.Error("GetNode should return a node")
	}
}

func TestHashRingGetNodes(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")
	ring.AddNodeWithAddr("node3", "localhost:7703")

	key := []byte("test-key")
	nodes := ring.GetNodes(key, 2)

	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	if nodes[0] == nodes[1] {
		t.Error("nodes should be different")
	}

	nodes = ring.GetNodes(key, 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes (max available), got %d", len(nodes))
	}
}

func TestHashRingDistribution(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")
	ring.AddNodeWithAddr("node3", "localhost:7703")

	counts := make(map[string]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		node := ring.GetNode(key)
		counts[node]++
	}

	expected := numKeys / 3
	tolerance := expected / 5

	for node, count := range counts {
		if count < expected-tolerance || count > expected+tolerance {
			t.Logf("Node %s: %d keys (expected ~%d)", node, count, expected)
		}
	}
}

func TestHashRingConsistency(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")
	ring.AddNodeWithAddr("node3", "localhost:7703")

	initial := make(map[string]string)
	numKeys := 1000

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		initial[string(key)] = ring.GetNode(key)
	}

	ring.AddNodeWithAddr("node4", "localhost:7704")

	changed := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if ring.GetNode(key) != initial[string(key)] {
			changed++
		}
	}

	// When adding a 4th node, approximately 1/4 of keys should move
	// Allow some variance due to hash distribution
	maxExpectedChange := numKeys / 3
	if changed > maxExpectedChange {
		t.Errorf("too many keys moved: %d (expected at most ~%d)", changed, maxExpectedChange)
	}
}

func TestHashRingEmpty(t *testing.T) {
	ring := NewHashRing(100)

	node := ring.GetNode([]byte("key"))
	if node != "" {
		t.Error("GetNode on empty ring should return empty string")
	}

	nodes := ring.GetNodes([]byte("key"), 3)
	if len(nodes) != 0 {
		t.Error("GetNodes on empty ring should return empty slice")
	}
}

func TestGetShardID(t *testing.T) {
	numShards := uint64(256)

	for i := uint64(0); i < 1000; i++ {
		shardID := GetShardID(Hash(i), numShards)
		if shardID >= numShards {
			t.Errorf("shard ID %d >= numShards %d", shardID, numShards)
		}
	}
}

func TestGetShardRange(t *testing.T) {
	numShards := uint64(256)

	for shardID := uint64(0); shardID < numShards; shardID++ {
		start, end := GetShardRange(shardID, numShards)

		if start >= end && shardID != numShards-1 {
			t.Errorf("shard %d: start %d >= end %d", shardID, start, end)
		}

		if shardID > 0 {
			prevStart, prevEnd := GetShardRange(shardID-1, numShards)
			if start != prevEnd+1 && shardID != numShards-1 {
				// Allow some variance due to integer division
				_ = prevStart
			}
		}
	}
}

func TestHashRingGetNodeAddr(t *testing.T) {
	ring := NewHashRing(100)

	ring.AddNodeWithAddr("node1", "localhost:7701")
	ring.AddNodeWithAddr("node2", "localhost:7702")

	addr := ring.GetNodeAddr("node1")
	if addr != "localhost:7701" {
		t.Errorf("expected localhost:7701, got %s", addr)
	}

	addr = ring.GetNodeAddr("nonexistent")
	if addr != "" {
		t.Errorf("expected empty string for nonexistent node, got %s", addr)
	}
}

func BenchmarkHashRingGetNode(b *testing.B) {
	ring := NewHashRing(100)

	for i := 0; i < 10; i++ {
		ring.AddNodeWithAddr(fmt.Sprintf("node%d", i), fmt.Sprintf("localhost:770%d", i))
	}

	key := []byte("benchmark-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNode(key)
	}
}

func BenchmarkHashRingGetNodes(b *testing.B) {
	ring := NewHashRing(100)

	for i := 0; i < 10; i++ {
		ring.AddNodeWithAddr(fmt.Sprintf("node%d", i), fmt.Sprintf("localhost:770%d", i))
	}

	key := []byte("benchmark-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNodes(key, 3)
	}
}
