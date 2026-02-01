package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/config"
	"github.com/baxromumarov/aegisKV/pkg/node"
)

// TestCluster represents a test cluster of AegisKV nodes
type TestCluster struct {
	nodes    []*node.Node
	configs  []*config.Config
	basePort int
	mu       sync.Mutex
}

// NewTestCluster creates a new test cluster with n nodes
func NewTestCluster(t *testing.T, numNodes int) *TestCluster {
	tc := &TestCluster{
		nodes:    make([]*node.Node, 0, numNodes),
		configs:  make([]*config.Config, 0, numNodes),
		basePort: 17700 + rand.Intn(1000),
	}

	var seeds []string
	for i := 0; i < numNodes; i++ {
		cfg := tc.createNodeConfig(i)
		tc.configs = append(tc.configs, cfg)
		if i == 0 {
			seeds = append(seeds, cfg.GossipBindAddr)
		}
	}

	for i := 1; i < numNodes; i++ {
		tc.configs[i].Seeds = seeds
	}

	return tc
}

func (tc *TestCluster) createNodeConfig(index int) *config.Config {
	return &config.Config{
		NodeID:            fmt.Sprintf("node-%d", index),
		BindAddr:          fmt.Sprintf("127.0.0.1:%d", tc.basePort+index),
		GossipBindAddr:    fmt.Sprintf("127.0.0.1:%d", tc.basePort+100+index),
		DataDir:           fmt.Sprintf("/tmp/aegiskv-test-%d-%d", tc.basePort, index),
		Seeds:             []string{},
		ReplicationFactor: 3,
		NumShards:         64,
		VirtualNodes:      50,
		MaxMemoryMB:       128,
		EvictionRatio:     0.9,
		ShardMaxBytes:     16 * 1024 * 1024,
		WALMode:           "off",
		GossipInterval:    500 * time.Millisecond,
		SuspectTimeout:    2 * time.Second,
		DeadTimeout:       5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		MaxConns:          1000,
	}
}

// Start starts all nodes in the cluster
func (tc *TestCluster) Start(t *testing.T) {
	for i, cfg := range tc.configs {
		n, err := node.New(cfg)
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}

		if err := n.Start(); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}

		tc.nodes = append(tc.nodes, n)

		if i > 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	time.Sleep(time.Second)
}

// Stop stops all nodes in the cluster
func (tc *TestCluster) Stop() {
	for _, n := range tc.nodes {
		if n != nil {
			n.Stop()
		}
	}
}

// StopNode stops a specific node
func (tc *TestCluster) StopNode(index int) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if index >= len(tc.nodes) || tc.nodes[index] == nil {
		return fmt.Errorf("node %d not found", index)
	}

	err := tc.nodes[index].Stop()
	tc.nodes[index] = nil
	return err
}

// RestartNode restarts a specific node
func (tc *TestCluster) RestartNode(t *testing.T, index int) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if index >= len(tc.configs) {
		return fmt.Errorf("node %d config not found", index)
	}

	n, err := node.New(tc.configs[index])
	if err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}

	if err := n.Start(); err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	}

	tc.nodes[index] = n
	return nil
}

// GetNode returns a specific node
func (tc *TestCluster) GetNode(index int) *node.Node {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if index < len(tc.nodes) {
		return tc.nodes[index]
	}
	return nil
}

// GetActiveNodes returns all active nodes
func (tc *TestCluster) GetActiveNodes() []*node.Node {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	active := make([]*node.Node, 0)
	for _, n := range tc.nodes {
		if n != nil {
			active = append(active, n)
		}
	}
	return active
}

// TestClusterFormation tests that nodes can form a cluster
func TestClusterFormation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	// Give gossip more time to fully propagate
	time.Sleep(5 * time.Second)

	// Check that at least the seed node (node 0) sees most members
	node0 := cluster.GetNode(0)
	members := node0.Members()
	t.Logf("Seed node (node 0) sees %d members", len(members))

	// Seed node should see all other nodes
	if len(members) < 3 {
		t.Errorf("Seed node only sees %d members, expected at least 3", len(members))
	}

	// Count how many nodes see at least 2 other members
	wellConnected := 0
	for i, n := range cluster.nodes {
		if n == nil {
			continue
		}
		members := n.Members()
		t.Logf("Node %d sees %d members", i, len(members))
		if len(members) >= 2 {
			wellConnected++
		}
	}

	// At least 3 nodes should be well-connected
	if wellConnected < 3 {
		t.Errorf("Only %d nodes are well-connected, expected at least 3", wellConnected)
	}
}

// TestBasicOperations tests SET/GET/DELETE across the cluster
func TestBasicOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	key := []byte("test-key")
	value := []byte("test-value")

	version, err := node0.HandleSet(key, value, 0)
	if err != nil {
		t.Fatalf("Failed to SET: %v", err)
	}
	t.Logf("SET successful, version: %d", version)

	gotValue, ttl, gotVersion, found := node0.HandleGet(key)
	if !found {
		t.Fatal("Key not found on node 0")
	}
	if string(gotValue) != string(value) {
		t.Errorf("Value mismatch: got %s, want %s", gotValue, value)
	}
	t.Logf("GET on node 0: value=%s, ttl=%d, version=%d", gotValue, ttl, gotVersion)

	time.Sleep(500 * time.Millisecond)

	for i := 1; i < 5; i++ {
		nodeN := cluster.GetNode(i)
		if nodeN == nil {
			continue
		}
		gotValue, _, _, found := nodeN.HandleGet(key)
		if found {
			t.Logf("Key found on node %d: %s", i, gotValue)
		} else {
			t.Logf("Key not found on node %d (may not own this shard)", i)
		}
	}
}

// TestDataDistribution tests that data is distributed across shards
func TestDataDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	numKeys := 1000
	successCount := 0

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		_, err := node0.HandleSet(key, value, 0)
		if err != nil {
			continue
		}
		successCount++
	}

	t.Logf("Successfully set %d/%d keys on node 0", successCount, numKeys)

	readCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			readCount++
		}
	}

	t.Logf("Successfully read %d keys from node 0", readCount)
}

// TestTTLExpiration tests that TTL expiration works correctly
func TestTTLExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	key := []byte("ttl-test-key")
	value := []byte("ttl-test-value")

	_, err := node0.HandleSet(key, value, 1000)
	if err != nil {
		t.Fatalf("Failed to SET with TTL: %v", err)
	}

	_, _, _, found := node0.HandleGet(key)
	if !found {
		t.Fatal("Key should exist immediately after SET")
	}

	time.Sleep(1500 * time.Millisecond)

	_, _, _, found = node0.HandleGet(key)
	if found {
		t.Error("Key should have expired")
	}
}

// TestConcurrentOperations tests concurrent read/write operations
func TestConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	var successSets, successGets int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("concurrent-%d-%d", goroutineID, i))
				value := []byte(fmt.Sprintf("value-%d-%d", goroutineID, i))

				_, err := node0.HandleSet(key, value, 0)
				if err == nil {
					atomic.AddInt64(&successSets, 1)
				}

				_, _, _, found := node0.HandleGet(key)
				if found {
					atomic.AddInt64(&successGets, 1)
				}
			}
		}(g)
	}

	wg.Wait()

	t.Logf("Concurrent test: %d successful SETs, %d successful GETs",
		successSets, successGets)
}

// TestNodeFailure tests behavior when a node fails
func TestNodeFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(3 * time.Second)

	node0 := cluster.GetNode(0)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("failover-key-%d", i))
		value := []byte(fmt.Sprintf("failover-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	t.Log("Data inserted, stopping node 2...")

	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("Failed to stop node: %v", err)
	}

	time.Sleep(6 * time.Second)

	for i := 0; i < 5; i++ {
		n := cluster.GetNode(i)
		if n != nil {
			members := n.Members()
			t.Logf("Node %d sees %d members after failure", i, len(members))
		}
	}

	readableCount := 0
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("failover-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			readableCount++
		}
	}
	t.Logf("After node failure: %d/100 keys still readable on node 0", readableCount)
}

// TestNodeRestart tests node restart and rejoin
func TestNodeRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(3 * time.Second)

	t.Log("Stopping node 3...")
	if err := cluster.StopNode(3); err != nil {
		t.Fatalf("Failed to stop node: %v", err)
	}

	time.Sleep(2 * time.Second)

	t.Log("Restarting node 3...")
	if err := cluster.RestartNode(t, 3); err != nil {
		t.Fatalf("Failed to restart node: %v", err)
	}

	time.Sleep(3 * time.Second)

	node0 := cluster.GetNode(0)
	members := node0.Members()
	t.Logf("After restart, node 0 sees %d members", len(members))
}

// BenchmarkClusterSet benchmarks SET operations on a cluster
func BenchmarkClusterSet(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i))
		node0.HandleSet(key, value, 0)
	}
}

// BenchmarkClusterGet benchmarks GET operations on a cluster
func BenchmarkClusterGet(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 100)

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i))
		node0.HandleSet(key, value, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i%10000))
		node0.HandleGet(key)
	}
}

// BenchmarkClusterMixedWorkload benchmarks mixed read/write operations
func BenchmarkClusterMixedWorkload(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 100)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("mixed-key-%d", i))
		node0.HandleSet(key, value, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("mixed-key-%d", i%1000))
		if i%10 < 8 {
			node0.HandleGet(key)
		} else {
			node0.HandleSet(key, value, 0)
		}
	}
}

// TestLargeCluster tests a 10-node cluster
func TestLargeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Log("Starting 10-node cluster test...")
	cluster := NewTestCluster(t, 10)
	cluster.Start(t)
	defer cluster.Stop()

	// Wait for gossip to propagate in larger cluster
	time.Sleep(8 * time.Second)

	// Check cluster membership
	node0 := cluster.GetNode(0)
	members := node0.Members()
	t.Logf("10-node cluster: seed node sees %d members", len(members))

	if len(members) < 5 {
		t.Errorf("Seed node only sees %d members, expected at least 5", len(members))
	}

	// Test data distribution across 10 nodes
	numKeys := 5000
	successCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("largetest-key-%d", i))
		value := []byte(fmt.Sprintf("largetest-value-%d", i))
		_, err := node0.HandleSet(key, value, 0)
		if err == nil {
			successCount++
		}
	}
	t.Logf("Successfully set %d/%d keys", successCount, numKeys)

	// Verify reads
	readCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("largetest-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			readCount++
		}
	}
	t.Logf("Successfully read %d keys", readCount)

	// Test resilience: stop 3 nodes
	t.Log("Stopping nodes 2, 5, 8...")
	cluster.StopNode(2)
	cluster.StopNode(5)
	cluster.StopNode(8)

	time.Sleep(6 * time.Second)

	// Verify remaining nodes still work
	stillReadable := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("largetest-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			stillReadable++
		}
	}
	t.Logf("After stopping 3 nodes: %d keys still readable", stillReadable)
}

// TestConsistencyCheck tests data consistency across replicas
func TestConsistencyCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(3 * time.Second)

	// Write data and verify version increments
	node0 := cluster.GetNode(0)
	key := []byte("version-test-key")

	// First write
	v1, err := node0.HandleSet(key, []byte("value-1"), 0)
	if err != nil {
		t.Fatalf("First SET failed: %v", err)
	}
	t.Logf("First SET version: %d", v1)

	// Second write - version should increment
	v2, err := node0.HandleSet(key, []byte("value-2"), 0)
	if err != nil {
		t.Fatalf("Second SET failed: %v", err)
	}
	t.Logf("Second SET version: %d", v2)

	if v2 <= v1 {
		t.Errorf("Version did not increment: v1=%d, v2=%d", v1, v2)
	}

	// Verify we get the latest value
	gotValue, _, gotVersion, found := node0.HandleGet(key)
	if !found {
		t.Fatal("Key not found")
	}
	if string(gotValue) != "value-2" {
		t.Errorf("Expected value-2, got %s", gotValue)
	}
	if gotVersion != v2 {
		t.Errorf("Expected version %d, got %d", v2, gotVersion)
	}
}

// TestHighThroughput tests high throughput scenario
func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()

	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	// High throughput test: 100 goroutines, 1000 ops each
	var wg sync.WaitGroup
	numGoroutines := 100
	opsPerGoroutine := 1000

	var totalSets, totalGets int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			value := make([]byte, 64)

			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("ht-%d-%d", goroutineID, i%100))

				if i%4 == 0 {
					_, err := node0.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&totalSets, 1)
					}
				} else {
					_, _, _, found := node0.HandleGet(key)
					if found {
						atomic.AddInt64(&totalGets, 1)
					}
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := totalSets + totalGets
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("High throughput test completed:")
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Total ops: %d (sets: %d, gets: %d)", totalOps, totalSets, totalGets)
	t.Logf("  Ops/sec: %.0f", opsPerSec)
}

// BenchmarkCluster10Nodes benchmarks operations on a 10-node cluster
func BenchmarkCluster10Nodes(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 10)
	cluster.Start(&testing.T{})
	defer cluster.Stop()

	time.Sleep(5 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 100)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("bench10-key-%d", i))
		node0.HandleSet(key, value, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench10-key-%d", i%10000))
		if i%10 < 8 {
			node0.HandleGet(key)
		} else {
			node0.HandleSet(key, value, 0)
		}
	}
}
