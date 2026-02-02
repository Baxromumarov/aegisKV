//go:build docker
// +build docker

// Package docker_test provides tests against a Docker-based cluster.
// These tests require a running Docker Compose cluster:
//   docker compose up -d
//   go test ./tests/docker/... -tags docker

package docker_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/client"
)

// Docker cluster nodes (run with: docker compose up -d)
var dockerNodes = []string{
	"localhost:7700",
	"localhost:7710",
	"localhost:7720",
	"localhost:7730",
	"localhost:7740",
}

func createClient(addrs ...string) *client.Client {
	return client.New(client.Config{
		Addrs:       addrs,
		MaxConns:    5,
		ConnTimeout: 5 * time.Second,
		MaxRetries:  3,
	})
}

func TestDockerClusterConnectivity(t *testing.T) {
	t.Log("Testing Docker cluster connectivity...")

	connectedNodes := 0
	for _, addr := range dockerNodes {
		c := createClient(addr)
		// Try a simple operation to test connectivity
		key := fmt.Appendf(nil, "conn-test-%d", time.Now().UnixNano())
		err := c.Set(key, []byte("test"))
		if err != nil {
			t.Logf("  ❌ %s: %v", addr, err)
			continue
		}
		c.Close()
		t.Logf("  ✅ %s: connected", addr)
		connectedNodes++
	}

	if connectedNodes == 0 {
		t.Skip("No Docker nodes available. Run: docker compose up -d")
	}

	t.Logf("Connected to %d/%d nodes", connectedNodes, len(dockerNodes))
}

func TestDockerClusterBasicOperations(t *testing.T) {
	c := createClient(dockerNodes...)
	defer c.Close()

	t.Log("Testing basic SET/GET operations...")

	// Test SET
	testKey := []byte(fmt.Sprintf("test-key-%d", time.Now().UnixNano()))
	testValue := []byte("hello-docker-cluster")

	if err := c.Set(testKey, testValue); err != nil {
		t.Skipf("Docker cluster not available: %v. Run: docker compose up -d", err)
	}
	t.Logf("  ✅ SET %s = %s", testKey, testValue)

	// Test GET
	value, err := c.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	if string(value) != string(testValue) {
		t.Fatalf("Value mismatch: got %q, want %q", value, testValue)
	}
	t.Logf("  ✅ GET %s = %s", testKey, value)

	// Test DELETE
	if err := c.Delete(testKey); err != nil {
		t.Fatalf("Failed to DELETE: %v", err)
	}
	t.Logf("  ✅ DELETE %s", testKey)

	// Verify deletion
	_, err = c.Get(testKey)
	if err == nil {
		t.Fatal("Expected error for deleted key")
	}
	t.Log("  ✅ Verified key deleted")
}

func TestDockerClusterMultiNodeConsistency(t *testing.T) {
	// Connect to first node and write
	c1 := createClient(dockerNodes[0])
	defer c1.Close()

	testKey := []byte(fmt.Sprintf("consistency-test-%d", time.Now().UnixNano()))
	testValue := []byte("distributed-value")

	if err := c1.Set(testKey, testValue); err != nil {
		t.Skipf("Docker cluster not available: %v", err)
	}
	t.Logf("Wrote key %s to node1", testKey)

	// Wait for potential replication
	time.Sleep(500 * time.Millisecond)

	// Try to read from other nodes
	for i, addr := range dockerNodes[1:] {
		c := createClient(addr)

		value, err := c.Get(testKey)
		c.Close()

		if err != nil {
			// Key might be on a different shard
			t.Logf("  ⚠️ Node%d (%s): %v (key may be on different shard)", i+2, addr, err)
		} else if string(value) == string(testValue) {
			t.Logf("  ✅ Node%d (%s): found correct value", i+2, addr)
		} else {
			t.Logf("  ❌ Node%d (%s): value mismatch: %q", i+2, addr, value)
		}
	}
}

func TestDockerClusterConcurrentWrites(t *testing.T) {
	c := createClient(dockerNodes...)
	defer c.Close()

	numKeys := 100
	t.Logf("Writing %d keys concurrently...", numKeys)

	var wg sync.WaitGroup
	var success, failed int64
	start := time.Now()

	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("concurrent-key-%d", idx))
			value := []byte(fmt.Sprintf("value-%d", idx))
			if err := c.Set(key, value); err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			atomic.AddInt64(&success, 1)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Results: %d success, %d failed in %v", success, failed, elapsed)
	if elapsed.Seconds() > 0 {
		t.Logf("Throughput: %.0f ops/sec", float64(success)/elapsed.Seconds())
	}

	if success == 0 {
		t.Fatal("All concurrent writes failed")
	}
}

func TestDockerClusterMultiNodeParallel(t *testing.T) {
	t.Log("Testing parallel operations across all nodes...")

	var clients []*client.Client
	for _, addr := range dockerNodes {
		c := createClient(addr)
		// Test connectivity with a quick set
		key := []byte(fmt.Sprintf("test-conn-%d", time.Now().UnixNano()))
		if err := c.Set(key, []byte("test")); err != nil {
			c.Close()
			continue
		}
		clients = append(clients, c)
	}

	if len(clients) == 0 {
		t.Skip("No Docker nodes available")
	}

	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	t.Logf("Connected to %d nodes", len(clients))

	numOpsPerClient := 50
	var wg sync.WaitGroup
	var totalSuccess int64
	start := time.Now()

	for clientIdx, cli := range clients {
		wg.Add(1)
		go func(idx int, c *client.Client) {
			defer wg.Done()
			for i := 0; i < numOpsPerClient; i++ {
				key := []byte(fmt.Sprintf("parallel-node%d-key%d", idx, i))
				value := []byte(fmt.Sprintf("value-%d-%d", idx, i))
				if err := c.Set(key, value); err == nil {
					atomic.AddInt64(&totalSuccess, 1)
				}
			}
		}(clientIdx, cli)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := len(clients) * numOpsPerClient
	t.Logf("Total: %d/%d successful operations in %v", totalSuccess, totalOps, elapsed)
	if elapsed.Seconds() > 0 {
		t.Logf("Throughput: %.0f ops/sec", float64(totalSuccess)/elapsed.Seconds())
	}
}

func TestDockerClusterNodeFailure(t *testing.T) {
	t.Log("Testing cluster behavior with node connection failures...")

	// Write data to first node
	c := createClient(dockerNodes[0])

	testKey := []byte(fmt.Sprintf("resilience-test-%d", time.Now().UnixNano()))
	if err := c.Set(testKey, []byte("test-value")); err != nil {
		c.Close()
		t.Skipf("Docker cluster not available: %v", err)
	}
	c.Close()

	// Try to read from each available node
	successfulReads := 0
	for _, addr := range dockerNodes {
		c := createClient(addr)

		_, err := c.Get(testKey)
		c.Close()

		if err == nil {
			t.Logf("  Node %s: ✅ key found", addr)
			successfulReads++
		} else {
			t.Logf("  Node %s: ⚠️ %v", addr, err)
		}
	}

	t.Logf("Key readable from %d nodes", successfulReads)
}

func TestDockerClusterBenchmark(t *testing.T) {
	c := createClient(dockerNodes...)
	defer c.Close()

	// Test connectivity first
	testKey := []byte("bench-connectivity-test")
	if err := c.Set(testKey, []byte("test")); err != nil {
		t.Skipf("Docker cluster not available: %v", err)
	}

	t.Log("Running benchmark...")

	// Benchmark SET operations
	numOps := 1000
	start := time.Now()
	successSets := 0
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i))
		value := []byte(fmt.Sprintf("bench-value-%d", i))
		if c.Set(key, value) == nil {
			successSets++
		}
	}
	setElapsed := time.Since(start)
	t.Logf("SET: %d/%d ops in %v (%.0f ops/sec)", successSets, numOps, setElapsed, float64(successSets)/setElapsed.Seconds())

	// Benchmark GET operations
	start = time.Now()
	successGets := 0
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", rand.Intn(numOps)))
		if _, err := c.Get(key); err == nil {
			successGets++
		}
	}
	getElapsed := time.Since(start)
	t.Logf("GET: %d/%d ops in %v (%.0f ops/sec)", successGets, numOps, getElapsed, float64(successGets)/getElapsed.Seconds())
}

func TestDockerCluster10Nodes(t *testing.T) {
	// 10-node cluster addresses (run with: docker compose -f docker-compose.10nodes.yml up -d)
	tenNodeAddrs := []string{
		"localhost:7700", "localhost:7710", "localhost:7720", "localhost:7730", "localhost:7740",
		"localhost:7750", "localhost:7760", "localhost:7770", "localhost:7780", "localhost:7790",
	}

	t.Log("Testing 10-node cluster (if available)...")

	availableNodes := 0
	for _, addr := range tenNodeAddrs {
		c := createClient(addr)
		key := []byte(fmt.Sprintf("ten-node-test-%d", time.Now().UnixNano()))
		if err := c.Set(key, []byte("test")); err == nil {
			availableNodes++
		}
		c.Close()
	}

	if availableNodes < 6 {
		t.Skipf("Only %d nodes available, need at least 6 for 10-node test", availableNodes)
	}

	t.Logf("10-node cluster: %d nodes available", availableNodes)

	// Run concurrent workload
	c := createClient(tenNodeAddrs[:availableNodes]...)
	defer c.Close()

	var wg sync.WaitGroup
	var success int64
	numOps := 500
	start := time.Now()

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("10node-key-%d", idx))
			value := []byte(fmt.Sprintf("value-%d", idx))
			if c.Set(key, value) == nil {
				atomic.AddInt64(&success, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("10-node cluster: %d/%d ops in %v (%.0f ops/sec)", success, numOps, elapsed, float64(success)/elapsed.Seconds())
}
