package process

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBasicClusterFormation tests that a cluster of nodes can form.
func TestBasicClusterFormation(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	// Start a 3-node cluster
	if err := cluster.StartCluster(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Verify all nodes are running
	allNodes := cluster.Nodes()
	runningNodes := cluster.RunningNodes()
	t.Logf("Total nodes: %d, Running nodes: %d", len(allNodes), len(runningNodes))
	for _, n := range allNodes {
		t.Logf("  Node %s: running=%v, addr=%s", n.ID, n.IsRunning(), n.ClientAddr)
		if !n.IsRunning() {
			output := n.Output()
			if len(output) > 0 {
				t.Logf("  Last 10 output lines from %s:", n.ID)
				start := len(output) - 10
				if start < 0 {
					start = 0
				}
				for _, line := range output[start:] {
					t.Logf("    %s", line)
				}
			}
		}
	}
	if len(runningNodes) != 3 {
		t.Fatalf("Expected 3 running nodes, got %d", len(runningNodes))
	}

	// Verify we can connect and do basic operations
	client := cluster.Client()
	defer client.Close()

	// Set a value
	key := []byte("test-key")
	value := []byte("test-value")
	if err := client.Set(key, value); err != nil {
		t.Fatalf("Failed to SET: %v", err)
	}

	// Get the value
	got, err := client.Get(key)
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("Expected %q, got %q", value, got)
	}

	t.Logf("Cluster formation test passed with 3 nodes")
}

// TestNodeCrashAndRecovery tests that a node can recover after being killed.
func TestNodeCrashAndRecovery(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "write", // Enable WAL for persistence
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	// Start a 3-node cluster
	if err := cluster.StartCluster(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	client := cluster.Client()
	defer client.Close()

	// Write some data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("crash-test-key-%d", i))
		value := []byte(fmt.Sprintf("crash-test-value-%d", i))
		if err := client.Set(key, value); err != nil {
			t.Fatalf("Failed to SET key %d: %v", i, err)
		}
	}

	t.Log("Data written, now crashing node-0...")

	// Kill node-0 (simulating crash)
	if err := cluster.KillNode("node-0"); err != nil {
		t.Fatalf("Failed to kill node-0: %v", err)
	}

	// Give cluster time to notice the crash
	time.Sleep(3 * time.Second)

	// Verify remaining nodes still work
	client2 := cluster.Client()
	defer client2.Close()

	// We should still be able to read data from remaining nodes
	successCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("crash-test-key-%d", i))
		_, err := client2.Get(key)
		if err == nil {
			successCount++
		}
	}

	t.Logf("After crash, recovered %d/%d keys from remaining nodes", successCount, numKeys)

	// Restart the crashed node
	t.Log("Restarting node-0...")
	if err := cluster.RestartNode("node-0"); err != nil {
		t.Fatalf("Failed to restart node-0: %v", err)
	}

	// Give it time to rejoin
	time.Sleep(5 * time.Second)

	// Verify all nodes are running again
	nodes := cluster.RunningNodes()
	if len(nodes) != 3 {
		t.Errorf("Expected 3 running nodes after restart, got %d", len(nodes))
	}

	t.Log("Node crash and recovery test passed")
}

// TestGracefulShutdown tests that nodes can shut down gracefully.
func TestGracefulShutdown(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	if err := cluster.StartCluster(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Write some data
	client := cluster.Client()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("shutdown-key-%d", i))
		value := []byte(fmt.Sprintf("shutdown-value-%d", i))
		client.Set(key, value)
	}
	client.Close()

	// Gracefully stop node-1
	t.Log("Stopping node-1 gracefully...")
	start := time.Now()
	if err := cluster.StopNode("node-1"); err != nil {
		t.Fatalf("Failed to stop node-1: %v", err)
	}
	elapsed := time.Since(start)

	// Shutdown should be relatively quick
	if elapsed > 10*time.Second {
		t.Errorf("Graceful shutdown took too long: %v", elapsed)
	}

	t.Logf("Graceful shutdown completed in %v", elapsed)

	// Verify node is stopped
	node := cluster.GetNode("node-1")
	if node.IsRunning() {
		t.Error("Node should not be running after stop")
	}
}

// TestMultipleRestarts tests that a node can restart multiple times.
func TestMultipleRestarts(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	if err := cluster.StartCluster(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	numRestarts := 5
	for i := 0; i < numRestarts; i++ {
		t.Logf("Restart cycle %d/%d", i+1, numRestarts)

		// Restart node-0
		if err := cluster.RestartNode("node-0"); err != nil {
			t.Fatalf("Failed to restart on cycle %d: %v", i+1, err)
		}

		// Verify it's running
		node := cluster.GetNode("node-0")
		if !node.IsRunning() {
			t.Fatalf("Node not running after restart %d", i+1)
		}

		// Do a quick operation
		client := node.Client()
		key := []byte(fmt.Sprintf("restart-test-%d", i))
		if err := client.Set(key, []byte("value")); err != nil {
			t.Fatalf("Failed to SET after restart %d: %v", i+1, err)
		}
		client.Close()
	}

	t.Logf("Successfully completed %d restarts", numRestarts)
}

// TestConcurrentOperationsDuringRestart tests that operations continue during node restarts.
func TestConcurrentOperationsDuringRestart(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	if err := cluster.StartCluster(3); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Start background operations
	stopCh := make(chan struct{})
	var successCount, errorCount int64
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := cluster.Client()
		defer client.Close()

		i := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				key := []byte(fmt.Sprintf("concurrent-key-%d", i))
				value := []byte(fmt.Sprintf("concurrent-value-%d", i))

				if err := client.Set(key, value); err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
				i++
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Let operations run for a bit
	time.Sleep(2 * time.Second)

	// Restart a node while operations are running
	t.Log("Restarting node-1 while operations are running...")
	if err := cluster.RestartNode("node-1"); err != nil {
		t.Logf("Warning: restart error (may be expected): %v", err)
	}

	// Let operations continue
	time.Sleep(3 * time.Second)

	close(stopCh)
	wg.Wait()

	success := atomic.LoadInt64(&successCount)
	errors := atomic.LoadInt64(&errorCount)

	t.Logf("Concurrent operations: %d success, %d errors", success, errors)

	// Most operations should succeed
	if success == 0 {
		t.Error("No successful operations during test")
	}
	if float64(errors)/float64(success+errors) > 0.3 {
		t.Errorf("Too many errors: %d/%d (%.1f%%)", errors, success+errors,
			float64(errors)*100/float64(success+errors))
	}
}

// TestSequentialNodeFailures tests the cluster's behavior when nodes fail sequentially.
func TestSequentialNodeFailures(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	if err := cluster.StartCluster(5); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	client := cluster.Client()
	defer client.Close()

	// Write initial data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("seq-fail-key-%d", i))
		value := []byte(fmt.Sprintf("seq-fail-value-%d", i))
		client.Set(key, value)
	}

	// Kill nodes one by one
	for nodeNum := 0; nodeNum < 3; nodeNum++ {
		nodeID := fmt.Sprintf("node-%d", nodeNum)
		t.Logf("Killing %s...", nodeID)

		if err := cluster.KillNode(nodeID); err != nil {
			t.Logf("Kill error: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Check remaining nodes
		running := cluster.RunningNodes()
		t.Logf("Running nodes after killing %s: %d", nodeID, len(running))

		// Try operations with remaining nodes
		if len(running) > 0 {
			testClient := cluster.Client()
			key := []byte(fmt.Sprintf("after-kill-%d", nodeNum))
			err := testClient.Set(key, []byte("test"))
			testClient.Close()

			if err != nil {
				t.Logf("Operation after killing %s failed: %v", nodeID, err)
			} else {
				t.Logf("Operations still working after killing %s", nodeID)
			}
		}
	}

	// Verify 2 nodes are still running
	running := cluster.RunningNodes()
	if len(running) != 2 {
		t.Errorf("Expected 2 running nodes, got %d", len(running))
	}
}

// TestDataDurabilityWithWAL tests that data persists across restarts when WAL is enabled.
func TestDataDurabilityWithWAL(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "fsync", // Full durability
		NumShards:  64,
		ReplFactor: 1, // Single replica for simpler testing
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	// Start single node
	node, err := cluster.StartNode("node-0", nil)
	if err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}

	t.Logf("Node started with data dir: %s", node.DataDir)

	client := node.Client()

	// Write data
	testData := map[string]string{
		"durable-key-1": "durable-value-1",
		"durable-key-2": "durable-value-2",
		"durable-key-3": "durable-value-3",
	}

	for k, v := range testData {
		if err := client.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to SET %s: %v", k, err)
		}
	}

	// Verify data is readable before crash
	for k, v := range testData {
		got, err := client.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to GET %s before crash: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("Data mismatch before crash for %s: expected %q, got %q", k, v, got)
		}
	}
	t.Log("Data verified before crash")

	client.Close()

	// List data directory contents before crash
	entries, _ := os.ReadDir(node.DataDir)
	t.Logf("Data dir contents before crash: %d files", len(entries))
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			t.Logf("  %s (%d bytes)", e.Name(), info.Size())
		} else {
			t.Logf("  %s", e.Name())
		}
	}

	// List WAL directory
	walDir := filepath.Join(node.DataDir, "wal")
	walEntries, _ := os.ReadDir(walDir)
	t.Logf("WAL dir contents: %d files", len(walEntries))
	for _, e := range walEntries {
		info, _ := e.Info()
		if info != nil {
			t.Logf("  %s (%d bytes)", e.Name(), info.Size())
		} else {
			t.Logf("  %s", e.Name())
		}
	}

	// Kill the node (hard crash)
	t.Log("Crashing node...")
	if err := node.Kill(); err != nil {
		t.Fatalf("Failed to kill node: %v", err)
	}

	// Wait a bit
	time.Sleep(1 * time.Second)

	// List data directory contents after crash
	entries, _ = os.ReadDir(node.DataDir)
	t.Logf("Data dir contents after crash: %d files", len(entries))
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			t.Logf("  %s (%d bytes)", e.Name(), info.Size())
		} else {
			t.Logf("  %s", e.Name())
		}
	}

	// Restart
	t.Log("Restarting node...")
	if err := node.Restart(); err != nil {
		t.Fatalf("Failed to restart node: %v", err)
	}

	// Check node output for recovery messages
	output := node.Output()
	t.Logf("Node output after restart (%d lines):", len(output))
	for _, line := range output {
		t.Logf("  %s", line)
	}

	// Verify data was recovered from WAL
	client = node.Client()
	defer client.Close()

	recovered := 0
	for k, v := range testData {
		got, err := client.Get([]byte(k))
		if err != nil {
			t.Logf("Failed to GET %s after restart: %v", k, err)
			continue
		}
		if string(got) != v {
			t.Errorf("Data mismatch for %s: expected %q, got %q", k, v, got)
			continue
		}
		recovered++
	}

	t.Logf("Recovered %d/%d keys after crash", recovered, len(testData))

	// With fsync mode, we should recover all data
	if recovered != len(testData) {
		t.Errorf("Expected to recover all %d keys, only got %d", len(testData), recovered)
	}
}

// TestStartupRace tests that multiple nodes can start concurrently without races.
func TestStartupRace(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	// Start seed node
	seedNode, err := cluster.StartNode("seed", nil)
	if err != nil {
		t.Fatalf("Failed to start seed: %v", err)
	}

	addrs := []string{seedNode.GossipAddr}

	// Start 5 nodes concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node-%d", idx)
			_, err := cluster.StartNode(nodeID, addrs)
			if err != nil {
				errCh <- fmt.Errorf("failed to start %s: %w", nodeID, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	var startErrors []error
	for err := range errCh {
		startErrors = append(startErrors, err)
	}

	if len(startErrors) > 0 {
		t.Errorf("Startup errors: %v", startErrors)
	}

	// Give cluster time to stabilize
	time.Sleep(3 * time.Second)

	// Verify all nodes are running
	running := cluster.RunningNodes()
	if len(running) != 6 { // seed + 5 nodes
		t.Errorf("Expected 6 running nodes, got %d", len(running))
	}

	t.Logf("Successfully started 6 nodes concurrently")
}

// TestPortReuseAfterCrash tests that ports can be reused after a node crashes.
func TestPortReuseAfterCrash(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	node, err := cluster.StartNode("node-0", nil)
	if err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}

	clientAddr := node.ClientAddr
	gossipAddr := node.GossipAddr

	// Crash the node
	t.Log("Crashing node...")
	if err := node.Kill(); err != nil {
		t.Fatalf("Failed to kill node: %v", err)
	}

	// Wait for OS to release ports
	time.Sleep(2 * time.Second)

	// Restart on same ports
	t.Log("Restarting on same ports...")
	if err := node.Restart(); err != nil {
		t.Fatalf("Failed to restart: %v", err)
	}

	// Verify ports are the same
	if node.ClientAddr != clientAddr {
		t.Errorf("Client address changed: %s -> %s", clientAddr, node.ClientAddr)
	}
	if node.GossipAddr != gossipAddr {
		t.Errorf("Gossip address changed: %s -> %s", gossipAddr, node.GossipAddr)
	}

	// Verify node is functional
	client := node.Client()
	defer client.Close()

	if err := client.Set([]byte("port-reuse-test"), []byte("value")); err != nil {
		t.Fatalf("Failed to SET after port reuse: %v", err)
	}

	t.Log("Port reuse test passed")
}

// TestDataDirectoryPersistence tests that data directories are handled correctly.
func TestDataDirectoryPersistence(t *testing.T) {
	binaryPath := buildBinary(t)

	cluster, err := NewCluster(ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "write",
		NumShards:  64,
		ReplFactor: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	node, err := cluster.StartNode("node-0", nil)
	if err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}

	// Verify data directory was created
	dataDir := node.DataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Errorf("Data directory not created: %s", dataDir)
	}

	client := node.Client()

	// Write some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("persist-key-%d", i))
		client.Set(key, []byte("value"))
	}
	client.Close()

	// Stop the node
	if err := node.Stop(); err != nil {
		t.Fatalf("Failed to stop: %v", err)
	}

	// Check that data files exist in data directory
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("Failed to read data dir: %v", err)
	}

	t.Logf("Data directory contents: %d entries", len(entries))
	for _, e := range entries {
		t.Logf("  - %s", e.Name())
	}
}

// buildBinary builds the aegis binary for testing.
func buildBinary(t *testing.T) string {
	t.Helper()

	// First check if binary already exists
	projectRoot := findProjectRoot(t)
	binaryPath := filepath.Join(projectRoot, "bin", "aegis")

	// Build the binary
	t.Log("Building aegis binary...")
	buildCmd := fmt.Sprintf("cd %s && go build -o bin/aegis ./cmd/aegis", projectRoot)
	cmd := exec.Command("sh", "-c", buildCmd)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build binary: %v\n%s", err, output)
	}

	return binaryPath
}

// findProjectRoot finds the project root directory.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	// Start from current directory and look for go.mod
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find project root (go.mod)")
		}
		dir = parent
	}
}
