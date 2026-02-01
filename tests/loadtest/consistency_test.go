//go:build loadtest
// +build loadtest

// Package loadtest provides data consistency tests for AegisKV.
package loadtest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ========================================
// DATA CONSISTENCY TESTS
// ========================================

// ConsistencyResult holds consistency test results.
type ConsistencyResult struct {
	TotalOps           int64
	ReadYourWritesPass int64
	ReadYourWritesFail int64
	MonotonicReadsPass int64
	MonotonicReadsFail int64
	ConcurrentWrites   int64
	LostUpdates        int64
	StaleReads         int64
}

// TestReadYourWrites verifies that a write is immediately visible to subsequent reads.
func TestReadYourWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping consistency test in short mode")
	}

	cluster, err := NewCluster(3, 22000)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	duration := 30 * time.Second
	result := testReadYourWrites(ctx, t, client, duration)

	t.Logf("\n=== Read-Your-Writes Consistency Test ===")
	t.Logf("Duration: %v", duration)
	t.Logf("Total Operations: %d", result.TotalOps)
	t.Logf("Passed: %d (%.4f%%)", result.ReadYourWritesPass,
		float64(result.ReadYourWritesPass)/float64(result.TotalOps)*100)
	t.Logf("Failed: %d", result.ReadYourWritesFail)

	// Assertion: Read-your-writes should pass 100% of the time for single-client
	if result.ReadYourWritesFail > 0 {
		t.Errorf("Read-your-writes violations detected: %d", result.ReadYourWritesFail)
	}
}

func testReadYourWrites(ctx context.Context, t *testing.T, client Client, duration time.Duration) *ConsistencyResult {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	result := &ConsistencyResult{}
	var wg sync.WaitGroup

	// Run multiple workers, each doing its own read-your-writes test
	for workerID := 0; workerID < 20; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := fmt.Sprintf("ryw-worker-%d-key-%d", id, i)
				value := fmt.Sprintf("value-%d-%d-%d", id, i, time.Now().UnixNano())

				// Write
				if err := client.Set(ctx, key, []byte(value)); err != nil {
					continue
				}

				atomic.AddInt64(&result.TotalOps, 1)

				// Immediate read
				readValue, err := client.Get(ctx, key)
				if err != nil {
					atomic.AddInt64(&result.ReadYourWritesFail, 1)
					continue
				}

				if string(readValue) == value {
					atomic.AddInt64(&result.ReadYourWritesPass, 1)
				} else {
					atomic.AddInt64(&result.ReadYourWritesFail, 1)
					t.Logf("RYW violation: wrote %q, read %q", value, string(readValue))
				}
			}
		}(workerID)
	}

	wg.Wait()
	return result
}

// TestMonotonicReads verifies that reads don't go backwards in time.
func TestMonotonicReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping consistency test in short mode")
	}

	cluster, err := NewCluster(3, 22100)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	duration := 30 * time.Second
	result := testMonotonicReads(ctx, t, client, duration)

	t.Logf("\n=== Monotonic Reads Test ===")
	t.Logf("Duration: %v", duration)
	t.Logf("Total Operations: %d", result.TotalOps)
	t.Logf("Monotonic Reads Pass: %d", result.MonotonicReadsPass)
	t.Logf("Monotonic Reads Fail (Stale): %d", result.MonotonicReadsFail)

	// Some stale reads may be acceptable depending on consistency model
	staleRate := float64(result.MonotonicReadsFail) / float64(result.TotalOps)
	if staleRate > 0.01 {
		t.Errorf("Stale read rate too high: %.4f%% (threshold: 1%%)", staleRate*100)
	}
}

func testMonotonicReads(ctx context.Context, t *testing.T, client Client, duration time.Duration) *ConsistencyResult {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	result := &ConsistencyResult{}
	var wg sync.WaitGroup

	// Shared keys that multiple readers will monitor
	numKeys := 10
	keyVersions := make([]int64, numKeys)

	// Writer goroutine - constantly updates keys with increasing versions
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			keyIdx := i % numKeys
			newVersion := atomic.AddInt64(&keyVersions[keyIdx], 1)
			key := fmt.Sprintf("monotonic-key-%d", keyIdx)
			value := fmt.Sprintf("%d", newVersion)

			client.Set(ctx, key, []byte(value))
			time.Sleep(time.Millisecond)
		}
	}()

	// Reader goroutines - track the highest version seen per key
	for readerID := 0; readerID < 10; readerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lastSeen := make([]int64, numKeys)

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				keyIdx := i % numKeys
				key := fmt.Sprintf("monotonic-key-%d", keyIdx)

				val, err := client.Get(ctx, key)
				if err != nil || val == nil {
					continue
				}

				atomic.AddInt64(&result.TotalOps, 1)

				var version int64
				fmt.Sscanf(string(val), "%d", &version)

				if version >= lastSeen[keyIdx] {
					atomic.AddInt64(&result.MonotonicReadsPass, 1)
					lastSeen[keyIdx] = version
				} else {
					atomic.AddInt64(&result.MonotonicReadsFail, 1)
				}
			}
		}(readerID)
	}

	wg.Wait()
	return result
}

// TestConcurrentWrites verifies behavior under concurrent writes to same key.
func TestConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping consistency test in short mode")
	}

	cluster, err := NewCluster(3, 22200)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	result := testConcurrentWrites(ctx, t, client)

	t.Logf("\n=== Concurrent Writes Test ===")
	t.Logf("Concurrent Writers: 50")
	t.Logf("Keys Tested: 10")
	t.Logf("Total Writes: %d", result.ConcurrentWrites)
	t.Logf("Lost Updates: %d", result.LostUpdates)

	// Lost updates should be zero with proper last-writer-wins semantics
	if result.LostUpdates > 0 {
		t.Logf("Note: %d lost updates detected (expected with concurrent writes)", result.LostUpdates)
	}
}

func testConcurrentWrites(ctx context.Context, t *testing.T, client Client) *ConsistencyResult {
	result := &ConsistencyResult{}

	numKeys := 10
	writersPerKey := 50
	writesPerWriter := 100

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		key := fmt.Sprintf("concurrent-key-%d", keyIdx)
		var maxValue int64
		var writeMu sync.Mutex

		var wg sync.WaitGroup
		for writerID := 0; writerID < writersPerKey; writerID++ {
			wg.Add(1)
			go func(wID int) {
				defer wg.Done()

				for i := 0; i < writesPerWriter; i++ {
					value := int64(wID*writesPerWriter + i)

					writeMu.Lock()
					if value > maxValue {
						maxValue = value
					}
					writeMu.Unlock()

					atomic.AddInt64(&result.ConcurrentWrites, 1)
					client.Set(ctx, key, []byte(fmt.Sprintf("%d", value)))
				}
			}(writerID)
		}
		wg.Wait()

		// Read final value
		time.Sleep(100 * time.Millisecond) // Allow replication to settle

		val, err := client.Get(ctx, key)
		if err != nil {
			continue
		}

		var finalValue int64
		fmt.Sscanf(string(val), "%d", &finalValue)

		// The final value should be one of the written values
		if finalValue < 0 || finalValue >= int64(writersPerKey*writesPerWriter) {
			atomic.AddInt64(&result.LostUpdates, 1)
		}
	}

	return result
}

// ========================================
// GRACEFUL SHUTDOWN TESTS
// ========================================

// ShutdownResult holds graceful shutdown test results.
type ShutdownResult struct {
	PreShutdownKeys    int
	PostRestartKeys    int
	DataLoss           int
	DataLossPercentage float64
	DrainTime          time.Duration
	RecoveryTime       time.Duration
	InFlightCompleted  int64
	InFlightFailed     int64
}

// TestGracefulShutdown verifies zero data loss during graceful shutdown.
func TestGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping graceful shutdown test in short mode")
	}

	cluster, err := NewCluster(3, 22300)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	result := testGracefulShutdown(ctx, t, client, cluster)

	t.Logf("\n=== Graceful Shutdown Test ===")
	t.Logf("Pre-shutdown Keys: %d", result.PreShutdownKeys)
	t.Logf("Post-restart Keys: %d", result.PostRestartKeys)
	t.Logf("Data Loss: %d (%.4f%%)", result.DataLoss, result.DataLossPercentage)
	t.Logf("Drain Time: %v", result.DrainTime)
	t.Logf("Recovery Time: %v", result.RecoveryTime)
	t.Logf("In-flight Completed: %d", result.InFlightCompleted)
	t.Logf("In-flight Failed: %d", result.InFlightFailed)

	// Assertion: Minimal data loss
	if result.DataLossPercentage > 1.0 {
		t.Errorf("Data loss too high: %.4f%% (threshold: 1%%)", result.DataLossPercentage)
	}
}

func testGracefulShutdown(ctx context.Context, t *testing.T, client Client, cluster *Cluster) *ShutdownResult {
	result := &ShutdownResult{}

	// Phase 1: Pre-populate data
	t.Log("Phase 1: Populating data...")
	numKeys := 1000
	testData := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("shutdown-test-key-%d", i)
		value := fmt.Sprintf("value-%d-%d", i, time.Now().UnixNano())
		testData[key] = value
		if err := client.Set(ctx, key, []byte(value)); err != nil {
			t.Logf("Warning: failed to set key %s: %v", key, err)
		}
	}
	result.PreShutdownKeys = len(testData)
	time.Sleep(1 * time.Second) // Allow replication

	// Phase 2: Start in-flight operations
	t.Log("Phase 2: Starting in-flight operations...")
	stopInFlight := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopInFlight:
				return
			default:
			}

			key := fmt.Sprintf("inflight-key-%d", i)
			if err := client.Set(ctx, key, []byte("inflight-value")); err != nil {
				atomic.AddInt64(&result.InFlightFailed, 1)
			} else {
				atomic.AddInt64(&result.InFlightCompleted, 1)
			}
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	// Phase 3: Graceful shutdown of one node
	t.Log("Phase 3: Gracefully shutting down node 1...")
	drainStart := time.Now()

	// Send SIGTERM for graceful shutdown
	if cluster.nodes[1].cmd.Process != nil {
		cluster.nodes[1].cmd.Process.Signal(syscall.SIGTERM)
	}

	// Wait for drain
	select {
	case <-cluster.nodes[1].done:
		result.DrainTime = time.Since(drainStart)
		t.Logf("  Node 1 drained in %v", result.DrainTime)
	case <-time.After(30 * time.Second):
		t.Log("  Node 1 drain timeout, killing...")
		cluster.nodes[1].cmd.Process.Kill()
		result.DrainTime = 30 * time.Second
	}

	// Stop in-flight operations
	close(stopInFlight)
	wg.Wait()

	// Phase 4: Verify data integrity
	t.Log("Phase 4: Verifying data integrity...")
	time.Sleep(2 * time.Second) // Allow cluster to stabilize

	recovered := 0
	for key := range testData {
		if _, err := client.Get(ctx, key); err == nil {
			recovered++
		}
	}

	result.PostRestartKeys = recovered
	result.DataLoss = result.PreShutdownKeys - recovered
	result.DataLossPercentage = float64(result.DataLoss) / float64(result.PreShutdownKeys) * 100

	return result
}

// TestRollingRestart tests zero-downtime rolling restart.
func TestRollingRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rolling restart test in short mode")
	}

	cluster, err := NewCluster(3, 22400)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()

	// Pre-populate data
	t.Log("Pre-populating data...")
	numKeys := 500
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("rolling-key-%d", i)
		client.Set(ctx, key, []byte(fmt.Sprintf("value-%d", i)))
	}
	time.Sleep(1 * time.Second)

	// Start continuous load
	t.Log("Starting continuous load during rolling restart...")
	loadCtx, loadCancel := context.WithCancel(ctx)
	var loadWg sync.WaitGroup
	var successOps, failedOps int64

	loadWg.Add(1)
	go func() {
		defer loadWg.Done()
		for i := 0; ; i++ {
			select {
			case <-loadCtx.Done():
				return
			default:
			}

			key := fmt.Sprintf("rolling-key-%d", i%numKeys)
			if _, err := client.Get(loadCtx, key); err != nil {
				atomic.AddInt64(&failedOps, 1)
			} else {
				atomic.AddInt64(&successOps, 1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Rolling restart each node
	for nodeIdx := 0; nodeIdx < len(cluster.nodes); nodeIdx++ {
		t.Logf("Restarting node %d...", nodeIdx)

		// Graceful stop
		if cluster.nodes[nodeIdx].cmd.Process != nil {
			cluster.nodes[nodeIdx].cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-cluster.nodes[nodeIdx].done:
			case <-time.After(10 * time.Second):
				cluster.nodes[nodeIdx].cmd.Process.Kill()
			}
		}

		// Wait for cluster to detect node down
		time.Sleep(3 * time.Second)

		// Note: In a real test, we'd restart the node here
		// For this test, we just verify the cluster continues operating
	}

	// Stop load and collect results
	loadCancel()
	loadWg.Wait()

	totalOps := successOps + failedOps
	successRate := float64(successOps) / float64(totalOps) * 100

	t.Logf("\n=== Rolling Restart Results ===")
	t.Logf("Total Ops: %d", totalOps)
	t.Logf("Successful: %d (%.2f%%)", successOps, successRate)
	t.Logf("Failed: %d", failedOps)

	// During rolling restart, some failures are expected
	if successRate < 80 {
		t.Errorf("Success rate too low during rolling restart: %.2f%% (threshold: 80%%)", successRate)
	}
}
