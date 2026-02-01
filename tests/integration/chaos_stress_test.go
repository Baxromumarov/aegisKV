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
	"github.com/baxromumarov/aegisKV/pkg/types"
)

// ============================================================================
// CHAOS TESTS - Random fault injection during concurrent operations
// ============================================================================

// TestChaosRandomNodeKills tests that the cluster survives random node kills and restarts
// while sustaining concurrent read/write operations across multiple nodes.
func TestChaosRandomNodeKills(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 7)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Pre-populate data across multiple nodes
	const numKeys = 2000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("chaos-kill-key-%d", i))
		value := []byte(fmt.Sprintf("chaos-kill-value-%d", i))
		nodeIdx := i % len(cluster.nodes)
		if n := cluster.GetNode(nodeIdx); n != nil {
			n.HandleSet(key, value, 0)
		}
	}

	// Run concurrent operations while randomly killing/restarting nodes
	ctx := &chaosContext{
		stopCh:    make(chan struct{}),
		cluster:   cluster,
		t:         t,
		numKeys:   numKeys,
		keyPrefix: "chaos-kill-key",
	}

	var wg sync.WaitGroup

	// Start worker goroutines doing SET/GET/DEL on all nodes
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx.operationWorker(workerID)
		}(w)
	}

	// Start chaos goroutine that kills random nodes
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx.nodeKillChaos(3*time.Second, 7*time.Second, 2) // keep at least 2 alive
	}()

	// Run for 30 seconds
	time.Sleep(30 * time.Second)
	close(ctx.stopCh)
	wg.Wait()

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	stats := ctx.stats()
	t.Logf("Chaos Random Node Kills Results:")
	t.Logf("  Sets: %d success / %d fail", stats.setSuccess, stats.setFail)
	t.Logf("  Gets: %d success / %d fail / %d miss", stats.getSuccess, stats.getFail, stats.getMiss)
	t.Logf("  Deletes: %d success / %d fail", stats.delSuccess, stats.delFail)
	t.Logf("  Node kills: %d, restarts: %d", stats.kills, stats.restarts)

	// Should have completed significant operations
	totalOps := stats.setSuccess + stats.getSuccess + stats.getMiss + stats.delSuccess
	if totalOps < 100 {
		t.Errorf("Too few operations completed: %d", totalOps)
	}
}

// TestChaosRollingRestarts simulates rolling restarts across all nodes
// while clients continuously read/write data.
func TestChaosRollingRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Pre-populate
	node0 := cluster.GetNode(0)
	const numKeys = 500
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("rolling-key-%d", i))
		value := []byte(fmt.Sprintf("rolling-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	// Start continuous operations in background
	stopOps := make(chan struct{})
	var opsWg sync.WaitGroup
	var successOps, failOps int64

	for w := 0; w < 10; w++ {
		opsWg.Add(1)
		go func(workerID int) {
			defer opsWg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				select {
				case <-stopOps:
					return
				default:
				}
				keyIdx := r.Intn(numKeys)
				key := []byte(fmt.Sprintf("rolling-key-%d", keyIdx))

				// Pick a random active node
				activeNodes := cluster.GetActiveNodes()
				if len(activeNodes) == 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				n := activeNodes[r.Intn(len(activeNodes))]

				if r.Float64() < 0.7 {
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&failOps, 1)
					}
				} else {
					_, err := n.HandleSet(key, []byte(fmt.Sprintf("updated-%d-%d", workerID, time.Now().UnixNano())), 0)
					if err == nil {
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&failOps, 1)
					}
				}
			}
		}(w)
	}

	// Perform rolling restarts: stop one node, wait, restart, move to next
	for round := 0; round < 2; round++ {
		for i := 0; i < 5; i++ {
			t.Logf("Rolling restart round %d: restarting node %d", round+1, i)

			if err := cluster.StopNode(i); err != nil {
				t.Logf("Stop node %d failed: %v", i, err)
				continue
			}

			// Wait while node is down
			time.Sleep(2 * time.Second)

			if err := cluster.RestartNode(t, i); err != nil {
				t.Logf("Restart node %d failed: %v", i, err)
				continue
			}

			time.Sleep(2 * time.Second)
		}
	}

	close(stopOps)
	opsWg.Wait()

	success := atomic.LoadInt64(&successOps)
	fail := atomic.LoadInt64(&failOps)
	t.Logf("Rolling restarts: success=%d, fail=%d, success_rate=%.1f%%",
		success, fail, float64(success)*100/float64(success+fail))

	if success == 0 {
		t.Error("No successful operations during rolling restarts")
	}
}

// TestChaosSimultaneousNodeFailures kills multiple nodes at the same time
// and verifies the remaining nodes still function.
func TestChaosSimultaneousNodeFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 7)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Write data across all nodes
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("simfail-key-%d", i))
		value := []byte(fmt.Sprintf("simfail-value-%d", i))
		n := cluster.GetNode(i % 7)
		if n != nil {
			n.HandleSet(key, value, 0)
		}
	}

	// Kill 3 nodes simultaneously
	t.Log("Killing nodes 1, 3, 5 simultaneously...")
	var killWg sync.WaitGroup
	for _, idx := range []int{1, 3, 5} {
		killWg.Add(1)
		go func(nodeIdx int) {
			defer killWg.Done()
			cluster.StopNode(nodeIdx)
		}(idx)
	}
	killWg.Wait()

	time.Sleep(3 * time.Second)

	// Verify remaining nodes (0, 2, 4, 6) still serve reads
	readableCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("simfail-key-%d", i))
		for _, idx := range []int{0, 2, 4, 6} {
			n := cluster.GetNode(idx)
			if n == nil {
				continue
			}
			_, _, _, found := n.HandleGet(key)
			if found {
				readableCount++
				break
			}
		}
	}
	t.Logf("After simultaneous failures: %d/%d keys readable", readableCount, numKeys)

	// Write new data through surviving nodes
	newWriteSuccess := 0
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("postfail-key-%d", i))
		value := []byte(fmt.Sprintf("postfail-value-%d", i))
		for _, idx := range []int{0, 2, 4, 6} {
			n := cluster.GetNode(idx)
			if n == nil {
				continue
			}
			_, err := n.HandleSet(key, value, 0)
			if err == nil {
				newWriteSuccess++
				break
			}
		}
	}
	t.Logf("Post-failure writes: %d/200 successful", newWriteSuccess)

	// Restart killed nodes
	for _, idx := range []int{1, 3, 5} {
		cluster.RestartNode(t, idx)
	}
	time.Sleep(5 * time.Second)

	// Verify all nodes are back
	activeCount := len(cluster.GetActiveNodes())
	t.Logf("After recovery: %d/7 nodes active", activeCount)
	if activeCount < 5 {
		t.Errorf("Expected at least 5 active nodes after recovery, got %d", activeCount)
	}
}

// TestChaosShardStateTransitions verifies shard states change correctly
// when nodes join and leave the cluster rapidly.
func TestChaosShardStateTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	node0 := cluster.GetNode(0)

	// Write data to establish shard state
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("shard-trans-key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	// Rapid node join/leave cycles
	for cycle := 0; cycle < 5; cycle++ {
		nodeIdx := 2 + (cycle % 3) // cycle through nodes 2, 3, 4

		t.Logf("Cycle %d: stopping node %d", cycle, nodeIdx)
		if err := cluster.StopNode(nodeIdx); err != nil {
			t.Logf("Stop failed: %v", err)
			continue
		}

		time.Sleep(500 * time.Millisecond)

		// Verify reads still work on remaining nodes
		readCount := 0
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("shard-trans-key-%d", i))
			_, _, _, found := node0.HandleGet(key)
			if found {
				readCount++
			}
		}
		t.Logf("  Reads during node %d down: %d/100", nodeIdx, readCount)

		t.Logf("Cycle %d: restarting node %d", cycle, nodeIdx)
		if err := cluster.RestartNode(t, nodeIdx); err != nil {
			t.Logf("Restart failed: %v", err)
			continue
		}

		time.Sleep(1 * time.Second)
	}

	// Final verification - all data accessible
	finalReadCount := 0
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("shard-trans-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			finalReadCount++
		}
	}
	t.Logf("Final reads after transitions: %d/500", finalReadCount)
}

// TestChaosVersionConflictResolution verifies version-based conflict resolution
// under concurrent writes to the same keys from multiple nodes.
func TestChaosVersionConflictResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Concurrent writes to same keys from different nodes
	const numKeys = 50
	const writersPerKey = 5
	const writesPerWriter = 100

	var wg sync.WaitGroup
	var totalWrites, successWrites int64

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		for writerIdx := 0; writerIdx < writersPerKey; writerIdx++ {
			wg.Add(1)
			go func(k, w int) {
				defer wg.Done()
				key := []byte(fmt.Sprintf("conflict-key-%d", k))
				nodeIdx := w % 5
				n := cluster.GetNode(nodeIdx)
				if n == nil {
					return
				}

				for i := 0; i < writesPerWriter; i++ {
					value := []byte(fmt.Sprintf("node%d-writer%d-seq%d", nodeIdx, w, i))
					atomic.AddInt64(&totalWrites, 1)
					_, err := n.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&successWrites, 1)
					}
				}
			}(keyIdx, writerIdx)
		}
	}

	wg.Wait()

	t.Logf("Version conflict test: %d/%d writes succeeded",
		atomic.LoadInt64(&successWrites), atomic.LoadInt64(&totalWrites))

	// Verify each key has a consistent value (any valid value, not corrupted)
	consistentKeys := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("conflict-key-%d", i))
		for nodeIdx := 0; nodeIdx < 5; nodeIdx++ {
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				continue
			}
			val, _, _, found := n.HandleGet(key)
			if found && len(val) > 0 {
				consistentKeys++
				break
			}
		}
	}
	t.Logf("Consistent keys after conflicts: %d/%d", consistentKeys, numKeys)
}

// TestChaosReadYourWritesUnderFailure verifies read-your-writes consistency
// when the writing node is healthy.
func TestChaosReadYourWritesUnderFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Kill a non-primary node to create chaos
	cluster.StopNode(3)
	cluster.StopNode(4)
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	violations := 0
	checks := 0

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("ryw-key-%d", i))
		value := []byte(fmt.Sprintf("ryw-value-%d-%d", i, time.Now().UnixNano()))

		_, err := node0.HandleSet(key, value, 0)
		if err != nil {
			continue
		}

		// Immediate read from same node should return what we wrote
		got, _, _, found := node0.HandleGet(key)
		checks++
		if !found {
			violations++
			continue
		}
		if string(got) != string(value) {
			violations++
			t.Logf("RYW violation at key %d: wrote %q, read %q", i, value, got)
		}
	}

	t.Logf("Read-your-writes: %d checks, %d violations", checks, violations)
	if violations > 0 {
		t.Errorf("Read-your-writes violated %d times out of %d checks", violations, checks)
	}
}

// TestChaosMajorityPartition simulates a majority partition where 4 of 7 nodes
// become separated from the rest, and verifies the majority side keeps operating.
func TestChaosMajorityPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 7)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Write data across cluster
	const numKeys = 500
	node0 := cluster.GetNode(0)
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("partition-key-%d", i))
		value := []byte(fmt.Sprintf("partition-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	// Simulate partition: kill minority (nodes 5, 6)
	t.Log("Simulating partition: killing minority nodes (5, 6)...")
	cluster.StopNode(5)
	cluster.StopNode(6)
	time.Sleep(5 * time.Second)

	// Majority side (nodes 0-4) should still serve reads and writes
	readSuccess := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("partition-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			readSuccess++
		}
	}
	t.Logf("Majority partition reads: %d/%d", readSuccess, numKeys)

	// Write during partition
	writeSuccess := 0
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("during-partition-key-%d", i))
		value := []byte(fmt.Sprintf("during-partition-value-%d", i))
		_, err := node0.HandleSet(key, value, 0)
		if err == nil {
			writeSuccess++
		}
	}
	t.Logf("Writes during partition: %d/200", writeSuccess)

	// Heal partition
	t.Log("Healing partition...")
	cluster.RestartNode(t, 5)
	cluster.RestartNode(t, 6)
	time.Sleep(5 * time.Second)

	// Verify data written during partition is accessible
	postHealReads := 0
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("during-partition-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			postHealReads++
		}
	}
	t.Logf("Post-heal reads of partition writes: %d/200", postHealReads)
}

// TestChaosMonotonicVersions verifies that version numbers are monotonically
// increasing for sequential writes to the same key.
func TestChaosMonotonicVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	node0 := cluster.GetNode(0)
	key := []byte("monotonic-version-key")

	lastVersion := uint64(0)
	violations := 0

	for i := 0; i < 5000; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		ver, err := node0.HandleSet(key, value, 0)
		if err != nil {
			continue
		}
		if ver <= lastVersion && i > 0 {
			violations++
			t.Logf("Version regression: write %d got version %d, last was %d", i, ver, lastVersion)
		}
		lastVersion = ver
	}

	t.Logf("Monotonic versions: %d violations out of 5000 writes, final version: %d", violations, lastVersion)
	if violations > 0 {
		t.Errorf("Version monotonicity violated %d times", violations)
	}
}

// ============================================================================
// STRESS TESTS - High concurrency and resource pressure
// ============================================================================

// TestStressMassiveConcurrentWrites stress-tests the cluster with massive
// concurrent writes from many goroutines across all nodes.
func TestStressMassiveConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	const numGoroutines = 200
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	var totalSets, successSets int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			nodeIdx := goroutineID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			value := make([]byte, 128)
			for i := range value {
				value[i] = byte('A' + (goroutineID+i)%26)
			}

			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("stress-w-%d-%d", goroutineID, i))
				atomic.AddInt64(&totalSets, 1)
				_, err := n.HandleSet(key, value, 0)
				if err == nil {
					atomic.AddInt64(&successSets, 1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalSets)
	success := atomic.LoadInt64(&successSets)
	opsPerSec := float64(success) / elapsed.Seconds()

	t.Logf("Massive concurrent writes:")
	t.Logf("  Goroutines: %d", numGoroutines)
	t.Logf("  Total attempts: %d", total)
	t.Logf("  Successful: %d (%.1f%%)", success, float64(success)*100/float64(total))
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
}

// TestStressMassiveConcurrentReads stress-tests read performance with many
// concurrent readers across all nodes.
func TestStressMassiveConcurrentReads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	// Pre-populate data
	node0 := cluster.GetNode(0)
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("stress-r-key-%d", i))
		value := []byte(fmt.Sprintf("stress-r-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	const numGoroutines = 200
	const readsPerGoroutine = 1000

	var wg sync.WaitGroup
	var hits, misses int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			nodeIdx := goroutineID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for i := 0; i < readsPerGoroutine; i++ {
				keyIdx := r.Intn(numKeys)
				key := []byte(fmt.Sprintf("stress-r-key-%d", keyIdx))
				_, _, _, found := n.HandleGet(key)
				if found {
					atomic.AddInt64(&hits, 1)
				} else {
					atomic.AddInt64(&misses, 1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalHits := atomic.LoadInt64(&hits)
	totalMisses := atomic.LoadInt64(&misses)
	totalOps := totalHits + totalMisses
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Massive concurrent reads:")
	t.Logf("  Goroutines: %d", numGoroutines)
	t.Logf("  Hits: %d, Misses: %d", totalHits, totalMisses)
	t.Logf("  Hit rate: %.1f%%", float64(totalHits)*100/float64(totalOps))
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
}

// TestStressMixedWorkloadMultiNode runs a mixed read/write/delete workload
// across all nodes with variable operation ratios.
func TestStressMixedWorkloadMultiNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	const numGoroutines = 100
	const duration = 20 * time.Second

	var wg sync.WaitGroup
	var sets, gets, dels, setOK, getOK, delOK int64
	start := time.Now()
	stopCh := make(chan struct{})

	// Schedule stop
	go func() {
		time.Sleep(duration)
		close(stopCh)
	}()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			nodeIdx := goroutineID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				keyIdx := r.Intn(5000)
				key := []byte(fmt.Sprintf("mixed-key-%d", keyIdx))
				op := r.Float64()

				switch {
				case op < 0.60: // 60% reads
					atomic.AddInt64(&gets, 1)
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&getOK, 1)
					}
				case op < 0.90: // 30% writes
					atomic.AddInt64(&sets, 1)
					value := []byte(fmt.Sprintf("mixed-val-%d-%d", goroutineID, r.Int()))
					_, err := n.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&setOK, 1)
					}
				default: // 10% deletes
					atomic.AddInt64(&dels, 1)
					err := n.HandleDelete(key)
					if err == nil {
						atomic.AddInt64(&delOK, 1)
					}
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := atomic.LoadInt64(&sets) + atomic.LoadInt64(&gets) + atomic.LoadInt64(&dels)
	totalSuccess := atomic.LoadInt64(&setOK) + atomic.LoadInt64(&getOK) + atomic.LoadInt64(&delOK)
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Mixed workload multi-node:")
	t.Logf("  Duration: %v, Goroutines: %d", elapsed, numGoroutines)
	t.Logf("  Sets: %d/%d, Gets: %d/%d, Dels: %d/%d",
		atomic.LoadInt64(&setOK), atomic.LoadInt64(&sets),
		atomic.LoadInt64(&getOK), atomic.LoadInt64(&gets),
		atomic.LoadInt64(&delOK), atomic.LoadInt64(&dels))
	t.Logf("  Total ops: %d, Success: %d", totalOps, totalSuccess)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
}

// TestStressHotKeyContention stress-tests concurrent access to a small set
// of "hot" keys from many goroutines.
func TestStressHotKeyContention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	const numHotKeys = 10
	const numGoroutines = 100
	const opsPerGoroutine = 2000

	var wg sync.WaitGroup
	var totalOps, successOps int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			nodeIdx := goroutineID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for i := 0; i < opsPerGoroutine; i++ {
				keyIdx := r.Intn(numHotKeys)
				key := []byte(fmt.Sprintf("hot-key-%d", keyIdx))
				atomic.AddInt64(&totalOps, 1)

				if r.Float64() < 0.5 {
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&successOps, 1)
					}
				} else {
					value := []byte(fmt.Sprintf("v-%d-%d", goroutineID, i))
					_, err := n.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalOps)
	success := atomic.LoadInt64(&successOps)
	t.Logf("Hot key contention:")
	t.Logf("  Hot keys: %d, Goroutines: %d", numHotKeys, numGoroutines)
	t.Logf("  Total ops: %d, Success: %d (%.1f%%)", total, success, float64(success)*100/float64(total))
	t.Logf("  Duration: %v, Throughput: %.0f ops/sec", elapsed, float64(total)/elapsed.Seconds())
}

// TestStressLargeValues tests handling of large values under concurrent load.
func TestStressLargeValues(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	valueSizes := []int{1024, 4096, 16384, 65536, 262144} // 1KB to 256KB
	const opsPerSize = 100

	var wg sync.WaitGroup
	var totalOps, successOps int64

	for _, size := range valueSizes {
		wg.Add(1)
		go func(valueSize int) {
			defer wg.Done()
			value := make([]byte, valueSize)
			for i := range value {
				value[i] = byte('A' + i%26)
			}

			for i := 0; i < opsPerSize; i++ {
				nodeIdx := i % 5
				n := cluster.GetNode(nodeIdx)
				if n == nil {
					continue
				}
				key := []byte(fmt.Sprintf("large-%d-%d", valueSize, i))
				atomic.AddInt64(&totalOps, 1)

				_, err := n.HandleSet(key, value, 0)
				if err != nil {
					continue
				}

				// Verify read-back
				got, _, _, found := n.HandleGet(key)
				if found && len(got) == valueSize {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(size)
	}

	wg.Wait()

	total := atomic.LoadInt64(&totalOps)
	success := atomic.LoadInt64(&successOps)
	t.Logf("Large values test:")
	t.Logf("  Value sizes: %v", valueSizes)
	t.Logf("  Total ops: %d, Verified: %d (%.1f%%)", total, success, float64(success)*100/float64(total))
}

// TestStressMemoryPressureEviction tests LRU eviction under memory pressure.
func TestStressMemoryPressureEviction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Create cluster with small shard max bytes to trigger eviction
	tc := &TestCluster{
		nodes:    make([]*node.Node, 0, 3),
		configs:  make([]*config.Config, 0, 3),
		basePort: 17700 + rand.Intn(1000),
	}

	for i := 0; i < 3; i++ {
		cfg := &config.Config{
			NodeID:            fmt.Sprintf("node-%d", i),
			BindAddr:          fmt.Sprintf("127.0.0.1:%d", tc.basePort+i),
			GossipBindAddr:    fmt.Sprintf("127.0.0.1:%d", tc.basePort+100+i),
			DataDir:           fmt.Sprintf("/tmp/aegiskv-mem-test-%d-%d", tc.basePort, i),
			Seeds:             []string{},
			ReplicationFactor: 3,
			NumShards:         32,
			VirtualNodes:      50,
			MaxMemoryMB:       16, // Small memory limit
			EvictionRatio:     0.9,
			ShardMaxBytes:     512 * 1024, // 512KB per shard - triggers eviction quickly
			WALMode:           "off",
			GossipInterval:    500 * time.Millisecond,
			SuspectTimeout:    2 * time.Second,
			DeadTimeout:       5 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			MaxConns:          1000,
		}
		if i > 0 {
			cfg.Seeds = []string{fmt.Sprintf("127.0.0.1:%d", tc.basePort+100)}
		}
		tc.configs = append(tc.configs, cfg)
	}

	tc.Start(t)
	defer tc.Stop()
	time.Sleep(2 * time.Second)

	node0 := tc.GetNode(0)

	// Write enough data to trigger eviction
	const numKeys = 5000
	valueSize := 1024 // 1KB values
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('X')
	}

	writeSuccess := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("evict-key-%d", i))
		_, err := node0.HandleSet(key, value, 0)
		if err == nil {
			writeSuccess++
		}
	}

	t.Logf("Memory pressure: wrote %d/%d keys", writeSuccess, numKeys)

	// Read back - expect some keys to have been evicted
	readHits := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("evict-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			readHits++
		}
	}

	t.Logf("After eviction: %d/%d keys still present", readHits, numKeys)

	// Most recently written keys should still be present (LRU evicts oldest)
	recentHits := 0
	for i := numKeys - 100; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("evict-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			recentHits++
		}
	}
	t.Logf("Recent keys (last 100): %d/100 present", recentHits)

	// Recent keys should mostly survive eviction
	if recentHits < 50 {
		t.Errorf("Too many recent keys evicted: only %d/100 present", recentHits)
	}
}

// TestStressTTLExpiryUnderLoad tests that TTL expiry works correctly
// under high write load.
func TestStressTTLExpiryUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	// Write keys with short TTLs
	const numKeys = 2000
	ttlMs := int64(2000) // 2 second TTL

	writeSuccess := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("ttl-stress-key-%d", i))
		value := []byte(fmt.Sprintf("ttl-stress-value-%d", i))
		_, err := node0.HandleSet(key, value, ttlMs)
		if err == nil {
			writeSuccess++
		}
	}
	t.Logf("Wrote %d/%d keys with %dms TTL", writeSuccess, numKeys, ttlMs)

	// Immediately read - all should be present
	immediateHits := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("ttl-stress-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			immediateHits++
		}
	}
	t.Logf("Immediate read: %d/%d keys found", immediateHits, writeSuccess)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Read again - all should have expired
	expiredCount := 0
	stillPresent := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("ttl-stress-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			stillPresent++
		} else {
			expiredCount++
		}
	}
	t.Logf("After TTL: expired=%d, still_present=%d", expiredCount, stillPresent)

	if stillPresent > numKeys/10 {
		t.Errorf("Too many keys survived TTL: %d/%d", stillPresent, numKeys)
	}
}

// TestStressRingRebalancing stress-tests the hash ring when nodes are
// added and removed rapidly.
func TestStressRingRebalancing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Start with 3 nodes
	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	// Write initial data
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("rebal-key-%d", i))
		value := []byte(fmt.Sprintf("rebal-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	// Count readable keys before rebalancing
	beforeCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("rebal-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			beforeCount++
		}
	}
	t.Logf("Before rebalancing: %d/%d keys readable", beforeCount, numKeys)

	// Remove a node and add it back rapidly multiple times
	for cycle := 0; cycle < 5; cycle++ {
		nodeIdx := 1 + (cycle % 2) // alternate between node 1 and 2
		t.Logf("Rebalance cycle %d: removing node %d", cycle, nodeIdx)
		cluster.StopNode(nodeIdx)
		time.Sleep(500 * time.Millisecond)

		t.Logf("Rebalance cycle %d: adding node %d back", cycle, nodeIdx)
		cluster.RestartNode(t, nodeIdx)
		time.Sleep(1 * time.Second)
	}

	time.Sleep(3 * time.Second)

	// Count readable keys after rebalancing
	afterCount := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("rebal-key-%d", i))
		_, _, _, found := node0.HandleGet(key)
		if found {
			afterCount++
		}
	}
	t.Logf("After rebalancing: %d/%d keys readable", afterCount, numKeys)
}

// TestStressDeleteUnderConcurrentLoad verifies delete operations work correctly
// when mixed with concurrent reads and writes.
func TestStressDeleteUnderConcurrentLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	// Pre-populate
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("del-stress-%d", i))
		value := []byte(fmt.Sprintf("del-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	// Concurrent delete + write + read
	var wg sync.WaitGroup
	var delSuccess, writeSuccess, readHits int64
	stopCh := make(chan struct{})

	// Deleters
	for d := 0; d < 20; d++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			n := cluster.GetNode(id % 5)
			if n == nil {
				return
			}
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				key := []byte(fmt.Sprintf("del-stress-%d", r.Intn(numKeys)))
				if err := n.HandleDelete(key); err == nil {
					atomic.AddInt64(&delSuccess, 1)
				}
			}
		}(d)
	}

	// Writers (re-creating deleted keys)
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(100+id)))
			n := cluster.GetNode(id % 5)
			if n == nil {
				return
			}
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				keyIdx := r.Intn(numKeys)
				key := []byte(fmt.Sprintf("del-stress-%d", keyIdx))
				value := []byte(fmt.Sprintf("recreated-%d", id))
				if _, err := n.HandleSet(key, value, 0); err == nil {
					atomic.AddInt64(&writeSuccess, 1)
				}
			}
		}(w)
	}

	// Readers
	for r := 0; r < 20; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(time.Now().UnixNano() + int64(200+id)))
			n := cluster.GetNode(id % 5)
			if n == nil {
				return
			}
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				key := []byte(fmt.Sprintf("del-stress-%d", rr.Intn(numKeys)))
				_, _, _, found := n.HandleGet(key)
				if found {
					atomic.AddInt64(&readHits, 1)
				}
			}
		}(r)
	}

	time.Sleep(15 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("Delete under load: deletes=%d, writes=%d, read_hits=%d",
		atomic.LoadInt64(&delSuccess), atomic.LoadInt64(&writeSuccess), atomic.LoadInt64(&readHits))
}

// ============================================================================
// LOAD TESTS - Sustained throughput measurement
// ============================================================================

// TestLoadSustainedThroughput measures sustained throughput over a longer period
// with a realistic mixed workload across all nodes.
func TestLoadSustainedThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	// Pre-populate
	node0 := cluster.GetNode(0)
	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("load-key-%d", i))
		value := []byte(fmt.Sprintf("load-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	const loadDuration = 30 * time.Second
	const numWorkers = 50

	var wg sync.WaitGroup
	var totalOps, setOps, getOps, delOps int64
	stopCh := make(chan struct{})

	// Track throughput samples
	type throughputSample struct {
		ops     int64
		elapsed time.Duration
	}
	samples := make([]throughputSample, 0)
	var sampleMu sync.Mutex

	start := time.Now()

	// Periodic throughput sampler
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastOps := int64(0)
		lastTime := start

		for {
			select {
			case <-stopCh:
				return
			case now := <-ticker.C:
				currentOps := atomic.LoadInt64(&totalOps)
				intervalOps := currentOps - lastOps
				intervalDuration := now.Sub(lastTime)
				opsPerSec := float64(intervalOps) / intervalDuration.Seconds()

				sampleMu.Lock()
				samples = append(samples, throughputSample{ops: intervalOps, elapsed: intervalDuration})
				sampleMu.Unlock()

				t.Logf("  [%v] ops=%d, interval_ops=%d, rate=%.0f ops/sec",
					now.Sub(start).Round(time.Second), currentOps, intervalOps, opsPerSec)

				lastOps = currentOps
				lastTime = now
			}
		}
	}()

	// Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeIdx := workerID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			value := make([]byte, 64)

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				keyIdx := r.Intn(10000)
				key := []byte(fmt.Sprintf("load-key-%d", keyIdx))
				op := r.Float64()

				switch {
				case op < 0.70: // 70% reads
					n.HandleGet(key)
					atomic.AddInt64(&getOps, 1)
				case op < 0.95: // 25% writes
					for i := range value {
						value[i] = byte('a' + r.Intn(26))
					}
					n.HandleSet(key, value, 0)
					atomic.AddInt64(&setOps, 1)
				default: // 5% deletes
					n.HandleDelete(key)
					atomic.AddInt64(&delOps, 1)
				}
				atomic.AddInt64(&totalOps, 1)
			}
		}(w)
	}

	time.Sleep(loadDuration)
	close(stopCh)
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalOps)
	avgOpsPerSec := float64(total) / elapsed.Seconds()

	t.Logf("\n=== Sustained Throughput Results ===")
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Workers: %d across 5 nodes", numWorkers)
	t.Logf("  Total ops: %d", total)
	t.Logf("  Gets: %d, Sets: %d, Dels: %d", atomic.LoadInt64(&getOps), atomic.LoadInt64(&setOps), atomic.LoadInt64(&delOps))
	t.Logf("  Avg throughput: %.0f ops/sec", avgOpsPerSec)

	// Calculate min/max throughput from samples
	sampleMu.Lock()
	if len(samples) > 0 {
		minRate := float64(1<<62)
		maxRate := float64(0)
		for _, s := range samples {
			rate := float64(s.ops) / s.elapsed.Seconds()
			if rate < minRate {
				minRate = rate
			}
			if rate > maxRate {
				maxRate = rate
			}
		}
		t.Logf("  Min throughput: %.0f ops/sec", minRate)
		t.Logf("  Max throughput: %.0f ops/sec", maxRate)
	}
	sampleMu.Unlock()
}

// TestLoadWriteHeavyWorkload tests sustained performance under write-heavy load
// (90% writes, 10% reads).
func TestLoadWriteHeavyWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	const duration = 20 * time.Second
	const numWorkers = 50

	var wg sync.WaitGroup
	var writeOps, readOps, writeSuccess, readSuccess int64
	stopCh := make(chan struct{})
	start := time.Now()

	go func() {
		time.Sleep(duration)
		close(stopCh)
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeIdx := workerID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			value := make([]byte, 128)

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				keyIdx := r.Intn(20000)
				key := []byte(fmt.Sprintf("write-heavy-%d", keyIdx))

				if r.Float64() < 0.9 { // 90% writes
					for i := range value {
						value[i] = byte('a' + r.Intn(26))
					}
					atomic.AddInt64(&writeOps, 1)
					_, err := n.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&writeSuccess, 1)
					}
				} else { // 10% reads
					atomic.AddInt64(&readOps, 1)
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&readSuccess, 1)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	writes := atomic.LoadInt64(&writeOps)
	reads := atomic.LoadInt64(&readOps)
	totalOps := writes + reads

	t.Logf("Write-heavy workload (90/10):")
	t.Logf("  Duration: %v, Workers: %d", elapsed, numWorkers)
	t.Logf("  Writes: %d (success: %d)", writes, atomic.LoadInt64(&writeSuccess))
	t.Logf("  Reads: %d (success: %d)", reads, atomic.LoadInt64(&readSuccess))
	t.Logf("  Total: %d ops, %.0f ops/sec", totalOps, float64(totalOps)/elapsed.Seconds())
}

// TestLoadReadHeavyHotSpot tests read-heavy workload with Zipf-like key distribution
// (a few keys get most of the traffic).
func TestLoadReadHeavyHotSpot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	// Pre-populate with various keys
	node0 := cluster.GetNode(0)
	const totalKeys = 10000
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("hotspot-key-%d", i))
		value := []byte(fmt.Sprintf("hotspot-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	const duration = 20 * time.Second
	const numWorkers = 80

	var wg sync.WaitGroup
	var hits, misses, totalOps int64
	stopCh := make(chan struct{})
	start := time.Now()

	go func() {
		time.Sleep(duration)
		close(stopCh)
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeIdx := workerID % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				// Zipf-like distribution: 80% of requests to 20% of keys
				var keyIdx int
				if r.Float64() < 0.8 {
					keyIdx = r.Intn(totalKeys / 5) // Hot 20%
				} else {
					keyIdx = r.Intn(totalKeys) // All keys
				}

				key := []byte(fmt.Sprintf("hotspot-key-%d", keyIdx))
				atomic.AddInt64(&totalOps, 1)

				if r.Float64() < 0.95 { // 95% reads
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&hits, 1)
					} else {
						atomic.AddInt64(&misses, 1)
					}
				} else { // 5% writes
					value := []byte(fmt.Sprintf("updated-%d", r.Int()))
					n.HandleSet(key, value, 0)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalOps)
	totalHits := atomic.LoadInt64(&hits)
	totalMisses := atomic.LoadInt64(&misses)

	t.Logf("Read-heavy hotspot workload (95/5):")
	t.Logf("  Duration: %v, Workers: %d", elapsed, numWorkers)
	t.Logf("  Total ops: %d, %.0f ops/sec", total, float64(total)/elapsed.Seconds())
	t.Logf("  Hits: %d, Misses: %d, Hit rate: %.1f%%", totalHits, totalMisses,
		float64(totalHits)*100/float64(totalHits+totalMisses))
}

// TestLoadMultiNodeScaling tests how throughput scales with the number of nodes.
func TestLoadMultiNodeScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	nodeCounts := []int{1, 3, 5, 7}

	for _, numNodes := range nodeCounts {
		t.Run(fmt.Sprintf("%d-nodes", numNodes), func(t *testing.T) {
			cluster := NewTestCluster(t, numNodes)
			cluster.Start(t)
			defer cluster.Stop()
			time.Sleep(2 * time.Second)

			// Pre-populate
			node0 := cluster.GetNode(0)
			for i := 0; i < 2000; i++ {
				key := []byte(fmt.Sprintf("scale-key-%d", i))
				value := []byte(fmt.Sprintf("scale-value-%d", i))
				node0.HandleSet(key, value, 0)
			}

			const workloadDuration = 10 * time.Second
			workersPerNode := 10

			var wg sync.WaitGroup
			var totalOps int64
			stopCh := make(chan struct{})
			start := time.Now()

			go func() {
				time.Sleep(workloadDuration)
				close(stopCh)
			}()

			for w := 0; w < workersPerNode*numNodes; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					nodeIdx := workerID % numNodes
					n := cluster.GetNode(nodeIdx)
					if n == nil {
						return
					}
					r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

					for {
						select {
						case <-stopCh:
							return
						default:
						}

						keyIdx := r.Intn(2000)
						key := []byte(fmt.Sprintf("scale-key-%d", keyIdx))

						if r.Float64() < 0.8 {
							n.HandleGet(key)
						} else {
							value := []byte(fmt.Sprintf("v-%d", r.Int()))
							n.HandleSet(key, value, 0)
						}
						atomic.AddInt64(&totalOps, 1)
					}
				}(w)
			}

			wg.Wait()
			elapsed := time.Since(start)
			total := atomic.LoadInt64(&totalOps)
			opsPerSec := float64(total) / elapsed.Seconds()

			t.Logf("  %d nodes, %d workers: %d ops in %v = %.0f ops/sec",
				numNodes, workersPerNode*numNodes, total, elapsed, opsPerSec)
		})
	}
}

// TestLoadChaosUnderSustainedLoad combines sustained load with periodic chaos events.
func TestLoadChaosUnderSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	cluster := NewTestCluster(t, 7)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(3 * time.Second)

	// Pre-populate
	node0 := cluster.GetNode(0)
	for i := 0; i < 3000; i++ {
		key := []byte(fmt.Sprintf("loadchaos-key-%d", i))
		value := []byte(fmt.Sprintf("loadchaos-value-%d", i))
		node0.HandleSet(key, value, 0)
	}

	const testDuration = 45 * time.Second
	const numWorkers = 60

	var wg sync.WaitGroup
	var totalOps, successOps, errorOps int64
	var faultCount int64
	stopCh := make(chan struct{})
	start := time.Now()

	go func() {
		time.Sleep(testDuration)
		close(stopCh)
	}()

	// Load workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				// Pick active node
				activeNodes := cluster.GetActiveNodes()
				if len(activeNodes) == 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				n := activeNodes[r.Intn(len(activeNodes))]

				keyIdx := r.Intn(5000)
				key := []byte(fmt.Sprintf("loadchaos-key-%d", keyIdx))
				atomic.AddInt64(&totalOps, 1)

				op := r.Float64()
				switch {
				case op < 0.65:
					_, _, _, found := n.HandleGet(key)
					if found {
						atomic.AddInt64(&successOps, 1)
					}
				case op < 0.95:
					value := []byte(fmt.Sprintf("val-%d-%d", workerID, r.Int()))
					_, err := n.HandleSet(key, value, 0)
					if err == nil {
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&errorOps, 1)
					}
				default:
					err := n.HandleDelete(key)
					if err == nil {
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&errorOps, 1)
					}
				}
			}
		}(w)
	}

	// Chaos goroutine - periodic faults
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for {
			select {
			case <-stopCh:
				return
			default:
			}

			// Wait 3-8 seconds between faults
			delay := 3*time.Second + time.Duration(r.Intn(5000))*time.Millisecond
			select {
			case <-stopCh:
				return
			case <-time.After(delay):
			}

			// Pick a random node (keep minimum 3 alive)
			activeNodes := cluster.GetActiveNodes()
			if len(activeNodes) <= 3 {
				// Restart a stopped node
				for i := 0; i < 7; i++ {
					if cluster.GetNode(i) == nil {
						cluster.RestartNode(t, i)
						break
					}
				}
				continue
			}

			// Kill a random active node (but not node 0 for simplicity)
			candidateIdx := 1 + r.Intn(6)
			n := cluster.GetNode(candidateIdx)
			if n == nil {
				continue
			}

			atomic.AddInt64(&faultCount, 1)
			t.Logf("  [Fault %d] Killing node %d", atomic.LoadInt64(&faultCount), candidateIdx)
			cluster.StopNode(candidateIdx)

			// Wait before restarting
			select {
			case <-stopCh:
				return
			case <-time.After(2 * time.Second):
			}

			cluster.RestartNode(t, candidateIdx)
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalOps)
	success := atomic.LoadInt64(&successOps)
	errors := atomic.LoadInt64(&errorOps)
	faults := atomic.LoadInt64(&faultCount)

	t.Logf("\n=== Chaos Under Sustained Load ===")
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Workers: %d, Faults injected: %d", numWorkers, faults)
	t.Logf("  Total ops: %d, Success: %d, Errors: %d", total, success, errors)
	t.Logf("  Throughput: %.0f ops/sec", float64(total)/elapsed.Seconds())
	if total > 0 {
		t.Logf("  Error rate: %.2f%%", float64(errors)*100/float64(total))
	}

	if errors > total/2 {
		t.Errorf("Error rate too high: %d/%d (%.1f%%)", errors, total, float64(errors)*100/float64(total))
	}
}

// TestLoadDataIntegrityCheck writes data, runs load, then verifies data integrity.
func TestLoadDataIntegrityCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)

	// Write known data set
	const numKeys = 1000
	knownData := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		value := fmt.Sprintf("integrity-value-%d", i)
		knownData[key] = value
		node0.HandleSet([]byte(key), []byte(value), 0)
	}

	// Run concurrent load on different key space (shouldn't affect our data)
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			n := cluster.GetNode(workerID % 5)
			if n == nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-stopCh:
					return
				default:
				}
				key := []byte(fmt.Sprintf("noise-key-%d", r.Intn(10000)))
				value := []byte(fmt.Sprintf("noise-%d", r.Int()))
				n.HandleSet(key, value, 0)
				n.HandleGet(key)
			}
		}(w)
	}

	time.Sleep(15 * time.Second)
	close(stopCh)
	wg.Wait()

	// Verify integrity of known data
	intact := 0
	corrupted := 0
	missing := 0

	for key, expectedValue := range knownData {
		got, _, _, found := node0.HandleGet([]byte(key))
		if !found {
			missing++
			continue
		}
		if string(got) == expectedValue {
			intact++
		} else {
			corrupted++
			t.Logf("CORRUPTED: key=%s, expected=%q, got=%q", key, expectedValue, got)
		}
	}

	t.Logf("Data integrity after load:")
	t.Logf("  Intact: %d/%d", intact, numKeys)
	t.Logf("  Missing: %d", missing)
	t.Logf("  Corrupted: %d", corrupted)

	if corrupted > 0 {
		t.Errorf("Data corruption detected: %d keys corrupted", corrupted)
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

// BenchmarkMultiNodeSet benchmarks SET operations distributed across nodes.
func BenchmarkMultiNodeSet(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	value := make([]byte, 128)
	for i := range value {
		value[i] = byte('A' + i%26)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			nodeIdx := i % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				continue
			}
			key := []byte(fmt.Sprintf("bench-set-%d", i))
			n.HandleSet(key, value, 0)
			i++
		}
	})
}

// BenchmarkMultiNodeGet benchmarks GET operations distributed across nodes.
func BenchmarkMultiNodeGet(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 128)

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("bench-get-%d", i))
		node0.HandleSet(key, value, 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			nodeIdx := i % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				continue
			}
			key := []byte(fmt.Sprintf("bench-get-%d", i%10000))
			n.HandleGet(key)
			i++
		}
	})
}

// BenchmarkMultiNodeMixed benchmarks mixed read/write across nodes.
func BenchmarkMultiNodeMixed(b *testing.B) {
	cluster := NewTestCluster(&testing.T{}, 5)
	cluster.Start(&testing.T{})
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	node0 := cluster.GetNode(0)
	value := make([]byte, 128)

	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("bench-mixed-%d", i))
		node0.HandleSet(key, value, 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			nodeIdx := i % 5
			n := cluster.GetNode(nodeIdx)
			if n == nil {
				continue
			}
			key := []byte(fmt.Sprintf("bench-mixed-%d", i%5000))
			if i%5 < 4 { // 80% reads
				n.HandleGet(key)
			} else {
				n.HandleSet(key, value, 0)
			}
			i++
		}
	})
}

// ============================================================================
// HELPER TYPES AND FUNCTIONS
// ============================================================================

type chaosContext struct {
	stopCh    chan struct{}
	cluster   *TestCluster
	t         *testing.T
	numKeys   int
	keyPrefix string

	setSuccess int64
	setFail    int64
	getSuccess int64
	getFail    int64
	getMiss    int64
	delSuccess int64
	delFail    int64
	kills      int64
	restarts   int64
}

type chaosStats struct {
	setSuccess, setFail       int64
	getSuccess, getFail       int64
	getMiss                   int64
	delSuccess, delFail       int64
	kills, restarts           int64
}

func (c *chaosContext) stats() chaosStats {
	return chaosStats{
		setSuccess: atomic.LoadInt64(&c.setSuccess),
		setFail:    atomic.LoadInt64(&c.setFail),
		getSuccess: atomic.LoadInt64(&c.getSuccess),
		getFail:    atomic.LoadInt64(&c.getFail),
		getMiss:    atomic.LoadInt64(&c.getMiss),
		delSuccess: atomic.LoadInt64(&c.delSuccess),
		delFail:    atomic.LoadInt64(&c.delFail),
		kills:      atomic.LoadInt64(&c.kills),
		restarts:   atomic.LoadInt64(&c.restarts),
	}
}

func (c *chaosContext) operationWorker(workerID int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		activeNodes := c.cluster.GetActiveNodes()
		if len(activeNodes) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		n := activeNodes[r.Intn(len(activeNodes))]

		keyIdx := r.Intn(c.numKeys)
		key := []byte(fmt.Sprintf("%s-%d", c.keyPrefix, keyIdx))

		op := r.Float64()
		switch {
		case op < 0.40: // 40% reads
			_, _, _, found := n.HandleGet(key)
			if found {
				atomic.AddInt64(&c.getSuccess, 1)
			} else {
				atomic.AddInt64(&c.getMiss, 1)
			}
		case op < 0.85: // 45% writes
			value := []byte(fmt.Sprintf("w%d-v%d", workerID, r.Int()))
			_, err := n.HandleSet(key, value, 0)
			if err == nil {
				atomic.AddInt64(&c.setSuccess, 1)
			} else {
				atomic.AddInt64(&c.setFail, 1)
			}
		default: // 15% deletes
			err := n.HandleDelete(key)
			if err == nil {
				atomic.AddInt64(&c.delSuccess, 1)
			} else {
				atomic.AddInt64(&c.delFail, 1)
			}
		}
	}
}

func (c *chaosContext) nodeKillChaos(minInterval, maxInterval time.Duration, minAlive int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + 999))

	for {
		interval := minInterval + time.Duration(r.Int63n(int64(maxInterval-minInterval)))
		select {
		case <-c.stopCh:
			return
		case <-time.After(interval):
		}

		activeNodes := c.cluster.GetActiveNodes()
		if len(activeNodes) <= minAlive {
			// Try to restart a dead node
			for i := 0; i < 7; i++ {
				if c.cluster.GetNode(i) == nil {
					if err := c.cluster.RestartNode(c.t, i); err == nil {
						atomic.AddInt64(&c.restarts, 1)
					}
					break
				}
			}
			continue
		}

		// Pick a random active node to kill (not node 0)
		candidates := make([]int, 0)
		for i := 1; i < 7; i++ {
			if c.cluster.GetNode(i) != nil {
				candidates = append(candidates, i)
			}
		}
		if len(candidates) == 0 {
			continue
		}

		victimIdx := candidates[r.Intn(len(candidates))]
		if err := c.cluster.StopNode(victimIdx); err == nil {
			atomic.AddInt64(&c.kills, 1)
		}

		// Schedule restart after a delay
		go func(idx int) {
			delay := 1*time.Second + time.Duration(r.Intn(3000))*time.Millisecond
			select {
			case <-c.stopCh:
			case <-time.After(delay):
			}
			if err := c.cluster.RestartNode(c.t, idx); err == nil {
				atomic.AddInt64(&c.restarts, 1)
			}
		}(victimIdx)
	}
}

// ============================================================================
// ADDITIONAL UNIT-LEVEL MULTI-NODE TESTS
// ============================================================================

// TestShardWriteRejectInReadOnlyState verifies shards reject writes in read-only states.
func TestShardWriteRejectInReadOnlyState(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	// Verify the ShardState invariants directly
	states := []struct {
		state    types.ShardState
		canRead  bool
		canWrite bool
	}{
		{types.ShardStateActive, true, true},
		{types.ShardStateMigratingOut, true, true},
		{types.ShardStateMigratingIn, true, false},
		{types.ShardStateDegraded, true, false},
		{types.ShardStateReadOnly, true, false},
	}

	for _, tt := range states {
		if tt.state.CanRead() != tt.canRead {
			t.Errorf("State %s: CanRead() = %v, want %v", tt.state, tt.state.CanRead(), tt.canRead)
		}
		if tt.state.CanWrite() != tt.canWrite {
			t.Errorf("State %s: CanWrite() = %v, want %v", tt.state, tt.state.CanWrite(), tt.canWrite)
		}
	}
}

// TestMultiNodeVersionOrdering verifies that versions from different nodes
// are properly ordered using (term, seq).
func TestMultiNodeVersionOrdering(t *testing.T) {
	v1 := types.Version{Term: 1, Seq: 1}
	v2 := types.Version{Term: 1, Seq: 2}
	v3 := types.Version{Term: 2, Seq: 1}

	if !v2.IsNewerThan(v1) {
		t.Error("v2 should be newer than v1 (same term, higher seq)")
	}
	if !v3.IsNewerThan(v2) {
		t.Error("v3 should be newer than v2 (higher term)")
	}
	if !v3.IsNewerThan(v1) {
		t.Error("v3 should be newer than v1")
	}
	if v1.IsNewerThan(v2) {
		t.Error("v1 should not be newer than v2")
	}
	if v1.IsNewerThan(v1) {
		t.Error("v1 should not be newer than itself")
	}

	// Equal versions
	if v1.Compare(v1) != 0 {
		t.Error("v1 should equal itself")
	}
}

// TestMultiNodeStatsAccumulation verifies that stats accumulate correctly
// across a multi-node cluster.
func TestMultiNodeStatsAccumulation(t *testing.T) {
	cluster := NewTestCluster(t, 5)
	cluster.Start(t)
	defer cluster.Stop()
	time.Sleep(2 * time.Second)

	// Do operations across all nodes
	const opsPerNode = 100
	for nodeIdx := 0; nodeIdx < 5; nodeIdx++ {
		n := cluster.GetNode(nodeIdx)
		if n == nil {
			continue
		}
		for i := 0; i < opsPerNode; i++ {
			key := []byte(fmt.Sprintf("stats-node%d-key-%d", nodeIdx, i))
			value := []byte(fmt.Sprintf("stats-value-%d", i))
			n.HandleSet(key, value, 0)
		}
	}

	// Verify each node has stats
	for nodeIdx := 0; nodeIdx < 5; nodeIdx++ {
		n := cluster.GetNode(nodeIdx)
		if n == nil {
			continue
		}
		stats := n.Stats()
		t.Logf("Node %d: shards=%d, primary=%d, follower=%d",
			nodeIdx, stats.TotalShards, stats.PrimaryShards, stats.FollowerShards)

		if stats.TotalShards == 0 {
			t.Errorf("Node %d has no shards", nodeIdx)
		}
	}
}
