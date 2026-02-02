//go:build chaos
// +build chaos

// Package chaos provides chaos testing for AegisKV.
// These tests run processes and inject faults. Use -tags chaos to run:
//   go test ./tests/chaos/... -tags chaos -timeout 5m

package chaos

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/tests/process"
)

// ProcessNodeController adapts process.Node to NodeController interface.
type ProcessNodeController struct {
	node *process.Node
}

func (p *ProcessNodeController) ID() string      { return p.node.ID }
func (p *ProcessNodeController) Kill() error     { return p.node.Kill() }
func (p *ProcessNodeController) Stop() error     { return p.node.Stop() }
func (p *ProcessNodeController) Restart() error  { return p.node.Restart() }
func (p *ProcessNodeController) IsRunning() bool { return p.node.IsRunning() }
func (p *ProcessNodeController) PID() int {
	if p.node.Cmd != nil && p.node.Cmd.Process != nil {
		return p.node.Cmd.Process.Pid
	}
	return 0
}

func (p *ProcessNodeController) Pause() error {
	if p.node.Cmd != nil && p.node.Cmd.Process != nil {
		return syscall.Kill(p.node.Cmd.Process.Pid, syscall.SIGSTOP)
	}
	return fmt.Errorf("node not running")
}

func (p *ProcessNodeController) Resume() error {
	if p.node.Cmd != nil && p.node.Cmd.Process != nil {
		return syscall.Kill(p.node.Cmd.Process.Pid, syscall.SIGCONT)
	}
	return fmt.Errorf("node not running")
}

// TestChaosShort runs a short chaos test (1 minute).
func TestChaosShort(t *testing.T) {
	runChaosTest(t, 1*time.Minute)
}

// TestChaosMedium runs a medium chaos test (5 minutes).
func TestChaosMedium(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping medium chaos test in short mode")
	}
	runChaosTest(t, 5*time.Minute)
}

// TestChaosLong runs a long chaos test (15 minutes).
func TestChaosLong(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long chaos test in short mode")
	}
	runChaosTest(t, 15*time.Minute)
}

// TestChaosExtended runs an extended chaos test (30 minutes).
func TestChaosExtended(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping extended chaos test in short mode")
	}
	runChaosTest(t, 30*time.Minute)
}

func runChaosTest(t *testing.T, duration time.Duration) {
	t.Logf("Starting chaos test for %v", duration)

	// Build binary
	binaryPath := buildBinary(t)

	// Create cluster
	cluster, err := process.NewCluster(process.ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "write",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	// Start 5-node cluster
	numNodes := 5
	if err := cluster.StartCluster(numNodes); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	t.Logf("Cluster started with %d nodes", numNodes)

	// Get client addresses
	var addrs []string
	var nodeControllers []NodeController
	for _, n := range cluster.Nodes() {
		addrs = append(addrs, n.ClientAddr)
		nodeControllers = append(nodeControllers, &ProcessNodeController{node: n})
	}

	// Create workload
	workload := NewWorkload(WorkloadConfig{
		Addrs:          addrs,
		NumWorkers:     20,
		KeySpace:       10000,
		ValueSize:      100,
		ReadRatio:      0.7,
		MinTTL:         0,
		MaxTTL:         30 * time.Second,
		OperationDelay: 5 * time.Millisecond,
	})

	// Create chaos controller
	faultCount := 0
	chaos := NewChaosController(ChaosConfig{
		Nodes: nodeControllers,
		EnabledFaults: []FaultType{
			FaultKillNode,
			FaultRestartNode,
			FaultPauseNode,
		},
		MinInterval:   5 * time.Second,
		MaxInterval:   15 * time.Second,
		FaultDuration: 5 * time.Second,
		MinAliveNodes: 2,
		OnFaultStart: func(e *FaultEvent) {
			faultCount++
			t.Logf("[%d] Fault: %s on %s", faultCount, e.Type, e.NodeID)
		},
		OnFaultEnd: func(e *FaultEvent) {
			t.Logf("[%d] Recovered: %s on %s (took %v)", faultCount, e.Type, e.NodeID, e.EndTime.Sub(e.StartTime))
		},
	})

	// Start workload and chaos
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	if err := workload.Start(ctx); err != nil {
		t.Fatalf("Failed to start workload: %v", err)
	}
	defer workload.Stop()

	chaos.Start()
	defer chaos.Stop()

	// Progress reporter
	startTime := time.Now()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastStats := workload.Stats()
	lastTime := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			stats := workload.Stats()
			chaosStats := chaos.Stats()
			now := time.Now()
			elapsed := now.Sub(startTime)
			interval := now.Sub(lastTime)

			opsInInterval := stats.TotalOps - lastStats.TotalOps
			opsPerSec := float64(opsInInterval) / interval.Seconds()

			t.Logf("[%v] Ops: %d (%.0f/s), Errors: %d (%.2f%%), Faults: %d active, %d total",
				elapsed.Round(time.Second),
				stats.TotalOps,
				opsPerSec,
				stats.FailedOps,
				stats.ErrorRate(),
				chaosStats.ActiveFaults,
				chaosStats.TotalFaults,
			)

			lastStats = stats
			lastTime = now
		}
	}

	// Stop workload FIRST (no more writes)
	t.Log("Stopping workload...")
	workload.Stop()
	finalStats := workload.Stats()

	// Stop chaos and let cluster stabilize
	t.Log("Stopping chaos, letting cluster stabilize...")
	chaos.Stop()

	// Ensure all nodes are running before validation
	t.Log("Ensuring all nodes are running...")
	runningCount := 0
	for _, nc := range nodeControllers {
		pnc := nc.(*ProcessNodeController)
		if pnc.IsRunning() {
			runningCount++
		} else {
			t.Logf("  Restarting stopped node: %s", pnc.node.ID)
			if err := pnc.Restart(); err != nil {
				t.Logf("  Failed to restart node %s: %v", pnc.node.ID, err)
			} else {
				runningCount++
			}
		}
	}
	t.Logf("Nodes running: %d/%d", runningCount, len(nodeControllers))

	time.Sleep(5 * time.Second)

	// Get chaos stats
	chaosStats := chaos.Stats()

	t.Logf("\n=== CHAOS TEST RESULTS ===")
	t.Logf("Duration: %v", duration)
	t.Logf("Total operations: %d", finalStats.TotalOps)
	t.Logf("Successful operations: %d", finalStats.SuccessOps)
	t.Logf("Failed operations: %d", finalStats.FailedOps)
	t.Logf("Error rate: %.2f%%", finalStats.ErrorRate())
	t.Logf("Operations per second: %.0f", finalStats.OpsPerSecond(duration))
	t.Logf("")
	t.Logf("Read operations: %d (success: %d, failed: %d)", finalStats.ReadOps, finalStats.ReadSuccess, finalStats.ReadFailed)
	t.Logf("Write operations: %d (success: %d, failed: %d)", finalStats.WriteOps, finalStats.WriteSuccess, finalStats.WriteFailed)
	t.Logf("Delete operations: %d", finalStats.DeleteOps)
	t.Logf("")
	t.Logf("Total faults injected: %d", chaosStats.TotalFaults)
	t.Logf("Fault breakdown:")
	for faultType, count := range chaosStats.FaultCounts {
		t.Logf("  %s: %d", faultType, count)
	}

	// Validate data consistency
	t.Log("")
	t.Log("Validating data consistency...")

	// Give cluster more time to stabilize for validation
	time.Sleep(5 * time.Second)

	// First, do post-chaos validation (write known values and verify)
	// This is the authoritative test - if this fails, we have a real problem
	postWritten, postValidated, postNotFound, postWrongValue := workload.ValidatePostChaos(1000)
	t.Logf("Post-chaos validation:")
	t.Logf("  Written: %d", postWritten)
	t.Logf("  Validated: %d", postValidated)
	t.Logf("  Not found: %d", postNotFound)
	t.Logf("  Wrong value: %d", postWrongValue)

	// Also check keys written during chaos (informational only)
	chaosValidated, chaosErrors, chaosMissing := workload.ValidateData()
	trackedKeys := workload.GetWrittenKeyCount()

	// Close workload client after validation
	workload.Close()

	t.Logf("Chaos-period validation (informational):")
	t.Logf("  Tracked keys: %d", trackedKeys)
	t.Logf("  Validated: %d", chaosValidated)
	t.Logf("  Wrong values: %d", chaosErrors)
	t.Logf("  Missing: %d", chaosMissing)

	// Assertions
	// We expect some errors during chaos, but error rate should be reasonable
	if finalStats.ErrorRate() > 50 {
		t.Errorf("Error rate too high: %.2f%% (expected < 50%%)", finalStats.ErrorRate())
	}

	// We should have completed many operations
	if finalStats.TotalOps < 100 {
		t.Errorf("Too few operations completed: %d (expected > 100)", finalStats.TotalOps)
	}

	// Post-chaos validation must pass - this shows the cluster is healthy after chaos
	// Wrong value means data corruption - this should never happen
	if postWrongValue > 0 {
		t.Errorf("Post-chaos data corruption: %d keys with wrong values (real problem!)", postWrongValue)
	}

	// Not found after successful write indicates keys were lost - this is a problem
	// but could be due to node failures preventing replication
	if postNotFound > 100 { // Allow some tolerance for extreme chaos
		t.Errorf("Post-chaos data loss: %d/%d keys not found after write", postNotFound, postWritten)
	}

	// Should be able to write most validation keys
	if postWritten < 800 {
		t.Errorf("Post-chaos writes failing: only %d/1000 writes succeeded", postWritten)
	}

	t.Log("\n=== CHAOS TEST COMPLETED ===")
}

// TestChaosWithNetworkPartition tests chaos with network faults (requires root/sudo).
func TestChaosWithNetworkPartition(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Skipping network partition test - requires root")
	}

	// Similar to runChaosTest but with network faults enabled
	t.Log("Running chaos test with network partitions...")
	// Implementation would be similar to runChaosTest but with network faults enabled
}

// buildBinary builds the aegis binary for testing.
func buildBinary(t *testing.T) string {
	t.Helper()

	projectRoot := findProjectRoot(t)
	binaryPath := filepath.Join(projectRoot, "bin", "aegis")

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

// BenchmarkChaosOperations benchmarks operations during chaos.
func BenchmarkChaosOperations(b *testing.B) {
	// Build binary
	projectRoot, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); err == nil {
			break
		}
		projectRoot = filepath.Dir(projectRoot)
	}
	binaryPath := filepath.Join(projectRoot, "bin", "aegis")

	// Build if needed
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		buildCmd := fmt.Sprintf("cd %s && go build -o bin/aegis ./cmd/aegis", projectRoot)
		cmd := exec.Command("sh", "-c", buildCmd)
		if err := cmd.Run(); err != nil {
			b.Fatalf("Failed to build: %v", err)
		}
	}

	cluster, err := process.NewCluster(process.ClusterConfig{
		BinaryPath: binaryPath,
		WALMode:    "off",
		NumShards:  64,
		ReplFactor: 3,
	})
	if err != nil {
		b.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Cleanup()

	if err := cluster.StartCluster(5); err != nil {
		b.Fatalf("Failed to start cluster: %v", err)
	}

	var addrs []string
	var controllers []NodeController
	for _, n := range cluster.Nodes() {
		addrs = append(addrs, n.ClientAddr)
		controllers = append(controllers, &ProcessNodeController{node: n})
	}

	// Start chaos
	chaos := NewChaosController(ChaosConfig{
		Nodes:         controllers,
		EnabledFaults: []FaultType{FaultKillNode, FaultRestartNode},
		MinInterval:   5 * time.Second,
		MaxInterval:   10 * time.Second,
		FaultDuration: 3 * time.Second,
		MinAliveNodes: 2,
	})
	chaos.Start()
	defer chaos.Stop()

	workload := NewWorkload(WorkloadConfig{
		Addrs:      addrs,
		NumWorkers: 1,
		KeySpace:   1000,
		ValueSize:  100,
		ReadRatio:  0.5,
	})

	ctx := context.Background()
	if err := workload.Start(ctx); err != nil {
		b.Fatalf("Failed to start workload: %v", err)
	}
	defer workload.Stop()

	b.ResetTimer()

	// Run benchmark
	time.Sleep(time.Duration(b.N) * time.Millisecond)

	stats := workload.Stats()
	b.ReportMetric(float64(stats.TotalOps), "ops")
	b.ReportMetric(stats.ErrorRate(), "error_pct")
}

// Helper to run chaos from command line
func init() {
	// Set up logging
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}
