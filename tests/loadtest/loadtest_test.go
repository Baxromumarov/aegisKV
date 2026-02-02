//go:build loadtest
// +build loadtest

// Package loadtest provides comprehensive load testing for AegisKV.
package loadtest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ClusterNode represents a running AegisKV node.
type ClusterNode struct {
	ID      string
	Addr    string
	DataDir string
	cmd     *exec.Cmd
	done    chan struct{}
}

// Cluster represents a multi-node AegisKV cluster.
type Cluster struct {
	nodes   []*ClusterNode
	baseDir string
}

// NewCluster creates a new test cluster.
func NewCluster(nodeCount int, basePort int) (*Cluster, error) {
	baseDir, err := os.MkdirTemp("", "aegis-loadtest-*")
	if err != nil {
		return nil, err
	}

	c := &Cluster{
		nodes:   make([]*ClusterNode, nodeCount),
		baseDir: baseDir,
	}

	// Find the binary
	binaryPath, err := findAegisBinary()
	if err != nil {
		return nil, err
	}

	// Build peer list (using gossip ports = basePort + i*2 + 1)
	peers := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		gossipPort := basePort + i*2 + 1
		peers[i] = fmt.Sprintf("127.0.0.1:%d", gossipPort)
	}
	peerList := strings.Join(peers, ",")

	// Start nodes (using port pairs: client=basePort+i*2, gossip=basePort+i*2+1)
	for i := 0; i < nodeCount; i++ {
		node, err := c.startNode(binaryPath, i, basePort+i*2, peerList)
		if err != nil {
			c.Stop()
			return nil, err
		}
		c.nodes[i] = node
	}

	// Wait for nodes to be ready
	time.Sleep(2 * time.Second)

	return c, nil
}

func findAegisBinary() (string, error) {
	// Try common paths
	paths := []string{
		"../../bin/aegis",
		"./bin/aegis",
		"/home/bakhromumarov/go/src/github.com/baxromumarov/aegisKV/bin/aegis",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return filepath.Abs(p)
		}
	}

	// Try to build it
	buildCmd := exec.Command("go", "build", "-o", "/tmp/aegis-loadtest", "../../cmd/aegis")
	if err := buildCmd.Run(); err != nil {
		return "", fmt.Errorf("binary not found and build failed: %v", err)
	}
	return "/tmp/aegis-loadtest", nil
}

func (c *Cluster) startNode(binaryPath string, index, port int, peers string) (*ClusterNode, error) {
	dataDir := filepath.Join(c.baseDir, fmt.Sprintf("node-%d", index))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	node := &ClusterNode{
		ID:      fmt.Sprintf("node-%d", index),
		Addr:    fmt.Sprintf("127.0.0.1:%d", port),
		DataDir: dataDir,
		done:    make(chan struct{}),
	}

	gossipPort := port + 1
	args := []string{
		"--bind", node.Addr,
		"--gossip", fmt.Sprintf("127.0.0.1:%d", gossipPort),
		"--data-dir", dataDir,
		"--addrs", peers,
		"--max-memory", "256",
		"--log-level", "warn",
	}

	node.cmd = exec.Command(binaryPath, args...)
	node.cmd.Stdout = nil
	node.cmd.Stderr = nil

	if err := node.cmd.Start(); err != nil {
		return nil, err
	}

	go func() {
		node.cmd.Wait()
		close(node.done)
	}()

	return node, nil
}

// Addrs returns all node addresses.
func (c *Cluster) Addrs() []string {
	addrs := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		addrs[i] = n.Addr
	}
	return addrs
}

// Stop stops all cluster nodes.
func (c *Cluster) Stop() {
	for _, n := range c.nodes {
		if n != nil && n.cmd != nil && n.cmd.Process != nil {
			n.cmd.Process.Signal(os.Interrupt)
			select {
			case <-n.done:
			case <-time.After(5 * time.Second):
				n.cmd.Process.Kill()
			}
		}
	}
	os.RemoveAll(c.baseDir)
}

// TestAegisLoadSingle tests a single-node AegisKV under load.
func TestAegisLoadSingle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	cluster, err := NewCluster(1, 19000)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	cfg := Config{
		Name:        "AegisKV-Single",
		Duration:    30 * time.Second,
		Concurrency: 100,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    100000,
		RampUp:      5 * time.Second,
	}

	runner := NewRunner(cfg, client)
	result := runner.Run(context.Background())

	t.Logf("Results for %s:", result.Name)
	t.Logf("  Duration: %v", result.Duration)
	t.Logf("  Total Ops: %d", result.TotalOps)
	t.Logf("  Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("  Avg Latency: %v", result.AvgLatency)
	t.Logf("  P50 Latency: %v", result.P50Latency)
	t.Logf("  P95 Latency: %v", result.P95Latency)
	t.Logf("  P99 Latency: %v", result.P99Latency)
	t.Logf("  Max Latency: %v", result.MaxLatency)
	t.Logf("  Throughput: %.2f MB/s", result.ThroughputMBps)
	t.Logf("  Errors: %d", result.FailedOps)

	// Baseline assertions
	if result.OpsPerSecond < 1000 {
		t.Errorf("expected at least 1000 ops/sec, got %.2f", result.OpsPerSecond)
	}
	if result.P99Latency > 100*time.Millisecond {
		t.Errorf("expected P99 < 100ms, got %v", result.P99Latency)
	}
}

// TestAegisLoadCluster tests a 3-node AegisKV cluster under load.
func TestAegisLoadCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	cluster, err := NewCluster(3, 19100)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	cfg := Config{
		Name:        "AegisKV-3Node",
		Duration:    30 * time.Second,
		Concurrency: 100,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    100000,
		RampUp:      5 * time.Second,
	}

	runner := NewRunner(cfg, client)
	result := runner.Run(context.Background())

	t.Logf("Results for %s:", result.Name)
	t.Logf("  Duration: %v", result.Duration)
	t.Logf("  Total Ops: %d", result.TotalOps)
	t.Logf("  Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("  Avg Latency: %v", result.AvgLatency)
	t.Logf("  P50 Latency: %v", result.P50Latency)
	t.Logf("  P95 Latency: %v", result.P95Latency)
	t.Logf("  P99 Latency: %v", result.P99Latency)
	t.Logf("  Max Latency: %v", result.MaxLatency)
	t.Logf("  Throughput: %.2f MB/s", result.ThroughputMBps)
	t.Logf("  Errors: %d", result.FailedOps)
}

// TestAegisScalability tests how AegisKV scales with increasing concurrency.
func TestAegisScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scalability test in short mode")
	}

	cluster, err := NewCluster(3, 19200)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	concurrencyLevels := []int{10, 50, 100, 200, 500}
	results := make([]*Result, len(concurrencyLevels))

	for i, concurrency := range concurrencyLevels {
		client := NewAegisClient(cluster.Addrs())

		cfg := Config{
			Name:        fmt.Sprintf("Concurrency-%d", concurrency),
			Duration:    15 * time.Second,
			Concurrency: concurrency,
			KeySize:     32,
			ValueSize:   256,
			ReadRatio:   0.8,
			KeySpace:    100000,
			RampUp:      2 * time.Second,
		}

		runner := NewRunner(cfg, client)
		results[i] = runner.Run(context.Background())
		client.Close()

		// Brief pause between tests
		time.Sleep(1 * time.Second)
	}

	// Print scalability report
	t.Log("\n=== Scalability Report ===")
	t.Log("Concurrency | Ops/sec    | P50 Latency | P99 Latency | Errors")
	t.Log("------------|------------|-------------|-------------|-------")
	for _, r := range results {
		var conc int
		fmt.Sscanf(r.Name, "Concurrency-%d", &conc)
		t.Logf("%11d | %10.2f | %11v | %11v | %d",
			conc, r.OpsPerSecond, r.P50Latency.Round(time.Microsecond),
			r.P99Latency.Round(time.Microsecond), r.FailedOps)
	}
}

// TestCompareAegisRedis compares AegisKV against Redis.
func TestCompareAegisRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping comparison test in short mode")
	}

	// Check if Redis is available
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	redisAvailable := checkRedisAvailable(redisAddr)
	if !redisAvailable {
		t.Log("Redis not available, starting local Redis...")
		redisCmd := exec.Command("redis-server", "--port", "6379", "--daemonize", "yes")
		if err := redisCmd.Run(); err != nil {
			t.Skip("Redis not available and could not start it")
		}
		time.Sleep(1 * time.Second)
		defer exec.Command("redis-cli", "shutdown").Run()
	}

	// Start AegisKV cluster
	cluster, err := NewCluster(3, 19300)
	if err != nil {
		t.Fatalf("failed to start AegisKV cluster: %v", err)
	}
	defer cluster.Stop()

	cfg := Config{
		Duration:    30 * time.Second,
		Concurrency: 100,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    100000,
		RampUp:      5 * time.Second,
	}

	var results []*Result

	// Test Redis
	if redisAvailable || checkRedisAvailable(redisAddr) {
		redisClient := NewRedisClient(redisAddr)
		cfg.Name = "Redis"
		redisRunner := NewRunner(cfg, redisClient)
		results = append(results, redisRunner.Run(context.Background()))
		redisClient.Close()

		time.Sleep(2 * time.Second)
	}

	// Test AegisKV
	aegisClient := NewAegisClient(cluster.Addrs())
	cfg.Name = "AegisKV-3Node"
	aegisRunner := NewRunner(cfg, aegisClient)
	results = append(results, aegisRunner.Run(context.Background()))
	aegisClient.Close()

	// Print comparison
	t.Log("\n" + CompareResults(results))
}

func checkRedisAvailable(addr string) bool {
	redisClient := NewRedisClient(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Try a simple SET/GET
	if err := redisClient.Set(ctx, "test", []byte("test")); err != nil {
		redisClient.Close()
		return false
	}
	redisClient.Close()
	return true
}

// TestValueSizeImpact tests how different value sizes affect performance.
func TestValueSizeImpact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping value size test in short mode")
	}

	cluster, err := NewCluster(3, 19400)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	valueSizes := []int{64, 256, 1024, 4096, 16384}
	results := make([]*Result, len(valueSizes))

	for i, valueSize := range valueSizes {
		client := NewAegisClient(cluster.Addrs())

		cfg := Config{
			Name:        fmt.Sprintf("ValueSize-%d", valueSize),
			Duration:    15 * time.Second,
			Concurrency: 100,
			KeySize:     32,
			ValueSize:   valueSize,
			ReadRatio:   0.8,
			KeySpace:    50000,
			RampUp:      2 * time.Second,
		}

		runner := NewRunner(cfg, client)
		results[i] = runner.Run(context.Background())
		client.Close()

		time.Sleep(1 * time.Second)
	}

	// Print report
	t.Log("\n=== Value Size Impact Report ===")
	t.Log("Value Size | Ops/sec    | Throughput MB/s | P99 Latency | Errors")
	t.Log("-----------|------------|-----------------|-------------|-------")
	for _, r := range results {
		var size int
		fmt.Sscanf(r.Name, "ValueSize-%d", &size)
		t.Logf("%10d | %10.2f | %15.2f | %11v | %d",
			size, r.OpsPerSecond, r.ThroughputMBps,
			r.P99Latency.Round(time.Microsecond), r.FailedOps)
	}
}

// TestReadWriteRatioImpact tests how different read/write ratios affect performance.
func TestReadWriteRatioImpact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping read/write ratio test in short mode")
	}

	cluster, err := NewCluster(3, 19500)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	ratios := []float64{0.0, 0.25, 0.5, 0.75, 0.9, 1.0}
	results := make([]*Result, len(ratios))

	for i, ratio := range ratios {
		client := NewAegisClient(cluster.Addrs())

		cfg := Config{
			Name:        fmt.Sprintf("ReadRatio-%.0f%%", ratio*100),
			Duration:    15 * time.Second,
			Concurrency: 100,
			KeySize:     32,
			ValueSize:   256,
			ReadRatio:   ratio,
			KeySpace:    50000,
			RampUp:      2 * time.Second,
		}

		runner := NewRunner(cfg, client)
		results[i] = runner.Run(context.Background())
		client.Close()

		time.Sleep(1 * time.Second)
	}

	// Print report
	t.Log("\n=== Read/Write Ratio Impact Report ===")
	t.Log("Read Ratio | Ops/sec    | P50 Latency | P99 Latency | Errors")
	t.Log("-----------|------------|-------------|-------------|-------")
	for _, r := range results {
		t.Logf("%10s | %10.2f | %11v | %11v | %d",
			r.Name, r.OpsPerSecond,
			r.P50Latency.Round(time.Microsecond),
			r.P99Latency.Round(time.Microsecond), r.FailedOps)
	}
}

// BenchmarkAegisKV runs Go benchmarks for AegisKV operations.
func BenchmarkAegisKV(b *testing.B) {
	cluster, err := NewCluster(1, 19600)
	if err != nil {
		b.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i % 256)
	}

	// Pre-populate some keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		client.Set(ctx, key, value)
	}

	b.ResetTimer()

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i)
			client.Set(ctx, key, value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-key-%d", i%1000)
			client.Get(ctx, key)
		}
	})

	b.Run("Mixed80Read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if i%5 == 0 {
				key := fmt.Sprintf("key-%d", i)
				client.Set(ctx, key, value)
			} else {
				key := fmt.Sprintf("bench-key-%d", i%1000)
				client.Get(ctx, key)
			}
		}
	})

	b.Run("ParallelSet", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("parallel-key-%d", i)
				client.Set(ctx, key, value)
				i++
			}
		})
	})

	b.Run("ParallelGet", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("bench-key-%d", i%1000)
				client.Get(ctx, key)
				i++
			}
		})
	})
}

// TestHighConcurrency tests behavior under very high concurrency.
func TestHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency test in short mode")
	}

	cluster, err := NewCluster(3, 19700)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	cfg := Config{
		Name:        "HighConcurrency",
		Duration:    60 * time.Second,
		Concurrency: 1000,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    100000,
		RampUp:      10 * time.Second,
	}

	runner := NewRunner(cfg, client)
	result := runner.Run(context.Background())

	t.Logf("High Concurrency Test Results:")
	t.Logf("  Concurrency: 1000")
	t.Logf("  Duration: %v", result.Duration)
	t.Logf("  Total Ops: %d", result.TotalOps)
	t.Logf("  Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("  Avg Latency: %v", result.AvgLatency)
	t.Logf("  P50 Latency: %v", result.P50Latency)
	t.Logf("  P95 Latency: %v", result.P95Latency)
	t.Logf("  P99 Latency: %v", result.P99Latency)
	t.Logf("  Max Latency: %v", result.MaxLatency)
	t.Logf("  Throughput: %.2f MB/s", result.ThroughputMBps)
	t.Logf("  Errors: %d (%.2f%%)", result.FailedOps, float64(result.FailedOps)/float64(result.TotalOps)*100)

	// High concurrency should still maintain reasonable latency
	if result.P99Latency > 500*time.Millisecond {
		t.Errorf("P99 latency too high under load: %v", result.P99Latency)
	}
}

// TestLongRunning tests stability over a longer period.
func TestLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long running test in short mode")
	}

	durationStr := os.Getenv("LOAD_TEST_DURATION")
	duration := 5 * time.Minute
	if durationStr != "" {
		if d, err := time.ParseDuration(durationStr); err == nil {
			duration = d
		}
	}

	cluster, err := NewCluster(3, 19800)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	cfg := Config{
		Name:        "LongRunning",
		Duration:    duration,
		Concurrency: 100,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    100000,
		RampUp:      10 * time.Second,
	}

	runner := NewRunner(cfg, client)
	result := runner.Run(context.Background())

	t.Logf("Long Running Test Results (%v):", duration)
	t.Logf("  Total Ops: %d", result.TotalOps)
	t.Logf("  Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("  P50 Latency: %v", result.P50Latency)
	t.Logf("  P99 Latency: %v", result.P99Latency)
	t.Logf("  Errors: %d (%.4f%%)", result.FailedOps, float64(result.FailedOps)/float64(result.TotalOps)*100)

	// Error rate should be very low
	errorRate := float64(result.FailedOps) / float64(result.TotalOps)
	if errorRate > 0.001 {
		t.Errorf("Error rate too high: %.4f%%", errorRate*100)
	}
}

// TestMultiNodeFailover tests behavior when a node fails.
func TestMultiNodeFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover test in short mode")
	}

	cluster, err := NewCluster(3, 19900)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	value := make([]byte, 256)

	// Pre-populate data
	t.Log("Pre-populating data...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("failover-key-%d", i)
		if err := client.Set(ctx, key, value); err != nil {
			t.Logf("Warning: failed to set key %s: %v", key, err)
		}
	}

	// Start load test in background
	cfg := Config{
		Name:        "Failover",
		Duration:    30 * time.Second,
		Concurrency: 50,
		KeySize:     32,
		ValueSize:   256,
		ReadRatio:   0.8,
		KeySpace:    10000,
		RampUp:      2 * time.Second,
	}

	var wg sync.WaitGroup
	var result *Result

	wg.Add(1)
	go func() {
		defer wg.Done()
		runner := NewRunner(cfg, client)
		result = runner.Run(context.Background())
	}()

	// Kill one node after 10 seconds
	time.Sleep(10 * time.Second)
	t.Log("Killing node 1...")
	if cluster.nodes[1].cmd.Process != nil {
		cluster.nodes[1].cmd.Process.Kill()
	}

	wg.Wait()

	t.Logf("Failover Test Results:")
	t.Logf("  Total Ops: %d", result.TotalOps)
	t.Logf("  Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("  Errors: %d (%.2f%%)", result.FailedOps, float64(result.FailedOps)/float64(result.TotalOps)*100)

	// Some errors expected during failover, but should be limited
	errorRate := float64(result.FailedOps) / float64(result.TotalOps)
	if errorRate > 0.10 { // Allow up to 10% errors during failover
		t.Errorf("Too many errors during failover: %.2f%%", errorRate*100)
	}
}

// generateReport generates a comprehensive load test report.
func generateReport(results []*Result) string {
	var sb strings.Builder

	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║                        AegisKV Load Test Report                              ║\n")
	sb.WriteString("║                        " + time.Now().Format("2006-01-02 15:04:05") + "                                   ║\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════════════╣\n")

	for _, r := range results {
		sb.WriteString(fmt.Sprintf("║ Test: %-71s ║\n", r.Name))
		sb.WriteString("╟──────────────────────────────────────────────────────────────────────────────╢\n")
		sb.WriteString(fmt.Sprintf("║   Duration:     %-60v ║\n", r.Duration))
		sb.WriteString(fmt.Sprintf("║   Total Ops:    %-60d ║\n", r.TotalOps))
		sb.WriteString(fmt.Sprintf("║   Ops/sec:      %-60.2f ║\n", r.OpsPerSecond))
		sb.WriteString(fmt.Sprintf("║   Throughput:   %-57.2f MB/s ║\n", r.ThroughputMBps))
		sb.WriteString("╟──────────────────────────────────────────────────────────────────────────────╢\n")
		sb.WriteString(fmt.Sprintf("║   Avg Latency:  %-60v ║\n", r.AvgLatency))
		sb.WriteString(fmt.Sprintf("║   P50 Latency:  %-60v ║\n", r.P50Latency))
		sb.WriteString(fmt.Sprintf("║   P95 Latency:  %-60v ║\n", r.P95Latency))
		sb.WriteString(fmt.Sprintf("║   P99 Latency:  %-60v ║\n", r.P99Latency))
		sb.WriteString(fmt.Sprintf("║   Max Latency:  %-60v ║\n", r.MaxLatency))
		sb.WriteString(fmt.Sprintf("║   Min Latency:  %-60v ║\n", r.MinLatency))
		sb.WriteString("╟──────────────────────────────────────────────────────────────────────────────╢\n")
		sb.WriteString(fmt.Sprintf("║   Errors:       %-60d ║\n", r.FailedOps))
		sb.WriteString("╠══════════════════════════════════════════════════════════════════════════════╣\n")
	}

	sb.WriteString("╚══════════════════════════════════════════════════════════════════════════════╝\n")

	return sb.String()
}

// RunAll runs all load tests and generates a comprehensive report.
func RunAll(t *testing.T) {
	var allResults []*Result

	// Single node test
	t.Run("SingleNode", func(t *testing.T) {
		cluster, err := NewCluster(1, 20000)
		if err != nil {
			t.Fatalf("failed to start cluster: %v", err)
		}
		defer cluster.Stop()

		client := NewAegisClient(cluster.Addrs())
		defer client.Close()

		cfg := Config{
			Name:        "Single Node",
			Duration:    30 * time.Second,
			Concurrency: 100,
			KeySize:     32,
			ValueSize:   256,
			ReadRatio:   0.8,
			KeySpace:    100000,
			RampUp:      5 * time.Second,
		}

		runner := NewRunner(cfg, client)
		result := runner.Run(context.Background())
		allResults = append(allResults, result)
	})

	// 3-node cluster test
	t.Run("ThreeNodeCluster", func(t *testing.T) {
		cluster, err := NewCluster(3, 20100)
		if err != nil {
			t.Fatalf("failed to start cluster: %v", err)
		}
		defer cluster.Stop()

		client := NewAegisClient(cluster.Addrs())
		defer client.Close()

		cfg := Config{
			Name:        "3-Node Cluster",
			Duration:    30 * time.Second,
			Concurrency: 100,
			KeySize:     32,
			ValueSize:   256,
			ReadRatio:   0.8,
			KeySpace:    100000,
			RampUp:      5 * time.Second,
		}

		runner := NewRunner(cfg, client)
		result := runner.Run(context.Background())
		allResults = append(allResults, result)
	})

	t.Log(generateReport(allResults))
}

// parseEnvInt parses an integer from environment variable.
func parseEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}
