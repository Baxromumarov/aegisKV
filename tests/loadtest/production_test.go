//go:build loadtest
// +build loadtest

// Package loadtest provides production readiness tests for AegisKV.
package loadtest

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ========================================
// 1. MEMORY/SOAK TEST
// ========================================

// MemoryStats holds memory usage statistics.
type MemoryStats struct {
	Timestamp   time.Time
	Alloc       uint64
	TotalAlloc  uint64
	Sys         uint64
	HeapAlloc   uint64
	HeapSys     uint64
	HeapObjects uint64
	NumGC       uint32
}

// SoakTestResult holds results from a soak test.
type SoakTestResult struct {
	Duration         time.Duration
	TotalOps         int64
	SuccessOps       int64
	FailedOps        int64
	OpsPerSecond     float64
	MemorySnapshots  []MemoryStats
	MemoryGrowth     float64 // Percentage growth from start to end
	LeakDetected     bool
	LeakDescription  string
	StabilityScore   float64 // 0-100, higher is better
	PerformanceDrift float64 // Percentage change in ops/sec from start to end
}

// TestMemorySoak runs a sustained load test to detect memory leaks.
func TestMemorySoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	// Allow configurable duration via environment
	durationStr := os.Getenv("SOAK_TEST_DURATION")
	duration := 10 * time.Minute
	if durationStr != "" {
		if d, err := time.ParseDuration(durationStr); err == nil {
			duration = d
		}
	}

	t.Logf("Starting soak test for %v", duration)

	cluster, err := NewCluster(3, 21000)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	result := runSoakTest(ctx, t, client, duration)

	// Report results
	t.Logf("\n=== Soak Test Results ===")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Total Ops: %d", result.TotalOps)
	t.Logf("Success Rate: %.4f%%", float64(result.SuccessOps)/float64(result.TotalOps)*100)
	t.Logf("Ops/sec: %.2f", result.OpsPerSecond)
	t.Logf("Memory Growth: %.2f%%", result.MemoryGrowth)
	t.Logf("Performance Drift: %.2f%%", result.PerformanceDrift)
	t.Logf("Stability Score: %.1f/100", result.StabilityScore)

	// Memory leak detection
	if result.LeakDetected {
		t.Errorf("MEMORY LEAK DETECTED: %s", result.LeakDescription)
	}

	// Stability checks
	if result.MemoryGrowth > 100 { // More than 2x growth suggests leak
		t.Errorf("Excessive memory growth: %.2f%% (threshold: 100%%)", result.MemoryGrowth)
	}

	if result.PerformanceDrift < -30 {
		t.Errorf("Significant performance degradation: %.2f%% (threshold: -30%%)", result.PerformanceDrift)
	}

	if result.StabilityScore < 70 {
		t.Errorf("Low stability score: %.1f (threshold: 70)", result.StabilityScore)
	}
}

func runSoakTest(ctx context.Context, t *testing.T, client Client, duration time.Duration) *SoakTestResult {
	var (
		totalOps   int64
		successOps int64
		failedOps  int64
	)

	memSnapshots := make([]MemoryStats, 0, 100)
	opsSnapshots := make([]float64, 0, 100)

	// Collect initial memory stats
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	memSnapshots = append(memSnapshots, getMemoryStats())

	snapshotInterval := duration / 20
	if snapshotInterval < 10*time.Second {
		snapshotInterval = 10 * time.Second
	}

	var wg sync.WaitGroup
	startTime := time.Now()
	var lastSnapshotOps int64
	lastSnapshotTime := startTime

	// Progress and snapshot reporter
	snapshotDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(snapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-snapshotDone:
				return
			case <-ticker.C:
				// Memory snapshot
				memSnapshots = append(memSnapshots, getMemoryStats())

				// Performance snapshot
				currentOps := atomic.LoadInt64(&successOps)
				elapsed := time.Since(lastSnapshotTime).Seconds()
				opsPerSec := float64(currentOps-lastSnapshotOps) / elapsed
				opsSnapshots = append(opsSnapshots, opsPerSec)
				lastSnapshotOps = currentOps
				lastSnapshotTime = time.Now()

				t.Logf("[%v] Ops: %d, Rate: %.0f ops/sec, Heap: %.2f MB",
					time.Since(startTime).Round(time.Second),
					currentOps, opsPerSec,
					float64(memSnapshots[len(memSnapshots)-1].HeapAlloc)/(1024*1024))
			}
		}
	}()

	// Run workers
	concurrency := 100
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			soakWorker(ctx, client, &totalOps, &successOps, &failedOps)
		}()
	}

	wg.Wait()
	close(snapshotDone)
	totalDuration := time.Since(startTime)

	// Final memory snapshot
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	memSnapshots = append(memSnapshots, getMemoryStats())

	// Analyze results
	result := &SoakTestResult{
		Duration:        totalDuration,
		TotalOps:        totalOps,
		SuccessOps:      successOps,
		FailedOps:       failedOps,
		OpsPerSecond:    float64(successOps) / totalDuration.Seconds(),
		MemorySnapshots: memSnapshots,
	}

	analyzeMemoryTrend(result)
	analyzePerformanceDrift(result, opsSnapshots)
	calculateStabilityScore(result, opsSnapshots)

	return result
}

func soakWorker(ctx context.Context, client Client, totalOps, successOps, failedOps *int64) {
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i % 256)
	}

	keySpace := 100000
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("soak-key-%d", i%keySpace)
		atomic.AddInt64(totalOps, 1)

		var err error
		if i%5 == 0 { // 20% writes
			err = client.Set(ctx, key, value)
		} else { // 80% reads
			_, err = client.Get(ctx, key)
		}

		if err != nil {
			atomic.AddInt64(failedOps, 1)
		} else {
			atomic.AddInt64(successOps, 1)
		}
		i++
	}
}

func getMemoryStats() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		Timestamp:   time.Now(),
		Alloc:       m.Alloc,
		TotalAlloc:  m.TotalAlloc,
		Sys:         m.Sys,
		HeapAlloc:   m.HeapAlloc,
		HeapSys:     m.HeapSys,
		HeapObjects: m.HeapObjects,
		NumGC:       m.NumGC,
	}
}

func analyzeMemoryTrend(result *SoakTestResult) {
	if len(result.MemorySnapshots) < 2 {
		return
	}

	first := result.MemorySnapshots[0]
	last := result.MemorySnapshots[len(result.MemorySnapshots)-1]

	if first.HeapAlloc > 0 {
		result.MemoryGrowth = (float64(last.HeapAlloc) - float64(first.HeapAlloc)) / float64(first.HeapAlloc) * 100
	}

	// Check for monotonic increase (leak pattern)
	if len(result.MemorySnapshots) > 5 {
		increasing := 0
		for i := 1; i < len(result.MemorySnapshots); i++ {
			if result.MemorySnapshots[i].HeapAlloc > result.MemorySnapshots[i-1].HeapAlloc {
				increasing++
			}
		}
		ratio := float64(increasing) / float64(len(result.MemorySnapshots)-1)
		if ratio > 0.8 && result.MemoryGrowth > 50 {
			result.LeakDetected = true
			result.LeakDescription = fmt.Sprintf("Memory increased in %.0f%% of samples, total growth: %.2f%%", ratio*100, result.MemoryGrowth)
		}
	}
}

func analyzePerformanceDrift(result *SoakTestResult, opsSnapshots []float64) {
	if len(opsSnapshots) < 4 {
		return
	}

	// Compare first quarter to last quarter
	quarterLen := len(opsSnapshots) / 4
	firstQuarter := opsSnapshots[:quarterLen]
	lastQuarter := opsSnapshots[len(opsSnapshots)-quarterLen:]

	firstAvg := average(firstQuarter)
	lastAvg := average(lastQuarter)

	if firstAvg > 0 {
		result.PerformanceDrift = (lastAvg - firstAvg) / firstAvg * 100
	}
}

func calculateStabilityScore(result *SoakTestResult, opsSnapshots []float64) {
	if len(opsSnapshots) < 2 {
		result.StabilityScore = 100
		return
	}

	// Calculate coefficient of variation (CV)
	avg := average(opsSnapshots)
	if avg == 0 {
		result.StabilityScore = 0
		return
	}

	var sumSqDiff float64
	for _, v := range opsSnapshots {
		diff := v - avg
		sumSqDiff += diff * diff
	}
	stdDev := math.Sqrt(sumSqDiff / float64(len(opsSnapshots)))
	cv := stdDev / avg

	// Convert CV to stability score (lower CV = higher stability)
	// CV of 0 = 100 score, CV of 0.5 = 50 score, CV of 1+ = 0 score
	result.StabilityScore = math.Max(0, 100*(1-cv))
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// ========================================
// 2. LATENCY PERCENTILE TEST (P99.9, P99.99)
// ========================================

// TailLatencyResult holds tail latency test results.
type TailLatencyResult struct {
	Duration  time.Duration
	TotalOps  int64
	P50       time.Duration
	P90       time.Duration
	P95       time.Duration
	P99       time.Duration
	P999      time.Duration // P99.9
	P9999     time.Duration // P99.99
	Max       time.Duration
	Histogram map[string]int64 // Latency buckets for distribution
	OpsPerSec float64
	ErrorRate float64
}

// TestTailLatency measures P99.9 and P99.99 latencies under load.
func TestTailLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping tail latency test in short mode")
	}

	cluster, err := NewCluster(3, 21100)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	duration := 60 * time.Second

	result := measureTailLatency(ctx, t, client, duration)

	t.Logf("\n=== Tail Latency Report ===")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Total Ops: %d", result.TotalOps)
	t.Logf("Ops/sec: %.2f", result.OpsPerSec)
	t.Logf("Error Rate: %.4f%%", result.ErrorRate*100)
	t.Log("")
	t.Logf("Latency Percentiles:")
	t.Logf("  P50:    %v", result.P50)
	t.Logf("  P90:    %v", result.P90)
	t.Logf("  P95:    %v", result.P95)
	t.Logf("  P99:    %v", result.P99)
	t.Logf("  P99.9:  %v", result.P999)
	t.Logf("  P99.99: %v", result.P9999)
	t.Logf("  Max:    %v", result.Max)
	t.Log("")
	t.Log("Latency Distribution:")
	buckets := []string{"<1ms", "1-5ms", "5-10ms", "10-50ms", "50-100ms", "100-500ms", ">500ms"}
	for _, bucket := range buckets {
		if count, ok := result.Histogram[bucket]; ok {
			pct := float64(count) / float64(result.TotalOps) * 100
			t.Logf("  %-10s: %8d (%.2f%%)", bucket, count, pct)
		}
	}

	// Assertions for production readiness
	if result.P999 > 100*time.Millisecond {
		t.Errorf("P99.9 latency too high: %v (threshold: 100ms)", result.P999)
	}
	if result.P9999 > 500*time.Millisecond {
		t.Errorf("P99.99 latency too high: %v (threshold: 500ms)", result.P9999)
	}
}

func measureTailLatency(ctx context.Context, t *testing.T, client Client, duration time.Duration) *TailLatencyResult {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Pre-allocate for high-precision measurements
	latencies := make([]time.Duration, 0, 5000000)
	var latMu sync.Mutex
	var totalOps, successOps int64

	var wg sync.WaitGroup
	startTime := time.Now()

	// Run concurrent workers
	concurrency := 100
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLatencies := make([]time.Duration, 0, 10000)
			value := make([]byte, 256)
			keySpace := 100000

			for j := 0; ; j++ {
				select {
				case <-ctx.Done():
					latMu.Lock()
					latencies = append(latencies, localLatencies...)
					latMu.Unlock()
					return
				default:
				}

				key := fmt.Sprintf("latency-key-%d-%d", workerID, j%keySpace)
				start := time.Now()

				var err error
				if j%5 == 0 {
					err = client.Set(ctx, key, value)
				} else {
					_, err = client.Get(ctx, key)
				}

				latency := time.Since(start)
				atomic.AddInt64(&totalOps, 1)

				if err == nil {
					atomic.AddInt64(&successOps, 1)
					localLatencies = append(localLatencies, latency)
				}

				// Periodically flush to main slice
				if len(localLatencies) >= 10000 {
					latMu.Lock()
					latencies = append(latencies, localLatencies...)
					latMu.Unlock()
					localLatencies = localLatencies[:0]
				}
			}
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	result := &TailLatencyResult{
		Duration:  totalDuration,
		TotalOps:  totalOps,
		OpsPerSec: float64(successOps) / totalDuration.Seconds(),
		ErrorRate: float64(totalOps-successOps) / float64(totalOps),
		Histogram: make(map[string]int64),
	}

	if len(latencies) > 0 {
		result.P50 = percentile(latencies, 50)
		result.P90 = percentile(latencies, 90)
		result.P95 = percentile(latencies, 95)
		result.P99 = percentile(latencies, 99)
		result.P999 = percentile(latencies, 99.9)
		result.P9999 = percentile(latencies, 99.99)
		result.Max = latencies[len(latencies)-1]

		// Build histogram
		for _, lat := range latencies {
			switch {
			case lat < time.Millisecond:
				result.Histogram["<1ms"]++
			case lat < 5*time.Millisecond:
				result.Histogram["1-5ms"]++
			case lat < 10*time.Millisecond:
				result.Histogram["5-10ms"]++
			case lat < 50*time.Millisecond:
				result.Histogram["10-50ms"]++
			case lat < 100*time.Millisecond:
				result.Histogram["50-100ms"]++
			case lat < 500*time.Millisecond:
				result.Histogram["100-500ms"]++
			default:
				result.Histogram[">500ms"]++
			}
		}
	}

	return result
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ========================================
// 3. BACKPRESSURE TEST
// ========================================

// BackpressureResult holds backpressure test results.
type BackpressureResult struct {
	Duration            time.Duration
	TotalRequests       int64
	SuccessRequests     int64
	RejectedRequests    int64
	TimeoutRequests     int64
	OtherErrors         int64
	OpsPerSec           float64
	RejectionRate       float64
	GracefulDegradation bool
	RecoveryTime        time.Duration
}

// TestBackpressure tests graceful degradation under overwhelming load.
func TestBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backpressure test in short mode")
	}

	cluster, err := NewCluster(3, 21200)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	result := runBackpressureTest(ctx, t, client)

	t.Logf("\n=== Backpressure Test Results ===")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Total Requests: %d", result.TotalRequests)
	t.Logf("Successful: %d (%.2f%%)", result.SuccessRequests,
		float64(result.SuccessRequests)/float64(result.TotalRequests)*100)
	t.Logf("Rejected: %d (%.2f%%)", result.RejectedRequests, result.RejectionRate*100)
	t.Logf("Timeouts: %d", result.TimeoutRequests)
	t.Logf("Other Errors: %d", result.OtherErrors)
	t.Logf("Ops/sec: %.2f", result.OpsPerSec)
	t.Logf("Graceful Degradation: %v", result.GracefulDegradation)
	t.Logf("Recovery Time: %v", result.RecoveryTime)

	// Assertions
	if !result.GracefulDegradation {
		t.Error("Server did not degrade gracefully under load")
	}
}

func runBackpressureTest(ctx context.Context, t *testing.T, client Client) *BackpressureResult {
	result := &BackpressureResult{}
	startTime := time.Now()

	var wg sync.WaitGroup
	var totalReqs, successReqs, rejects, timeouts, others int64

	// Phase 1: Overwhelming burst (5000 concurrent requests)
	t.Log("Phase 1: Overwhelming burst (5000 concurrent requests)...")
	burstStart := time.Now()
	burstCtx, burstCancel := context.WithTimeout(ctx, 30*time.Second)

	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("burst-key-%d", id)
			value := make([]byte, 256)

			opCtx, cancel := context.WithTimeout(burstCtx, 5*time.Second)
			defer cancel()

			atomic.AddInt64(&totalReqs, 1)
			err := client.Set(opCtx, key, value)

			if err == nil {
				atomic.AddInt64(&successReqs, 1)
			} else {
				errStr := err.Error()
				if contains(errStr, "rejected", "backpressure", "overloaded") {
					atomic.AddInt64(&rejects, 1)
				} else if contains(errStr, "timeout", "deadline") {
					atomic.AddInt64(&timeouts, 1)
				} else {
					atomic.AddInt64(&others, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	burstCancel()
	burstDuration := time.Since(burstStart)
	t.Logf("  Burst completed in %v", burstDuration)

	// Phase 2: Recovery - normal load
	t.Log("Phase 2: Recovery test (normal load)...")
	recoveryStart := time.Now()
	recoverySuccess := int64(0)
	recoveryTotal := int64(0)
	recovered := false
	var recoveryTime time.Duration

	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)

		// Try 10 concurrent requests
		var localSuccess int64
		var localWg sync.WaitGroup
		for j := 0; j < 10; j++ {
			localWg.Add(1)
			go func(id int) {
				defer localWg.Done()
				key := fmt.Sprintf("recovery-key-%d", id)
				opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()

				atomic.AddInt64(&recoveryTotal, 1)
				if err := client.Set(opCtx, key, []byte("test")); err == nil {
					atomic.AddInt64(&localSuccess, 1)
					atomic.AddInt64(&recoverySuccess, 1)
				}
			}(j)
		}
		localWg.Wait()

		// Consider recovered if >80% success rate
		if localSuccess >= 8 && !recovered {
			recovered = true
			recoveryTime = time.Since(recoveryStart)
			t.Logf("  Recovered after %v", recoveryTime)
		}
	}

	result.Duration = time.Since(startTime)
	result.TotalRequests = totalReqs + recoveryTotal
	result.SuccessRequests = successReqs + recoverySuccess
	result.RejectedRequests = rejects
	result.TimeoutRequests = timeouts
	result.OtherErrors = others
	result.OpsPerSec = float64(result.SuccessRequests) / result.Duration.Seconds()
	result.RejectionRate = float64(rejects) / float64(totalReqs)
	result.GracefulDegradation = recovered && (result.RejectedRequests+result.TimeoutRequests < result.TotalRequests/2)
	result.RecoveryTime = recoveryTime

	return result
}

func contains(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// ========================================
// 4. MIXED WORKLOAD TEST
// ========================================

// MixedWorkloadResult holds mixed workload test results.
type MixedWorkloadResult struct {
	Duration   time.Duration
	Workloads  map[string]*WorkloadStats
	TotalOps   int64
	SuccessOps int64
	OverallP99 time.Duration
}

// WorkloadStats holds stats for a specific workload pattern.
type WorkloadStats struct {
	Name       string
	Ops        int64
	Errors     int64
	AvgLatency time.Duration
	P99Latency time.Duration
}

// TestMixedWorkload tests realistic production workload patterns.
func TestMixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mixed workload test in short mode")
	}

	cluster, err := NewCluster(3, 21300)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	result := runMixedWorkloadTest(ctx, t, client, 60*time.Second)

	t.Logf("\n=== Mixed Workload Results ===")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Total Ops: %d", result.TotalOps)
	t.Logf("Success Rate: %.4f%%", float64(result.SuccessOps)/float64(result.TotalOps)*100)
	t.Logf("Overall P99: %v", result.OverallP99)
	t.Log("")
	t.Log("Per-Workload Statistics:")
	t.Logf("%-20s %10s %10s %12s %12s", "Workload", "Ops", "Errors", "Avg Lat", "P99 Lat")
	t.Log("------------------------------------------------------------")
	for name, stats := range result.Workloads {
		t.Logf("%-20s %10d %10d %12v %12v",
			name, stats.Ops, stats.Errors, stats.AvgLatency, stats.P99Latency)
	}
}

func runMixedWorkloadTest(ctx context.Context, t *testing.T, client Client, duration time.Duration) *MixedWorkloadResult {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	result := &MixedWorkloadResult{
		Workloads: make(map[string]*WorkloadStats),
	}

	var wg sync.WaitGroup
	startTime := time.Now()
	allLatencies := make([]time.Duration, 0, 100000)
	var latMu sync.Mutex
	var totalOps, successOps int64

	// Define workloads
	workloads := []struct {
		name        string
		concurrency int
		valueSize   int
		readRatio   float64
		keySpace    int
	}{
		{"hot-keys", 20, 128, 0.95, 100},        // Hot key access pattern
		{"cold-keys", 20, 128, 0.90, 1000000},   // Cold key access pattern
		{"large-values", 10, 4096, 0.80, 10000}, // Large value workload
		{"write-heavy", 20, 256, 0.20, 50000},   // Write-heavy workload
		{"scan-pattern", 10, 256, 1.0, 100000},  // Sequential read pattern
	}

	for _, wl := range workloads {
		stats := &WorkloadStats{Name: wl.name}
		result.Workloads[wl.name] = stats
		localLatencies := make([]time.Duration, 0, 10000)
		var localMu sync.Mutex

		for i := 0; i < wl.concurrency; i++ {
			wg.Add(1)
			go func(wl struct {
				name        string
				concurrency int
				valueSize   int
				readRatio   float64
				keySpace    int
			}) {
				defer wg.Done()
				value := make([]byte, wl.valueSize)

				for j := 0; ; j++ {
					select {
					case <-ctx.Done():
						localMu.Lock()
						// Calculate stats
						if len(localLatencies) > 0 {
							sort.Slice(localLatencies, func(a, b int) bool {
								return localLatencies[a] < localLatencies[b]
							})
							var sum time.Duration
							for _, l := range localLatencies {
								sum += l
							}
							stats.AvgLatency = sum / time.Duration(len(localLatencies))
							stats.P99Latency = localLatencies[len(localLatencies)*99/100]

							latMu.Lock()
							allLatencies = append(allLatencies, localLatencies...)
							latMu.Unlock()
						}
						localMu.Unlock()
						return
					default:
					}

					key := fmt.Sprintf("%s-key-%d", wl.name, j%wl.keySpace)
					start := time.Now()

					var err error
					isRead := float64(j%100)/100.0 < wl.readRatio
					if isRead {
						_, err = client.Get(ctx, key)
					} else {
						err = client.Set(ctx, key, value)
					}

					latency := time.Since(start)
					atomic.AddInt64(&totalOps, 1)
					atomic.AddInt64(&stats.Ops, 1)

					if err != nil {
						atomic.AddInt64(&stats.Errors, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
						localMu.Lock()
						localLatencies = append(localLatencies, latency)
						localMu.Unlock()
					}
				}
			}(wl)
		}
	}

	wg.Wait()

	result.Duration = time.Since(startTime)
	result.TotalOps = totalOps
	result.SuccessOps = successOps

	// Calculate overall P99
	latMu.Lock()
	if len(allLatencies) > 0 {
		sort.Slice(allLatencies, func(i, j int) bool {
			return allLatencies[i] < allLatencies[j]
		})
		result.OverallP99 = allLatencies[len(allLatencies)*99/100]
	}
	latMu.Unlock()

	return result
}

// ========================================
// 5. RECOVERY TIME TEST
// ========================================

// RecoveryResult holds recovery time test results.
type RecoveryResult struct {
	NodeRestartTime  time.Duration
	FirstSuccessTime time.Duration
	FullRecoveryTime time.Duration
	DataIntegrity    bool
	KeysVerified     int
	KeysMissing      int
}

// TestRecoveryTime measures how quickly the system recovers after a node restart.
func TestRecoveryTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping recovery time test in short mode")
	}

	cluster, err := NewCluster(3, 21400)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	client := NewAegisClient(cluster.Addrs())
	defer client.Close()

	ctx := context.Background()
	result := measureRecoveryTime(ctx, t, client, cluster)

	t.Logf("\n=== Recovery Time Results ===")
	t.Logf("Node Restart Time: %v", result.NodeRestartTime)
	t.Logf("First Success After Restart: %v", result.FirstSuccessTime)
	t.Logf("Full Recovery Time: %v", result.FullRecoveryTime)
	t.Logf("Data Integrity: %v", result.DataIntegrity)
	t.Logf("Keys Verified: %d", result.KeysVerified)
	t.Logf("Keys Missing: %d", result.KeysMissing)

	// Assertions
	if result.FullRecoveryTime > 30*time.Second {
		t.Errorf("Recovery time too slow: %v (threshold: 30s)", result.FullRecoveryTime)
	}
}

func measureRecoveryTime(ctx context.Context, t *testing.T, client Client, cluster *Cluster) *RecoveryResult {
	result := &RecoveryResult{}

	// Phase 1: Pre-populate data
	t.Log("Phase 1: Populating data...")
	testKeys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("recovery-test-key-%d", i)
		testKeys[i] = key
		if err := client.Set(ctx, key, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Logf("Warning: failed to set key %s: %v", key, err)
		}
	}
	time.Sleep(1 * time.Second) // Allow replication

	// Phase 2: Kill a node
	t.Log("Phase 2: Killing node 1...")
	killStart := time.Now()
	if cluster.nodes[1].cmd.Process != nil {
		cluster.nodes[1].cmd.Process.Kill()
	}

	// Phase 3: Measure time to first successful operation
	t.Log("Phase 3: Measuring recovery...")
	restartStart := time.Now()
	result.NodeRestartTime = restartStart.Sub(killStart)

	firstSuccess := false
	consecutiveSuccess := 0
	targetSuccess := 10

	for i := 0; i < 1000; i++ {
		time.Sleep(100 * time.Millisecond)

		key := fmt.Sprintf("recovery-test-key-%d", i%100)
		if _, err := client.Get(ctx, key); err == nil {
			if !firstSuccess {
				result.FirstSuccessTime = time.Since(restartStart)
				firstSuccess = true
				t.Logf("  First success after %v", result.FirstSuccessTime)
			}
			consecutiveSuccess++
			if consecutiveSuccess >= targetSuccess {
				result.FullRecoveryTime = time.Since(restartStart)
				t.Logf("  Full recovery after %v", result.FullRecoveryTime)
				break
			}
		} else {
			consecutiveSuccess = 0
		}
	}

	// Phase 4: Verify data integrity
	t.Log("Phase 4: Verifying data integrity...")
	for _, key := range testKeys {
		if _, err := client.Get(ctx, key); err == nil {
			result.KeysVerified++
		} else {
			result.KeysMissing++
		}
	}

	result.DataIntegrity = result.KeysMissing == 0 || float64(result.KeysMissing)/float64(len(testKeys)) < 0.01

	return result
}

// ========================================
// 6. CONNECTION CHURN TEST
// ========================================

// ConnectionChurnResult holds connection churn test results.
type ConnectionChurnResult struct {
	Duration          time.Duration
	TotalConnections  int64
	SuccessfulConns   int64
	FailedConnections int64
	TotalOps          int64
	SuccessOps        int64
	AvgConnTime       time.Duration
	MaxConnTime       time.Duration
	ResourceLeaks     bool
}

// TestConnectionChurn tests rapid connect/disconnect cycles.
func TestConnectionChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping connection churn test in short mode")
	}

	cluster, err := NewCluster(3, 21500)
	if err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	ctx := context.Background()
	result := runConnectionChurnTest(ctx, t, cluster.Addrs(), 60*time.Second)

	t.Logf("\n=== Connection Churn Results ===")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Total Connections: %d", result.TotalConnections)
	t.Logf("Successful Connections: %d (%.2f%%)", result.SuccessfulConns,
		float64(result.SuccessfulConns)/float64(result.TotalConnections)*100)
	t.Logf("Failed Connections: %d", result.FailedConnections)
	t.Logf("Total Ops: %d", result.TotalOps)
	t.Logf("Successful Ops: %d (%.2f%%)", result.SuccessOps,
		float64(result.SuccessOps)/float64(result.TotalOps)*100)
	t.Logf("Avg Connection Time: %v", result.AvgConnTime)
	t.Logf("Max Connection Time: %v", result.MaxConnTime)
	t.Logf("Resource Leaks Detected: %v", result.ResourceLeaks)

	// Assertions
	successRate := float64(result.SuccessfulConns) / float64(result.TotalConnections)
	if successRate < 0.95 {
		t.Errorf("Connection success rate too low: %.2f%% (threshold: 95%%)", successRate*100)
	}
}

func runConnectionChurnTest(ctx context.Context, t *testing.T, addrs []string, duration time.Duration) *ConnectionChurnResult {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	result := &ConnectionChurnResult{}
	startTime := time.Now()

	var wg sync.WaitGroup
	var totalConns, successConns, failedConns int64
	var totalOps, successOps int64
	connTimes := make([]time.Duration, 0, 10000)
	var connMu sync.Mutex

	// Goroutine counter for leak detection
	initialGoroutines := runtime.NumGoroutine()

	// Run churn workers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localConnTimes := make([]time.Duration, 0, 100)

			for {
				select {
				case <-ctx.Done():
					connMu.Lock()
					connTimes = append(connTimes, localConnTimes...)
					connMu.Unlock()
					return
				default:
				}

				// Create new connection
				connStart := time.Now()
				conn, err := net.DialTimeout("tcp", addrs[0], 5*time.Second)
				connTime := time.Since(connStart)
				atomic.AddInt64(&totalConns, 1)

				if err != nil {
					atomic.AddInt64(&failedConns, 1)
					continue
				}

				atomic.AddInt64(&successConns, 1)
				localConnTimes = append(localConnTimes, connTime)

				// Do a quick operation
				client := NewAegisClient([]string{addrs[0]})
				atomic.AddInt64(&totalOps, 1)
				if err := client.Set(ctx, "churn-key", []byte("value")); err == nil {
					atomic.AddInt64(&successOps, 1)
				}
				client.Close()
				conn.Close()

				// Small delay between churns
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	result.Duration = time.Since(startTime)
	result.TotalConnections = totalConns
	result.SuccessfulConns = successConns
	result.FailedConnections = failedConns
	result.TotalOps = totalOps
	result.SuccessOps = successOps

	// Calculate connection time stats
	connMu.Lock()
	if len(connTimes) > 0 {
		var sum time.Duration
		var maxTime time.Duration
		for _, ct := range connTimes {
			sum += ct
			if ct > maxTime {
				maxTime = ct
			}
		}
		result.AvgConnTime = sum / time.Duration(len(connTimes))
		result.MaxConnTime = maxTime
	}
	connMu.Unlock()

	// Check for goroutine leaks
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+10 {
		result.ResourceLeaks = true
		t.Logf("Warning: Goroutine count increased from %d to %d", initialGoroutines, finalGoroutines)
	}

	return result
}
