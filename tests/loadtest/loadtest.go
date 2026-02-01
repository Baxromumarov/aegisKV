// Package loadtest provides load testing utilities for AegisKV.
package loadtest

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Result represents the result of a load test.
type Result struct {
	Name           string
	Duration       time.Duration
	TotalOps       int64
	SuccessfulOps  int64
	FailedOps      int64
	OpsPerSecond   float64
	AvgLatency     time.Duration
	P50Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	MaxLatency     time.Duration
	MinLatency     time.Duration
	BytesWritten   int64
	BytesRead      int64
	ThroughputMBps float64
}

// String returns a formatted string representation of results.
func (r *Result) String() string {
	return fmt.Sprintf(`
=== %s ===
Duration:        %v
Total Ops:       %d
Successful:      %d (%.2f%%)
Failed:          %d
Ops/sec:         %.2f
Avg Latency:     %v
P50 Latency:     %v
P95 Latency:     %v
P99 Latency:     %v
Max Latency:     %v
Min Latency:     %v
Bytes Written:   %.2f MB
Bytes Read:      %.2f MB
Throughput:      %.2f MB/s
`,
		r.Name,
		r.Duration,
		r.TotalOps,
		r.SuccessfulOps, float64(r.SuccessfulOps)/float64(r.TotalOps)*100,
		r.FailedOps,
		r.OpsPerSecond,
		r.AvgLatency,
		r.P50Latency,
		r.P95Latency,
		r.P99Latency,
		r.MaxLatency,
		r.MinLatency,
		float64(r.BytesWritten)/(1024*1024),
		float64(r.BytesRead)/(1024*1024),
		r.ThroughputMBps,
	)
}

// Config holds load test configuration.
type Config struct {
	Name        string
	Duration    time.Duration
	Concurrency int
	KeySize     int
	ValueSize   int
	ReadRatio   float64 // 0.0 = all writes, 1.0 = all reads
	KeySpace    int     // Number of unique keys
	RampUp      time.Duration
	ReportEvery time.Duration
}

// DefaultConfig returns default load test configuration.
func DefaultConfig() Config {
	return Config{
		Name:        "default",
		Duration:    30 * time.Second,
		Concurrency: 50,
		KeySize:     16,
		ValueSize:   100,
		ReadRatio:   0.8, // 80% reads, 20% writes
		KeySpace:    100000,
		RampUp:      2 * time.Second,
		ReportEvery: 5 * time.Second,
	}
}

// Client interface for load testing different systems.
type Client interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Close() error
}

// Runner executes load tests.
type Runner struct {
	cfg       Config
	client    Client
	latencies []time.Duration
	latMu     sync.Mutex

	totalOps     int64
	successOps   int64
	failedOps    int64
	bytesWritten int64
	bytesRead    int64
}

// NewRunner creates a new load test runner.
func NewRunner(cfg Config, client Client) *Runner {
	// Apply defaults if not set
	if cfg.ReportEvery == 0 {
		cfg.ReportEvery = 5 * time.Second
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 50
	}
	if cfg.KeySpace == 0 {
		cfg.KeySpace = 100000
	}
	if cfg.KeySize == 0 {
		cfg.KeySize = 16
	}
	if cfg.ValueSize == 0 {
		cfg.ValueSize = 100
	}

	return &Runner{
		cfg:       cfg,
		client:    client,
		latencies: make([]time.Duration, 0, 1000000),
	}
}

// Run executes the load test.
func (r *Runner) Run(ctx context.Context) *Result {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Duration+r.cfg.RampUp)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Progress reporter
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(r.cfg.ReportEvery)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)
				ops := atomic.LoadInt64(&r.successOps)
				opsPerSec := float64(ops) / elapsed.Seconds()
				fmt.Printf("[%v] Ops: %d, Rate: %.0f ops/sec\n", elapsed.Round(time.Second), ops, opsPerSec)
			}
		}
	}()

	// Ramp up workers gradually
	workersPerStep := r.cfg.Concurrency / 5
	if workersPerStep < 1 {
		workersPerStep = 1
	}
	stepDelay := r.cfg.RampUp / 5

	workersLaunched := 0
	for workersLaunched < r.cfg.Concurrency {
		toSpawn := workersPerStep
		if workersLaunched+toSpawn > r.cfg.Concurrency {
			toSpawn = r.cfg.Concurrency - workersLaunched
		}

		for i := 0; i < toSpawn; i++ {
			wg.Add(1)
			go r.worker(ctx, &wg)
		}
		workersLaunched += toSpawn

		if workersLaunched < r.cfg.Concurrency {
			time.Sleep(stepDelay)
		}
	}

	wg.Wait()
	close(progressDone)
	duration := time.Since(startTime)

	return r.computeResult(duration)
}

// worker executes operations until context is cancelled.
func (r *Runner) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	value := make([]byte, r.cfg.ValueSize)
	rng.Read(value)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("key-%0*d", r.cfg.KeySize-4, rng.Intn(r.cfg.KeySpace))
		isRead := rng.Float64() < r.cfg.ReadRatio

		start := time.Now()
		var err error
		var bytesTransferred int64

		if isRead {
			var val []byte
			val, err = r.client.Get(ctx, key)
			if err == nil && val != nil {
				bytesTransferred = int64(len(val))
			}
		} else {
			err = r.client.Set(ctx, key, value)
			if err == nil {
				bytesTransferred = int64(len(value))
			}
		}

		latency := time.Since(start)
		atomic.AddInt64(&r.totalOps, 1)

		if err != nil {
			atomic.AddInt64(&r.failedOps, 1)
		} else {
			atomic.AddInt64(&r.successOps, 1)
			if isRead {
				atomic.AddInt64(&r.bytesRead, bytesTransferred)
			} else {
				atomic.AddInt64(&r.bytesWritten, bytesTransferred)
			}

			r.latMu.Lock()
			r.latencies = append(r.latencies, latency)
			r.latMu.Unlock()
		}
	}
}

// computeResult calculates final results.
func (r *Runner) computeResult(duration time.Duration) *Result {
	r.latMu.Lock()
	latencies := make([]time.Duration, len(r.latencies))
	copy(latencies, r.latencies)
	r.latMu.Unlock()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var avgLatency, p50, p95, p99, maxLat, minLat time.Duration
	if len(latencies) > 0 {
		var sum time.Duration
		for _, l := range latencies {
			sum += l
		}
		avgLatency = sum / time.Duration(len(latencies))
		minLat = latencies[0]
		maxLat = latencies[len(latencies)-1]
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}

	totalBytes := float64(r.bytesWritten + r.bytesRead)
	throughputMBps := totalBytes / duration.Seconds() / (1024 * 1024)

	return &Result{
		Name:           r.cfg.Name,
		Duration:       duration,
		TotalOps:       r.totalOps,
		SuccessfulOps:  r.successOps,
		FailedOps:      r.failedOps,
		OpsPerSecond:   float64(r.successOps) / duration.Seconds(),
		AvgLatency:     avgLatency,
		P50Latency:     p50,
		P95Latency:     p95,
		P99Latency:     p99,
		MaxLatency:     maxLat,
		MinLatency:     minLat,
		BytesWritten:   r.bytesWritten,
		BytesRead:      r.bytesRead,
		ThroughputMBps: throughputMBps,
	}
}

// CompareResults returns a comparison table of multiple results.
func CompareResults(results []*Result) string {
	if len(results) == 0 {
		return ""
	}

	var sb strings.Builder
	separator := strings.Repeat("=", 80)
	line := strings.Repeat("-", 80)

	sb.WriteString("\n" + separator + "\n")
	sb.WriteString("LOAD TEST COMPARISON\n")
	sb.WriteString(separator + "\n")
	sb.WriteString(fmt.Sprintf("\n%-20s %15s %15s %15s %15s\n", "System", "Ops/sec", "Avg Latency", "P99 Latency", "Throughput"))
	sb.WriteString(line + "\n")

	for _, r := range results {
		sb.WriteString(fmt.Sprintf("%-20s %15.0f %15v %15v %12.2f MB/s\n",
			r.Name,
			r.OpsPerSecond,
			r.AvgLatency.Round(time.Microsecond),
			r.P99Latency.Round(time.Microsecond),
			r.ThroughputMBps,
		))
	}
	sb.WriteString(line + "\n")

	// Find winner
	var bestOps *Result
	for _, r := range results {
		if bestOps == nil || r.OpsPerSecond > bestOps.OpsPerSecond {
			bestOps = r
		}
	}
	sb.WriteString(fmt.Sprintf("\n🏆 Best throughput: %s (%.0f ops/sec)\n", bestOps.Name, bestOps.OpsPerSecond))

	return sb.String()
}

// Repeat string helper
func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
