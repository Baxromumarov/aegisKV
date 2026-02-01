// Package metrics provides Prometheus-compatible metrics for AegisKV.
// Uses no external dependencies - exposes /metrics endpoint with text format.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds all application metrics.
type Metrics struct {
	mu sync.RWMutex

	// Counters
	opsTotal       map[string]*uint64 // op -> count
	opsErrors      map[string]*uint64 // op -> error count
	bytesRead      uint64
	bytesWritten   uint64
	replicaSuccess uint64
	replicaFail    uint64

	// Gauges
	cacheSize       int64
	cacheItems      int64
	activeConns     int64
	clusterMembers  int64
	shardsOwned     int64
	memoryUsedBytes int64

	// Histograms (simplified: just track sum and count for avg)
	latencySum   map[string]*uint64 // op -> sum of microseconds
	latencyCount map[string]*uint64 // op -> count

	startTime time.Time
}

var (
	global     *Metrics
	globalOnce sync.Once
)

// Global returns the global metrics instance.
func Global() *Metrics {
	globalOnce.Do(func() {
		global = New()
	})
	return global
}

// New creates a new Metrics instance.
func New() *Metrics {
	ops := []string{"get", "set", "delete", "mget", "mset", "replicate", "auth"}
	m := &Metrics{
		opsTotal:     make(map[string]*uint64),
		opsErrors:    make(map[string]*uint64),
		latencySum:   make(map[string]*uint64),
		latencyCount: make(map[string]*uint64),
		startTime:    time.Now(),
	}
	for _, op := range ops {
		var total, errors, latSum, latCount uint64
		m.opsTotal[op] = &total
		m.opsErrors[op] = &errors
		m.latencySum[op] = &latSum
		m.latencyCount[op] = &latCount
	}
	return m
}

// IncOp increments the operation counter.
func (m *Metrics) IncOp(op string) {
	if p := m.opsTotal[op]; p != nil {
		atomic.AddUint64(p, 1)
	}
}

// IncOpError increments the operation error counter.
func (m *Metrics) IncOpError(op string) {
	if p := m.opsErrors[op]; p != nil {
		atomic.AddUint64(p, 1)
	}
}

// RecordLatency records operation latency.
func (m *Metrics) RecordLatency(op string, d time.Duration) {
	us := uint64(d.Microseconds())
	if p := m.latencySum[op]; p != nil {
		atomic.AddUint64(p, us)
	}
	if p := m.latencyCount[op]; p != nil {
		atomic.AddUint64(p, 1)
	}
}

// AddBytesRead adds to bytes read counter.
func (m *Metrics) AddBytesRead(n int64) {
	atomic.AddUint64(&m.bytesRead, uint64(n))
}

// AddBytesWritten adds to bytes written counter.
func (m *Metrics) AddBytesWritten(n int64) {
	atomic.AddUint64(&m.bytesWritten, uint64(n))
}

// IncReplicaSuccess increments successful replica count.
func (m *Metrics) IncReplicaSuccess() {
	atomic.AddUint64(&m.replicaSuccess, 1)
}

// IncReplicaFail increments failed replica count.
func (m *Metrics) IncReplicaFail() {
	atomic.AddUint64(&m.replicaFail, 1)
}

// SetCacheSize sets the current cache size in bytes.
func (m *Metrics) SetCacheSize(bytes int64) {
	atomic.StoreInt64(&m.cacheSize, bytes)
}

// SetCacheItems sets the current number of items in cache.
func (m *Metrics) SetCacheItems(n int64) {
	atomic.StoreInt64(&m.cacheItems, n)
}

// SetActiveConns sets the active connection count.
func (m *Metrics) SetActiveConns(n int64) {
	atomic.StoreInt64(&m.activeConns, n)
}

// IncActiveConns increments active connections.
func (m *Metrics) IncActiveConns() {
	atomic.AddInt64(&m.activeConns, 1)
}

// DecActiveConns decrements active connections.
func (m *Metrics) DecActiveConns() {
	atomic.AddInt64(&m.activeConns, -1)
}

// SetClusterMembers sets the cluster member count.
func (m *Metrics) SetClusterMembers(n int64) {
	atomic.StoreInt64(&m.clusterMembers, n)
}

// SetShardsOwned sets the number of shards this node owns.
func (m *Metrics) SetShardsOwned(n int64) {
	atomic.StoreInt64(&m.shardsOwned, n)
}

// SetMemoryUsed sets the memory used in bytes.
func (m *Metrics) SetMemoryUsed(bytes int64) {
	atomic.StoreInt64(&m.memoryUsedBytes, bytes)
}

// Handler returns an HTTP handler for /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WritePrometheus(w)
	})
}

// WritePrometheus writes metrics in Prometheus text format.
func (m *Metrics) WritePrometheus(w http.ResponseWriter) {
	var lines []string

	// Helper to add a metric line
	add := func(name, help, typ string, value any, labels ...string) {
		labelStr := ""
		if len(labels) > 0 {
			pairs := make([]string, 0, len(labels)/2)
			for i := 0; i < len(labels); i += 2 {
				pairs = append(pairs, fmt.Sprintf(`%s="%s"`, labels[i], labels[i+1]))
			}
			labelStr = "{" + strings.Join(pairs, ",") + "}"
		}
		lines = append(lines, fmt.Sprintf("# HELP %s %s", name, help))
		lines = append(lines, fmt.Sprintf("# TYPE %s %s", name, typ))
		lines = append(lines, fmt.Sprintf("%s%s %v", name, labelStr, value))
	}

	// Uptime
	add("aegis_uptime_seconds", "Uptime in seconds", "gauge",
		int64(time.Since(m.startTime).Seconds()))

	// Operations total
	ops := make([]string, 0, len(m.opsTotal))
	for op := range m.opsTotal {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	lines = append(lines, "# HELP aegis_ops_total Total operations by type")
	lines = append(lines, "# TYPE aegis_ops_total counter")
	for _, op := range ops {
		v := atomic.LoadUint64(m.opsTotal[op])
		lines = append(lines, fmt.Sprintf(`aegis_ops_total{op="%s"} %d`, op, v))
	}

	lines = append(lines, "# HELP aegis_ops_errors_total Operation errors by type")
	lines = append(lines, "# TYPE aegis_ops_errors_total counter")
	for _, op := range ops {
		v := atomic.LoadUint64(m.opsErrors[op])
		lines = append(lines, fmt.Sprintf(`aegis_ops_errors_total{op="%s"} %d`, op, v))
	}

	// Latency (average in seconds)
	lines = append(lines, "# HELP aegis_ops_latency_avg_seconds Average operation latency")
	lines = append(lines, "# TYPE aegis_ops_latency_avg_seconds gauge")
	for _, op := range ops {
		sum := atomic.LoadUint64(m.latencySum[op])
		count := atomic.LoadUint64(m.latencyCount[op])
		avg := 0.0
		if count > 0 {
			avg = float64(sum) / float64(count) / 1e6 // microseconds to seconds
		}
		lines = append(lines, fmt.Sprintf(`aegis_ops_latency_avg_seconds{op="%s"} %.6f`, op, avg))
	}

	// Bytes
	add("aegis_bytes_read_total", "Total bytes read", "counter",
		atomic.LoadUint64(&m.bytesRead))
	add("aegis_bytes_written_total", "Total bytes written", "counter",
		atomic.LoadUint64(&m.bytesWritten))

	// Replication
	add("aegis_replication_success_total", "Successful replications", "counter",
		atomic.LoadUint64(&m.replicaSuccess))
	add("aegis_replication_fail_total", "Failed replications", "counter",
		atomic.LoadUint64(&m.replicaFail))

	// Gauges
	add("aegis_cache_size_bytes", "Cache size in bytes", "gauge",
		atomic.LoadInt64(&m.cacheSize))
	add("aegis_cache_items", "Number of items in cache", "gauge",
		atomic.LoadInt64(&m.cacheItems))
	add("aegis_connections_active", "Active client connections", "gauge",
		atomic.LoadInt64(&m.activeConns))
	add("aegis_cluster_members", "Number of cluster members", "gauge",
		atomic.LoadInt64(&m.clusterMembers))
	add("aegis_shards_owned", "Number of shards owned by this node", "gauge",
		atomic.LoadInt64(&m.shardsOwned))
	add("aegis_memory_used_bytes", "Memory used by cache", "gauge",
		atomic.LoadInt64(&m.memoryUsedBytes))

	fmt.Fprintln(w, strings.Join(lines, "\n"))
}

// Snapshot returns a snapshot of current metrics as a map.
func (m *Metrics) Snapshot() map[string]any {
	snap := make(map[string]any)

	snap["uptime_seconds"] = int64(time.Since(m.startTime).Seconds())
	snap["bytes_read"] = atomic.LoadUint64(&m.bytesRead)
	snap["bytes_written"] = atomic.LoadUint64(&m.bytesWritten)
	snap["replication_success"] = atomic.LoadUint64(&m.replicaSuccess)
	snap["replication_fail"] = atomic.LoadUint64(&m.replicaFail)
	snap["cache_size_bytes"] = atomic.LoadInt64(&m.cacheSize)
	snap["cache_items"] = atomic.LoadInt64(&m.cacheItems)
	snap["active_connections"] = atomic.LoadInt64(&m.activeConns)
	snap["cluster_members"] = atomic.LoadInt64(&m.clusterMembers)
	snap["shards_owned"] = atomic.LoadInt64(&m.shardsOwned)
	snap["memory_used_bytes"] = atomic.LoadInt64(&m.memoryUsedBytes)

	ops := make(map[string]any)
	for op := range m.opsTotal {
		ops[op] = map[string]uint64{
			"total":  atomic.LoadUint64(m.opsTotal[op]),
			"errors": atomic.LoadUint64(m.opsErrors[op]),
		}
	}
	snap["operations"] = ops

	return snap
}
