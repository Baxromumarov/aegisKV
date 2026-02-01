package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNew tests creating a new Metrics instance.
func TestNew(t *testing.T) {
	m := New()

	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}

	// Check that all ops are initialized
	ops := []string{"get", "set", "delete", "mget", "mset", "replicate", "auth"}
	for _, op := range ops {
		if m.opsTotal[op] == nil {
			t.Errorf("opsTotal[%s] not initialized", op)
		}
		if m.opsErrors[op] == nil {
			t.Errorf("opsErrors[%s] not initialized", op)
		}
		if m.latencySum[op] == nil {
			t.Errorf("latencySum[%s] not initialized", op)
		}
		if m.latencyCount[op] == nil {
			t.Errorf("latencyCount[%s] not initialized", op)
		}
	}
}

// TestGlobal tests the global singleton.
func TestGlobal(t *testing.T) {
	m1 := Global()
	m2 := Global()

	if m1 != m2 {
		t.Error("Global() should return same instance")
	}
	if m1 == nil {
		t.Error("Global() should not return nil")
	}
}

// TestIncOp tests incrementing operation counters.
func TestIncOp(t *testing.T) {
	m := New()

	m.IncOp("get")
	m.IncOp("get")
	m.IncOp("set")

	if *m.opsTotal["get"] != 2 {
		t.Errorf("expected 2 get ops, got %d", *m.opsTotal["get"])
	}
	if *m.opsTotal["set"] != 1 {
		t.Errorf("expected 1 set op, got %d", *m.opsTotal["set"])
	}
}

// TestIncOpError tests incrementing error counters.
func TestIncOpError(t *testing.T) {
	m := New()

	m.IncOpError("get")
	m.IncOpError("get")
	m.IncOpError("set")

	if *m.opsErrors["get"] != 2 {
		t.Errorf("expected 2 get errors, got %d", *m.opsErrors["get"])
	}
	if *m.opsErrors["set"] != 1 {
		t.Errorf("expected 1 set error, got %d", *m.opsErrors["set"])
	}
}

// TestRecordLatency tests recording operation latency.
func TestRecordLatency(t *testing.T) {
	m := New()

	m.RecordLatency("get", 100*time.Microsecond)
	m.RecordLatency("get", 200*time.Microsecond)
	m.RecordLatency("set", 150*time.Microsecond)

	if *m.latencyCount["get"] != 2 {
		t.Errorf("expected 2 get latency records, got %d", *m.latencyCount["get"])
	}
	if *m.latencySum["get"] != 300 {
		t.Errorf("expected 300us total latency, got %d", *m.latencySum["get"])
	}
}

// TestAddBytesRead tests adding bytes read.
func TestAddBytesRead(t *testing.T) {
	m := New()

	m.AddBytesRead(100)
	m.AddBytesRead(200)

	if m.bytesRead != 300 {
		t.Errorf("expected 300 bytes read, got %d", m.bytesRead)
	}
}

// TestAddBytesWritten tests adding bytes written.
func TestAddBytesWritten(t *testing.T) {
	m := New()

	m.AddBytesWritten(500)
	m.AddBytesWritten(500)

	if m.bytesWritten != 1000 {
		t.Errorf("expected 1000 bytes written, got %d", m.bytesWritten)
	}
}

// TestReplicaCounters tests replica success/fail counters.
func TestReplicaCounters(t *testing.T) {
	m := New()

	m.IncReplicaSuccess()
	m.IncReplicaSuccess()
	m.IncReplicaFail()

	if m.replicaSuccess != 2 {
		t.Errorf("expected 2 replica successes, got %d", m.replicaSuccess)
	}
	if m.replicaFail != 1 {
		t.Errorf("expected 1 replica failure, got %d", m.replicaFail)
	}
}

// TestCacheSizeGauge tests cache size gauge.
func TestCacheSizeGauge(t *testing.T) {
	m := New()

	m.SetCacheSize(1024 * 1024)

	if m.cacheSize != 1024*1024 {
		t.Errorf("expected 1MB cache size, got %d", m.cacheSize)
	}
}

// TestCacheItemsGauge tests cache items gauge.
func TestCacheItemsGauge(t *testing.T) {
	m := New()

	m.SetCacheItems(1000)

	if m.cacheItems != 1000 {
		t.Errorf("expected 1000 items, got %d", m.cacheItems)
	}
}

// TestActiveConnsGauge tests active connections gauge.
func TestActiveConnsGauge(t *testing.T) {
	m := New()

	m.SetActiveConns(50)
	if m.activeConns != 50 {
		t.Errorf("expected 50 active conns, got %d", m.activeConns)
	}

	m.IncActiveConns()
	m.IncActiveConns()
	if m.activeConns != 52 {
		t.Errorf("expected 52 active conns, got %d", m.activeConns)
	}

	m.DecActiveConns()
	if m.activeConns != 51 {
		t.Errorf("expected 51 active conns, got %d", m.activeConns)
	}
}

// TestClusterMembersGauge tests cluster members gauge.
func TestClusterMembersGauge(t *testing.T) {
	m := New()

	m.SetClusterMembers(5)

	if m.clusterMembers != 5 {
		t.Errorf("expected 5 cluster members, got %d", m.clusterMembers)
	}
}

// TestShardsOwnedGauge tests shards owned gauge.
func TestShardsOwnedGauge(t *testing.T) {
	m := New()

	m.SetShardsOwned(32)

	if m.shardsOwned != 32 {
		t.Errorf("expected 32 shards, got %d", m.shardsOwned)
	}
}

// TestMemoryUsedGauge tests memory used gauge.
func TestMemoryUsedGauge(t *testing.T) {
	m := New()

	m.SetMemoryUsed(512 * 1024 * 1024)

	if m.memoryUsedBytes != 512*1024*1024 {
		t.Errorf("expected 512MB, got %d", m.memoryUsedBytes)
	}
}

// TestHandler tests the HTTP handler.
func TestHandler(t *testing.T) {
	m := New()

	// Set some metrics
	m.IncOp("get")
	m.IncOp("set")
	m.SetCacheSize(1000)
	m.SetActiveConns(10)

	handler := m.Handler()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "aegis_") {
		t.Error("expected aegis_ prefixed metrics")
	}
}

// TestWritePrometheus tests Prometheus format output.
func TestWritePrometheus(t *testing.T) {
	m := New()

	m.IncOp("get")
	m.IncOp("get")
	m.IncOpError("get")
	m.RecordLatency("get", 100*time.Microsecond)
	m.SetCacheSize(1000)
	m.SetCacheItems(50)
	m.SetActiveConns(5)
	m.SetClusterMembers(3)
	m.SetShardsOwned(16)
	m.AddBytesRead(1024)
	m.AddBytesWritten(2048)
	m.IncReplicaSuccess()

	var buf bytes.Buffer
	rec := &responseWriter{buf: &buf}
	m.WritePrometheus(rec)

	output := buf.String()

	// Verify expected metrics are present
	checks := []string{
		"aegis_uptime_seconds",
		"aegis_ops_total",
		"# HELP",
		"# TYPE",
	}

	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("expected %q in output", check)
		}
	}
}

// responseWriter is a simple http.ResponseWriter for testing.
type responseWriter struct {
	buf *bytes.Buffer
}

func (rw *responseWriter) Header() http.Header {
	return http.Header{}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	return rw.buf.Write(b)
}

func (rw *responseWriter) WriteHeader(code int) {}

// TestConcurrentMetrics tests thread safety of metrics operations.
func TestConcurrentMetrics(t *testing.T) {
	m := New()

	var wg sync.WaitGroup
	n := 100

	// Concurrent counter increments
	wg.Add(n * 3)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			m.IncOp("get")
		}()
		go func() {
			defer wg.Done()
			m.IncOpError("get")
		}()
		go func() {
			defer wg.Done()
			m.RecordLatency("get", time.Microsecond)
		}()
	}
	wg.Wait()

	if *m.opsTotal["get"] != uint64(n) {
		t.Errorf("expected %d ops, got %d", n, *m.opsTotal["get"])
	}
	if *m.opsErrors["get"] != uint64(n) {
		t.Errorf("expected %d errors, got %d", n, *m.opsErrors["get"])
	}
	if *m.latencyCount["get"] != uint64(n) {
		t.Errorf("expected %d latency records, got %d", n, *m.latencyCount["get"])
	}

	// Concurrent gauge updates
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			m.IncActiveConns()
		}()
		go func() {
			defer wg.Done()
			m.DecActiveConns()
		}()
	}
	wg.Wait()

	// Should be balanced
	if m.activeConns != 0 {
		t.Errorf("expected 0 active conns after balanced inc/dec, got %d", m.activeConns)
	}
}

// TestUnknownOp tests behavior with unknown operations.
func TestUnknownOp(t *testing.T) {
	m := New()

	// Should not panic on unknown op
	m.IncOp("unknown")
	m.IncOpError("unknown")
	m.RecordLatency("unknown", time.Millisecond)
}

// BenchmarkIncOp benchmarks operation counter increment.
func BenchmarkIncOp(b *testing.B) {
	m := New()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.IncOp("get")
	}
}

// BenchmarkRecordLatency benchmarks latency recording.
func BenchmarkRecordLatency(b *testing.B) {
	m := New()
	d := 100 * time.Microsecond
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.RecordLatency("get", d)
	}
}

// BenchmarkConcurrentIncOp benchmarks concurrent counter increments.
func BenchmarkConcurrentIncOp(b *testing.B) {
	m := New()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.IncOp("get")
		}
	})
}

// BenchmarkWritePrometheus benchmarks Prometheus output generation.
func BenchmarkWritePrometheus(b *testing.B) {
	m := New()
	m.IncOp("get")
	m.IncOp("set")
	m.SetCacheSize(1000)

	var buf bytes.Buffer
	rec := &responseWriter{buf: &buf}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		m.WritePrometheus(rec)
	}
}
