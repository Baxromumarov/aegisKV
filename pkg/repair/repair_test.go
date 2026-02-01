package repair

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

func TestNew(t *testing.T) {
	r := New(Config{})
	if r == nil {
		t.Fatal("New returned nil")
	}
	if cap(r.queue) != 1000 {
		t.Errorf("expected default queue size 1000, got %d", cap(r.queue))
	}
}

func TestNewWithConfig(t *testing.T) {
	r := New(Config{
		QueueSize: 500,
		Workers:   4,
	})
	if cap(r.queue) != 500 {
		t.Errorf("expected queue size 500, got %d", cap(r.queue))
	}
}

func TestQueueRepair(t *testing.T) {
	r := New(Config{QueueSize: 10})

	req := &RepairRequest{
		Key:     []byte("test-key"),
		Value:   []byte("test-value"),
		ShardID: 1,
		Targets: []string{"node-2", "node-3"},
	}

	if !r.QueueRepair(req) {
		t.Error("QueueRepair should return true")
	}

	stats := r.Stats()
	if stats["queued"] != 1 {
		t.Errorf("expected 1 queued, got %d", stats["queued"])
	}
	if stats["pending"] != 1 {
		t.Errorf("expected 1 pending, got %d", stats["pending"])
	}
}

func TestQueueRepairFull(t *testing.T) {
	r := New(Config{QueueSize: 2})

	// Fill the queue
	for i := 0; i < 2; i++ {
		r.QueueRepair(&RepairRequest{Key: []byte("key")})
	}

	// Should return false when full
	if r.QueueRepair(&RepairRequest{Key: []byte("overflow")}) {
		t.Error("QueueRepair should return false when queue is full")
	}
}

func TestStartStop(t *testing.T) {
	r := New(Config{QueueSize: 10})

	r.Start(2)

	// Give workers time to start
	time.Sleep(10 * time.Millisecond)

	r.Stop()
}

func TestRepairProcessing(t *testing.T) {
	var processedCount int32

	r := New(Config{
		QueueSize: 10,
		ReplicateFn: func(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	})

	r.Start(2)

	// Queue some repairs
	for i := 0; i < 5; i++ {
		r.QueueRepair(&RepairRequest{
			Key:     []byte("key"),
			Value:   []byte("value"),
			ShardID: 1,
			Targets: []string{"node-2"},
		})
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	r.Stop()

	if atomic.LoadInt32(&processedCount) != 5 {
		t.Errorf("expected 5 processed, got %d", processedCount)
	}

	stats := r.Stats()
	if stats["completed"] != 5 {
		t.Errorf("expected 5 completed, got %d", stats["completed"])
	}
}

func TestRepairWithTTL(t *testing.T) {
	var receivedTTL time.Duration

	r := New(Config{
		ReplicateFn: func(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
			receivedTTL = ttl
			return nil
		},
	})

	r.Start(1)

	r.QueueRepair(&RepairRequest{
		Key:     []byte("key"),
		Value:   []byte("value"),
		TTL:     5 * time.Minute,
		ShardID: 1,
		Targets: []string{"node-2"},
	})

	time.Sleep(50 * time.Millisecond)
	r.Stop()

	if receivedTTL != 5*time.Minute {
		t.Errorf("expected TTL 5m, got %v", receivedTTL)
	}
}

func TestRepairWithVersion(t *testing.T) {
	var receivedVersion types.Version

	r := New(Config{
		ReplicateFn: func(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
			receivedVersion = version
			return nil
		},
	})

	r.Start(1)

	expectedVersion := types.Version{
		Term: 5,
		Seq:  42,
	}

	r.QueueRepair(&RepairRequest{
		Key:     []byte("key"),
		Value:   []byte("value"),
		Version: expectedVersion,
		ShardID: 1,
		Targets: []string{"node-2"},
	})

	time.Sleep(50 * time.Millisecond)
	r.Stop()

	if receivedVersion.Seq != 42 {
		t.Errorf("expected seq 42, got %d", receivedVersion.Seq)
	}
}

func TestRepairNilReplicateFunc(t *testing.T) {
	r := New(Config{
		ReplicateFn: nil,
	})

	r.Start(1)

	r.QueueRepair(&RepairRequest{
		Key:     []byte("key"),
		Value:   []byte("value"),
		ShardID: 1,
		Targets: []string{"node-2"},
	})

	time.Sleep(50 * time.Millisecond)
	r.Stop()

	// Should not panic, just skip
	stats := r.Stats()
	if stats["completed"] != 0 {
		t.Error("completed should be 0 with nil replicate func")
	}
}

func TestConcurrentQueueing(t *testing.T) {
	var processedCount int32

	r := New(Config{
		QueueSize: 1000,
		ReplicateFn: func(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	})

	r.Start(4)

	var wg sync.WaitGroup
	numGoroutines := 10
	requestsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				r.QueueRepair(&RepairRequest{
					Key:     []byte("key"),
					Value:   []byte("value"),
					ShardID: uint64(id),
					Targets: []string{"node-2"},
				})
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	r.Stop()

	expected := int32(numGoroutines * requestsPerGoroutine)
	if atomic.LoadInt32(&processedCount) != expected {
		t.Errorf("expected %d processed, got %d", expected, processedCount)
	}
}

func BenchmarkQueueRepair(b *testing.B) {
	r := New(Config{
		QueueSize: 100000,
		ReplicateFn: func(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
			return nil
		},
	})

	r.Start(4)
	defer r.Stop()

	req := &RepairRequest{
		Key:     []byte("benchmark-key"),
		Value:   []byte("benchmark-value"),
		ShardID: 1,
		Targets: []string{"node-2"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.QueueRepair(req)
	}
}
