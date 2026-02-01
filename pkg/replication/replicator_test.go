package replication

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// TestReplicatorNew tests Replicator creation.
func TestReplicatorNew(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
	})

	if r.nodeID != "node-1" {
		t.Errorf("expected nodeID 'node-1', got %s", r.nodeID)
	}

	// Check defaults
	if r.batchSize != 100 {
		t.Errorf("expected default batch size 100, got %d", r.batchSize)
	}
	if r.batchTimeout != 10*time.Millisecond {
		t.Errorf("expected default batch timeout 10ms, got %v", r.batchTimeout)
	}
	if r.maxRetries != 3 {
		t.Errorf("expected default max retries 3, got %d", r.maxRetries)
	}
}

// TestReplicatorStartStop tests starting and stopping.
func TestReplicatorStartStop(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
	})

	if err := r.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if err := r.Stop(); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}
}

// TestReplicatorReplicateToSelf tests that self-replication is skipped.
func TestReplicatorReplicateToSelf(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
	})
	r.Start()
	defer r.Stop()

	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 1,
		Key:     []byte("key"),
		Value:   []byte("value"),
		Version: types.Version{Term: 1, Seq: 1},
	}

	// Should not create a queue for self
	err := r.Replicate(event, []string{"node-1"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	r.mu.Lock()
	queueCount := len(r.queues)
	r.mu.Unlock()

	if queueCount != 0 {
		t.Errorf("expected no queues (self-replication skipped), got %d", queueCount)
	}
}

// TestReplicatorReplicateCreatesSenderLoop tests queue creation.
func TestReplicatorReplicateCreatesSenderLoop(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999" // Non-existent, will fail
		},
	})
	r.Start()
	defer r.Stop()

	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 1,
		Key:     []byte("key"),
		Value:   []byte("value"),
		Version: types.Version{Term: 1, Seq: 1},
	}

	err := r.Replicate(event, []string{"node-2"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	r.mu.Lock()
	_, hasQueue := r.queues["node-2"]
	r.mu.Unlock()

	if !hasQueue {
		t.Error("expected queue to be created for node-2")
	}
}

// TestReplicatorReplicateSet tests SET event helper.
func TestReplicatorReplicateSet(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999"
		},
	})
	r.Start()
	defer r.Stop()

	err := r.ReplicateSet(
		1,
		[]byte("key"),
		[]byte("value"),
		time.Hour,
		types.Version{Term: 1, Seq: 1},
		[]string{"node-2"},
	)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestReplicatorReplicateDelete tests DELETE event helper.
func TestReplicatorReplicateDelete(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999"
		},
	})
	r.Start()
	defer r.Stop()

	err := r.ReplicateDelete(
		1,
		[]byte("key"),
		types.Version{Term: 1, Seq: 2},
		[]string{"node-2"},
	)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestReplicatorEventTypes tests event type constants.
func TestReplicatorEventTypes(t *testing.T) {
	if EventTypeSet != 0 {
		t.Errorf("expected EventTypeSet to be 0, got %d", EventTypeSet)
	}
	if EventTypeDelete != 1 {
		t.Errorf("expected EventTypeDelete to be 1, got %d", EventTypeDelete)
	}
}

// TestReplicatorReplicationEvent tests ReplicationEvent structure.
func TestReplicatorReplicationEvent(t *testing.T) {
	now := time.Now()
	event := &ReplicationEvent{
		Type:      EventTypeSet,
		ShardID:   42,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		TTL:       time.Hour,
		Version:   types.Version{Term: 10, Seq: 100},
		Timestamp: now,
	}

	if event.Type != EventTypeSet {
		t.Error("type mismatch")
	}
	if event.ShardID != 42 {
		t.Error("shardID mismatch")
	}
	if string(event.Key) != "test-key" {
		t.Error("key mismatch")
	}
	if string(event.Value) != "test-value" {
		t.Error("value mismatch")
	}
	if event.TTL != time.Hour {
		t.Error("TTL mismatch")
	}
	if event.Version.Term != 10 || event.Version.Seq != 100 {
		t.Error("version mismatch")
	}
}

// TestReplicatorQueueFull tests behavior when queue is full.
func TestReplicatorQueueFull(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999"
		},
	})

	// Manually create a full queue
	r.mu.Lock()
	smallQueue := make(chan *ReplicationEvent, 1)
	smallQueue <- &ReplicationEvent{} // Fill it
	r.queues["node-2"] = smallQueue
	r.mu.Unlock()

	// Should not block, just increment dropped counter
	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 1,
		Key:     []byte("key"),
		Value:   []byte("value"),
	}

	// This should not block
	done := make(chan struct{})
	go func() {
		r.Replicate(event, []string{"node-2"})
		close(done)
	}()

	select {
	case <-done:
		// Good, didn't block
	case <-time.After(time.Second):
		t.Error("Replicate blocked on full queue")
	}

	dropped := atomic.LoadUint64(&r.droppedEvents)
	if dropped != 1 {
		t.Errorf("expected 1 dropped event, got %d", dropped)
	}
}

// TestReplicatorMultipleTargets tests replication to multiple nodes.
func TestReplicatorMultipleTargets(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999"
		},
	})
	r.Start()
	defer r.Stop()

	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 1,
		Key:     []byte("key"),
		Value:   []byte("value"),
		Version: types.Version{Term: 1, Seq: 1},
	}

	targets := []string{"node-2", "node-3", "node-4", "node-1"} // Include self
	err := r.Replicate(event, targets)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	r.mu.Lock()
	queueCount := len(r.queues)
	r.mu.Unlock()

	// Should have 3 queues (node-1 is self, so skipped)
	if queueCount != 3 {
		t.Errorf("expected 3 queues, got %d", queueCount)
	}
}

// TestReplicatorEmptyTargets tests replication with no targets.
func TestReplicatorEmptyTargets(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
	})

	event := &ReplicationEvent{
		Type: EventTypeSet,
		Key:  []byte("key"),
	}

	err := r.Replicate(event, []string{})
	if err != nil {
		t.Errorf("unexpected error with empty targets: %v", err)
	}

	err = r.Replicate(event, nil)
	if err != nil {
		t.Errorf("unexpected error with nil targets: %v", err)
	}
}

// TestReplicatorConfig tests configuration options.
func TestReplicatorConfig(t *testing.T) {
	r := New(Config{
		NodeID:       "custom-node",
		BatchSize:    50,
		BatchTimeout: 50 * time.Millisecond,
		MaxRetries:   5,
		AuthToken:    "secret",
	})

	if r.nodeID != "custom-node" {
		t.Errorf("nodeID mismatch")
	}
	if r.batchSize != 50 {
		t.Errorf("batchSize mismatch")
	}
	if r.batchTimeout != 50*time.Millisecond {
		t.Errorf("batchTimeout mismatch")
	}
	if r.maxRetries != 5 {
		t.Errorf("maxRetries mismatch")
	}
	if r.authToken != "secret" {
		t.Errorf("authToken mismatch")
	}
}

// TestReplicatorEventToRequest tests event to request conversion.
func TestReplicatorEventToRequest(t *testing.T) {
	r := New(Config{
		NodeID: "node-1",
	})

	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 123,
		Key:     []byte("test-key"),
		Value:   []byte("test-value"),
		TTL:     time.Hour,
		Version: types.Version{Term: 5, Seq: 99},
	}

	req := r.eventToRequest(event)

	if req == nil {
		t.Fatal("expected non-nil request")
	}

	if string(req.Key) != "test-key" {
		t.Errorf("expected key 'test-key', got %s", string(req.Key))
	}

	// Value should have metadata prefix
	if len(req.Value) <= len(event.Value) {
		t.Error("expected value to have metadata prefix")
	}
}

// BenchmarkReplicatorReplicate benchmarks replication queueing.
func BenchmarkReplicatorReplicate(b *testing.B) {
	r := New(Config{
		NodeID: "node-1",
		GetNodeAddr: func(nodeID string) string {
			return "127.0.0.1:9999"
		},
	})
	r.Start()
	defer r.Stop()

	event := &ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: 1,
		Key:     []byte("benchmark-key"),
		Value:   make([]byte, 256),
		Version: types.Version{Term: 1, Seq: 0},
	}
	targets := []string{"node-2", "node-3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event.Version.Seq = uint64(i)
		r.Replicate(event, targets)
	}
}
