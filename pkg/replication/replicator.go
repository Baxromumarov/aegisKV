package replication

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// ReplicationEvent represents an event to be replicated.
type ReplicationEvent struct {
	Type      EventType
	ShardID   uint64
	Key       []byte
	Value     []byte
	TTL       time.Duration
	Version   types.Version
	Timestamp time.Time
}

// EventType represents the type of replication event.
type EventType uint8

const (
	EventTypeSet EventType = iota
	EventTypeDelete
)

// PendingAck tracks pending acknowledgments.
type PendingAck struct {
	Event   *ReplicationEvent
	Targets []string
	Acked   map[string]bool
	SentAt  time.Time
	Retries int
}

// Replicator handles async replication to follower nodes.
type Replicator struct {
	mu            sync.RWMutex
	nodeID        string
	queues        map[string]chan *ReplicationEvent
	pending       map[uint64]*PendingAck
	nextEventID   uint64
	batchSize     int
	batchTimeout  time.Duration
	maxRetries    int
	retryInterval time.Duration
	connections   map[string]net.Conn
	connMu        sync.RWMutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	getNodeAddr   func(string) string
}

// Config holds replicator configuration.
type Config struct {
	NodeID        string
	BatchSize     int
	BatchTimeout  time.Duration
	MaxRetries    int
	RetryInterval time.Duration
	GetNodeAddr   func(string) string
}

// New creates a new Replicator.
func New(cfg Config) *Replicator {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = 10 * time.Millisecond
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 100 * time.Millisecond
	}

	return &Replicator{
		nodeID:        cfg.NodeID,
		queues:        make(map[string]chan *ReplicationEvent),
		pending:       make(map[uint64]*PendingAck),
		batchSize:     cfg.BatchSize,
		batchTimeout:  cfg.BatchTimeout,
		maxRetries:    cfg.MaxRetries,
		retryInterval: cfg.RetryInterval,
		connections:   make(map[string]net.Conn),
		stopCh:        make(chan struct{}),
		getNodeAddr:   cfg.GetNodeAddr,
	}
}

// Start starts the replicator.
func (r *Replicator) Start() error {
	r.wg.Add(1)
	go r.retryLoop()
	return nil
}

// Stop stops the replicator.
func (r *Replicator) Stop() error {
	close(r.stopCh)
	r.wg.Wait()

	r.connMu.Lock()
	for _, conn := range r.connections {
		conn.Close()
	}
	r.connMu.Unlock()

	return nil
}

// Replicate queues an event for replication to the specified nodes.
func (r *Replicator) Replicate(event *ReplicationEvent, targets []string) error {
	if len(targets) == 0 {
		return nil
	}

	r.mu.Lock()
	r.nextEventID++
	eventID := r.nextEventID
	event.Timestamp = time.Now()
	r.pending[eventID] = &PendingAck{
		Event:   event,
		Targets: targets,
		Acked:   make(map[string]bool),
		SentAt:  time.Now(),
	}
	r.mu.Unlock()

	for _, target := range targets {
		if target == r.nodeID {
			continue
		}

		r.mu.Lock()
		queue, ok := r.queues[target]
		if !ok {
			queue = make(chan *ReplicationEvent, 10000)
			r.queues[target] = queue
			r.wg.Add(1)
			go r.senderLoop(target, queue)
		}
		r.mu.Unlock()

		select {
		case queue <- event:
		default:
		}
	}

	return nil
}

// ReplicateSet queues a SET event for replication.
func (r *Replicator) ReplicateSet(shardID uint64, key, value []byte, ttl time.Duration, version types.Version, targets []string) error {
	return r.Replicate(&ReplicationEvent{
		Type:    EventTypeSet,
		ShardID: shardID,
		Key:     key,
		Value:   value,
		TTL:     ttl,
		Version: version,
	}, targets)
}

// ReplicateDelete queues a DELETE event for replication.
func (r *Replicator) ReplicateDelete(shardID uint64, key []byte, version types.Version, targets []string) error {
	return r.Replicate(&ReplicationEvent{
		Type:    EventTypeDelete,
		ShardID: shardID,
		Key:     key,
		Version: version,
	}, targets)
}

// senderLoop sends events to a specific target node.
func (r *Replicator) senderLoop(target string, queue chan *ReplicationEvent) {
	defer r.wg.Done()

	batch := make([]*ReplicationEvent, 0, r.batchSize)
	timer := time.NewTimer(r.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-r.stopCh:
			if len(batch) > 0 {
				r.sendBatch(target, batch)
			}
			return

		case event := <-queue:
			batch = append(batch, event)
			if len(batch) >= r.batchSize {
				r.sendBatch(target, batch)
				batch = batch[:0]
				timer.Reset(r.batchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				r.sendBatch(target, batch)
				batch = batch[:0]
			}
			timer.Reset(r.batchTimeout)
		}
	}
}

// sendBatch sends a batch of events to a target.
func (r *Replicator) sendBatch(target string, batch []*ReplicationEvent) error {
	conn, err := r.getConnection(target)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	for _, event := range batch {
		data := r.encodeEvent(event)
		if _, err := conn.Write(data); err != nil {
			r.closeConnection(target)
			return fmt.Errorf("failed to send event: %w", err)
		}
	}

	return nil
}

// encodeEvent encodes an event to bytes.
func (r *Replicator) encodeEvent(event *ReplicationEvent) []byte {
	size := 1 + 8 + 4 + len(event.Key) + 4 + len(event.Value) + 8 + 8 + 8 + 8
	data := make([]byte, size)

	offset := 0

	data[offset] = byte(event.Type)
	offset++

	putUint64(data[offset:], event.ShardID)
	offset += 8

	putUint32(data[offset:], uint32(len(event.Key)))
	offset += 4
	copy(data[offset:], event.Key)
	offset += len(event.Key)

	putUint32(data[offset:], uint32(len(event.Value)))
	offset += 4
	copy(data[offset:], event.Value)
	offset += len(event.Value)

	putUint64(data[offset:], uint64(event.TTL))
	offset += 8

	putUint64(data[offset:], event.Version.Term)
	offset += 8
	putUint64(data[offset:], event.Version.Seq)
	offset += 8

	putUint64(data[offset:], uint64(event.Timestamp.UnixNano()))

	return data
}

// getConnection gets or creates a connection to a target.
func (r *Replicator) getConnection(target string) (net.Conn, error) {
	r.connMu.RLock()
	conn, ok := r.connections[target]
	r.connMu.RUnlock()

	if ok {
		return conn, nil
	}

	addr := target
	if r.getNodeAddr != nil {
		addr = r.getNodeAddr(target)
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	r.connMu.Lock()
	r.connections[target] = conn
	r.connMu.Unlock()

	return conn, nil
}

// closeConnection closes a connection to a target.
func (r *Replicator) closeConnection(target string) {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	if conn, ok := r.connections[target]; ok {
		conn.Close()
		delete(r.connections, target)
	}
}

// retryLoop handles retrying failed replications.
func (r *Replicator) retryLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.processRetries()
		}
	}
}

// processRetries processes pending acknowledgments for retry.
func (r *Replicator) processRetries() {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for eventID, pending := range r.pending {
		if now.Sub(pending.SentAt) < r.retryInterval {
			continue
		}

		allAcked := true
		for _, target := range pending.Targets {
			if !pending.Acked[target] && target != r.nodeID {
				allAcked = false
				break
			}
		}

		if allAcked {
			delete(r.pending, eventID)
			continue
		}

		if pending.Retries >= r.maxRetries {
			delete(r.pending, eventID)
			continue
		}

		pending.Retries++
		pending.SentAt = now
	}
}

// Ack acknowledges receipt of a replication event.
func (r *Replicator) Ack(eventID uint64, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pending, ok := r.pending[eventID]; ok {
		pending.Acked[nodeID] = true
	}
}

// Stats returns replicator statistics.
func (r *Replicator) Stats() ReplicatorStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := ReplicatorStats{
		PendingEvents: len(r.pending),
		QueueSizes:    make(map[string]int),
	}

	for target, queue := range r.queues {
		stats.QueueSizes[target] = len(queue)
	}

	return stats
}

// ReplicatorStats contains replicator statistics.
type ReplicatorStats struct {
	PendingEvents int
	QueueSizes    map[string]int
}

func putUint64(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
