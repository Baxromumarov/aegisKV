// Package replication handles async replication to follower nodes.
package replication

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/logger"
	"github.com/baxromumarov/aegisKV/pkg/protocol"
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

// replicaConn wraps a connection with protocol encoder/decoder.
type replicaConn struct {
	conn    net.Conn
	writer  *bufio.Writer
	reader  *bufio.Reader
	encoder *protocol.Encoder
	decoder *protocol.Decoder
}

// Replicator handles async replication to follower nodes.
type Replicator struct {
	mu            sync.RWMutex
	nodeID        string
	queues        map[string]chan *ReplicationEvent
	batchSize     int
	batchTimeout  time.Duration
	maxRetries    int
	connections   map[string]*replicaConn
	connMu        sync.RWMutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	getNodeAddr   func(string) string
	authToken     string
	tlsConfig     *tls.Config
	droppedEvents uint64
	log           *logger.Logger
}

// Config holds replicator configuration.
type Config struct {
	NodeID       string
	BatchSize    int
	BatchTimeout time.Duration
	MaxRetries   int
	GetNodeAddr  func(string) string
	AuthToken    string
	TLSConfig    *tls.Config
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

	return &Replicator{
		nodeID:       cfg.NodeID,
		queues:       make(map[string]chan *ReplicationEvent),
		batchSize:    cfg.BatchSize,
		batchTimeout: cfg.BatchTimeout,
		maxRetries:   cfg.MaxRetries,
		connections:  make(map[string]*replicaConn),
		stopCh:       make(chan struct{}),
		getNodeAddr:  cfg.GetNodeAddr,
		authToken:    cfg.AuthToken,
		tlsConfig:    cfg.TLSConfig,
		log:          logger.New("replicator", logger.LevelInfo, nil),
	}
}

// Start starts the replicator.
func (r *Replicator) Start() error {
	return nil
}

// Stop stops the replicator.
func (r *Replicator) Stop() error {
	close(r.stopCh)
	r.wg.Wait()

	r.connMu.Lock()
	for _, rc := range r.connections {
		rc.conn.Close()
	}
	r.connMu.Unlock()

	return nil
}

// Replicate queues an event for replication to the specified nodes.
func (r *Replicator) Replicate(event *ReplicationEvent, targets []string) error {
	if len(targets) == 0 {
		return nil
	}

	event.Timestamp = time.Now()

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
			// Queue full - log and count dropped event
			atomic.AddUint64(&r.droppedEvents, 1)
			r.log.Warn("replication queue full for %s, dropped event (total dropped: %d)",
				target, atomic.LoadUint64(&r.droppedEvents))
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
				if err := r.sendBatch(target, batch); err != nil {
					r.requeueBatch(target, batch)
				}
				batch = batch[:0]
				timer.Reset(r.batchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				if err := r.sendBatch(target, batch); err != nil {
					r.requeueBatch(target, batch)
				}
				batch = batch[:0]
			}
			timer.Reset(r.batchTimeout)
		}
	}
}

// eventToRequest converts a replication event to a protocol request.
// Value format: [1B type][8B shardID][8B term][8B seq][8B ttlNanos][actual value]
func (r *Replicator) eventToRequest(event *ReplicationEvent) *protocol.Request {
	// Pack metadata into value prefix
	metaSize := 1 + 8 + 8 + 8 + 8 // type + shardID + term + seq + ttl
	value := make([]byte, metaSize+len(event.Value))

	offset := 0
	value[offset] = byte(event.Type)
	offset++

	binary.BigEndian.PutUint64(value[offset:], event.ShardID)
	offset += 8

	binary.BigEndian.PutUint64(value[offset:], event.Version.Term)
	offset += 8

	binary.BigEndian.PutUint64(value[offset:], event.Version.Seq)
	offset += 8

	binary.BigEndian.PutUint64(value[offset:], uint64(event.TTL.Nanoseconds()))
	offset += 8

	copy(value[offset:], event.Value)

	return &protocol.Request{
		Command:   protocol.CmdReplicate,
		Key:       event.Key,
		Value:     value,
		RequestID: uint64(time.Now().UnixNano()),
	}
}

// sendBatch sends a batch of events to a target using protocol encoding.
func (r *Replicator) sendBatch(target string, batch []*ReplicationEvent) error {
	rc, err := r.getConnection(target)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	for _, event := range batch {
		req := r.eventToRequest(event)

		if err := rc.encoder.EncodeRequest(req); err != nil {
			r.closeConnection(target)
			return fmt.Errorf("failed to encode event: %w", err)
		}

		if err := rc.writer.Flush(); err != nil {
			r.closeConnection(target)
			return fmt.Errorf("failed to flush: %w", err)
		}

		// Read response (synchronous ack)
		rc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp, err := rc.decoder.DecodeResponse()
		if err != nil {
			r.closeConnection(target)
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.Status != protocol.StatusOK {
			r.log.Warn("replication failed for key %s: %s", string(event.Key), resp.Error)
		}
	}

	return nil
}

// requeueBatch re-queues failed events for retry.
func (r *Replicator) requeueBatch(target string, batch []*ReplicationEvent) {
	r.mu.RLock()
	queue, ok := r.queues[target]
	r.mu.RUnlock()

	if !ok {
		return
	}

	for _, event := range batch {
		select {
		case queue <- event:
		default:
			atomic.AddUint64(&r.droppedEvents, 1)
		}
	}
}

// getConnection gets or creates a connection to a target.
func (r *Replicator) getConnection(target string) (*replicaConn, error) {
	r.connMu.RLock()
	rc, ok := r.connections[target]
	r.connMu.RUnlock()

	if ok {
		return rc, nil
	}

	addr := target
	if r.getNodeAddr != nil {
		addr = r.getNodeAddr(target)
	}

	var conn net.Conn
	var err error

	if r.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, r.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}

	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriterSize(conn, 64*1024)
	reader := bufio.NewReaderSize(conn, 64*1024)

	rc = &replicaConn{
		conn:    conn,
		writer:  writer,
		reader:  reader,
		encoder: protocol.NewEncoder(writer),
		decoder: protocol.NewDecoder(reader),
	}

	// Authenticate if token is set
	if r.authToken != "" {
		if err := r.authenticate(rc); err != nil {
			conn.Close()
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	r.connMu.Lock()
	r.connections[target] = rc
	r.connMu.Unlock()

	return rc, nil
}

// authenticate sends an AUTH request.
func (r *Replicator) authenticate(rc *replicaConn) error {
	req := &protocol.Request{
		Command:   protocol.CmdAuth,
		Value:     []byte(r.authToken),
		RequestID: uint64(time.Now().UnixNano()),
	}

	if err := rc.encoder.EncodeRequest(req); err != nil {
		return err
	}
	if err := rc.writer.Flush(); err != nil {
		return err
	}

	rc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := rc.decoder.DecodeResponse()
	if err != nil {
		return err
	}

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("auth rejected: %s", resp.Error)
	}

	return nil
}

// closeConnection closes a connection to a target.
func (r *Replicator) closeConnection(target string) {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	if rc, ok := r.connections[target]; ok {
		rc.conn.Close()
		delete(r.connections, target)
	}
}

// Stats returns replicator statistics.
func (r *Replicator) Stats() ReplicatorStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := ReplicatorStats{
		DroppedEvents: atomic.LoadUint64(&r.droppedEvents),
		QueueSizes:    make(map[string]int),
	}

	for target, queue := range r.queues {
		stats.QueueSizes[target] = len(queue)
	}

	return stats
}

// ReplicatorStats contains replicator statistics.
type ReplicatorStats struct {
	DroppedEvents uint64
	QueueSizes    map[string]int
}
