// Package repair provides background read repair functionality.
package repair

import (
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/logger"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

// RepairRequest represents a key that needs repair.
type RepairRequest struct {
	Key       []byte
	Value     []byte
	TTL       time.Duration
	Version   types.Version
	ShardID   uint64
	Targets   []string // Nodes that need the updated value
	Timestamp time.Time
}

// ReplicateFunc is the function to call for replicating repairs.
type ReplicateFunc func(
	shardID uint64,
	key []byte,
	value []byte,
	ttl time.Duration,
	version types.Version,
	targets []string,
) error

// Repairer handles background read repairs.
type Repairer struct {
	mu        sync.Mutex
	queue     chan *RepairRequest
	stopCh    chan struct{}
	wg        sync.WaitGroup
	replicate ReplicateFunc
	log       *logger.Logger

	// Statistics
	repairsQueued    uint64
	repairsCompleted uint64
	repairsFailed    uint64
}

// Config holds repairer configuration.
type Config struct {
	QueueSize   int
	Workers     int
	ReplicateFn ReplicateFunc
}

// New creates a new Repairer.
func New(cfg Config) *Repairer {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}

	return &Repairer{
		queue:     make(chan *RepairRequest, cfg.QueueSize),
		stopCh:    make(chan struct{}),
		replicate: cfg.ReplicateFn,
		log:       logger.New("repair", logger.LevelInfo, nil),
	}
}

// Start starts the repair workers.
func (r *Repairer) Start(workers int) {
	for i := 0; i < workers; i++ {
		r.wg.Add(1)
		go r.worker()
	}
	r.log.Info("started %d repair workers", workers)
}

// Stop stops the repairer.
func (r *Repairer) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// QueueRepair queues a key for background repair.
func (r *Repairer) QueueRepair(req *RepairRequest) bool {
	req.Timestamp = time.Now()

	select {
	case r.queue <- req:
		r.mu.Lock()
		r.repairsQueued++
		r.mu.Unlock()
		return true
	default:
		// Queue full
		return false
	}
}

// worker processes repair requests.
func (r *Repairer) worker() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case req := <-r.queue:
			r.processRepair(req)
		}
	}
}

// processRepair sends the repair to stale replicas.
func (r *Repairer) processRepair(req *RepairRequest) {
	if r.replicate == nil {
		return
	}

	err := r.replicate(req.ShardID, req.Key, req.Value, req.TTL, req.Version, req.Targets)

	r.mu.Lock()
	if err != nil {
		r.repairsFailed++
		r.log.Warn("repair failed for key %s: %v", string(req.Key), err)
	} else {
		r.repairsCompleted++
	}
	r.mu.Unlock()
}

// Stats returns repair statistics.
func (r *Repairer) Stats() map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[string]uint64{
		"queued":    r.repairsQueued,
		"completed": r.repairsCompleted,
		"failed":    r.repairsFailed,
		"pending":   uint64(len(r.queue)),
	}
}
