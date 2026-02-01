package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/client"
)

// WorkloadConfig configures the chaos workload generator.
type WorkloadConfig struct {
	Seeds          []string
	NumWorkers     int
	KeySpace       int           // Number of unique keys
	ValueSize      int           // Size of values in bytes
	ReadRatio      float64       // Ratio of reads (0.0-1.0)
	MinTTL         time.Duration // Minimum TTL for keys
	MaxTTL         time.Duration // Maximum TTL (0 = no TTL)
	OperationDelay time.Duration // Delay between operations per worker
}

// Workload generates continuous read/write operations.
type Workload struct {
	cfg    WorkloadConfig
	client *client.Client
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Stats
	totalOps     int64
	successOps   int64
	failedOps    int64
	readOps      int64
	writeOps     int64
	deleteOps    int64
	readSuccess  int64
	writeSuccess int64
	readFailed   int64
	writeFailed  int64

	// Tracking written keys for validation
	// key -> writeRecord{seq, value} - only store successful writes with seq number
	writtenKeys sync.Map
	writeSeq    int64 // Global write sequence counter
	mu          sync.RWMutex
	running     bool
}

// writeRecord tracks a successful write with its sequence number.
type writeRecord struct {
	seq   int64
	value string
}

// NewWorkload creates a new workload generator.
func NewWorkload(cfg WorkloadConfig) *Workload {
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = 10
	}
	if cfg.KeySpace == 0 {
		cfg.KeySpace = 10000
	}
	if cfg.ValueSize == 0 {
		cfg.ValueSize = 100
	}
	if cfg.ReadRatio == 0 {
		cfg.ReadRatio = 0.7
	}
	if cfg.OperationDelay == 0 {
		cfg.OperationDelay = 10 * time.Millisecond
	}

	return &Workload{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start starts the workload generator.
func (w *Workload) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("workload already running")
	}
	w.running = true
	w.mu.Unlock()

	// Create client
	w.client = client.New(client.Config{
		Seeds:        w.cfg.Seeds,
		MaxConns:     w.cfg.NumWorkers * 2,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   3,
	})

	// Start workers
	for i := 0; i < w.cfg.NumWorkers; i++ {
		w.wg.Add(1)
		go w.worker(ctx, i)
	}

	return nil
}

// Stop stops the workload generator.
func (w *Workload) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	w.mu.Unlock()

	close(w.stopCh)
	w.wg.Wait()

	// Note: Don't close client here - it may still be used for validation
}

// Close closes the workload and releases resources (call after validation).
func (w *Workload) Close() {
	if w.client != nil {
		w.client.Close()
		w.client = nil
	}
}

// worker runs continuous operations.
func (w *Workload) worker(ctx context.Context, id int) {
	defer w.wg.Done()

	localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}

		// Random operation
		op := localRand.Float64()
		key := fmt.Sprintf("chaos-key-%d", localRand.Intn(w.cfg.KeySpace))

		if op < w.cfg.ReadRatio {
			// Read
			w.doRead(key)
		} else if op < w.cfg.ReadRatio+0.05 { // 5% deletes
			// Delete
			w.doDelete(key)
		} else {
			// Write
			value := w.generateValue(localRand)
			ttl := w.generateTTL(localRand)
			w.doWrite(key, value, ttl)
		}

		atomic.AddInt64(&w.totalOps, 1)

		if w.cfg.OperationDelay > 0 {
			time.Sleep(w.cfg.OperationDelay)
		}
	}
}

// doRead performs a read operation.
func (w *Workload) doRead(key string) {
	atomic.AddInt64(&w.readOps, 1)

	_, err := w.client.Get([]byte(key))
	if err != nil {
		// Key not found is not necessarily an error
		if err.Error() != "key not found" {
			atomic.AddInt64(&w.readFailed, 1)
			atomic.AddInt64(&w.failedOps, 1)
			return
		}
	}

	atomic.AddInt64(&w.readSuccess, 1)
	atomic.AddInt64(&w.successOps, 1)
}

// doWrite performs a write operation.
func (w *Workload) doWrite(key string, value []byte, ttl time.Duration) {
	atomic.AddInt64(&w.writeOps, 1)

	// Get a sequence number BEFORE the write for ordering
	seq := atomic.AddInt64(&w.writeSeq, 1)

	var err error
	if ttl > 0 {
		err = w.client.SetWithTTL([]byte(key), value, ttl)
	} else {
		err = w.client.Set([]byte(key), value)
	}

	if err != nil {
		atomic.AddInt64(&w.writeFailed, 1)
		atomic.AddInt64(&w.failedOps, 1)
		return
	}

	// Track written keys for validation (only non-TTL keys)
	// Use CompareAndSwap-like logic: only update if our seq is higher
	if ttl == 0 {
		valueStr := string(value)
		for {
			existing, loaded := w.writtenKeys.Load(key)
			if !loaded {
				// Key doesn't exist, try to store
				if w.writtenKeys.CompareAndSwap(key, nil, writeRecord{seq: seq, value: valueStr}) {
					break
				}
				// First store - use LoadOrStore
				actual, loaded := w.writtenKeys.LoadOrStore(key, writeRecord{seq: seq, value: valueStr})
				if !loaded {
					break // We stored successfully
				}
				existing = actual
			}

			rec := existing.(writeRecord)
			if rec.seq > seq {
				// A later write already recorded, skip
				break
			}

			// Try to update with our newer value
			if w.writtenKeys.CompareAndSwap(key, existing, writeRecord{seq: seq, value: valueStr}) {
				break
			}
			// Another write won the race, retry
		}
	}

	atomic.AddInt64(&w.writeSuccess, 1)
	atomic.AddInt64(&w.successOps, 1)
}

// doDelete performs a delete operation.
func (w *Workload) doDelete(key string) {
	atomic.AddInt64(&w.deleteOps, 1)

	err := w.client.Delete([]byte(key))
	if err != nil && err.Error() != "key not found" {
		atomic.AddInt64(&w.failedOps, 1)
		return
	}

	w.writtenKeys.Delete(key)
	atomic.AddInt64(&w.successOps, 1)
}

// generateValue generates a random value.
func (w *Workload) generateValue(r *rand.Rand) []byte {
	value := make([]byte, w.cfg.ValueSize)
	for i := range value {
		value[i] = byte('a' + r.Intn(26))
	}
	return value
}

// generateTTL generates a random TTL.
// 30% of writes have no TTL (permanent) so we can validate them.
func (w *Workload) generateTTL(r *rand.Rand) time.Duration {
	if w.cfg.MaxTTL == 0 {
		return 0
	}
	// 30% chance of no TTL for data validation
	if r.Float64() < 0.3 {
		return 0
	}
	if w.cfg.MinTTL >= w.cfg.MaxTTL {
		return w.cfg.MinTTL
	}
	return w.cfg.MinTTL + time.Duration(r.Int63n(int64(w.cfg.MaxTTL-w.cfg.MinTTL)))
}

// Stats returns workload statistics.
func (w *Workload) Stats() WorkloadStats {
	return WorkloadStats{
		TotalOps:     atomic.LoadInt64(&w.totalOps),
		SuccessOps:   atomic.LoadInt64(&w.successOps),
		FailedOps:    atomic.LoadInt64(&w.failedOps),
		ReadOps:      atomic.LoadInt64(&w.readOps),
		WriteOps:     atomic.LoadInt64(&w.writeOps),
		DeleteOps:    atomic.LoadInt64(&w.deleteOps),
		ReadSuccess:  atomic.LoadInt64(&w.readSuccess),
		WriteSuccess: atomic.LoadInt64(&w.writeSuccess),
		ReadFailed:   atomic.LoadInt64(&w.readFailed),
		WriteFailed:  atomic.LoadInt64(&w.writeFailed),
	}
}

// WorkloadStats contains workload statistics.
type WorkloadStats struct {
	TotalOps     int64
	SuccessOps   int64
	FailedOps    int64
	ReadOps      int64
	WriteOps     int64
	DeleteOps    int64
	ReadSuccess  int64
	WriteSuccess int64
	ReadFailed   int64
	WriteFailed  int64
}

// ErrorRate returns the error rate as a percentage.
func (s WorkloadStats) ErrorRate() float64 {
	if s.TotalOps == 0 {
		return 0
	}
	return float64(s.FailedOps) * 100 / float64(s.TotalOps)
}

// OpsPerSecond calculates operations per second given duration.
func (s WorkloadStats) OpsPerSecond(duration time.Duration) float64 {
	if duration == 0 {
		return 0
	}
	return float64(s.TotalOps) / duration.Seconds()
}

// ValidateData validates that written data is still readable.
// For each tracked key, it verifies the stored value matches the last successful write.
func (w *Workload) ValidateData() (validated int, errors int, missing int) {
	w.writtenKeys.Range(func(key, value any) bool {
		keyStr := key.(string)
		rec := value.(writeRecord)
		expectedValue := rec.value

		got, err := w.client.Get([]byte(keyStr))
		if err != nil {
			if err.Error() == "key not found" {
				missing++
			} else {
				errors++
			}
			return true
		}

		if string(got) != expectedValue {
			// Data mismatch - this is a serious error
			errors++
			return true
		}

		validated++
		return true
	})

	return validated, errors, missing
}

// GetWrittenKeyCount returns the number of tracked written keys.
func (w *Workload) GetWrittenKeyCount() int {
	count := 0
	w.writtenKeys.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// ValidatePostChaos performs post-chaos validation by writing known values and reading them back.
// Uses a dedicated client to a single seed to ensure consistency.
// Returns: written, validated, notFound, wrongValue
func (w *Workload) ValidatePostChaos(numKeys int) (written int, validated int, notFound int, wrongValue int) {
	if len(w.cfg.Seeds) == 0 {
		return 0, 0, 0, 0
	}

	// Create a fresh client for validation - use only first seed for consistency
	valClient := client.New(client.Config{
		Seeds:        w.cfg.Seeds[:1], // Only use first seed
		MaxConns:     2,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxRetries:   5,
	})
	defer valClient.Close()

	// Track which keys we successfully wrote
	writtenKeys := make(map[int]bool)

	// Write validation keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("validation-key-%d", i)
		value := fmt.Sprintf("validation-value-%d", i)

		err := valClient.Set([]byte(key), []byte(value))
		if err != nil {
			continue
		}
		writtenKeys[i] = true
		written++
	}

	// Delay for propagation
	time.Sleep(2 * time.Second)

	// Read back only the keys we successfully wrote
	for i := range writtenKeys {
		key := fmt.Sprintf("validation-key-%d", i)
		expectedValue := fmt.Sprintf("validation-value-%d", i)

		got, err := valClient.Get([]byte(key))
		if err != nil {
			if err.Error() == "key not found" {
				notFound++
			} else {
				notFound++
			}
			continue
		}

		if string(got) != expectedValue {
			wrongValue++
			continue
		}

		validated++
	}

	return written, validated, notFound, wrongValue
}
