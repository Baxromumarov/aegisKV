// Package quorum provides quorum-based consistency for reads and writes.
package quorum

import (
	"context"
	"sync"
	"time"
)

// Level represents consistency level.
type Level int

const (
	LevelOne    Level = 1  // Single node response
	LevelQuorum Level = 0  // Majority of replicas (special value, computed at runtime)
	LevelAll    Level = -1 // All replicas must respond
)

// WriteResult represents the result of a write to one replica.
type WriteResult struct {
	NodeID  string
	Version uint64
	Err     error
}

// ReadResult represents the result of a read from one replica.
type ReadResult struct {
	NodeID  string
	Value   []byte
	TTL     int64
	Version uint64
	Found   bool
	Err     error
}

// Coordinator handles quorum operations.
type Coordinator struct {
	writeLevel Level
	readLevel  Level
	timeout    time.Duration
}

// Config holds coordinator configuration.
type Config struct {
	WriteLevel Level         // Minimum writes for success (0 = quorum)
	ReadLevel  Level         // Minimum reads for success (0 = quorum)
	Timeout    time.Duration // Timeout for quorum operations
}

// NewCoordinator creates a new quorum coordinator.
func NewCoordinator(cfg Config) *Coordinator {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Coordinator{
		writeLevel: cfg.WriteLevel,
		readLevel:  cfg.ReadLevel,
		timeout:    cfg.Timeout,
	}
}

// ComputeQuorum returns the quorum size for n replicas.
func ComputeQuorum(n int) int {
	return (n / 2) + 1
}

// ResolveLevel resolves a level to an actual count given total replicas.
func ResolveLevel(level Level, total int) int {
	switch level {
	case LevelOne:
		return 1
	case LevelQuorum:
		return ComputeQuorum(total)
	case LevelAll:
		return total
	default:
		if int(level) > 0 && int(level) <= total {
			return int(level)
		}
		return ComputeQuorum(total)
	}
}

// WaitForWrites waits for enough write results to satisfy the consistency level.
func (c *Coordinator) WaitForWrites(ctx context.Context, results <-chan WriteResult, total int) ([]WriteResult, error) {
	required := ResolveLevel(c.writeLevel, total)
	return waitForResults(ctx, results, total, required, c.timeout, func(r WriteResult) bool {
		return r.Err == nil
	})
}

// WaitForReads waits for enough read results to satisfy the consistency level.
// Returns all results received, including failures.
func (c *Coordinator) WaitForReads(ctx context.Context, results <-chan ReadResult, total int) ([]ReadResult, error) {
	required := ResolveLevel(c.readLevel, total)
	collected := make([]ReadResult, 0, total)

	deadline := time.Now().Add(c.timeout)
	successCount := 0

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return collected, ctx.Err()
		case r, ok := <-results:
			if !ok {
				goto done
			}
			collected = append(collected, r)
			if r.Err == nil && r.Found {
				successCount++
			}
			if successCount >= required {
				goto done
			}
		case <-time.After(remaining):
			goto done
		}
	}

done:
	if successCount < required {
		return collected, ErrQuorumNotMet
	}
	return collected, nil
}

// ResolveRead picks the best value from multiple read results.
// Returns the value with the highest version, and nodes that need repair.
func ResolveRead(results []ReadResult) (best *ReadResult, staleNodes []string) {
	var maxVersion uint64
	for i := range results {
		r := &results[i]
		if r.Err == nil && r.Found && r.Version >= maxVersion {
			maxVersion = r.Version
			best = r
		}
	}

	if best == nil {
		return nil, nil
	}

	// Find nodes with stale data
	for _, r := range results {
		if r.Err == nil && r.Found && r.Version < maxVersion {
			staleNodes = append(staleNodes, r.NodeID)
		}
	}

	return best, staleNodes
}

// Error types
type quorumError string

func (e quorumError) Error() string { return string(e) }

const (
	ErrQuorumNotMet quorumError = "quorum not met"
	ErrTimeout      quorumError = "operation timed out"
)

// waitForResults is a generic helper to wait for results.
func waitForResults[T any](ctx context.Context, results <-chan T, total, required int, timeout time.Duration, isSuccess func(T) bool) ([]T, error) {
	collected := make([]T, 0, total)
	deadline := time.Now().Add(timeout)
	successCount := 0

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return collected, ctx.Err()
		case r, ok := <-results:
			if !ok {
				goto done
			}
			collected = append(collected, r)
			if isSuccess(r) {
				successCount++
			}
			if successCount >= required {
				goto done
			}
		case <-time.After(remaining):
			goto done
		}
	}

done:
	if successCount < required {
		return collected, ErrQuorumNotMet
	}
	return collected, nil
}

// ScatterGather executes operations on multiple nodes and gathers results.
type ScatterGather[T any] struct {
	mu      sync.Mutex
	results chan T
	wg      sync.WaitGroup
}

// NewScatterGather creates a scatter-gather coordinator.
func NewScatterGather[T any](size int) *ScatterGather[T] {
	return &ScatterGather[T]{
		results: make(chan T, size),
	}
}

// Go launches an operation that will send its result.
func (sg *ScatterGather[T]) Go(fn func() T) {
	sg.wg.Add(1)
	go func() {
		defer sg.wg.Done()
		sg.results <- fn()
	}()
}

// Results returns the results channel.
func (sg *ScatterGather[T]) Results() <-chan T {
	return sg.results
}

// Close closes the results channel after all goroutines complete.
func (sg *ScatterGather[T]) Close() {
	go func() {
		sg.wg.Wait()
		close(sg.results)
	}()
}
