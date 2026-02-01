// Package invariant provides runtime invariant checks for distributed correctness.
// These checks detect violations that indicate serious bugs or corruption.
// Violations are logged and can optionally trigger a panic (fail-fast mode).
package invariant

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// Violation represents a detected invariant violation.
type Violation struct {
	Name      string
	Message   string
	Timestamp time.Time
	Stack     string
}

// Checker tracks and checks invariants.
type Checker struct {
	mu          sync.RWMutex
	nodeID      string
	failFast    bool
	violations  []Violation
	totalChecks int64
	failedCheck int64

	// Callbacks for external logging
	onViolation func(v Violation)
}

// New creates a new invariant checker.
func New(nodeID string, failFast bool) *Checker {
	return &Checker{
		nodeID:     nodeID,
		failFast:   failFast,
		violations: make([]Violation, 0),
	}
}

// SetViolationCallback sets a callback for violations.
func (c *Checker) SetViolationCallback(fn func(v Violation)) {
	c.mu.Lock()
	c.onViolation = fn
	c.mu.Unlock()
}

// Check checks an invariant condition.
// If the condition is false, it records a violation.
func (c *Checker) Check(name string, condition bool, format string, args ...any) bool {
	atomic.AddInt64(&c.totalChecks, 1)

	if condition {
		return true
	}

	atomic.AddInt64(&c.failedCheck, 1)

	message := fmt.Sprintf(format, args...)
	stack := getStack()

	v := Violation{
		Name:      name,
		Message:   message,
		Timestamp: time.Now(),
		Stack:     stack,
	}

	c.mu.Lock()
	c.violations = append(c.violations, v)
	callback := c.onViolation
	c.mu.Unlock()

	if callback != nil {
		callback(v)
	}

	// Log the violation
	fmt.Printf("[INVARIANT VIOLATION] %s: %s\n  at: %s\n", name, message, stack)

	if c.failFast {
		panic(fmt.Sprintf("INVARIANT VIOLATION [%s]: %s", name, message))
	}

	return false
}

// CheckNoPanic checks an invariant but never panics (for soft checks).
func (c *Checker) CheckNoPanic(name string, condition bool, format string, args ...any) bool {
	atomic.AddInt64(&c.totalChecks, 1)

	if condition {
		return true
	}

	atomic.AddInt64(&c.failedCheck, 1)

	message := fmt.Sprintf(format, args...)
	v := Violation{
		Name:      name,
		Message:   message,
		Timestamp: time.Now(),
		Stack:     getStack(),
	}

	c.mu.Lock()
	c.violations = append(c.violations, v)
	c.mu.Unlock()

	fmt.Printf("[INVARIANT VIOLATION] %s: %s\n", name, message)
	return false
}

// Violations returns all recorded violations.
func (c *Checker) Violations() []Violation {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]Violation, len(c.violations))
	copy(result, c.violations)
	return result
}

// Stats returns checker statistics.
func (c *Checker) Stats() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]any{
		"total_checks":    atomic.LoadInt64(&c.totalChecks),
		"failed_checks":   atomic.LoadInt64(&c.failedCheck),
		"violation_count": len(c.violations),
		"fail_fast":       c.failFast,
		"last_violation":  c.lastViolationTime(),
	}
}

func (c *Checker) lastViolationTime() string {
	if len(c.violations) == 0 {
		return ""
	}
	return c.violations[len(c.violations)-1].Timestamp.Format(time.RFC3339)
}

// Clear clears all recorded violations.
func (c *Checker) Clear() {
	c.mu.Lock()
	c.violations = c.violations[:0]
	c.mu.Unlock()
}

// ===== Common Invariant Checks =====

// CheckSinglePrimary checks that at most one node claims primary for a shard.
func (c *Checker) CheckSinglePrimary(shardID uint64, claimedPrimary string, allClaims map[string]string) bool {
	primaries := make([]string, 0)
	for nodeID, primary := range allClaims {
		if primary == nodeID {
			primaries = append(primaries, nodeID)
		}
	}

	return c.Check(
		"SINGLE_PRIMARY",
		len(primaries) <= 1,
		"shard %d has multiple primaries: %v", shardID, primaries,
	)
}

// CheckWriteOnPrimary checks that writes only occur on the primary.
func (c *Checker) CheckWriteOnPrimary(shardID uint64, nodeID, shardPrimary string) bool {
	return c.Check(
		"WRITE_ON_PRIMARY",
		nodeID == shardPrimary,
		"write attempted on shard %d by %s, but primary is %s",
		shardID, nodeID, shardPrimary,
	)
}

// CheckTermNeverDecreases checks that term never goes backwards.
func (c *Checker) CheckTermNeverDecreases(shardID uint64, oldTerm, newTerm uint64) bool {
	return c.Check(
		"TERM_MONOTONIC",
		newTerm >= oldTerm,
		"shard %d term decreased from %d to %d",
		shardID, oldTerm, newTerm,
	)
}

// CheckMigratingInNoWrites checks that MIGRATING_IN shards don't accept direct writes.
func (c *Checker) CheckMigratingInNoWrites(shardID uint64, state types.ShardState, isDirectWrite bool) bool {
	if !isDirectWrite {
		return true // Replicated writes are OK
	}
	return c.Check(
		"MIGRATING_IN_NO_DIRECT_WRITES",
		state != types.ShardStateMigratingIn,
		"direct write to shard %d in MIGRATING_IN state",
		shardID,
	)
}

// CheckWALShardOwnership checks that WAL entries are only applied to owned shards.
func (c *Checker) CheckWALShardOwnership(shardID uint64, ownedShards map[uint64]bool) bool {
	return c.Check(
		"WAL_SHARD_OWNERSHIP",
		ownedShards[shardID],
		"WAL replay attempted on shard %d which is not owned",
		shardID,
	)
}

// CheckVersionOrdering checks that versions are strictly ordered within a shard.
func (c *Checker) CheckVersionOrdering(shardID uint64, old, new types.Version) bool {
	return c.Check(
		"VERSION_ORDERING",
		new.IsNewerThan(old) || new.Term == old.Term && new.Seq == old.Seq,
		"shard %d version ordering violated: old=%d.%d new=%d.%d",
		shardID, old.Term, old.Seq, new.Term, new.Seq,
	)
}

// CheckShardStateTransition checks valid state transitions.
func (c *Checker) CheckShardStateTransition(shardID uint64, from, to types.ShardState) bool {
	valid := isValidStateTransition(from, to)
	return c.Check(
		"SHARD_STATE_TRANSITION",
		valid,
		"shard %d invalid state transition: %s -> %s",
		shardID, from.String(), to.String(),
	)
}

// isValidStateTransition checks if a state transition is valid.
func isValidStateTransition(from, to types.ShardState) bool {
	// All transitions are documented here for clarity
	validTransitions := map[types.ShardState][]types.ShardState{
		types.ShardStateDegraded: {
			types.ShardStateActive,
			types.ShardStateReadOnly,
			types.ShardStateMigratingIn,
			types.ShardStateMigratingOut,
		},
		types.ShardStateActive: {
			types.ShardStateDegraded,
			types.ShardStateReadOnly,
			types.ShardStateMigratingOut,
		},
		types.ShardStateMigratingIn: {
			types.ShardStateActive,
			types.ShardStateDegraded,
		},
		types.ShardStateMigratingOut: {
			types.ShardStateDegraded,
		},
		types.ShardStateReadOnly: {
			types.ShardStateActive,
			types.ShardStateDegraded,
		},
	}

	allowed, ok := validTransitions[from]
	if !ok {
		return true // Unknown state, allow
	}

	for _, s := range allowed {
		if s == to {
			return true
		}
	}

	// Same state is always valid
	return from == to
}

func getStack() string {
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", file, line)
}

// ===== Global Instance (optional convenience) =====

var globalChecker *Checker
var globalOnce sync.Once

// Global returns the global invariant checker.
func Global() *Checker {
	globalOnce.Do(func() {
		globalChecker = New("", false)
	})
	return globalChecker
}

// InitGlobal initializes the global checker with specific settings.
func InitGlobal(nodeID string, failFast bool) {
	globalChecker = New(nodeID, failFast)
}
