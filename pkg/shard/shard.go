// Package shard manages shards for the distributed cache.
package shard

import (
	"fmt"
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/cache"
	"github.com/baxromumarov/aegisKV/pkg/consistent"
	"github.com/baxromumarov/aegisKV/pkg/invariant"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

// Shard represents a single shard of the cache.
type Shard struct {
	mu sync.RWMutex

	ID         uint64
	RangeStart consistent.Hash
	RangeEnd   consistent.Hash
	State      types.ShardState
	Term       uint64
	Seq        uint64

	Primary   string
	Followers []string

	cache *cache.Cache
	lease *types.LeaseInfo
}

// NewShard creates a new shard with the given parameters.
func NewShard(id uint64, rangeStart, rangeEnd consistent.Hash, maxBytes int64) *Shard {
	return &Shard{
		ID:         id,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
		State:      types.ShardStateDegraded,
		Term:       0,
		Seq:        0,
		cache:      cache.NewCache(maxBytes),
	}
}

// Get retrieves an entry from the shard.
func (s *Shard) Get(key string) (*types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.State.CanRead() {
		return nil, fmt.Errorf("shard %d cannot serve reads in state %s", s.ID, s.State)
	}

	entry := s.cache.Get(key)
	return entry, nil
}

// Set stores an entry in the shard.
func (s *Shard) Set(key string, value []byte, ttlMs int64) (*types.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.State.CanWrite() {
		return nil, fmt.Errorf("shard %d cannot accept writes in state %s", s.ID, s.State)
	}

	// Invariant: MIGRATING_IN shards should not accept direct writes
	invariant.Global().CheckNoPanic(
		"MIGRATING_IN_NO_DIRECT_WRITES",
		s.State != types.ShardStateMigratingIn,
		"direct write to shard %d in MIGRATING_IN state", s.ID,
	)

	s.Seq++

	var expiry int64
	if ttlMs > 0 {
		expiry = time.Now().UnixMilli() + ttlMs
	}

	entry := &types.Entry{
		Key:    []byte(key),
		Value:  value,
		Expiry: expiry,
		Version: types.Version{
			Term: s.Term,
			Seq:  s.Seq,
		},
		Created: time.Now(),
	}

	s.cache.Set(key, entry)
	return entry, nil
}

// Delete removes an entry from the shard.
func (s *Shard) Delete(key string) (*types.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.State.CanWrite() {
		return nil, fmt.Errorf("shard %d cannot accept writes in state %s", s.ID, s.State)
	}

	// Invariant: MIGRATING_IN shards should not accept direct writes
	invariant.Global().CheckNoPanic(
		"MIGRATING_IN_NO_DIRECT_WRITES",
		s.State != types.ShardStateMigratingIn,
		"direct delete on shard %d in MIGRATING_IN state", s.ID,
	)

	s.Seq++

	entry := s.cache.Delete(key)
	if entry == nil {
		entry = &types.Entry{
			Key:   []byte(key),
			Value: nil,
			Version: types.Version{
				Term: s.Term,
				Seq:  s.Seq,
			},
		}
	}
	return entry, nil
}

// ApplyReplicated applies a replicated entry from the primary.
func (s *Shard) ApplyReplicated(entry *types.Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(entry.Key)

	// Check version ordering (soft check - don't block, just log)
	existing := s.cache.Get(key)
	if existing != nil {
		invariant.Global().CheckNoPanic(
			"VERSION_ORDERING",
			entry.Version.IsNewerThan(existing.Version) || entry.Version.Term == existing.Version.Term && entry.Version.Seq == existing.Version.Seq,
			"shard %d version ordering: existing=%d.%d new=%d.%d",
			s.ID, existing.Version.Term, existing.Version.Seq, entry.Version.Term, entry.Version.Seq,
		)
	}

	if entry.Value == nil {
		if existing != nil && !entry.Version.IsNewerThan(existing.Version) {
			return false
		}
		s.cache.Delete(key)
		return true
	}

	return s.cache.SetWithVersion(key, entry)
}

// SetState updates the shard state.
func (s *Shard) SetState(state types.ShardState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Invariant: Check valid state transitions
	if s.State != state {
		invariant.Global().CheckShardStateTransition(s.ID, s.State, state)
	}

	s.State = state
}

// GetState returns the current shard state.
func (s *Shard) GetState() types.ShardState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// IncrementTerm increments the term.
func (s *Shard) IncrementTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Term++
	s.Seq = 0
	return s.Term
}

// SetTermFromReplication updates the term from a replicated value with monotonicity check.
func (s *Shard) SetTermFromReplication(newTerm uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Invariant: Term should never decrease
	if newTerm < s.Term {
		invariant.Global().CheckNoPanic(
			"TERM_MONOTONIC",
			false,
			"shard %d term would decrease from %d to %d", s.ID, s.Term, newTerm,
		)
		return false
	}

	if newTerm > s.Term {
		s.Term = newTerm
		s.Seq = 0
	}
	return true
}

// SetPrimary sets the primary node for this shard.
func (s *Shard) SetPrimary(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Primary = nodeID
}

// SetFollowers sets the follower nodes for this shard.
func (s *Shard) SetFollowers(followers []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Followers = make([]string, len(followers))
	copy(s.Followers, followers)
}

// SetLease sets the leadership lease for this shard.
func (s *Shard) SetLease(lease *types.LeaseInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = lease
}

// HasValidLease returns true if the shard has a valid leadership lease.
func (s *Shard) HasValidLease() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lease != nil && s.lease.IsValid()
}

// GetVersion returns the current version.
func (s *Shard) GetVersion() types.Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return types.Version{Term: s.Term, Seq: s.Seq}
}

// Stats returns statistics for this shard.
func (s *Shard) Stats() ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cacheStats := s.cache.Stats()
	return ShardStats{
		ID:         s.ID,
		State:      s.State,
		Term:       s.Term,
		Seq:        s.Seq,
		Primary:    s.Primary,
		Followers:  s.Followers,
		CacheStats: cacheStats,
	}
}

// ShardStats contains statistics for a shard.
type ShardStats struct {
	ID         uint64
	State      types.ShardState
	Term       uint64
	Seq        uint64
	Primary    string
	Followers  []string
	CacheStats cache.CacheStats
}

// Entries returns all entries in the shard.
func (s *Shard) Entries() []*types.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache.Entries()
}

// ContainsKey returns true if the key's hash falls within this shard's range.
func (s *Shard) ContainsKey(keyHash consistent.Hash) bool {
	if s.RangeStart <= s.RangeEnd {
		return keyHash >= s.RangeStart && keyHash <= s.RangeEnd
	}
	return keyHash >= s.RangeStart || keyHash <= s.RangeEnd
}

// CleanExpired removes expired entries from the shard.
func (s *Shard) CleanExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache.CleanExpired()
}

// Close closes the shard and releases resources.
func (s *Shard) Close() {
	s.cache.Close()
}
