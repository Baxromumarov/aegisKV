// Package types defines core data types for the distributed cache system.
package types

import (
	"sync"
	"time"
)

// Version represents a monotonic version for conflict resolution.
// Term changes on leadership, Seq increments per write.
type Version struct {
	Term uint64 `json:"term"`
	Seq  uint64 `json:"seq"`
}

// Compare returns -1 if v < other, 0 if equal, 1 if v > other.
func (v Version) Compare(other Version) int {
	if v.Term < other.Term {
		return -1
	}
	if v.Term > other.Term {
		return 1
	}
	if v.Seq < other.Seq {
		return -1
	}
	if v.Seq > other.Seq {
		return 1
	}
	return 0
}

// IsNewerThan returns true if this version is newer than other.
func (v Version) IsNewerThan(other Version) bool {
	return v.Compare(other) > 0
}

// Entry represents a key-value entry in the cache.
type Entry struct {
	Key     []byte    `json:"key"`
	Value   []byte    `json:"value"`
	Expiry  int64     `json:"expiry,omitempty"`
	Version Version   `json:"version"`
	Created time.Time `json:"created"`
}

// IsExpired returns true if the entry has expired.
func (e *Entry) IsExpired() bool {
	if e.Expiry == 0 {
		return false
	}
	return time.Now().UnixMilli() > e.Expiry
}

// TTL returns remaining time-to-live, or 0 if no expiry.
func (e *Entry) TTL() time.Duration {
	if e.Expiry == 0 {
		return 0
	}
	remaining := e.Expiry - time.Now().UnixMilli()
	if remaining < 0 {
		return 0
	}
	return time.Duration(remaining) * time.Millisecond
}

// ShardState represents the current state of a shard.
type ShardState int

const (
	ShardStateActive ShardState = iota
	ShardStateMigratingOut
	ShardStateMigratingIn
	ShardStateDegraded
	ShardStateReadOnly
)

func (s ShardState) String() string {
	switch s {
	case ShardStateActive:
		return "ACTIVE"
	case ShardStateMigratingOut:
		return "MIGRATING_OUT"
	case ShardStateMigratingIn:
		return "MIGRATING_IN"
	case ShardStateDegraded:
		return "DEGRADED"
	case ShardStateReadOnly:
		return "READ_ONLY"
	default:
		return "UNKNOWN"
	}
}

// CanRead returns true if reads are allowed in this state.
func (s ShardState) CanRead() bool {
	return true
}

// CanWrite returns true if writes are allowed in this state.
func (s ShardState) CanWrite() bool {
	return s == ShardStateActive || s == ShardStateMigratingOut
}

// NodeState represents the state of a node in the cluster.
type NodeState int

const (
	NodeStateAlive NodeState = iota
	NodeStateSuspect
	NodeStateDead
	NodeStateLeft
)

func (s NodeState) String() string {
	switch s {
	case NodeStateAlive:
		return "ALIVE"
	case NodeStateSuspect:
		return "SUSPECT"
	case NodeStateDead:
		return "DEAD"
	case NodeStateLeft:
		return "LEFT"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo represents information about a cluster node.
type NodeInfo struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"`
	IntraAddr string    `json:"intraAddr"`
	State     NodeState `json:"state"`
	LastSeen  time.Time `json:"lastSeen"`
	Meta      NodeMeta  `json:"meta"`
}

// NodeMeta contains metadata about a node.
type NodeMeta struct {
	VirtualNodes int    `json:"virtualNodes"`
	Zone         string `json:"zone,omitempty"`
	Weight       int    `json:"weight"`
}

// ShardInfo represents information about a shard.
type ShardInfo struct {
	ID         uint64     `json:"id"`
	RangeStart uint64     `json:"rangeStart"`
	RangeEnd   uint64     `json:"rangeEnd"`
	Primary    string     `json:"primary"`
	Followers  []string   `json:"followers"`
	State      ShardState `json:"state"`
	Term       uint64     `json:"term"`
	mu         sync.RWMutex
}

// OpType defines operation types for the cache.
type OpType int

const (
	OpGet OpType = iota
	OpSet
	OpDel
	OpCAS
	OpGetMeta
	OpReplicate
	OpMigrate
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	case OpDel:
		return "DEL"
	case OpCAS:
		return "CAS"
	case OpGetMeta:
		return "GETMETA"
	case OpReplicate:
		return "REPLICATE"
	case OpMigrate:
		return "MIGRATE"
	default:
		return "UNKNOWN"
	}
}

// Request represents a cache request.
type Request struct {
	ID      uint64  `json:"id"`
	Op      OpType  `json:"op"`
	Key     []byte  `json:"key"`
	Value   []byte  `json:"value,omitempty"`
	TTL     int64   `json:"ttl,omitempty"`
	Version Version `json:"version,omitempty"`
	ShardID uint64  `json:"shardId,omitempty"`
}

// Response represents a cache response.
type Response struct {
	ID      uint64  `json:"id"`
	Success bool    `json:"success"`
	Value   []byte  `json:"value,omitempty"`
	Version Version `json:"version,omitempty"`
	Error   string  `json:"error,omitempty"`
}

// WALMode represents the write-ahead log mode.
type WALMode int

const (
	WALModeOff WALMode = iota
	WALModeWrite
	WALModeFsync
)

func (m WALMode) String() string {
	switch m {
	case WALModeOff:
		return "off"
	case WALModeWrite:
		return "write"
	case WALModeFsync:
		return "fsync"
	default:
		return "unknown"
	}
}

// LeaseInfo represents a leadership lease.
type LeaseInfo struct {
	ShardID   uint64    `json:"shardId"`
	LeaderID  string    `json:"leaderId"`
	Term      uint64    `json:"term"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// IsValid returns true if the lease is still valid.
func (l *LeaseInfo) IsValid() bool {
	return time.Now().Before(l.ExpiresAt)
}
