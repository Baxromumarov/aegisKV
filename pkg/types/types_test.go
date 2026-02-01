package types

import (
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	v1 := Version{Term: 1, Seq: 1}
	v2 := Version{Term: 1, Seq: 2}
	v3 := Version{Term: 2, Seq: 1}

	if !v2.IsNewerThan(v1) {
		t.Error("v2 should be after v1 (same term, higher seq)")
	}

	if !v3.IsNewerThan(v2) {
		t.Error("v3 should be after v2 (higher term)")
	}

	if v1.IsNewerThan(v2) {
		t.Error("v1 should not be after v2")
	}
}

func TestEntry(t *testing.T) {
	now := time.Now()
	entry := Entry{
		Key:     []byte("test"),
		Value:   []byte("value"),
		Expiry:  now.Add(time.Hour).UnixMilli(),
		Version: Version{Term: 1, Seq: 1},
	}

	if entry.IsExpired() {
		t.Error("entry should not be expired")
	}

	expiredEntry := Entry{
		Key:     []byte("expired"),
		Value:   []byte("value"),
		Expiry:  now.Add(-time.Hour).UnixMilli(),
		Version: Version{Term: 1, Seq: 1},
	}

	if !expiredEntry.IsExpired() {
		t.Error("entry should be expired")
	}

	noExpiryEntry := Entry{
		Key:   []byte("no-expiry"),
		Value: []byte("value"),
	}

	if noExpiryEntry.IsExpired() {
		t.Error("entry with no expiry should not be expired")
	}
}

func TestShardState(t *testing.T) {
	states := []ShardState{
		ShardStateActive,
		ShardStateMigratingOut,
		ShardStateMigratingIn,
		ShardStateDegraded,
		ShardStateReadOnly,
	}

	names := []string{
		"ACTIVE",
		"MIGRATING_OUT",
		"MIGRATING_IN",
		"DEGRADED",
		"READ_ONLY",
	}

	for i, state := range states {
		if state.String() != names[i] {
			t.Errorf("expected %s, got %s", names[i], state.String())
		}
	}
}

func TestNodeState(t *testing.T) {
	states := []NodeState{
		NodeStateAlive,
		NodeStateSuspect,
		NodeStateDead,
		NodeStateLeft,
	}

	names := []string{
		"ALIVE",
		"SUSPECT",
		"DEAD",
		"LEFT",
	}

	for i, state := range states {
		if state.String() != names[i] {
			t.Errorf("expected %s, got %s", names[i], state.String())
		}
	}
}

func TestWALMode(t *testing.T) {
	modes := []WALMode{
		WALModeOff,
		WALModeWrite,
		WALModeFsync,
	}

	names := []string{
		"off",
		"write",
		"fsync",
	}

	for i, mode := range modes {
		if mode.String() != names[i] {
			t.Errorf("expected %s, got %s", names[i], mode.String())
		}
	}
}
