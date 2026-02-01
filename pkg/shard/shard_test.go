package shard

import (
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/consistent"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

func TestNewShard(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	if s == nil {
		t.Fatal("NewShard returned nil")
	}
	if s.ID != 1 {
		t.Errorf("expected ID 1, got %d", s.ID)
	}
	if s.RangeStart != 0 {
		t.Errorf("expected RangeStart 0, got %d", s.RangeStart)
	}
	if s.RangeEnd != 1000 {
		t.Errorf("expected RangeEnd 1000, got %d", s.RangeEnd)
	}
	if s.State != types.ShardStateDegraded {
		t.Errorf("expected initial state Degraded, got %s", s.State)
	}
	if s.Term != 0 {
		t.Errorf("expected initial term 0, got %d", s.Term)
	}
}

func TestShardGetSet(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	entry, err := s.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if entry == nil {
		t.Fatal("Set returned nil entry")
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value 'value1', got '%s'", string(entry.Value))
	}

	retrieved, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Get returned nil for existing key")
	}
	if string(retrieved.Value) != "value1" {
		t.Errorf("expected value 'value1', got '%s'", string(retrieved.Value))
	}
}

func TestShardSetWithTTL(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	entry, err := s.Set("key1", []byte("value1"), 1000) // 1 second TTL
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if entry.Expiry == 0 {
		t.Error("expected non-zero expiry for TTL > 0")
	}
}

func TestShardDelete(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	s.Set("key1", []byte("value1"), 0)

	entry, err := s.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if entry == nil {
		t.Fatal("Delete returned nil")
	}

	retrieved, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if retrieved != nil {
		t.Error("Get should return nil after delete")
	}
}

func TestShardDeleteNonExistent(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	entry, err := s.Delete("nonexistent")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if entry == nil {
		t.Fatal("Delete should return a tombstone entry")
	}
}

func TestShardStateCanRead(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	// CanRead always returns true in current implementation
	_, err := s.Get("key1")
	if err != nil {
		t.Errorf("Get should succeed, got: %v", err)
	}
}

func TestShardStateCannotWrite(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	// Shard starts in Degraded state which cannot write
	defer s.Close()

	_, err := s.Set("key1", []byte("value"), 0)
	if err == nil {
		t.Error("expected error when writing to degraded shard")
	}
}

func TestShardSetState(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	s.SetState(types.ShardStateActive)

	if s.GetState() != types.ShardStateActive {
		t.Errorf("expected state Active, got %s", s.GetState())
	}
}

func TestShardIncrementTerm(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	if s.Term != 0 {
		t.Error("initial term should be 0")
	}

	term := s.IncrementTerm()
	if term != 1 {
		t.Errorf("expected term 1, got %d", term)
	}
	if s.Seq != 0 {
		t.Error("seq should be reset to 0 after term increment")
	}

	term = s.IncrementTerm()
	if term != 2 {
		t.Errorf("expected term 2, got %d", term)
	}
}

func TestShardSetTermFromReplication(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	// Increase term
	ok := s.SetTermFromReplication(5)
	if !ok {
		t.Error("SetTermFromReplication should succeed for higher term")
	}
	if s.Term != 5 {
		t.Errorf("expected term 5, got %d", s.Term)
	}

	// Same term - should succeed without change
	ok = s.SetTermFromReplication(5)
	if !ok {
		t.Error("SetTermFromReplication should succeed for same term")
	}
}

func TestShardSetPrimary(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	s.SetPrimary("node1")

	if s.Primary != "node1" {
		t.Errorf("expected Primary 'node1', got '%s'", s.Primary)
	}
}

func TestShardSetFollowers(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	followers := []string{"node2", "node3"}
	s.SetFollowers(followers)

	if len(s.Followers) != 2 {
		t.Errorf("expected 2 followers, got %d", len(s.Followers))
	}

	// Verify copy (not reference)
	followers[0] = "changed"
	if s.Followers[0] == "changed" {
		t.Error("Followers should be a copy, not reference")
	}
}

func TestShardLease(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	defer s.Close()

	if s.HasValidLease() {
		t.Error("should not have valid lease initially")
	}

	lease := &types.LeaseInfo{
		ShardID:   1,
		LeaderID:  "node1",
		Term:      1,
		ExpiresAt: time.Now().Add(time.Minute),
	}
	s.SetLease(lease)

	if !s.HasValidLease() {
		t.Error("should have valid lease after SetLease")
	}
}

func TestShardGetVersion(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	v := s.GetVersion()
	if v.Term != 0 || v.Seq != 0 {
		t.Error("initial version should be 0.0")
	}

	s.Set("key1", []byte("value"), 0)
	v = s.GetVersion()
	if v.Seq != 1 {
		t.Errorf("expected seq 1 after set, got %d", v.Seq)
	}
}

func TestShardStats(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	s.SetPrimary("node1")
	s.SetFollowers([]string{"node2"})
	defer s.Close()

	s.Set("key1", []byte("value"), 0)

	stats := s.Stats()

	if stats.ID != 1 {
		t.Errorf("expected ID 1, got %d", stats.ID)
	}
	if stats.State != types.ShardStateActive {
		t.Errorf("expected state Active, got %s", stats.State)
	}
	if stats.Primary != "node1" {
		t.Error("Primary should be node1")
	}
	if len(stats.Followers) != 1 {
		t.Error("should have 1 follower")
	}
}

func TestShardEntries(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	s.Set("key1", []byte("value1"), 0)
	s.Set("key2", []byte("value2"), 0)

	entries := s.Entries()
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestShardContainsKey(t *testing.T) {
	s := NewShard(1, 100, 200, 1024*1024)
	defer s.Close()

	tests := []struct {
		hash     consistent.Hash
		expected bool
	}{
		{50, false},
		{100, true},
		{150, true},
		{200, true},
		{250, false},
	}

	for _, tt := range tests {
		if s.ContainsKey(tt.hash) != tt.expected {
			t.Errorf("ContainsKey(%d) = %v, expected %v", tt.hash, !tt.expected, tt.expected)
		}
	}
}

func TestShardContainsKeyWrapAround(t *testing.T) {
	// Wrap-around case: range starts at high and ends at low
	s := NewShard(1, 900, 100, 1024*1024)
	defer s.Close()

	tests := []struct {
		hash     consistent.Hash
		expected bool
	}{
		{950, true},  // >= 900
		{50, true},   // <= 100
		{500, false}, // in between
	}

	for _, tt := range tests {
		if s.ContainsKey(tt.hash) != tt.expected {
			t.Errorf("ContainsKey(%d) = %v, expected %v", tt.hash, !tt.expected, tt.expected)
		}
	}
}

func TestShardCleanExpired(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	// Set with 1ms TTL
	s.Set("key1", []byte("value1"), 1)
	time.Sleep(10 * time.Millisecond)

	cleaned := s.CleanExpired()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned, got %d", cleaned)
	}
}

func TestShardApplyReplicated(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	entry := &types.Entry{
		Key:     []byte("key1"),
		Value:   []byte("value1"),
		Version: types.Version{Term: 1, Seq: 1},
	}

	applied := s.ApplyReplicated(entry)
	if !applied {
		t.Error("expected ApplyReplicated to succeed")
	}

	retrieved, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("expected entry after apply")
	}
	if string(retrieved.Value) != "value1" {
		t.Errorf("expected value 'value1', got '%s'", string(retrieved.Value))
	}
}

func TestShardApplyReplicatedDelete(t *testing.T) {
	s := NewShard(1, 0, 1000, 1024*1024)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	// First add an entry
	entry := &types.Entry{
		Key:     []byte("key1"),
		Value:   []byte("value1"),
		Version: types.Version{Term: 1, Seq: 1},
	}
	s.ApplyReplicated(entry)

	// Then delete via replication
	deleteEntry := &types.Entry{
		Key:     []byte("key1"),
		Value:   nil, // nil value = delete
		Version: types.Version{Term: 1, Seq: 2},
	}
	applied := s.ApplyReplicated(deleteEntry)
	if !applied {
		t.Error("expected delete to be applied")
	}

	retrieved, _ := s.Get("key1")
	if retrieved != nil {
		t.Error("entry should be deleted")
	}
}

func BenchmarkShardSet(b *testing.B) {
	s := NewShard(1, 0, 1000, 1024*1024*10)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	value := []byte("test-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key", value, 0)
	}
}

func BenchmarkShardGet(b *testing.B) {
	s := NewShard(1, 0, 1000, 1024*1024*10)
	s.SetState(types.ShardStateActive)
	defer s.Close()

	s.Set("key", []byte("value"), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}
