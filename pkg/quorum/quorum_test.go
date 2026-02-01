package quorum

import (
	"context"
	"testing"
	"time"
)

func TestComputeQuorum(t *testing.T) {
	tests := []struct {
		n        int
		expected int
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
		{7, 4},
	}

	for _, tt := range tests {
		result := ComputeQuorum(tt.n)
		if result != tt.expected {
			t.Errorf("ComputeQuorum(%d) = %d, expected %d", tt.n, result, tt.expected)
		}
	}
}

func TestResolveLevel(t *testing.T) {
	tests := []struct {
		level    Level
		total    int
		expected int
	}{
		{LevelOne, 3, 1},
		{LevelOne, 5, 1},
		{LevelQuorum, 3, 2},
		{LevelQuorum, 5, 3},
		{LevelAll, 3, 3},
		{LevelAll, 5, 5},
		{Level(2), 3, 2},
		{Level(4), 5, 4},
		{Level(10), 5, 3}, // Exceeds total, defaults to quorum
	}

	for _, tt := range tests {
		result := ResolveLevel(tt.level, tt.total)
		if result != tt.expected {
			t.Errorf("ResolveLevel(%d, %d) = %d, expected %d", tt.level, tt.total, result, tt.expected)
		}
	}
}

func TestNewCoordinator(t *testing.T) {
	c := NewCoordinator(Config{
		WriteLevel: LevelQuorum,
		ReadLevel:  LevelOne,
		Timeout:    2 * time.Second,
	})

	if c == nil {
		t.Fatal("NewCoordinator returned nil")
	}
	if c.writeLevel != LevelQuorum {
		t.Errorf("expected write level Quorum, got %d", c.writeLevel)
	}
	if c.readLevel != LevelOne {
		t.Errorf("expected read level One, got %d", c.readLevel)
	}
	if c.timeout != 2*time.Second {
		t.Errorf("expected timeout 2s, got %v", c.timeout)
	}
}

func TestNewCoordinatorDefaults(t *testing.T) {
	c := NewCoordinator(Config{})

	if c.timeout != 5*time.Second {
		t.Errorf("expected default timeout 5s, got %v", c.timeout)
	}
}

func TestWaitForWritesSuccess(t *testing.T) {
	c := NewCoordinator(Config{
		WriteLevel: LevelQuorum,
		Timeout:    time.Second,
	})

	results := make(chan WriteResult, 3)
	results <- WriteResult{NodeID: "node1", Version: 1, Err: nil}
	results <- WriteResult{NodeID: "node2", Version: 1, Err: nil}
	close(results)

	ctx := context.Background()
	collected, err := c.WaitForWrites(ctx, results, 3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(collected) != 2 {
		t.Errorf("expected 2 results, got %d", len(collected))
	}
}

func TestWaitForWritesQuorumNotMet(t *testing.T) {
	c := NewCoordinator(Config{
		WriteLevel: LevelQuorum,
		Timeout:    100 * time.Millisecond,
	})

	results := make(chan WriteResult, 3)
	results <- WriteResult{NodeID: "node1", Version: 1, Err: nil}
	close(results)

	ctx := context.Background()
	_, err := c.WaitForWrites(ctx, results, 3)

	if err != ErrQuorumNotMet {
		t.Errorf("expected ErrQuorumNotMet, got %v", err)
	}
}

func TestWaitForWritesContextCancel(t *testing.T) {
	c := NewCoordinator(Config{
		WriteLevel: LevelAll,
		Timeout:    time.Second,
	})

	results := make(chan WriteResult, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.WaitForWrites(ctx, results, 3)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestWaitForReadsSuccess(t *testing.T) {
	c := NewCoordinator(Config{
		ReadLevel: LevelOne,
		Timeout:   time.Second,
	})

	results := make(chan ReadResult, 3)
	results <- ReadResult{NodeID: "node1", Value: []byte("value"), Found: true, Version: 1}
	close(results)

	ctx := context.Background()
	collected, err := c.WaitForReads(ctx, results, 3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(collected) != 1 {
		t.Errorf("expected 1 result, got %d", len(collected))
	}
}

func TestWaitForReadsQuorum(t *testing.T) {
	c := NewCoordinator(Config{
		ReadLevel: LevelQuorum,
		Timeout:   time.Second,
	})

	results := make(chan ReadResult, 3)
	results <- ReadResult{NodeID: "node1", Value: []byte("value"), Found: true, Version: 1}
	results <- ReadResult{NodeID: "node2", Value: []byte("value"), Found: true, Version: 1}
	close(results)

	ctx := context.Background()
	collected, err := c.WaitForReads(ctx, results, 3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(collected) != 2 {
		t.Errorf("expected 2 results, got %d", len(collected))
	}
}

func TestWaitForReadsNotFound(t *testing.T) {
	c := NewCoordinator(Config{
		ReadLevel: LevelQuorum,
		Timeout:   100 * time.Millisecond,
	})

	results := make(chan ReadResult, 3)
	results <- ReadResult{NodeID: "node1", Found: false, Version: 0}
	results <- ReadResult{NodeID: "node2", Found: false, Version: 0}
	close(results)

	ctx := context.Background()
	_, err := c.WaitForReads(ctx, results, 3)

	if err != ErrQuorumNotMet {
		t.Errorf("expected ErrQuorumNotMet, got %v", err)
	}
}

func TestResolveRead(t *testing.T) {
	results := []ReadResult{
		{NodeID: "node1", Value: []byte("old"), Found: true, Version: 1},
		{NodeID: "node2", Value: []byte("new"), Found: true, Version: 3},
		{NodeID: "node3", Value: []byte("mid"), Found: true, Version: 2},
	}

	best, stale := ResolveRead(results)

	if best == nil {
		t.Fatal("expected best result")
	}
	if best.NodeID != "node2" {
		t.Errorf("expected node2 as best, got %s", best.NodeID)
	}
	if best.Version != 3 {
		t.Errorf("expected version 3, got %d", best.Version)
	}
	if len(stale) != 2 {
		t.Errorf("expected 2 stale nodes, got %d", len(stale))
	}
}

func TestResolveReadNoResults(t *testing.T) {
	var results []ReadResult

	best, stale := ResolveRead(results)

	if best != nil {
		t.Error("expected nil best for empty results")
	}
	if stale != nil {
		t.Error("expected nil stale for empty results")
	}
}

func TestResolveReadAllErrors(t *testing.T) {
	results := []ReadResult{
		{NodeID: "node1", Err: ErrTimeout},
		{NodeID: "node2", Err: ErrTimeout},
	}

	best, stale := ResolveRead(results)

	if best != nil {
		t.Error("expected nil best when all have errors")
	}
	if stale != nil {
		t.Error("expected nil stale when all have errors")
	}
}

func TestResolveReadSameVersion(t *testing.T) {
	results := []ReadResult{
		{NodeID: "node1", Value: []byte("v"), Found: true, Version: 5},
		{NodeID: "node2", Value: []byte("v"), Found: true, Version: 5},
		{NodeID: "node3", Value: []byte("v"), Found: true, Version: 5},
	}

	best, stale := ResolveRead(results)

	if best == nil {
		t.Fatal("expected best result")
	}
	if len(stale) != 0 {
		t.Errorf("expected 0 stale nodes when all same version, got %d", len(stale))
	}
}

func TestQuorumErrors(t *testing.T) {
	if ErrQuorumNotMet.Error() != "quorum not met" {
		t.Error("ErrQuorumNotMet has wrong message")
	}
	if ErrTimeout.Error() != "operation timed out" {
		t.Error("ErrTimeout has wrong message")
	}
}

func TestScatterGather(t *testing.T) {
	sg := NewScatterGather[int](3)

	sg.Go(func() int { return 1 })
	sg.Go(func() int { return 2 })
	sg.Go(func() int { return 3 })
	sg.Close()

	sum := 0
	for r := range sg.Results() {
		sum += r
	}

	if sum != 6 {
		t.Errorf("expected sum 6, got %d", sum)
	}
}

func TestScatterGatherWithDelay(t *testing.T) {
	sg := NewScatterGather[string](2)

	sg.Go(func() string {
		time.Sleep(10 * time.Millisecond)
		return "slow"
	})
	sg.Go(func() string {
		return "fast"
	})
	sg.Close()

	results := make([]string, 0, 2)
	for r := range sg.Results() {
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func BenchmarkComputeQuorum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ComputeQuorum(5)
	}
}

func BenchmarkResolveLevel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ResolveLevel(LevelQuorum, 5)
	}
}

func BenchmarkResolveRead(b *testing.B) {
	results := []ReadResult{
		{NodeID: "node1", Value: []byte("old"), Found: true, Version: 1},
		{NodeID: "node2", Value: []byte("new"), Found: true, Version: 3},
		{NodeID: "node3", Value: []byte("mid"), Found: true, Version: 2},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ResolveRead(results)
	}
}
