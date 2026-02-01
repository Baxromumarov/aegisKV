package circuitbreaker

import (
	"sync"
	"testing"
	"time"
)

// TestNewCircuitBreaker tests creating a new circuit breaker.
func TestNewCircuitBreaker(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		HalfOpenMax:      3,
		SuccessThreshold: 2,
	})

	if cb == nil {
		t.Fatal("expected non-nil circuit breaker")
	}
	if cb.name != "test" {
		t.Errorf("expected name 'test', got %q", cb.name)
	}
}

// TestNewCircuitBreakerDefaults tests default configuration values.
func TestNewCircuitBreakerDefaults(t *testing.T) {
	cb := New(Config{Name: "test"})

	if cb.maxFailures != 5 {
		t.Errorf("expected MaxFailures=5, got %d", cb.maxFailures)
	}
	if cb.resetTimeout != 30*time.Second {
		t.Errorf("expected ResetTimeout=30s, got %v", cb.resetTimeout)
	}
	if cb.halfOpenMax != 3 {
		t.Errorf("expected HalfOpenMax=3, got %d", cb.halfOpenMax)
	}
	if cb.successThreshold != 2 {
		t.Errorf("expected SuccessThreshold=2, got %d", cb.successThreshold)
	}
}

// TestCircuitBreakerInitialState tests that CB starts in closed state.
func TestCircuitBreakerInitialState(t *testing.T) {
	cb := New(Config{Name: "test"})

	if cb.State() != StateClosed {
		t.Errorf("expected initial state Closed, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Error("should allow requests in closed state")
	}
}

// TestStateString tests the string representation of states.
func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}

	for _, tc := range tests {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("State(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

// TestCircuitBreakerOpensOnFailures tests that CB opens after MaxFailures.
func TestCircuitBreakerOpensOnFailures(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      3,
		ResetTimeout:     100 * time.Millisecond,
		HalfOpenMax:      1,
		SuccessThreshold: 1,
	})

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		if !cb.Allow() {
			t.Errorf("should allow request %d before opening", i)
		}
		cb.RecordFailure()
	}

	// Should now be open
	if cb.State() != StateOpen {
		t.Errorf("expected state Open, got %v", cb.State())
	}
	if cb.Allow() {
		t.Error("should not allow requests in open state")
	}
}

// TestCircuitBreakerHalfOpen tests transition to half-open state.
func TestCircuitBreakerHalfOpen(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      2,
		ResetTimeout:     50 * time.Millisecond,
		HalfOpenMax:      2,
		SuccessThreshold: 1,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected Open state, got %v", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open on next Allow()
	if !cb.Allow() {
		t.Error("should allow first request in half-open state")
	}
	if cb.State() != StateHalfOpen {
		t.Errorf("expected HalfOpen state, got %v", cb.State())
	}
}

// TestCircuitBreakerCloses tests transition from half-open to closed.
func TestCircuitBreakerCloses(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      2,
		ResetTimeout:     50 * time.Millisecond,
		HalfOpenMax:      2,
		SuccessThreshold: 2,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow() // Transition to half-open

	// Record successes to close
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Errorf("expected Closed state after successes, got %v", cb.State())
	}
}

// TestCircuitBreakerReopensOnFailure tests that half-open reopens on failure.
func TestCircuitBreakerReopensOnFailure(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      2,
		ResetTimeout:     50 * time.Millisecond,
		HalfOpenMax:      5,
		SuccessThreshold: 2,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow() // Transition to half-open

	// Fail in half-open
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Errorf("expected Open state after failure in half-open, got %v", cb.State())
	}
}

// TestCircuitBreakerReset tests the Reset method.
func TestCircuitBreakerReset(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      2,
		ResetTimeout:     1 * time.Hour, // Long timeout
		HalfOpenMax:      1,
		SuccessThreshold: 1,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected Open state")
	}

	// Manual reset
	cb.Reset()

	if cb.State() != StateClosed {
		t.Errorf("expected Closed state after reset, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Error("should allow requests after reset")
	}
}

// TestCircuitBreakerHalfOpenLimit tests HalfOpenMax limit.
func TestCircuitBreakerHalfOpenLimit(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      1,
		ResetTimeout:     50 * time.Millisecond,
		HalfOpenMax:      2,
		SuccessThreshold: 5,
	})

	// Open the circuit
	cb.RecordFailure()

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	// First Allow() transitions to half-open (doesn't count toward limit)
	if !cb.Allow() {
		t.Error("first Allow should transition to half-open")
	}

	// Now in half-open, should allow HalfOpenMax more requests
	if !cb.Allow() {
		t.Error("should allow request 1 in half-open")
	}
	if !cb.Allow() {
		t.Error("should allow request 2 in half-open")
	}

	// Next request should be rejected (halfOpenCount == HalfOpenMax)
	if cb.Allow() {
		t.Error("should not allow more than HalfOpenMax requests in half-open")
	}
}

// TestCircuitBreakerConcurrency tests thread safety.
func TestCircuitBreakerConcurrency(t *testing.T) {
	cb := New(Config{
		Name:             "test",
		MaxFailures:      1000,
		ResetTimeout:     100 * time.Millisecond,
		HalfOpenMax:      10,
		SuccessThreshold: 5,
	})

	var wg sync.WaitGroup
	n := 100

	// Concurrent Allow + RecordSuccess
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			cb.Allow()
			cb.RecordSuccess()
		}()
	}
	wg.Wait()

	// Should still be closed
	if cb.State() != StateClosed {
		t.Errorf("expected Closed state, got %v", cb.State())
	}
}

// TestCircuitBreakerStats tests the Stats method.
func TestCircuitBreakerStats(t *testing.T) {
	cb := New(Config{Name: "test"})

	cb.RecordSuccess()
	cb.RecordFailure()

	stats := cb.Stats()

	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats["name"] != "test" {
		t.Errorf("expected name 'test' in stats, got %v", stats["name"])
	}
}

// TestManagerNew tests creating a new manager.
func TestManagerNew(t *testing.T) {
	mgr := NewManager(Config{Name: "default"})

	if mgr == nil {
		t.Error("expected non-nil manager")
	}
}

// TestManagerGet tests getting/creating circuit breakers.
func TestManagerGet(t *testing.T) {
	mgr := NewManager(Config{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		HalfOpenMax:      3,
		SuccessThreshold: 2,
	})

	cb1 := mgr.Get("service-a")
	cb2 := mgr.Get("service-b")

	if cb1 == nil || cb2 == nil {
		t.Error("expected non-nil circuit breakers")
	}

	// Same name should return same instance
	cb1Again := mgr.Get("service-a")
	if cb1 != cb1Again {
		t.Error("expected same circuit breaker for same name")
	}
}

// TestManagerRemove tests removing a circuit breaker.
func TestManagerRemove(t *testing.T) {
	mgr := NewManager(Config{})

	cb1 := mgr.Get("test")
	mgr.Remove("test")

	// Getting again should return a new instance
	cb2 := mgr.Get("test")
	if cb1 == cb2 {
		t.Error("expected different circuit breaker after remove")
	}
}

// TestManagerStats tests getting all stats.
func TestManagerStats(t *testing.T) {
	mgr := NewManager(Config{})

	mgr.Get("service-a").RecordSuccess()
	mgr.Get("service-b").RecordFailure()

	stats := mgr.Stats()

	if len(stats) != 2 {
		t.Errorf("expected 2 stats, got %d", len(stats))
	}
}

// BenchmarkCircuitBreakerAllow benchmarks the Allow method.
func BenchmarkCircuitBreakerAllow(b *testing.B) {
	cb := New(Config{Name: "bench"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.Allow()
	}
}

// BenchmarkCircuitBreakerRecordSuccess benchmarks recording success.
func BenchmarkCircuitBreakerRecordSuccess(b *testing.B) {
	cb := New(Config{Name: "bench"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.RecordSuccess()
	}
}

// BenchmarkManagerGet benchmarks getting circuit breakers.
func BenchmarkManagerGet(b *testing.B) {
	mgr := NewManager(Config{})
	// Pre-populate
	for i := 0; i < 100; i++ {
		mgr.Get("service-" + string(rune('a'+i%26)))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mgr.Get("service-" + string(rune('a'+i%26)))
	}
}
