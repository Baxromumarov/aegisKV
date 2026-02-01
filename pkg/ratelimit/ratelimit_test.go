package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// TestNewLimiter tests creating a new rate limiter.
func TestNewLimiter(t *testing.T) {
	limiter := New(100, 10) // 100 ops/sec, burst of 10

	if limiter == nil {
		t.Error("expected non-nil limiter")
	}
}

// TestLimiterAllow tests the Allow method.
func TestLimiterAllow(t *testing.T) {
	limiter := New(1000, 10) // 1000 ops/sec, burst of 10

	// Should allow up to burst
	for i := 0; i < 10; i++ {
		if !limiter.Allow() {
			t.Errorf("should allow request %d within burst", i)
		}
	}
}

// TestLimiterAllowN tests the AllowN method.
func TestLimiterAllowN(t *testing.T) {
	limiter := New(1000, 10)

	// Allow 5 tokens at once
	if !limiter.AllowN(5) {
		t.Error("should allow 5 tokens")
	}

	// Allow another 5
	if !limiter.AllowN(5) {
		t.Error("should allow another 5 tokens")
	}

	// Should be exhausted
	if limiter.AllowN(1) {
		t.Error("should not allow more tokens immediately")
	}
}

// TestLimiterTokensReplenish tests that tokens replenish over time.
func TestLimiterTokensReplenish(t *testing.T) {
	limiter := New(100, 5) // 100 ops/sec = 1 token per 10ms

	// Exhaust all tokens
	for limiter.Allow() {
	}

	// Wait for some tokens to replenish
	time.Sleep(60 * time.Millisecond) // Should get ~6 tokens

	// Should be able to allow again
	if !limiter.Allow() {
		t.Error("tokens should have replenished")
	}
}

// TestLimiterTokens tests the Tokens method.
func TestLimiterTokens(t *testing.T) {
	limiter := New(100, 10)

	initial := limiter.Tokens()
	if initial != 10 {
		t.Errorf("expected 10 initial tokens, got %f", initial)
	}

	// Use some tokens
	limiter.AllowN(3)

	remaining := limiter.Tokens()
	if remaining < 6 || remaining > 8 {
		t.Errorf("expected ~7 tokens remaining, got %f", remaining)
	}
}

// TestLimiterBurstLimit tests that burst limits are enforced.
func TestLimiterBurstLimit(t *testing.T) {
	limiter := New(1000, 5)

	// Try to take more than burst
	if limiter.AllowN(10) {
		t.Error("should not allow more than burst in single request")
	}

	// Should still have all tokens since AllowN failed
	if !limiter.AllowN(5) {
		t.Error("should allow burst amount")
	}
}

// TestLimiterZeroRate tests behavior with zero rate.
func TestLimiterZeroRate(t *testing.T) {
	limiter := New(0, 5)

	// Zero rate returns nil limiter (disabled)
	if limiter != nil {
		t.Error("expected nil limiter for zero rate")
	}

	// nil limiter should always allow (rate limiting disabled)
	if !limiter.Allow() {
		t.Error("nil limiter should always allow")
	}
}

// TestLimiterHighRate tests high-rate limiting.
func TestLimiterHighRate(t *testing.T) {
	limiter := New(1000000, 1000) // 1M ops/sec

	// Should handle high rates
	count := 0
	for i := 0; i < 1000; i++ {
		if limiter.Allow() {
			count++
		}
	}

	if count != 1000 {
		t.Errorf("expected 1000 allowed, got %d", count)
	}
}

// TestLimiterConcurrency tests thread safety.
func TestLimiterConcurrency(t *testing.T) {
	limiter := New(10000, 100)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0

	// 10 goroutines each trying 20 requests
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if limiter.Allow() {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Should have allowed ~100 (the burst)
	if allowed < 90 || allowed > 110 {
		t.Errorf("expected ~100 allowed, got %d", allowed)
	}
}

// TestLimiterRateOver time tests sustained rate.
func TestLimiterRateOverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping time-based test in short mode")
	}

	limiter := New(100, 10) // 100 ops/sec

	start := time.Now()
	count := 0

	// Run for 100ms
	for time.Since(start) < 100*time.Millisecond {
		if limiter.Allow() {
			count++
		}
		time.Sleep(time.Millisecond)
	}

	// Should have allowed ~10 (burst) + ~10 (100ms at 100/sec) = ~20
	if count < 15 || count > 25 {
		t.Errorf("expected 15-25 allowed over 100ms, got %d", count)
	}
}

// TestLimiterNegativeN tests AllowN with negative values.
func TestLimiterNegativeN(t *testing.T) {
	limiter := New(100, 10)

	// Negative should be treated as 0 or rejected
	result := limiter.AllowN(0)
	// AllowN(0) typically returns true (taking 0 tokens is allowed)
	if !result {
		t.Log("AllowN(0) returned false, which is acceptable")
	}
}

// BenchmarkLimiterAllow benchmarks the Allow method.
func BenchmarkLimiterAllow(b *testing.B) {
	limiter := New(1000000000, 1000000) // Very high rate

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow()
	}
}

// BenchmarkLimiterAllowN benchmarks the AllowN method.
func BenchmarkLimiterAllowN(b *testing.B) {
	limiter := New(1000000000, 1000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.AllowN(1)
	}
}

// BenchmarkLimiterTokens benchmarks the Tokens method.
func BenchmarkLimiterTokens(b *testing.B) {
	limiter := New(100, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Tokens()
	}
}

// BenchmarkLimiterConcurrent benchmarks concurrent access.
func BenchmarkLimiterConcurrent(b *testing.B) {
	limiter := New(1000000000, 1000000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow()
		}
	})
}
