// Package ratelimit provides a token-bucket rate limiter.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter implements a token-bucket rate limiter.
type Limiter struct {
	mu         sync.Mutex
	rate       float64   // tokens per second
	burst      int       // maximum tokens
	tokens     float64   // current tokens
	lastUpdate time.Time // last update time
}

// New creates a new rate limiter.
// rate is the number of operations per second.
// burst is the maximum burst size.
func New(rate float64, burst int) *Limiter {
	if rate <= 0 {
		return nil // Disabled
	}
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

// Allow checks if an operation is allowed.
// Returns true if allowed, false if rate limited.
func (l *Limiter) Allow() bool {
	if l == nil {
		return true // Limiter disabled
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

	// Add tokens based on elapsed time
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}

	if l.tokens >= 1 {
		l.tokens--
		return true
	}

	return false
}

// AllowN checks if n operations are allowed.
func (l *Limiter) AllowN(n int) bool {
	if l == nil {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}

	if l.tokens >= float64(n) {
		l.tokens -= float64(n)
		return true
	}

	return false
}

// Tokens returns the current number of available tokens.
func (l *Limiter) Tokens() float64 {
	if l == nil {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()

	tokens := l.tokens + elapsed*l.rate
	if tokens > float64(l.burst) {
		tokens = float64(l.burst)
	}

	return tokens
}
