// Package circuitbreaker provides a circuit breaker pattern implementation.
package circuitbreaker

import (
	"sync"
	"time"
)

// State represents the circuit breaker state.
type State int

const (
	StateClosed   State = iota // Normal operation, requests pass through
	StateOpen                  // Circuit is open, requests fail fast
	StateHalfOpen              // Testing if service recovered
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	mu sync.Mutex

	name             string
	maxFailures      int           // failures before opening
	resetTimeout     time.Duration // time before trying half-open
	halfOpenMax      int           // max requests in half-open state
	successThreshold int           // successes needed to close from half-open

	state           State
	failures        int
	successes       int
	lastFailureTime time.Time
	halfOpenCount   int
}

// Config holds circuit breaker configuration.
type Config struct {
	Name             string
	MaxFailures      int           // Default: 5
	ResetTimeout     time.Duration // Default: 30s
	HalfOpenMax      int           // Default: 3
	SuccessThreshold int           // Default: 2
}

// New creates a new circuit breaker.
func New(cfg Config) *CircuitBreaker {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 5
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}

	return &CircuitBreaker{
		name:             cfg.Name,
		maxFailures:      cfg.MaxFailures,
		resetTimeout:     cfg.ResetTimeout,
		halfOpenMax:      cfg.HalfOpenMax,
		successThreshold: cfg.SuccessThreshold,
		state:            StateClosed,
	}
}

// Allow checks if a request should be allowed through.
// Returns true if allowed, false if circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if reset timeout has passed
		if time.Since(cb.lastFailureTime) >= cb.resetTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenCount = 0
			cb.successes = 0
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited requests in half-open state
		if cb.halfOpenCount < cb.halfOpenMax {
			cb.halfOpenCount++
			return true
		}
		return false

	default:
		return true
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failures = 0

	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.successThreshold {
			// Enough successes, close the circuit
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
		}
	}
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		cb.failures++
		if cb.failures >= cb.maxFailures {
			cb.state = StateOpen
		}

	case StateHalfOpen:
		// Any failure in half-open immediately opens circuit
		cb.state = StateOpen
		cb.failures = cb.maxFailures
	}
}

// State returns the current state.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Reset resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenCount = 0
}

// Stats returns circuit breaker statistics.
func (cb *CircuitBreaker) Stats() map[string]any {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return map[string]any{
		"name":     cb.name,
		"state":    cb.state.String(),
		"failures": cb.failures,
	}
}

// Manager manages multiple circuit breakers by name/address.
type Manager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	config   Config
}

// NewManager creates a circuit breaker manager.
func NewManager(cfg Config) *Manager {
	return &Manager{
		breakers: make(map[string]*CircuitBreaker),
		config:   cfg,
	}
}

// Get returns or creates a circuit breaker for the given key.
func (m *Manager) Get(key string) *CircuitBreaker {
	m.mu.RLock()
	cb, ok := m.breakers[key]
	m.mu.RUnlock()

	if ok {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if cb, ok = m.breakers[key]; ok {
		return cb
	}

	cfg := m.config
	cfg.Name = key
	cb = New(cfg)
	m.breakers[key] = cb
	return cb
}

// Remove removes a circuit breaker.
func (m *Manager) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.breakers, key)
}

// Stats returns stats for all circuit breakers.
func (m *Manager) Stats() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]any)
	for k, cb := range m.breakers {
		stats[k] = cb.Stats()
	}
	return stats
}
