// Package pressure provides memory pressure monitoring and callbacks.
package pressure

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/logger"
)

// Level represents memory pressure level.
type Level int

const (
	LevelNone     Level = iota // < 70% used
	LevelLow                   // 70-80% used
	LevelMedium                // 80-90% used
	LevelHigh                  // 90-95% used
	LevelCritical              // > 95% used
)

func (l Level) String() string {
	switch l {
	case LevelNone:
		return "none"
	case LevelLow:
		return "low"
	case LevelMedium:
		return "medium"
	case LevelHigh:
		return "high"
	case LevelCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// Callback is called when pressure level changes.
type Callback func(level Level, usedBytes, limitBytes int64)

// Monitor watches memory usage and triggers callbacks.
type Monitor struct {
	mu            sync.RWMutex
	limitBytes    int64
	checkInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// Callbacks for different pressure levels
	onLow      []Callback
	onMedium   []Callback
	onHigh     []Callback
	onCritical []Callback

	// Current state
	currentLevel Level
	lastUsed     int64
	log          *logger.Logger

	// Custom memory provider (for cache size)
	getUsedBytes func() int64
}

// Config holds monitor configuration.
type Config struct {
	LimitBytes    int64
	CheckInterval time.Duration
	GetUsedBytes  func() int64 // Custom function to get used bytes (e.g., cache size)
}

// NewMonitor creates a new memory pressure monitor.
func NewMonitor(cfg Config) *Monitor {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = time.Second
	}
	if cfg.GetUsedBytes == nil {
		cfg.GetUsedBytes = getHeapBytes
	}

	return &Monitor{
		limitBytes:    cfg.LimitBytes,
		checkInterval: cfg.CheckInterval,
		stopCh:        make(chan struct{}),
		log:           logger.New("pressure", logger.LevelInfo, nil),
		getUsedBytes:  cfg.GetUsedBytes,
	}
}

// getHeapBytes returns current heap allocation.
func getHeapBytes() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// OnLow registers a callback for low pressure.
func (m *Monitor) OnLow(cb Callback) {
	m.mu.Lock()
	m.onLow = append(m.onLow, cb)
	m.mu.Unlock()
}

// OnMedium registers a callback for medium pressure.
func (m *Monitor) OnMedium(cb Callback) {
	m.mu.Lock()
	m.onMedium = append(m.onMedium, cb)
	m.mu.Unlock()
}

// OnHigh registers a callback for high pressure.
func (m *Monitor) OnHigh(cb Callback) {
	m.mu.Lock()
	m.onHigh = append(m.onHigh, cb)
	m.mu.Unlock()
}

// OnCritical registers a callback for critical pressure.
func (m *Monitor) OnCritical(cb Callback) {
	m.mu.Lock()
	m.onCritical = append(m.onCritical, cb)
	m.mu.Unlock()
}

// Start starts the pressure monitor.
func (m *Monitor) Start() {
	m.wg.Add(1)
	go m.loop()
}

// Stop stops the monitor.
func (m *Monitor) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// loop is the main monitoring loop.
func (m *Monitor) loop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.check()
		}
	}
}

// check checks current memory usage and triggers callbacks if level changed.
func (m *Monitor) check() {
	used := m.getUsedBytes()
	atomic.StoreInt64(&m.lastUsed, used)

	level := m.calculateLevel(used)

	m.mu.RLock()
	prevLevel := m.currentLevel
	m.mu.RUnlock()

	if level != prevLevel {
		m.mu.Lock()
		m.currentLevel = level
		m.mu.Unlock()

		m.log.Info("memory pressure changed: %s -> %s (used=%d, limit=%d, %.1f%%)",
			prevLevel, level, used, m.limitBytes, float64(used)/float64(m.limitBytes)*100)

		m.triggerCallbacks(level, used)
	}
}

// calculateLevel determines pressure level from usage ratio.
func (m *Monitor) calculateLevel(used int64) Level {
	if m.limitBytes <= 0 {
		return LevelNone
	}

	ratio := float64(used) / float64(m.limitBytes)

	switch {
	case ratio >= 0.95:
		return LevelCritical
	case ratio >= 0.90:
		return LevelHigh
	case ratio >= 0.80:
		return LevelMedium
	case ratio >= 0.70:
		return LevelLow
	default:
		return LevelNone
	}
}

// triggerCallbacks triggers callbacks for the given level (and all levels above).
func (m *Monitor) triggerCallbacks(level Level, used int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Trigger callbacks for current level and all lower levels
	if level >= LevelLow {
		for _, cb := range m.onLow {
			go cb(level, used, m.limitBytes)
		}
	}
	if level >= LevelMedium {
		for _, cb := range m.onMedium {
			go cb(level, used, m.limitBytes)
		}
	}
	if level >= LevelHigh {
		for _, cb := range m.onHigh {
			go cb(level, used, m.limitBytes)
		}
	}
	if level >= LevelCritical {
		for _, cb := range m.onCritical {
			go cb(level, used, m.limitBytes)
		}
	}
}

// CurrentLevel returns the current pressure level.
func (m *Monitor) CurrentLevel() Level {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentLevel
}

// UsedBytes returns the last measured used bytes.
func (m *Monitor) UsedBytes() int64 {
	return atomic.LoadInt64(&m.lastUsed)
}

// Stats returns monitor statistics.
func (m *Monitor) Stats() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	used := atomic.LoadInt64(&m.lastUsed)
	ratio := 0.0
	if m.limitBytes > 0 {
		ratio = float64(used) / float64(m.limitBytes)
	}

	return map[string]any{
		"level":       m.currentLevel.String(),
		"used_bytes":  used,
		"limit_bytes": m.limitBytes,
		"usage_ratio": ratio,
	}
}

// ForceGC triggers garbage collection.
func ForceGC() {
	runtime.GC()
}
