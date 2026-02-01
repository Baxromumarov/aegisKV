package pressure

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelNone, "none"},
		{LevelLow, "low"},
		{LevelMedium, "medium"},
		{LevelHigh, "high"},
		{LevelCritical, "critical"},
		{Level(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.level.String() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.level.String())
			}
		})
	}
}

func TestNewMonitor(t *testing.T) {
	m := NewMonitor(Config{
		LimitBytes:    1024 * 1024 * 100, // 100MB
		CheckInterval: 100 * time.Millisecond,
	})

	if m == nil {
		t.Fatal("NewMonitor returned nil")
	}
	if m.limitBytes != 1024*1024*100 {
		t.Errorf("expected limit 100MB, got %d", m.limitBytes)
	}
	if m.checkInterval != 100*time.Millisecond {
		t.Errorf("expected interval 100ms, got %v", m.checkInterval)
	}
}

func TestNewMonitorDefaults(t *testing.T) {
	m := NewMonitor(Config{
		LimitBytes: 1024 * 1024,
	})

	if m.checkInterval != time.Second {
		t.Errorf("expected default interval 1s, got %v", m.checkInterval)
	}
	if m.getUsedBytes == nil {
		t.Error("getUsedBytes should default to getHeapBytes")
	}
}

func TestCalculateLevel(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	tests := []struct {
		used     int64
		expected Level
	}{
		{0, LevelNone},
		{500, LevelNone},     // 50%
		{690, LevelNone},     // 69%
		{700, LevelLow},      // 70%
		{750, LevelLow},      // 75%
		{800, LevelMedium},   // 80%
		{850, LevelMedium},   // 85%
		{900, LevelHigh},     // 90%
		{940, LevelHigh},     // 94%
		{950, LevelCritical}, // 95%
		{1000, LevelCritical},
	}

	for _, tt := range tests {
		level := m.calculateLevel(tt.used)
		if level != tt.expected {
			t.Errorf("used=%d: expected %s, got %s", tt.used, tt.expected, level)
		}
	}
}

func TestCalculateLevelZeroLimit(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 0})

	level := m.calculateLevel(1000)
	if level != LevelNone {
		t.Errorf("expected LevelNone for zero limit, got %s", level)
	}
}

func TestOnCallbacks(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	lowCalled := false
	mediumCalled := false
	highCalled := false
	criticalCalled := false

	m.OnLow(func(level Level, used, limit int64) {
		lowCalled = true
	})
	m.OnMedium(func(level Level, used, limit int64) {
		mediumCalled = true
	})
	m.OnHigh(func(level Level, used, limit int64) {
		highCalled = true
	})
	m.OnCritical(func(level Level, used, limit int64) {
		criticalCalled = true
	})

	// Trigger critical level - should call all callbacks
	m.triggerCallbacks(LevelCritical, 950)

	// Wait for goroutines
	time.Sleep(50 * time.Millisecond)

	if !lowCalled {
		t.Error("low callback should be called")
	}
	if !mediumCalled {
		t.Error("medium callback should be called")
	}
	if !highCalled {
		t.Error("high callback should be called")
	}
	if !criticalCalled {
		t.Error("critical callback should be called")
	}
}

func TestOnCallbacksLevelFiltering(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	highCalled := false
	criticalCalled := false

	m.OnHigh(func(level Level, used, limit int64) {
		highCalled = true
	})
	m.OnCritical(func(level Level, used, limit int64) {
		criticalCalled = true
	})

	// Trigger low level - should not call high or critical
	m.triggerCallbacks(LevelLow, 700)

	time.Sleep(50 * time.Millisecond)

	if highCalled {
		t.Error("high callback should NOT be called for low level")
	}
	if criticalCalled {
		t.Error("critical callback should NOT be called for low level")
	}
}

func TestCurrentLevel(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	if m.CurrentLevel() != LevelNone {
		t.Error("initial level should be None")
	}

	m.mu.Lock()
	m.currentLevel = LevelHigh
	m.mu.Unlock()

	if m.CurrentLevel() != LevelHigh {
		t.Error("level should be High")
	}
}

func TestUsedBytes(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	if m.UsedBytes() != 0 {
		t.Error("initial used bytes should be 0")
	}

	atomic.StoreInt64(&m.lastUsed, 500)

	if m.UsedBytes() != 500 {
		t.Errorf("expected 500, got %d", m.UsedBytes())
	}
}

func TestStats(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 1000})

	atomic.StoreInt64(&m.lastUsed, 800)
	m.mu.Lock()
	m.currentLevel = LevelMedium
	m.mu.Unlock()

	stats := m.Stats()

	if stats["level"] != "medium" {
		t.Errorf("expected level 'medium', got %v", stats["level"])
	}
	if stats["used_bytes"].(int64) != 800 {
		t.Errorf("expected used_bytes 800, got %v", stats["used_bytes"])
	}
	if stats["limit_bytes"].(int64) != 1000 {
		t.Errorf("expected limit_bytes 1000, got %v", stats["limit_bytes"])
	}
	if stats["usage_ratio"].(float64) != 0.8 {
		t.Errorf("expected usage_ratio 0.8, got %v", stats["usage_ratio"])
	}
}

func TestStatsZeroLimit(t *testing.T) {
	m := NewMonitor(Config{LimitBytes: 0})

	stats := m.Stats()

	if stats["usage_ratio"].(float64) != 0 {
		t.Errorf("expected usage_ratio 0 for zero limit, got %v", stats["usage_ratio"])
	}
}

func TestMonitorStartStop(t *testing.T) {
	var checkCount int32
	usedBytes := int64(500)

	m := NewMonitor(Config{
		LimitBytes:    1000,
		CheckInterval: 10 * time.Millisecond,
		GetUsedBytes: func() int64 {
			atomic.AddInt32(&checkCount, 1)
			return atomic.LoadInt64(&usedBytes)
		},
	})

	m.Start()
	time.Sleep(50 * time.Millisecond)
	m.Stop()

	if atomic.LoadInt32(&checkCount) == 0 {
		t.Error("check should have been called")
	}
}

func TestMonitorLevelChange(t *testing.T) {
	var usedBytes int64 = 500
	levelChanged := make(chan Level, 10)

	m := NewMonitor(Config{
		LimitBytes:    1000,
		CheckInterval: 10 * time.Millisecond,
		GetUsedBytes: func() int64 {
			return atomic.LoadInt64(&usedBytes)
		},
	})

	m.OnHigh(func(level Level, used, limit int64) {
		levelChanged <- level
	})

	m.Start()

	// Trigger high level
	atomic.StoreInt64(&usedBytes, 950)

	select {
	case level := <-levelChanged:
		if level < LevelHigh {
			t.Errorf("expected at least High level, got %s", level)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("callback should have been triggered")
	}

	m.Stop()
}

func TestCustomGetUsedBytes(t *testing.T) {
	customValue := int64(12345)

	m := NewMonitor(Config{
		LimitBytes: 100000,
		GetUsedBytes: func() int64 {
			return customValue
		},
	})

	// Trigger a check
	m.check()

	if m.UsedBytes() != customValue {
		t.Errorf("expected %d, got %d", customValue, m.UsedBytes())
	}
}

func TestForceGC(t *testing.T) {
	// Just verify it doesn't panic
	ForceGC()
}

func BenchmarkCalculateLevel(b *testing.B) {
	m := NewMonitor(Config{LimitBytes: 1000})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.calculateLevel(850)
	}
}

func BenchmarkCheck(b *testing.B) {
	m := NewMonitor(Config{
		LimitBytes: 1000,
		GetUsedBytes: func() int64 {
			return 500
		},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.check()
	}
}
