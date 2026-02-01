package hotkey

import (
	"sync"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.NumShards != 64 {
		t.Errorf("expected NumShards=64, got %d", cfg.NumShards)
	}
	if cfg.HotKeyThreshold != 1000 {
		t.Errorf("expected HotKeyThreshold=1000, got %d", cfg.HotKeyThreshold)
	}
	if cfg.MaxHotKeys != 100 {
		t.Errorf("expected MaxHotKeys=100, got %d", cfg.MaxHotKeys)
	}
}

func TestNew(t *testing.T) {
	d := New(DefaultConfig())
	defer d.Close()

	if d == nil {
		t.Fatal("expected non-nil detector")
	}
	if len(d.counters) != 64 {
		t.Errorf("expected 64 shards, got %d", len(d.counters))
	}
}

func TestRecord(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 10
	cfg.RateLimit = 0 // Disable rate limiting for this test

	d := New(cfg)
	defer d.Close()

	// Record some accesses
	for i := 0; i < 5; i++ {
		allowed := d.Record("key1")
		if !allowed {
			t.Error("expected access to be allowed")
		}
	}

	count := d.GetCount("key1")
	if count != 5 {
		t.Errorf("expected count=5, got %d", count)
	}
}

func TestHotKeyDetection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 10
	cfg.RateLimit = 0 // Disable rate limiting

	d := New(cfg)
	defer d.Close()

	// Not hot yet
	if d.IsHot("key1") {
		t.Error("key should not be hot yet")
	}

	// Record enough to become hot
	for i := 0; i < 15; i++ {
		d.Record("key1")
	}

	if !d.IsHot("key1") {
		t.Error("key should be hot now")
	}

	// Check hot keys list
	hotKeys := d.HotKeys()
	found := false
	for _, hk := range hotKeys {
		if hk.Key == "key1" {
			found = true
			if hk.Count < 10 {
				t.Errorf("expected count >= 10, got %d", hk.Count)
			}
		}
	}
	if !found {
		t.Error("key1 not found in hot keys list")
	}
}

func TestRateLimiting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 5
	cfg.RateLimit = 10 // 10 ops/sec
	cfg.RateBurst = 5

	d := New(cfg)
	defer d.Close()

	// Make key hot
	for i := 0; i < 10; i++ {
		d.Record("key1")
	}

	if !d.IsHot("key1") {
		t.Fatal("key should be hot")
	}

	// Now access rapidly - some should be rate limited
	limited := 0
	for i := 0; i < 20; i++ {
		if !d.Record("key1") {
			limited++
		}
	}

	if limited == 0 {
		t.Error("expected some requests to be rate limited")
	}
	t.Logf("Rate limited %d out of 20 requests", limited)
}

func TestStats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 10

	d := New(cfg)
	defer d.Close()

	// Record some accesses
	for i := 0; i < 25; i++ {
		d.Record("key1")
	}
	for i := 0; i < 5; i++ {
		d.Record("key2")
	}

	stats := d.Stats()

	if stats.TotalAccess != 30 {
		t.Errorf("expected TotalAccess=30, got %d", stats.TotalAccess)
	}
	if stats.HotKeyCount != 1 {
		t.Errorf("expected HotKeyCount=1, got %d", stats.HotKeyCount)
	}
}

func TestReset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 5

	d := New(cfg)
	defer d.Close()

	// Make a key hot
	for i := 0; i < 10; i++ {
		d.Record("key1")
	}

	if !d.IsHot("key1") {
		t.Fatal("key should be hot")
	}

	// Reset
	d.Reset()

	// Should no longer be hot
	if d.IsHot("key1") {
		t.Error("key should not be hot after reset")
	}
	if d.GetCount("key1") != 0 {
		t.Error("count should be 0 after reset")
	}

	stats := d.Stats()
	if stats.TotalAccess != 0 {
		t.Errorf("expected TotalAccess=0 after reset, got %d", stats.TotalAccess)
	}
}

func TestTopK(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 5
	cfg.MaxHotKeys = 3
	cfg.RateLimit = 0

	d := New(cfg)
	defer d.Close()

	// Create multiple hot keys with different counts
	for i := 0; i < 100; i++ {
		d.Record("hot1")
	}
	for i := 0; i < 50; i++ {
		d.Record("hot2")
	}
	for i := 0; i < 25; i++ {
		d.Record("hot3")
	}
	for i := 0; i < 10; i++ {
		d.Record("hot4")
	}

	// Rebuild top-K
	d.rebuildTopK()

	topK := d.TopK()

	// Should have at most MaxHotKeys entries
	if len(topK) > cfg.MaxHotKeys {
		t.Errorf("expected at most %d top keys, got %d", cfg.MaxHotKeys, len(topK))
	}

	// Should be sorted by count descending
	for i := 1; i < len(topK); i++ {
		if topK[i].Count > topK[i-1].Count {
			t.Error("top-K not sorted correctly")
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 100
	cfg.RateLimit = 0

	d := New(cfg)
	defer d.Close()

	var wg sync.WaitGroup
	n := 100

	// Concurrent access to multiple keys
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			for j := 0; j < 50; j++ {
				d.Record(key)
			}
		}(i)
	}
	wg.Wait()

	stats := d.Stats()
	if stats.TotalAccess != uint64(n*50) {
		t.Errorf("expected TotalAccess=%d, got %d", n*50, stats.TotalAccess)
	}
}

func TestDecay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotKeyThreshold = 10
	cfg.DecayInterval = 50 * time.Millisecond
	cfg.DecayFactor = 0.5
	cfg.RateLimit = 0

	d := New(cfg)
	defer d.Close()

	// Make a key hot
	for i := 0; i < 20; i++ {
		d.Record("key1")
	}

	if !d.IsHot("key1") {
		t.Fatal("key should be hot")
	}

	initialCount := d.GetCount("key1")

	// Wait for decay
	time.Sleep(100 * time.Millisecond)

	// Count should have decayed
	newCount := d.GetCount("key1")
	if newCount >= initialCount {
		t.Errorf("count should have decayed: was %d, now %d", initialCount, newCount)
	}
}

func TestTopKHeap(t *testing.T) {
	h := newTopKHeap(3)

	h.Add(&hotKeyInfo{Key: "a", Count: 10})
	h.Add(&hotKeyInfo{Key: "b", Count: 30})
	h.Add(&hotKeyInfo{Key: "c", Count: 20})
	h.Add(&hotKeyInfo{Key: "d", Count: 5})

	items := h.Items()

	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d", len(items))
	}

	// Should be in descending order
	if items[0].Key != "b" || items[0].Count != 30 {
		t.Errorf("first item should be 'b' with count 30")
	}
	if items[1].Key != "c" || items[1].Count != 20 {
		t.Errorf("second item should be 'c' with count 20")
	}
	if items[2].Key != "a" || items[2].Count != 10 {
		t.Errorf("third item should be 'a' with count 10")
	}
}

func BenchmarkRecord(b *testing.B) {
	d := New(DefaultConfig())
	defer d.Close()

	key := "benchmark-key"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.Record(key)
	}
}

func BenchmarkRecordParallel(b *testing.B) {
	d := New(DefaultConfig())
	defer d.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := "benchmark-key"
		for pb.Next() {
			d.Record(key)
		}
	})
}

func BenchmarkRecordMultipleKeys(b *testing.B) {
	d := New(DefaultConfig())
	defer d.Close()

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Record(keys[i%len(keys)])
	}
}
