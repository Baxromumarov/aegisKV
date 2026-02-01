// Package hotkey provides hot key detection and per-key rate limiting.
// It tracks access patterns to identify frequently accessed keys and applies
// rate limits to prevent hot keys from overwhelming the system.
package hotkey

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// Detector tracks key access patterns and detects hot keys.
type Detector struct {
	mu          sync.RWMutex
	config      Config
	counters    []keyCounter // Sharded counters for lock distribution
	hotKeys     map[string]*hotKeyInfo
	topK        *topKHeap
	lastDecay   time.Time
	totalAccess uint64
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// Config configures the hot key detector.
type Config struct {
	// NumShards is the number of counter shards for reducing lock contention.
	NumShards int

	// WindowSize is the time window for counting accesses.
	WindowSize time.Duration

	// DecayInterval is how often counts are decayed.
	DecayInterval time.Duration

	// DecayFactor is the multiplier applied during decay (0.5 = halve counts).
	DecayFactor float64

	// HotKeyThreshold is the minimum access count to be considered hot.
	HotKeyThreshold int64

	// MaxHotKeys is the maximum number of hot keys to track.
	MaxHotKeys int

	// RateLimit is the max ops/sec per hot key (0 = no limit).
	RateLimit float64

	// RateBurst is the burst size for hot key rate limiting.
	RateBurst int
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		NumShards:       64,
		WindowSize:      time.Minute,
		DecayInterval:   10 * time.Second,
		DecayFactor:     0.5,
		HotKeyThreshold: 1000,
		MaxHotKeys:      100,
		RateLimit:       10000,
		RateBurst:       100,
	}
}

// keyCounter is a sharded counter for a set of keys.
type keyCounter struct {
	mu     sync.Mutex
	counts map[string]*keyCount
}

type keyCount struct {
	count    int64
	lastSeen time.Time
	isHot    bool
	limiter  *rateLimiter
}

type hotKeyInfo struct {
	Key       string
	Count     int64
	Rate      float64 // accesses per second
	FirstSeen time.Time
	LastSeen  time.Time
	Limited   uint64 // number of times rate limited
}

type rateLimiter struct {
	rate       float64
	burst      int
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

func newRateLimiter(rate float64, burst int) *rateLimiter {
	return &rateLimiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

func (l *rateLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

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

// topKHeap maintains the top K hot keys.
type topKHeap struct {
	items []*hotKeyInfo
	maxK  int
}

func newTopKHeap(k int) *topKHeap {
	return &topKHeap{
		items: make([]*hotKeyInfo, 0, k),
		maxK:  k,
	}
}

func (h *topKHeap) Add(info *hotKeyInfo) {
	// Simple insertion sort for small K
	inserted := false
	for i, item := range h.items {
		if info.Count > item.Count {
			// Insert here
			h.items = append(h.items[:i], append([]*hotKeyInfo{info}, h.items[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted && len(h.items) < h.maxK {
		h.items = append(h.items, info)
	}
	if len(h.items) > h.maxK {
		h.items = h.items[:h.maxK]
	}
}

func (h *topKHeap) Items() []*hotKeyInfo {
	return h.items
}

func (h *topKHeap) Clear() {
	h.items = h.items[:0]
}

// New creates a new hot key detector.
func New(cfg Config) *Detector {
	if cfg.NumShards <= 0 {
		cfg.NumShards = 64
	}
	if cfg.MaxHotKeys <= 0 {
		cfg.MaxHotKeys = 100
	}

	d := &Detector{
		config:    cfg,
		counters:  make([]keyCounter, cfg.NumShards),
		hotKeys:   make(map[string]*hotKeyInfo),
		topK:      newTopKHeap(cfg.MaxHotKeys),
		lastDecay: time.Now(),
		stopCh:    make(chan struct{}),
	}

	for i := range d.counters {
		d.counters[i].counts = make(map[string]*keyCount)
	}

	// Start decay goroutine
	d.wg.Add(1)
	go d.decayLoop()

	return d
}

// Close stops the detector.
func (d *Detector) Close() {
	close(d.stopCh)
	d.wg.Wait()
}

// Record records an access to a key.
// Returns true if the access is allowed, false if rate limited.
func (d *Detector) Record(key string) bool {
	atomic.AddUint64(&d.totalAccess, 1)

	shard := d.shardFor(key)
	counter := &d.counters[shard]

	counter.mu.Lock()
	defer counter.mu.Unlock()

	kc, exists := counter.counts[key]
	if !exists {
		kc = &keyCount{
			lastSeen: time.Now(),
		}
		counter.counts[key] = kc
	}

	kc.count++
	kc.lastSeen = time.Now()

	// Check if this is now a hot key
	if !kc.isHot && kc.count >= d.config.HotKeyThreshold {
		kc.isHot = true
		if d.config.RateLimit > 0 {
			kc.limiter = newRateLimiter(d.config.RateLimit, d.config.RateBurst)
		}

		// Add to hot keys
		d.mu.Lock()
		d.hotKeys[key] = &hotKeyInfo{
			Key:       key,
			Count:     kc.count,
			FirstSeen: kc.lastSeen,
			LastSeen:  kc.lastSeen,
		}
		d.mu.Unlock()
	}

	// Apply rate limiting if hot
	if kc.isHot && kc.limiter != nil {
		if !kc.limiter.Allow() {
			d.mu.Lock()
			if info, ok := d.hotKeys[key]; ok {
				info.Limited++
			}
			d.mu.Unlock()
			return false
		}
	}

	return true
}

// IsHot returns true if the key is currently considered hot.
func (d *Detector) IsHot(key string) bool {
	shard := d.shardFor(key)
	counter := &d.counters[shard]

	counter.mu.Lock()
	defer counter.mu.Unlock()

	if kc, exists := counter.counts[key]; exists {
		return kc.isHot
	}
	return false
}

// GetCount returns the current count for a key.
func (d *Detector) GetCount(key string) int64 {
	shard := d.shardFor(key)
	counter := &d.counters[shard]

	counter.mu.Lock()
	defer counter.mu.Unlock()

	if kc, exists := counter.counts[key]; exists {
		return kc.count
	}
	return 0
}

// HotKeys returns information about current hot keys.
func (d *Detector) HotKeys() []HotKeyInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]HotKeyInfo, 0, len(d.hotKeys))
	for _, info := range d.hotKeys {
		result = append(result, HotKeyInfo{
			Key:       info.Key,
			Count:     info.Count,
			Rate:      info.Rate,
			FirstSeen: info.FirstSeen,
			LastSeen:  info.LastSeen,
			Limited:   info.Limited,
		})
	}
	return result
}

// HotKeyInfo is public info about a hot key.
type HotKeyInfo struct {
	Key       string
	Count     int64
	Rate      float64
	FirstSeen time.Time
	LastSeen  time.Time
	Limited   uint64
}

// TopK returns the top K hottest keys.
func (d *Detector) TopK() []HotKeyInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	items := d.topK.Items()
	result := make([]HotKeyInfo, len(items))
	for i, item := range items {
		result[i] = HotKeyInfo{
			Key:       item.Key,
			Count:     item.Count,
			Rate:      item.Rate,
			FirstSeen: item.FirstSeen,
			LastSeen:  item.LastSeen,
			Limited:   item.Limited,
		}
	}
	return result
}

// Stats returns detector statistics.
func (d *Detector) Stats() Stats {
	d.mu.RLock()
	hotCount := len(d.hotKeys)
	d.mu.RUnlock()

	return Stats{
		TotalAccess: atomic.LoadUint64(&d.totalAccess),
		HotKeyCount: hotCount,
	}
}

// Stats contains detector statistics.
type Stats struct {
	TotalAccess uint64
	HotKeyCount int
}

// Reset clears all counters and hot key state.
func (d *Detector) Reset() {
	for i := range d.counters {
		d.counters[i].mu.Lock()
		d.counters[i].counts = make(map[string]*keyCount)
		d.counters[i].mu.Unlock()
	}

	d.mu.Lock()
	d.hotKeys = make(map[string]*hotKeyInfo)
	d.topK.Clear()
	atomic.StoreUint64(&d.totalAccess, 0)
	d.mu.Unlock()
}

func (d *Detector) shardFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % len(d.counters)
}

func (d *Detector) decayLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.config.DecayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.decay()
		}
	}
}

func (d *Detector) decay() {
	// Decay all counters
	for i := range d.counters {
		d.counters[i].mu.Lock()

		for key, kc := range d.counters[i].counts {
			kc.count = int64(float64(kc.count) * d.config.DecayFactor)

			// Remove if count is too low and not seen recently
			if kc.count < d.config.HotKeyThreshold/10 &&
				time.Since(kc.lastSeen) > d.config.WindowSize {
				delete(d.counters[i].counts, key)

				// Remove from hot keys if present
				if kc.isHot {
					d.mu.Lock()
					delete(d.hotKeys, key)
					d.mu.Unlock()
				}
			} else if kc.isHot && kc.count < d.config.HotKeyThreshold/2 {
				// No longer hot
				kc.isHot = false
				kc.limiter = nil

				d.mu.Lock()
				delete(d.hotKeys, key)
				d.mu.Unlock()
			}
		}

		d.counters[i].mu.Unlock()
	}

	// Rebuild top-K
	d.rebuildTopK()
}

func (d *Detector) rebuildTopK() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.topK.Clear()

	for _, info := range d.hotKeys {
		// Update rate based on window
		elapsed := time.Since(info.FirstSeen).Seconds()
		if elapsed > 0 {
			info.Rate = float64(info.Count) / elapsed
		}
		d.topK.Add(info)
	}
}
