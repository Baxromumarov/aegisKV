// Package cache implements an in-memory cache with LRU eviction and TTL support.
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// Cache is an in-memory key-value store with LRU eviction and TTL support.
// Optimized for read-heavy workloads using RLock for reads with buffered LRU promotions.
type Cache struct {
	mu        sync.RWMutex
	items     map[string]*cacheItem
	lruList   *list.List
	maxBytes  int64
	curBytes  int64
	hits      uint64 // atomic
	misses    uint64 // atomic
	onEvict   func(key string, entry *types.Entry)
	promoteCh chan *list.Element
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type cacheItem struct {
	entry   *types.Entry
	element *list.Element
}

type lruEntry struct {
	key string
}

// NewCache creates a new cache with the specified maximum size in bytes.
func NewCache(maxBytes int64) *Cache {
	c := &Cache{
		items:     make(map[string]*cacheItem),
		lruList:   list.New(),
		maxBytes:  maxBytes,
		curBytes:  0,
		promoteCh: make(chan *list.Element, 256),
		stopCh:    make(chan struct{}),
	}

	// Start the promotion drainer goroutine
	c.wg.Add(1)
	go c.promotionDrainer()

	return c
}

// Close stops the cache's background goroutines.
func (c *Cache) Close() {
	close(c.stopCh)
	c.wg.Wait()
}

// promotionDrainer drains the promotion channel in batches and applies MoveToFront.
func (c *Cache) promotionDrainer() {
	defer c.wg.Done()

	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()

	batch := make([]*list.Element, 0, 64)

	for {
		select {
		case <-c.stopCh:
			return
		case elem := <-c.promoteCh:
			batch = append(batch, elem)
			// Drain any additional pending promotions
		drain:
			for {
				select {
				case e := <-c.promoteCh:
					batch = append(batch, e)
					if len(batch) >= 64 {
						break drain
					}
				default:
					break drain
				}
			}

			// Apply promotions under write lock
			if len(batch) > 0 {
				c.mu.Lock()
				for _, e := range batch {
					// Check element is still in the list (not evicted)
					if e.Value != nil {
						c.lruList.MoveToFront(e)
					}
				}
				c.mu.Unlock()
				batch = batch[:0]
			}

		case <-ticker.C:
			// Periodic flush of any pending promotions
			if len(batch) > 0 {
				c.mu.Lock()
				for _, e := range batch {
					if e.Value != nil {
						c.lruList.MoveToFront(e)
					}
				}
				c.mu.Unlock()
				batch = batch[:0]
			}
		}
	}
}

// SetEvictCallback sets a callback function to be called when an entry is evicted.
func (c *Cache) SetEvictCallback(fn func(key string, entry *types.Entry)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvict = fn
}

// Get retrieves an entry from the cache using RLock for better concurrency.
func (c *Cache) Get(key string) *types.Entry {
	c.mu.RLock()
	item, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		atomic.AddUint64(&c.misses, 1)
		return nil
	}

	// Fast path: check expiry inline before upgrading lock
	entry := item.entry
	if entry.Expiry > 0 && entry.IsExpired() {
		c.mu.RUnlock()
		// Upgrade to write lock to delete expired entry
		c.mu.Lock()
		// Double-check after acquiring write lock
		item, ok = c.items[key]
		if ok && item.entry.IsExpired() {
			c.deleteItem(key, item)
		}
		c.mu.Unlock()
		atomic.AddUint64(&c.misses, 1)
		return nil
	}

	// Queue LRU promotion (non-blocking)
	select {
	case c.promoteCh <- item.element:
	default:
		// Channel full, skip promotion this time
	}

	c.mu.RUnlock()
	atomic.AddUint64(&c.hits, 1)
	return entry
}

// GetNoExpiry retrieves an entry without checking expiry (caller handles).
// Use for hot paths where expiry is checked separately.
func (c *Cache) GetNoExpiry(key string) *types.Entry {
	c.mu.RLock()
	item, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		atomic.AddUint64(&c.misses, 1)
		return nil
	}

	entry := item.entry

	// Queue LRU promotion (non-blocking)
	select {
	case c.promoteCh <- item.element:
	default:
	}

	c.mu.RUnlock()
	atomic.AddUint64(&c.hits, 1)
	return entry
}

// Set stores an entry in the cache.
func (c *Cache) Set(key string, entry *types.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entrySize := c.entrySize(key, entry)

	if item, ok := c.items[key]; ok {
		oldSize := c.entrySize(key, item.entry)
		c.curBytes += entrySize - oldSize
		item.entry = entry
		c.lruList.MoveToFront(item.element)
	} else {
		element := c.lruList.PushFront(&lruEntry{key: key})
		c.items[key] = &cacheItem{
			entry:   entry,
			element: element,
		}
		c.curBytes += entrySize
	}

	c.evictIfNeeded()
}

// SetWithVersion stores an entry only if the version is newer.
func (c *Cache) SetWithVersion(key string, entry *types.Entry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if item, ok := c.items[key]; ok {
		if !entry.Version.IsNewerThan(item.entry.Version) {
			return false
		}
		oldSize := c.entrySize(key, item.entry)
		entrySize := c.entrySize(key, entry)
		c.curBytes += entrySize - oldSize
		item.entry = entry
		c.lruList.MoveToFront(item.element)
	} else {
		entrySize := c.entrySize(key, entry)
		element := c.lruList.PushFront(&lruEntry{key: key})
		c.items[key] = &cacheItem{
			entry:   entry,
			element: element,
		}
		c.curBytes += entrySize
	}

	c.evictIfNeeded()
	return true
}

// Delete removes an entry from the cache.
func (c *Cache) Delete(key string) *types.Entry {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		return nil
	}

	entry := item.entry
	c.deleteItem(key, item)
	return entry
}

func (c *Cache) deleteItem(key string, item *cacheItem) {
	c.lruList.Remove(item.element)
	item.element.Value = nil // Mark as removed for promotion drainer
	c.curBytes -= c.entrySize(key, item.entry)
	delete(c.items, key)
}

func (c *Cache) entrySize(key string, entry *types.Entry) int64 {
	return int64(len(key) + len(entry.Key) + len(entry.Value) + 64)
}

func (c *Cache) evictIfNeeded() {
	threshold := int64(float64(c.maxBytes) * 0.9)

	for c.curBytes > threshold && c.lruList.Len() > 0 {
		element := c.lruList.Back()
		if element == nil {
			break
		}

		lruE := element.Value.(*lruEntry)
		item := c.items[lruE.key]

		if c.onEvict != nil {
			c.onEvict(lruE.key, item.entry)
		}

		c.deleteItem(lruE.key, item)
	}
}

// CleanExpired removes all expired entries from the cache.
func (c *Cache) CleanExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for key, item := range c.items {
		if item.entry.IsExpired() {
			c.deleteItem(key, item)
			removed++
		}
	}
	return removed
}

// StartExpiryWorker starts a background goroutine that periodically cleans expired entries.
func (c *Cache) StartExpiryWorker(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.CleanExpired()
			case <-stop:
				return
			}
		}
	}()
}

// Stats returns cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		Items:    len(c.items),
		Bytes:    c.curBytes,
		MaxBytes: c.maxBytes,
		Hits:     atomic.LoadUint64(&c.hits),
		Misses:   atomic.LoadUint64(&c.misses),
	}
}

// CacheStats contains cache statistics.
type CacheStats struct {
	Items    int
	Bytes    int64
	MaxBytes int64
	Hits     uint64
	Misses   uint64
}

// HitRate returns the cache hit rate (0.0 to 1.0).
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Keys returns all keys in the cache.
func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Entries returns all non-expired entries.
func (c *Cache) Entries() []*types.Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries := make([]*types.Entry, 0, len(c.items))
	for _, item := range c.items {
		if !item.entry.IsExpired() {
			entries = append(entries, item.entry)
		}
	}
	return entries
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
