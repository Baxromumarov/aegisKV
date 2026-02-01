package cache

import (
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

func TestCacheSetGet(t *testing.T) {
	c := NewCache(1024 * 1024)

	key := "test-key"
	value := []byte("test-value")

	entry := &types.Entry{
		Key:     []byte(key),
		Value:   value,
		Version: types.Version{Term: 1, Seq: 1},
	}
	c.Set(key, entry)

	got := c.Get(key)
	if got == nil {
		t.Fatal("Get returned nil")
	}

	if string(got.Value) != string(value) {
		t.Errorf("expected %s, got %s", value, got.Value)
	}
}

func TestCacheSetWithTTL(t *testing.T) {
	c := NewCache(1024 * 1024)

	key := "ttl-key"
	value := []byte("ttl-value")

	entry := &types.Entry{
		Key:     []byte(key),
		Value:   value,
		Expiry:  time.Now().Add(100 * time.Millisecond).UnixMilli(),
		Version: types.Version{Term: 1, Seq: 1},
	}
	c.Set(key, entry)

	got := c.Get(key)
	if got == nil {
		t.Fatal("Get returned nil before expiry")
	}

	time.Sleep(150 * time.Millisecond)

	got = c.Get(key)
	if got != nil {
		t.Error("Get should return nil after expiry")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache(1024 * 1024)

	key := "delete-key"
	value := []byte("delete-value")

	entry := &types.Entry{
		Key:     []byte(key),
		Value:   value,
		Version: types.Version{Term: 1, Seq: 1},
	}
	c.Set(key, entry)

	got := c.Get(key)
	if got == nil {
		t.Fatal("Get returned nil before delete")
	}

	c.Delete(key)

	got = c.Get(key)
	if got != nil {
		t.Error("Get should return nil after delete")
	}
}

func TestCacheVersion(t *testing.T) {
	c := NewCache(1024 * 1024)

	key := "version-key"

	entry1 := &types.Entry{
		Key:     []byte(key),
		Value:   []byte("value1"),
		Version: types.Version{Term: 1, Seq: 1},
	}
	c.Set(key, entry1)

	entry2 := &types.Entry{
		Key:     []byte(key),
		Value:   []byte("value2"),
		Version: types.Version{Term: 1, Seq: 2},
	}
	c.Set(key, entry2)

	got := c.Get(key)
	if got == nil {
		t.Fatal("Get returned nil")
	}

	if got.Version.Seq != 2 {
		t.Error("second version should overwrite first")
	}
}

func TestCacheSetWithVersion(t *testing.T) {
	c := NewCache(1024 * 1024)

	key := "version-key"

	entry := &types.Entry{
		Key:     []byte(key),
		Value:   []byte("value"),
		Version: types.Version{Term: 5, Seq: 10},
	}
	c.SetWithVersion(key, entry)

	got := c.Get(key)
	if got == nil {
		t.Fatal("Get returned nil")
	}

	if got.Version.Term != 5 || got.Version.Seq != 10 {
		t.Errorf("expected version {5, 10}, got %+v", got.Version)
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCache(1000)

	for i := 0; i < 100; i++ {
		key := string([]byte{byte(i)})
		entry := &types.Entry{
			Key:     []byte(key),
			Value:   make([]byte, 100),
			Version: types.Version{Term: 1, Seq: uint64(i)},
		}
		c.Set(key, entry)
	}

	stats := c.Stats()
	if stats.Bytes >= 1000 {
		t.Error("eviction should have kicked in")
	}
}

func TestCacheCleanExpired(t *testing.T) {
	c := NewCache(1024 * 1024)

	c.Set("key1", &types.Entry{
		Key:    []byte("key1"),
		Value:  []byte("value1"),
		Expiry: time.Now().Add(50 * time.Millisecond).UnixMilli(),
	})
	c.Set("key2", &types.Entry{
		Key:   []byte("key2"),
		Value: []byte("value2"),
	})
	c.Set("key3", &types.Entry{
		Key:    []byte("key3"),
		Value:  []byte("value3"),
		Expiry: time.Now().Add(50 * time.Millisecond).UnixMilli(),
	})

	time.Sleep(100 * time.Millisecond)

	cleaned := c.CleanExpired()
	if cleaned != 2 {
		t.Errorf("expected 2 cleaned entries, got %d", cleaned)
	}

	got := c.Get("key2")
	if got == nil {
		t.Error("key2 should still exist")
	}
}

func TestCacheStats(t *testing.T) {
	c := NewCache(1024 * 1024)

	c.Set("key1", &types.Entry{Key: []byte("key1"), Value: []byte("value1")})
	c.Set("key2", &types.Entry{Key: []byte("key2"), Value: []byte("value2")})

	c.Get("key1")
	c.Get("key1")
	c.Get("missing")

	stats := c.Stats()
	if stats.Items != 2 {
		t.Errorf("expected 2 items, got %d", stats.Items)
	}
	if stats.Hits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
}

func BenchmarkCacheSet(b *testing.B) {
	c := NewCache(100 * 1024 * 1024)
	value := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string([]byte{byte(i % 256), byte((i / 256) % 256)})
		c.Set(key, &types.Entry{Key: []byte(key), Value: value})
	}
}

func BenchmarkCacheGet(b *testing.B) {
	c := NewCache(100 * 1024 * 1024)
	value := make([]byte, 100)

	for i := 0; i < 10000; i++ {
		key := string([]byte{byte(i % 256), byte((i / 256) % 256)})
		c.Set(key, &types.Entry{Key: []byte(key), Value: value})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string([]byte{byte(i % 256), byte((i / 256) % 256)})
		c.Get(key)
	}
}
