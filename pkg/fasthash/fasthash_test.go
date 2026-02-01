package fasthash

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

func TestSum64Basic(t *testing.T) {
	data := []byte("hello world")
	h1 := Sum64(data)
	h2 := Sum64(data)
	if h1 != h2 {
		t.Errorf("Same input produced different hashes: %d vs %d", h1, h2)
	}

	data2 := []byte("hello world!")
	h3 := Sum64(data2)
	if h1 == h3 {
		t.Errorf("Different inputs produced same hash")
	}
}

func TestSum64String(t *testing.T) {
	s := "test string"
	h1 := Sum64String(s)
	h2 := Sum64([]byte(s))
	if h1 != h2 {
		t.Errorf("Sum64String and Sum64 produced different results: %d vs %d", h1, h2)
	}
}

func TestSum64LargeKey(t *testing.T) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}
	h := Sum64(data)
	if h == 0 {
		t.Logf("Large key hash: %d", h)
	}
}

func sha256Hash(data []byte) uint64 {
	h := sha256.Sum256(data)
	return binary.BigEndian.Uint64(h[:8])
}

func BenchmarkSum64Short(b *testing.B) {
	data := []byte("short key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sum64(data)
	}
}

func BenchmarkSum64Medium(b *testing.B) {
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sum64(data)
	}
}

func BenchmarkSHA256Short(b *testing.B) {
	data := []byte("short key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sha256Hash(data)
	}
}

func BenchmarkSHA256Medium(b *testing.B) {
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sha256Hash(data)
	}
}
