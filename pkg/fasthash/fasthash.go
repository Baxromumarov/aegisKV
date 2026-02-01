// Package fasthash provides fast non-cryptographic hashing using xxHash.
// This is optimized for the hot path where hashing cost matters.
package fasthash

import (
	"unsafe"
)

// Constants for xxHash64.
const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

// Sum64 computes the 64-bit xxHash of data.
// This is a simplified inline version optimized for short keys.
func Sum64(data []byte) uint64 {
	n := len(data)
	var h uint64

	if n >= 32 {
		h = xxh64LargeKey(data)
	} else {
		h = prime5
	}

	h += uint64(n)

	// Process remaining bytes after 32-byte blocks
	i := n - (n % 32)
	if n >= 32 {
		i = n & (^31)
	} else {
		i = 0
	}

	// Process 8-byte chunks
	for ; i+8 <= n; i += 8 {
		k := read64(data[i:])
		k *= prime2
		k = rotl64(k, 31)
		k *= prime1
		h ^= k
		h = rotl64(h, 27)*prime1 + prime4
	}

	// Process 4-byte chunk
	if i+4 <= n {
		h ^= uint64(read32(data[i:])) * prime1
		h = rotl64(h, 23)*prime2 + prime3
		i += 4
	}

	// Process remaining bytes
	for ; i < n; i++ {
		h ^= uint64(data[i]) * prime5
		h = rotl64(h, 11) * prime1
	}

	// Final mix
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32

	return h
}

// Sum64String computes the 64-bit xxHash of a string without allocation.
func Sum64String(s string) uint64 {
	return Sum64(unsafeStringToBytes(s))
}

// xxh64LargeKey processes keys >= 32 bytes.
func xxh64LargeKey(data []byte) uint64 {
	// Initialize with seed (0) mixed with primes
	// Use explicit calculation to avoid compile-time overflow
	v1 := uint64(0)
	v1 = v1 + prime1
	v1 = v1 + prime2
	v2 := prime2
	v3 := uint64(0)
	v4 := uint64(0)
	v4 = v4 - prime1 // Wrapping subtraction

	n := len(data)
	nblocks := n / 32

	for i := 0; i < nblocks; i++ {
		block := data[i*32:]

		v1 = round(v1, read64(block[0:]))
		v2 = round(v2, read64(block[8:]))
		v3 = round(v3, read64(block[16:]))
		v4 = round(v4, read64(block[24:]))
	}

	h := rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18)

	h = mergeRound(h, v1)
	h = mergeRound(h, v2)
	h = mergeRound(h, v3)
	h = mergeRound(h, v4)

	return h
}

//go:inline
func round(v, input uint64) uint64 {
	v += input * prime2
	v = rotl64(v, 31)
	v *= prime1
	return v
}

//go:inline
func mergeRound(h, v uint64) uint64 {
	v = round(0, v)
	h ^= v
	h = h*prime1 + prime4
	return h
}

//go:inline
func rotl64(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}

//go:inline
func read64(b []byte) uint64 {
	_ = b[7] // bounds check hint
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

//go:inline
func read32(b []byte) uint32 {
	_ = b[3] // bounds check hint
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// unsafeStringToBytes converts a string to []byte without copying.
// The returned slice must not be modified.
//
//go:inline
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// Seed allows creating a seeded hasher for better distribution.
type Seed uint64

// Sum64 computes a seeded hash.
func (seed Seed) Sum64(data []byte) uint64 {
	return Sum64(data) ^ uint64(seed)*prime3
}

// Sum64String computes a seeded string hash.
func (seed Seed) Sum64String(s string) uint64 {
	return Sum64String(s) ^ uint64(seed)*prime3
}
