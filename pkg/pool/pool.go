// Package pool provides sync.Pool-based buffer and object pools for reducing allocations.
package pool

import (
	"sync"
)

// Buffer sizes for different use cases.
const (
	SmallBufSize  = 256
	MediumBufSize = 4096
	LargeBufSize  = 64 * 1024
)

// ByteSlice pools for different sizes.
var (
	smallBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, SmallBufSize)
			return &b
		},
	}

	mediumBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, MediumBufSize)
			return &b
		},
	}

	largeBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, LargeBufSize)
			return &b
		},
	}
)

// GetSmallBuf gets a small buffer (256 bytes) from the pool.
func GetSmallBuf() *[]byte {
	return smallBufPool.Get().(*[]byte)
}

// PutSmallBuf returns a small buffer to the pool.
func PutSmallBuf(b *[]byte) {
	if b == nil || cap(*b) < SmallBufSize {
		return
	}
	*b = (*b)[:SmallBufSize]
	smallBufPool.Put(b)
}

// GetMediumBuf gets a medium buffer (4KB) from the pool.
func GetMediumBuf() *[]byte {
	return mediumBufPool.Get().(*[]byte)
}

// PutMediumBuf returns a medium buffer to the pool.
func PutMediumBuf(b *[]byte) {
	if b == nil || cap(*b) < MediumBufSize {
		return
	}
	*b = (*b)[:MediumBufSize]
	mediumBufPool.Put(b)
}

// GetLargeBuf gets a large buffer (64KB) from the pool.
func GetLargeBuf() *[]byte {
	return largeBufPool.Get().(*[]byte)
}

// PutLargeBuf returns a large buffer to the pool.
func PutLargeBuf(b *[]byte) {
	if b == nil || cap(*b) < LargeBufSize {
		return
	}
	*b = (*b)[:LargeBufSize]
	largeBufPool.Put(b)
}

// GetBuf gets a buffer of at least the specified size from the appropriate pool.
func GetBuf(size int) *[]byte {
	switch {
	case size <= SmallBufSize:
		return GetSmallBuf()
	case size <= MediumBufSize:
		return GetMediumBuf()
	case size <= LargeBufSize:
		return GetLargeBuf()
	default:
		// For very large buffers, allocate directly
		b := make([]byte, size)
		return &b
	}
}

// PutBuf returns a buffer to the appropriate pool based on its capacity.
func PutBuf(b *[]byte) {
	if b == nil {
		return
	}
	switch {
	case cap(*b) >= LargeBufSize:
		PutLargeBuf(b)
	case cap(*b) >= MediumBufSize:
		PutMediumBuf(b)
	case cap(*b) >= SmallBufSize:
		PutSmallBuf(b)
		// Smaller buffers are not pooled
	}
}

// Request pool for protocol requests.
var requestPool = sync.Pool{
	New: func() any {
		return &Request{
			Key:   make([]byte, 0, 256),
			Value: make([]byte, 0, 4096),
		}
	},
}

// Request is a poolable request structure.
type Request struct {
	Key   []byte
	Value []byte
}

// GetRequest gets a request from the pool.
func GetRequest() *Request {
	return requestPool.Get().(*Request)
}

// PutRequest returns a request to the pool.
func PutRequest(r *Request) {
	if r == nil {
		return
	}
	// Reset but preserve capacity
	r.Key = r.Key[:0]
	r.Value = r.Value[:0]
	requestPool.Put(r)
}

// Response pool for protocol responses.
var responsePool = sync.Pool{
	New: func() any {
		return &Response{
			Key:   make([]byte, 0, 256),
			Value: make([]byte, 0, 4096),
		}
	},
}

// Response is a poolable response structure.
type Response struct {
	Key      []byte
	Value    []byte
	Error    string
	NodeAddr string
}

// GetResponse gets a response from the pool.
func GetResponse() *Response {
	return responsePool.Get().(*Response)
}

// PutResponse returns a response to the pool.
func PutResponse(r *Response) {
	if r == nil {
		return
	}
	r.Key = r.Key[:0]
	r.Value = r.Value[:0]
	r.Error = ""
	r.NodeAddr = ""
	responsePool.Put(r)
}
