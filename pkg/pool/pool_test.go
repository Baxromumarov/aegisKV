package pool

import (
	"sync"
	"testing"
)

func TestSmallBufPool(t *testing.T) {
	buf := GetSmallBuf()
	if buf == nil {
		t.Fatal("GetSmallBuf returned nil")
	}
	if len(*buf) != SmallBufSize {
		t.Errorf("expected length %d, got %d", SmallBufSize, len(*buf))
	}
	if cap(*buf) < SmallBufSize {
		t.Errorf("expected capacity >= %d, got %d", SmallBufSize, cap(*buf))
	}

	PutSmallBuf(buf)
}

func TestMediumBufPool(t *testing.T) {
	buf := GetMediumBuf()
	if buf == nil {
		t.Fatal("GetMediumBuf returned nil")
	}
	if len(*buf) != MediumBufSize {
		t.Errorf("expected length %d, got %d", MediumBufSize, len(*buf))
	}
	if cap(*buf) < MediumBufSize {
		t.Errorf("expected capacity >= %d, got %d", MediumBufSize, cap(*buf))
	}

	PutMediumBuf(buf)
}

func TestLargeBufPool(t *testing.T) {
	buf := GetLargeBuf()
	if buf == nil {
		t.Fatal("GetLargeBuf returned nil")
	}
	if len(*buf) != LargeBufSize {
		t.Errorf("expected length %d, got %d", LargeBufSize, len(*buf))
	}
	if cap(*buf) < LargeBufSize {
		t.Errorf("expected capacity >= %d, got %d", LargeBufSize, cap(*buf))
	}

	PutLargeBuf(buf)
}

func TestGetBuf(t *testing.T) {
	tests := []struct {
		name         string
		size         int
		expectedSize int
	}{
		{"small", 100, SmallBufSize},
		{"exactly small", SmallBufSize, SmallBufSize},
		{"medium", 1000, MediumBufSize},
		{"exactly medium", MediumBufSize, MediumBufSize},
		{"large", 10000, LargeBufSize},
		{"exactly large", LargeBufSize, LargeBufSize},
		{"very large", LargeBufSize + 1000, LargeBufSize + 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := GetBuf(tt.size)
			if buf == nil {
				t.Fatal("GetBuf returned nil")
			}
			if len(*buf) != tt.expectedSize {
				t.Errorf("expected length %d, got %d", tt.expectedSize, len(*buf))
			}
			PutBuf(buf)
		})
	}
}

func TestPutBufNil(t *testing.T) {
	// Should not panic
	PutBuf(nil)
	PutSmallBuf(nil)
	PutMediumBuf(nil)
	PutLargeBuf(nil)
}

func TestPutBufTooSmall(t *testing.T) {
	// Buffer smaller than pool size should not be put back
	small := make([]byte, 10)
	PutSmallBuf(&small) // Should not panic or corrupt pool

	medium := make([]byte, 100)
	PutMediumBuf(&medium) // Should not panic
}

func TestRequestPool(t *testing.T) {
	req := GetRequest()
	if req == nil {
		t.Fatal("GetRequest returned nil")
	}
	if cap(req.Key) == 0 {
		t.Error("Key should have capacity")
	}
	if cap(req.Value) == 0 {
		t.Error("Value should have capacity")
	}

	// Set some data
	req.Key = append(req.Key, []byte("test-key")...)
	req.Value = append(req.Value, []byte("test-value")...)

	PutRequest(req)

	// Get again - should be reset
	req2 := GetRequest()
	if len(req2.Key) != 0 {
		t.Error("Key should be reset")
	}
	if len(req2.Value) != 0 {
		t.Error("Value should be reset")
	}
	PutRequest(req2)
}

func TestPutRequestNil(t *testing.T) {
	PutRequest(nil) // Should not panic
}

func TestResponsePool(t *testing.T) {
	resp := GetResponse()
	if resp == nil {
		t.Fatal("GetResponse returned nil")
	}
	if cap(resp.Key) == 0 {
		t.Error("Key should have capacity")
	}
	if cap(resp.Value) == 0 {
		t.Error("Value should have capacity")
	}

	// Set some data
	resp.Key = append(resp.Key, []byte("key")...)
	resp.Value = append(resp.Value, []byte("value")...)
	resp.Error = "error"
	resp.NodeAddr = "node1"

	PutResponse(resp)

	// Get again - should be reset
	resp2 := GetResponse()
	if len(resp2.Key) != 0 {
		t.Error("Key should be reset")
	}
	if len(resp2.Value) != 0 {
		t.Error("Value should be reset")
	}
	if resp2.Error != "" {
		t.Error("Error should be reset")
	}
	if resp2.NodeAddr != "" {
		t.Error("NodeAddr should be reset")
	}
	PutResponse(resp2)
}

func TestPutResponseNil(t *testing.T) {
	PutResponse(nil) // Should not panic
}

func TestConcurrentBufPool(t *testing.T) {
	var wg sync.WaitGroup
	numGoroutines := 100
	iterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				buf := GetSmallBuf()
				(*buf)[0] = byte(j)
				PutSmallBuf(buf)

				buf2 := GetMediumBuf()
				(*buf2)[0] = byte(j)
				PutMediumBuf(buf2)

				buf3 := GetLargeBuf()
				(*buf3)[0] = byte(j)
				PutLargeBuf(buf3)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentRequestPool(t *testing.T) {
	var wg sync.WaitGroup
	numGoroutines := 100
	iterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := GetRequest()
				req.Key = append(req.Key, "key"...)
				req.Value = append(req.Value, "value"...)
				PutRequest(req)
			}
		}()
	}

	wg.Wait()
}

func TestConstants(t *testing.T) {
	if SmallBufSize != 256 {
		t.Errorf("SmallBufSize should be 256, got %d", SmallBufSize)
	}
	if MediumBufSize != 4096 {
		t.Errorf("MediumBufSize should be 4096, got %d", MediumBufSize)
	}
	if LargeBufSize != 64*1024 {
		t.Errorf("LargeBufSize should be 65536, got %d", LargeBufSize)
	}
}

func BenchmarkGetPutSmallBuf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetSmallBuf()
		PutSmallBuf(buf)
	}
}

func BenchmarkGetPutMediumBuf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetMediumBuf()
		PutMediumBuf(buf)
	}
}

func BenchmarkGetPutLargeBuf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetLargeBuf()
		PutLargeBuf(buf)
	}
}

func BenchmarkGetPutRequest(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := GetRequest()
		PutRequest(req)
	}
}

func BenchmarkGetPutResponse(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := GetResponse()
		PutResponse(resp)
	}
}

func BenchmarkAllocSmallBuf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = make([]byte, SmallBufSize)
	}
}

func BenchmarkPoolParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := GetSmallBuf()
			PutSmallBuf(buf)
		}
	})
}
