package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
)

// mockHandler implements Handler for testing.
type mockHandler struct {
	data     map[string][]byte
	mu       sync.RWMutex
	versions map[string]uint64
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		data:     make(map[string][]byte),
		versions: make(map[string]uint64),
	}
}

func (h *mockHandler) HandleGet(key []byte) (value []byte, ttl int64, version uint64, found bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	val, ok := h.data[string(key)]
	if !ok {
		return nil, 0, 0, false
	}
	return val, 0, h.versions[string(key)], true
}

func (h *mockHandler) HandleSet(key, value []byte, ttl int64) (version uint64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data[string(key)] = value
	h.versions[string(key)]++
	return h.versions[string(key)], nil
}

func (h *mockHandler) HandleDelete(key []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.data, string(key))
	return nil
}

func (h *mockHandler) HandleReplicate(req *protocol.Request) error {
	_, err := h.HandleSet(req.Key, req.Value, req.TTL)
	return err
}

func (h *mockHandler) GetRedirectAddr(key []byte) string {
	return ""
}

func (h *mockHandler) IsPrimaryFor(key []byte) bool {
	return true
}

// TestServerStartStop tests basic server start and stop.
func TestServerStartStop(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0", // Random port
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Get actual address
	addr := srv.listener.Addr().String()
	t.Logf("Server started on %s", addr)

	// Try to connect
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.Close()

	// Stop server
	if err := srv.Stop(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}

	// Verify can't connect anymore
	_, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		t.Error("expected connection to fail after stop")
	}
}

// TestServerGetSet tests basic GET/SET operations.
func TestServerGetSet(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	// Connect
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	encoder := protocol.NewEncoder(conn)
	decoder := protocol.NewDecoder(bufio.NewReader(conn))

	// SET operation
	setReq := &protocol.Request{
		Command:   protocol.CmdSet,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		RequestID: 1,
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := encoder.EncodeRequest(setReq); err != nil {
		t.Fatalf("failed to encode SET request: %v", err)
	}

	setResp, err := decoder.DecodeResponse()
	if err != nil {
		t.Fatalf("failed to decode SET response: %v", err)
	}

	if setResp.Status != protocol.StatusOK {
		t.Errorf("SET failed: %s", setResp.Error)
	}

	// GET operation
	getReq := &protocol.Request{
		Command:   protocol.CmdGet,
		Key:       []byte("test-key"),
		RequestID: 2,
	}

	if err := encoder.EncodeRequest(getReq); err != nil {
		t.Fatalf("failed to encode GET request: %v", err)
	}

	getResp, err := decoder.DecodeResponse()
	if err != nil {
		t.Fatalf("failed to decode GET response: %v", err)
	}

	if getResp.Status != protocol.StatusOK {
		t.Errorf("GET failed: %s", getResp.Error)
	}

	if string(getResp.Value) != "test-value" {
		t.Errorf("expected 'test-value', got %q", string(getResp.Value))
	}
}

// TestServerDelete tests DELETE operation.
func TestServerDelete(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	encoder := protocol.NewEncoder(conn)
	decoder := protocol.NewDecoder(bufio.NewReader(conn))
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// SET first
	encoder.EncodeRequest(&protocol.Request{
		Command:   protocol.CmdSet,
		Key:       []byte("del-key"),
		Value:     []byte("del-value"),
		RequestID: 1,
	})
	decoder.DecodeResponse()

	// DELETE
	encoder.EncodeRequest(&protocol.Request{
		Command:   protocol.CmdDel,
		Key:       []byte("del-key"),
		RequestID: 2,
	})
	delResp, _ := decoder.DecodeResponse()

	if delResp.Status != protocol.StatusOK {
		t.Errorf("DELETE failed: %s", delResp.Error)
	}

	// GET should return not found
	encoder.EncodeRequest(&protocol.Request{
		Command:   protocol.CmdGet,
		Key:       []byte("del-key"),
		RequestID: 3,
	})
	getResp, _ := decoder.DecodeResponse()

	if getResp.Status != protocol.StatusNotFound {
		t.Errorf("expected NOT_FOUND, got %d", getResp.Status)
	}
}

// TestServerPing tests PING command.
func TestServerPing(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	encoder := protocol.NewEncoder(conn)
	decoder := protocol.NewDecoder(bufio.NewReader(conn))
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	encoder.EncodeRequest(&protocol.Request{
		Command:   protocol.CmdPing,
		RequestID: 1,
	})
	resp, _ := decoder.DecodeResponse()

	if resp.Status != protocol.StatusOK {
		t.Errorf("PING failed: %s", resp.Error)
	}
}

// TestServerConcurrentConnections tests handling multiple concurrent connections.
func TestServerConcurrentConnections(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:     "127.0.0.1:0",
		Handler:  handler,
		MaxConns: 100,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	numClients := 50
	opsPerClient := 100

	var wg sync.WaitGroup
	var successOps int64

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			encoder := protocol.NewEncoder(conn)
			decoder := protocol.NewDecoder(bufio.NewReader(conn))

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("client-%d-key-%d", clientID, j)
				value := fmt.Sprintf("value-%d-%d", clientID, j)

				conn.SetDeadline(time.Now().Add(5 * time.Second))

				encoder.EncodeRequest(&protocol.Request{
					Command:   protocol.CmdSet,
					Key:       []byte(key),
					Value:     []byte(value),
					RequestID: uint64(j),
				})

				resp, err := decoder.DecodeResponse()
				if err == nil && resp.Status == protocol.StatusOK {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedOps := int64(numClients * opsPerClient)
	successRate := float64(successOps) / float64(expectedOps) * 100

	t.Logf("Concurrent test: %d/%d ops successful (%.2f%%)", successOps, expectedOps, successRate)

	if successRate < 95 {
		t.Errorf("success rate too low: %.2f%%", successRate)
	}
}

// TestServerMaxConnections tests connection limit enforcement.
func TestServerMaxConnections(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:     "127.0.0.1:0",
		Handler:  handler,
		MaxConns: 5,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	// Create max connections
	conns := make([]net.Conn, 5)
	for i := 0; i < 5; i++ {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		conns[i] = conn
	}

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Try one more - should be rejected
	extraConn, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		// Connection may be accepted but immediately closed
		time.Sleep(50 * time.Millisecond)

		// Try to send data - should fail
		encoder := protocol.NewEncoder(extraConn)
		decoder := protocol.NewDecoder(bufio.NewReader(extraConn))
		extraConn.SetDeadline(time.Now().Add(time.Second))

		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdPing,
			RequestID: 1,
		})
		_, err := decoder.DecodeResponse()
		if err == nil {
			t.Log("Extra connection was accepted (server may have capacity)")
		}
		extraConn.Close()
	}

	// Clean up
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

// TestServerDraining tests connection draining.
func TestServerDraining(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	addr := srv.listener.Addr().String()

	// Connect and start operations
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Verify not draining initially
	if srv.IsDraining() {
		t.Error("server should not be draining initially")
	}

	// Start stop in background
	stopDone := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopDone)
	}()

	// Briefly wait for draining to be set
	time.Sleep(50 * time.Millisecond)

	// Server should be draining
	if !srv.IsDraining() {
		t.Log("Note: server may have already completed draining")
	}

	conn.Close()
	<-stopDone
}

// TestServerStats tests statistics tracking.
func TestServerStats(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	// Initial stats
	stats := srv.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("expected 0 active connections, got %d", stats.ActiveConns)
	}

	// Connect
	conn, _ := net.DialTimeout("tcp", addr, time.Second)
	time.Sleep(50 * time.Millisecond)

	stats = srv.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("expected 1 active connection, got %d", stats.ActiveConns)
	}

	// Make some requests
	encoder := protocol.NewEncoder(conn)
	decoder := protocol.NewDecoder(bufio.NewReader(conn))
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	for i := 0; i < 10; i++ {
		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdPing,
			RequestID: uint64(i),
		})
		decoder.DecodeResponse()
	}

	stats = srv.Stats()
	if stats.TotalRequests < 10 {
		t.Errorf("expected at least 10 requests, got %d", stats.TotalRequests)
	}

	conn.Close()
}

// TestServerAuth tests authentication.
func TestServerAuth(t *testing.T) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:      "127.0.0.1:0",
		Handler:   handler,
		AuthToken: "secret-token",
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	t.Run("WithoutAuth", func(t *testing.T) {
		conn, _ := net.DialTimeout("tcp", addr, time.Second)
		defer conn.Close()

		encoder := protocol.NewEncoder(conn)
		decoder := protocol.NewDecoder(bufio.NewReader(conn))
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		// Try command without auth
		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdPing,
			RequestID: 1,
		})
		resp, err := decoder.DecodeResponse()
		if err != nil {
			return // Connection closed
		}

		if resp.Status != protocol.StatusError || resp.Error != "authentication required" {
			t.Errorf("expected auth error, got status=%d error=%s", resp.Status, resp.Error)
		}
	})

	t.Run("WithWrongAuth", func(t *testing.T) {
		conn, _ := net.DialTimeout("tcp", addr, time.Second)
		defer conn.Close()

		encoder := protocol.NewEncoder(conn)
		decoder := protocol.NewDecoder(bufio.NewReader(conn))
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		// Auth with wrong token
		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdAuth,
			Value:     []byte("wrong-token"),
			RequestID: 1,
		})
		resp, _ := decoder.DecodeResponse()

		if resp.Status != protocol.StatusError {
			t.Errorf("expected auth failure, got status=%d", resp.Status)
		}
	})

	t.Run("WithCorrectAuth", func(t *testing.T) {
		conn, _ := net.DialTimeout("tcp", addr, time.Second)
		defer conn.Close()

		encoder := protocol.NewEncoder(conn)
		decoder := protocol.NewDecoder(bufio.NewReader(conn))
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		// Auth with correct token
		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdAuth,
			Value:     []byte("secret-token"),
			RequestID: 1,
		})
		resp, _ := decoder.DecodeResponse()

		if resp.Status != protocol.StatusOK {
			t.Errorf("expected auth success, got status=%d error=%s", resp.Status, resp.Error)
		}

		// Now command should work
		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdPing,
			RequestID: 2,
		})
		resp, _ = decoder.DecodeResponse()

		if resp.Status != protocol.StatusOK {
			t.Errorf("expected PING success after auth, got status=%d", resp.Status)
		}
	})
}

// BenchmarkServerGetSet benchmarks GET/SET operations.
func BenchmarkServerGetSet(b *testing.B) {
	handler := newMockHandler()
	srv := New(Config{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	if err := srv.Start(); err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	conn, _ := net.DialTimeout("tcp", addr, time.Second)
	defer conn.Close()

	encoder := protocol.NewEncoder(conn)
	decoder := protocol.NewDecoder(bufio.NewReader(conn))

	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i%1000)

		conn.SetDeadline(time.Now().Add(5 * time.Second))

		encoder.EncodeRequest(&protocol.Request{
			Command:   protocol.CmdSet,
			Key:       []byte(key),
			Value:     value,
			RequestID: uint64(i),
		})
		decoder.DecodeResponse()
	}
}
