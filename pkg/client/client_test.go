package client

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
)

// TestNewClient tests creating a new client.
func TestNewClient(t *testing.T) {
	cfg := Config{
		Addrs:    []string{"localhost:7700"},
		MaxConns: 5,
	}

	c := New(cfg)

	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if len(c.addrs) != 1 {
		t.Errorf("expected 1 seed, got %d", len(c.addrs))
	}
	if c.maxConns != 5 {
		t.Errorf("expected maxConns 5, got %d", c.maxConns)
	}
}

// TestNewClientDefaults tests default configuration values.
func TestNewClientDefaults(t *testing.T) {
	c := New(Config{})

	if c.maxConns != 10 {
		t.Errorf("expected default maxConns 10, got %d", c.maxConns)
	}
	if c.connTimeout != 5*time.Second {
		t.Errorf("expected default connTimeout 5s, got %v", c.connTimeout)
	}
	if c.readTimeout != 5*time.Second {
		t.Errorf("expected default readTimeout 5s, got %v", c.readTimeout)
	}
	if c.writeTimeout != 5*time.Second {
		t.Errorf("expected default writeTimeout 5s, got %v", c.writeTimeout)
	}
	if c.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3, got %d", c.maxRetries)
	}
}

// TestClientClose tests closing the client.
func TestClientClose(t *testing.T) {
	c := New(Config{
		Addrs: []string{"localhost:7700"},
	})

	err := c.Close()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !c.closed {
		t.Error("client should be marked as closed")
	}

	// Operations after close should fail
	_, err = c.Get([]byte("key"))
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// TestErrors tests error types.
func TestErrors(t *testing.T) {
	if ErrNotFound == nil {
		t.Error("ErrNotFound should not be nil")
	}
	if ErrRedirect == nil {
		t.Error("ErrRedirect should not be nil")
	}
	if ErrClosed == nil {
		t.Error("ErrClosed should not be nil")
	}
	if ErrTimeout == nil {
		t.Error("ErrTimeout should not be nil")
	}
	if ErrMaxRetries == nil {
		t.Error("ErrMaxRetries should not be nil")
	}
}

// mockServer creates a mock server for testing.
type mockServer struct {
	listener net.Listener
	handler  func(req *protocol.Request) *protocol.Response
	wg       sync.WaitGroup
	closed   bool
	mu       sync.Mutex
}

func newMockServer(t *testing.T) *mockServer {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	s := &mockServer{
		listener: l,
		handler: func(req *protocol.Request) *protocol.Response {
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		},
	}

	s.wg.Add(1)
	go s.serve()

	return s
}

func (s *mockServer) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *mockServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	decoder := protocol.NewDecoder(conn)
	encoder := protocol.NewEncoder(conn)

	for {
		req, err := decoder.DecodeRequest()
		if err != nil {
			return
		}

		s.mu.Lock()
		handler := s.handler
		s.mu.Unlock()

		resp := handler(req)
		if err := encoder.EncodeResponse(resp); err != nil {
			return
		}
	}
}

func (s *mockServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *mockServer) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

func (s *mockServer) SetHandler(h func(req *protocol.Request) *protocol.Response) {
	s.mu.Lock()
	s.handler = h
	s.mu.Unlock()
}

// TestClientGetSet tests basic Get and Set operations.
func TestClientGetSet(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	// Set up handler
	data := make(map[string][]byte)
	var mu sync.Mutex

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		mu.Lock()
		defer mu.Unlock()

		switch req.Command {
		case protocol.CmdSet:
			data[string(req.Key)] = req.Value
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		case protocol.CmdGet:
			if val, ok := data[string(req.Key)]; ok {
				return &protocol.Response{
					Status:    protocol.StatusOK,
					Value:     val,
					RequestID: req.RequestID,
				}
			}
			return &protocol.Response{
				Status:    protocol.StatusNotFound,
				RequestID: req.RequestID,
			}
		default:
			return &protocol.Response{
				Status:    protocol.StatusError,
				Error:     "unknown command",
				RequestID: req.RequestID,
			}
		}
	})

	c := New(Config{
		Addrs:    []string{server.Addr()},
		MaxConns: 2,
	})
	defer c.Close()

	// Set a value
	if err := c.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get the value
	val, err := c.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got %q", val)
	}

	// Get non-existent key
	_, err = c.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestClientDelete tests Delete operation.
func TestClientDelete(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	data := make(map[string][]byte)
	var mu sync.Mutex

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		mu.Lock()
		defer mu.Unlock()

		switch req.Command {
		case protocol.CmdDel:
			delete(data, string(req.Key))
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		case protocol.CmdSet:
			data[string(req.Key)] = req.Value
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		case protocol.CmdGet:
			if val, ok := data[string(req.Key)]; ok {
				return &protocol.Response{
					Status:    protocol.StatusOK,
					Value:     val,
					RequestID: req.RequestID,
				}
			}
			return &protocol.Response{
				Status:    protocol.StatusNotFound,
				RequestID: req.RequestID,
			}
		default:
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		}
	})

	c := New(Config{
		Addrs:    []string{server.Addr()},
		MaxConns: 2,
	})
	defer c.Close()

	// Set and delete
	if err := c.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := c.Delete([]byte("key")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, err := c.Get([]byte("key"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

// TestClientPing tests the Ping operation.
func TestClientPing(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		if req.Command == protocol.CmdPing {
			return &protocol.Response{
				Status:    protocol.StatusOK,
				RequestID: req.RequestID,
			}
		}
		return &protocol.Response{
			Status:    protocol.StatusError,
			RequestID: req.RequestID,
		}
	})

	c := New(Config{
		Addrs: []string{server.Addr()},
	})
	defer c.Close()

	if err := c.Ping(server.Addr()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// TestClientSetWithTTL tests setting values with TTL.
func TestClientSetWithTTL(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	var capturedTTL int64
	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		capturedTTL = req.TTL
		return &protocol.Response{
			Status:    protocol.StatusOK,
			RequestID: req.RequestID,
		}
	})

	c := New(Config{
		Addrs: []string{server.Addr()},
	})
	defer c.Close()

	// Set with TTL
	if err := c.SetWithTTL([]byte("key"), []byte("value"), 60*time.Second); err != nil {
		t.Fatalf("SetWithTTL failed: %v", err)
	}

	if capturedTTL != 60000 {
		t.Errorf("expected TTL 60000ms, got %d", capturedTTL)
	}
}

// TestClientConcurrent tests concurrent operations.
func TestClientConcurrent(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		return &protocol.Response{
			Status:    protocol.StatusOK,
			RequestID: req.RequestID,
		}
	})

	c := New(Config{
		Addrs:    []string{server.Addr()},
		MaxConns: 5,
	})
	defer c.Close()

	var wg sync.WaitGroup
	n := 50

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte("key" + string(rune('0'+i%10)))
			c.Set(key, []byte("value"))
		}(i)
	}
	wg.Wait()
}

// TestClientConnectionError tests handling connection errors.
func TestClientConnectionError(t *testing.T) {
	c := New(Config{
		Addrs:       []string{"127.0.0.1:1"}, // Invalid address
		ConnTimeout: 100 * time.Millisecond,
		MaxRetries:  1,
	})
	defer c.Close()

	_, err := c.Get([]byte("key"))
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

// TestClientWithAuth tests authentication.
func TestClientWithAuth(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		return &protocol.Response{
			Status:    protocol.StatusOK,
			RequestID: req.RequestID,
		}
	})

	c := New(Config{
		Addrs:     []string{server.Addr()},
		AuthToken: "secret-token",
	})
	defer c.Close()

	if c.authToken != "secret-token" {
		t.Errorf("expected authToken 'secret-token', got %q", c.authToken)
	}
}

// TestClientRequestID tests request ID generation.
func TestClientRequestID(t *testing.T) {
	server := newMockServer(t)
	defer server.Close()

	var ids []uint64
	var mu sync.Mutex

	server.SetHandler(func(req *protocol.Request) *protocol.Response {
		mu.Lock()
		ids = append(ids, req.RequestID)
		mu.Unlock()
		return &protocol.Response{
			Status:    protocol.StatusOK,
			RequestID: req.RequestID,
		}
	})

	c := New(Config{
		Addrs: []string{server.Addr()},
	})
	defer c.Close()

	// Make several requests
	for i := 0; i < 5; i++ {
		c.Set([]byte("key"), []byte("value"))
	}

	// Request IDs should be unique and increasing
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("request IDs should be increasing: %v", ids)
			break
		}
	}
}

// BenchmarkClientGet benchmarks Get operations.
func BenchmarkClientGet(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				decoder := protocol.NewDecoder(c)
				encoder := protocol.NewEncoder(c)
				for {
					req, err := decoder.DecodeRequest()
					if err != nil {
						return
					}
					encoder.EncodeResponse(&protocol.Response{
						Status:    protocol.StatusOK,
						Value:     []byte("value"),
						RequestID: req.RequestID,
					})
				}
			}(conn)
		}
	}()

	c := New(Config{
		Addrs:    []string{l.Addr().String()},
		MaxConns: 10,
	})
	defer c.Close()

	key := []byte("benchmark-key")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Get(key)
	}
}

// BenchmarkClientSet benchmarks Set operations.
func BenchmarkClientSet(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				decoder := protocol.NewDecoder(c)
				encoder := protocol.NewEncoder(c)
				for {
					req, err := decoder.DecodeRequest()
					if err != nil {
						return
					}
					encoder.EncodeResponse(&protocol.Response{
						Status:    protocol.StatusOK,
						RequestID: req.RequestID,
					})
				}
			}(conn)
		}
	}()

	c := New(Config{
		Addrs:    []string{l.Addr().String()},
		MaxConns: 10,
	})
	defer c.Close()

	key := []byte("benchmark-key")
	value := []byte("benchmark-value")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Set(key, value)
	}
}

// BenchmarkClientMGet benchmarks MGet (pipelined multi-get) operations.
func BenchmarkClientMGet(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				decoder := protocol.NewDecoder(c)
				encoder := protocol.NewEncoder(c)
				for {
					req, err := decoder.DecodeRequest()
					if err != nil {
						return
					}
					encoder.EncodeResponse(&protocol.Response{
						Status:    protocol.StatusOK,
						Value:     []byte("value"),
						RequestID: req.RequestID,
					})
				}
			}(conn)
		}
	}()

	c := New(Config{
		Addrs:    []string{l.Addr().String()},
		MaxConns: 10,
	})
	defer c.Close()

	// Create 100 keys for batch
	keys := make([][]byte, 100)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.MGet(keys)
	}

	b.ReportMetric(float64(len(keys)), "keys/op")
}

// BenchmarkClientPipeline benchmarks Pipeline operations.
func BenchmarkClientPipeline(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				decoder := protocol.NewDecoder(c)
				encoder := protocol.NewEncoder(c)
				for {
					req, err := decoder.DecodeRequest()
					if err != nil {
						return
					}
					encoder.EncodeResponse(&protocol.Response{
						Status:    protocol.StatusOK,
						Value:     []byte("value"),
						RequestID: req.RequestID,
					})
				}
			}(conn)
		}
	}()

	c := New(Config{
		Addrs:    []string{l.Addr().String()},
		MaxConns: 10,
	})
	defer c.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pipe, _ := c.Pipeline()
		for j := 0; j < 100; j++ {
			pipe.Get([]byte(fmt.Sprintf("key-%d", j)))
		}
		pipe.Exec()
	}

	b.ReportMetric(100, "keys/op")
}
