package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
	"github.com/baxromumarov/aegisKV/pkg/ratelimit"
)

// Handler processes incoming requests.
type Handler interface {
	HandleGet(key []byte) (value []byte, ttl int64, version uint64, found bool)
	HandleSet(key, value []byte, ttl int64) (version uint64, err error)
	HandleDelete(key []byte) error
	HandleReplicate(req *protocol.Request) error
	GetRedirectAddr(key []byte) string
	IsPrimaryFor(key []byte) bool
}

// Server is a TCP server for the cache protocol.
type Server struct {
	addr         string
	listener     net.Listener
	handler      Handler
	connections  map[net.Conn]struct{}
	connMu       sync.Mutex
	stopCh       chan struct{}
	wg           sync.WaitGroup
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxConns     int
	activeConns  int64
	totalReqs    uint64
	authToken    string
	tlsConfig    *tls.Config
	rateLimit    float64
	rateBurst    int
	workSem      chan struct{} // Bounded semaphore for backpressure
	draining     int32         // Atomic flag: 1 = draining, no new requests
}

// Config holds server configuration.
type Config struct {
	Addr         string
	Handler      Handler
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxConns     int
	AuthToken    string
	TLSConfig    *tls.Config
	RateLimit    float64
	RateBurst    int
}

// New creates a new server.
func New(cfg Config) *Server {
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 10000
	}

	return &Server{
		addr:         cfg.Addr,
		handler:      cfg.Handler,
		connections:  make(map[net.Conn]struct{}),
		stopCh:       make(chan struct{}),
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
		maxConns:     cfg.MaxConns,
		authToken:    cfg.AuthToken,
		tlsConfig:    cfg.TLSConfig,
		rateLimit:    cfg.RateLimit,
		rateBurst:    cfg.RateBurst,
		workSem:      make(chan struct{}, 1000), // Max 1000 concurrent requests
	}
}

// Start starts the server.
func (s *Server) Start() error {
	// Use ListenConfig with SO_REUSEADDR only (removed SO_REUSEPORT for security)
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			if err := c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			}); err != nil {
				return err
			}
			return opErr
		},
	}

	ln, err := lc.Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Wrap with TLS if configured
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the server with graceful connection draining.
func (s *Server) Stop() error {
	// Set draining flag - existing connections can finish current request
	atomic.StoreInt32(&s.draining, 1)

	// Stop accepting new connections
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}

	// Wait for all connection handlers to finish
	// (they will exit after completing current request due to draining flag)
	s.wg.Wait()

	// Force close any remaining connections
	s.connMu.Lock()
	for conn := range s.connections {
		conn.Close()
	}
	s.connMu.Unlock()

	return nil
}

// IsDraining returns true if the server is draining connections.
func (s *Server) IsDraining() bool {
	return atomic.LoadInt32(&s.draining) == 1
}

// acceptLoop accepts new connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}

		if int(atomic.LoadInt64(&s.activeConns)) >= s.maxConns {
			conn.Close()
			continue
		}

		s.connMu.Lock()
		s.connections[conn] = struct{}{}
		s.connMu.Unlock()

		atomic.AddInt64(&s.activeConns, 1)

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		conn.Close()
		s.connMu.Lock()
		delete(s.connections, conn)
		s.connMu.Unlock()
		atomic.AddInt64(&s.activeConns, -1)
	}()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	decoder := protocol.NewDecoder(reader)
	encoder := protocol.NewEncoder(writer)

	// Create per-connection rate limiter if configured
	var limiter *ratelimit.Limiter
	if s.rateLimit > 0 {
		burst := s.rateBurst
		if burst <= 0 {
			burst = int(s.rateLimit)
		}
		limiter = ratelimit.New(s.rateLimit, burst)
	}

	authenticated := s.authToken == "" // No auth required if token empty

	for {
		// Check if draining - complete current request then exit
		if s.IsDraining() {
			return
		}

		select {
		case <-s.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(s.readTimeout))

		req, err := decoder.DecodeRequest()
		if err != nil {
			if err == io.EOF {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			return
		}

		atomic.AddUint64(&s.totalReqs, 1)

		// Handle AUTH command first (before authentication check)
		if req.Command == protocol.CmdAuth {
			resp := s.handleAuth(req)
			if resp.Status == protocol.StatusOK {
				authenticated = true
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			if err := encoder.EncodeResponse(resp); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
			continue
		}

		// Require authentication for all other commands
		if !authenticated {
			resp := &protocol.Response{
				RequestID: req.RequestID,
				Status:    protocol.StatusError,
				Error:     "authentication required",
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			encoder.EncodeResponse(resp)
			writer.Flush()
			return
		}

		// Rate limiting
		if limiter != nil && !limiter.Allow() {
			resp := &protocol.Response{
				RequestID: req.RequestID,
				Status:    protocol.StatusError,
				Error:     "rate limit exceeded",
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			if err := encoder.EncodeResponse(resp); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
			continue
		}

		// Backpressure - check if we can process this request
		select {
		case s.workSem <- struct{}{}:
			// Got a slot, process the request
		default:
			// Server overloaded
			resp := &protocol.Response{
				RequestID: req.RequestID,
				Status:    protocol.StatusError,
				Error:     "server overloaded",
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			if err := encoder.EncodeResponse(resp); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
			continue
		}

		resp := s.processRequest(req)

		// Release the semaphore slot
		<-s.workSem

		conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		if err := encoder.EncodeResponse(resp); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// handleAuth handles authentication requests.
func (s *Server) handleAuth(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
	}

	if s.authToken == "" {
		resp.Status = protocol.StatusOK
		return resp
	}

	if string(req.Value) == s.authToken {
		resp.Status = protocol.StatusOK
	} else {
		resp.Status = protocol.StatusError
		resp.Error = "invalid token"
	}

	return resp
}

// processRequest processes a single request.
func (s *Server) processRequest(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
	}

	switch req.Command {
	case protocol.CmdGet:
		if !s.handler.IsPrimaryFor(req.Key) {
			resp.Status = protocol.StatusRedirect
			resp.NodeAddr = s.handler.GetRedirectAddr(req.Key)
			return resp
		}

		value, ttl, version, found := s.handler.HandleGet(req.Key)
		if !found {
			resp.Status = protocol.StatusNotFound
		} else {
			resp.Status = protocol.StatusOK
			resp.Value = value
			resp.TTL = ttl
			resp.Version = version
		}

	case protocol.CmdSet:
		if !s.handler.IsPrimaryFor(req.Key) {
			resp.Status = protocol.StatusRedirect
			resp.NodeAddr = s.handler.GetRedirectAddr(req.Key)
			return resp
		}

		version, err := s.handler.HandleSet(req.Key, req.Value, req.TTL)
		if err != nil {
			resp.Status = protocol.StatusError
			resp.Error = err.Error()
		} else {
			resp.Status = protocol.StatusOK
			resp.Version = version
		}

	case protocol.CmdDel:
		if !s.handler.IsPrimaryFor(req.Key) {
			resp.Status = protocol.StatusRedirect
			resp.NodeAddr = s.handler.GetRedirectAddr(req.Key)
			return resp
		}

		if err := s.handler.HandleDelete(req.Key); err != nil {
			resp.Status = protocol.StatusError
			resp.Error = err.Error()
		} else {
			resp.Status = protocol.StatusOK
		}

	case protocol.CmdPing:
		resp.Status = protocol.StatusOK

	case protocol.CmdReplicate:
		if err := s.handler.HandleReplicate(req); err != nil {
			resp.Status = protocol.StatusError
			resp.Error = err.Error()
		} else {
			resp.Status = protocol.StatusOK
		}

	case protocol.CmdStats:
		resp.Status = protocol.StatusOK

	default:
		resp.Status = protocol.StatusError
		resp.Error = fmt.Sprintf("unknown command: %d", req.Command)
	}

	return resp
}

// Stats returns server statistics.
func (s *Server) Stats() ServerStats {
	return ServerStats{
		ActiveConns:   atomic.LoadInt64(&s.activeConns),
		TotalRequests: atomic.LoadUint64(&s.totalReqs),
	}
}

// ServerStats contains server statistics.
type ServerStats struct {
	ActiveConns   int64
	TotalRequests uint64
}
