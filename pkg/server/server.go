package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
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
}

// Config holds server configuration.
type Config struct {
	Addr         string
	Handler      Handler
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxConns     int
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
	}
}

// Start starts the server.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	close(s.stopCh)

	if s.listener != nil {
		s.listener.Close()
	}

	s.connMu.Lock()
	for conn := range s.connections {
		conn.Close()
	}
	s.connMu.Unlock()

	s.wg.Wait()
	return nil
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

	for {
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

		resp := s.processRequest(req)

		conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		if err := encoder.EncodeResponse(resp); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// processRequest processes a single request.
func (s *Server) processRequest(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
	}

	switch req.Command {
	case protocol.CmdGet:
		return s.handleGet(req)
	case protocol.CmdSet:
		return s.handleSet(req)
	case protocol.CmdDel:
		return s.handleDelete(req)
	case protocol.CmdPing:
		return s.handlePing(req)
	case protocol.CmdReplicate:
		return s.handleReplicate(req)
	case protocol.CmdStats:
		return s.handleStats(req)
	default:
		resp.Status = protocol.StatusError
		resp.Error = "unknown command"
	}

	return resp
}

// handleGet handles GET requests.
func (s *Server) handleGet(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
		Key:       req.Key,
	}

	// Check if we should redirect (we're not the primary)
	if !s.handler.IsPrimaryFor(req.Key) {
		addr := s.handler.GetRedirectAddr(req.Key)
		resp.Status = protocol.StatusRedirect
		resp.NodeAddr = addr
		return resp
	}

	value, ttl, version, found := s.handler.HandleGet(req.Key)
	if !found {
		resp.Status = protocol.StatusNotFound
		return resp
	}

	resp.Status = protocol.StatusOK
	resp.Value = value
	resp.TTL = ttl
	resp.Version = version

	return resp
}

// handleSet handles SET requests.
func (s *Server) handleSet(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
		Key:       req.Key,
	}

	if !s.handler.IsPrimaryFor(req.Key) {
		addr := s.handler.GetRedirectAddr(req.Key)
		resp.Status = protocol.StatusRedirect
		resp.NodeAddr = addr
		return resp
	}

	version, err := s.handler.HandleSet(req.Key, req.Value, req.TTL)
	if err != nil {
		resp.Status = protocol.StatusError
		resp.Error = err.Error()
		return resp
	}

	resp.Status = protocol.StatusOK
	resp.Version = version

	return resp
}

// handleDelete handles DELETE requests.
func (s *Server) handleDelete(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
		Key:       req.Key,
	}

	if !s.handler.IsPrimaryFor(req.Key) {
		addr := s.handler.GetRedirectAddr(req.Key)
		resp.Status = protocol.StatusRedirect
		resp.NodeAddr = addr
		return resp
	}

	if err := s.handler.HandleDelete(req.Key); err != nil {
		resp.Status = protocol.StatusError
		resp.Error = err.Error()
		return resp
	}

	resp.Status = protocol.StatusOK

	return resp
}

// handlePing handles PING requests.
func (s *Server) handlePing(req *protocol.Request) *protocol.Response {
	return &protocol.Response{
		RequestID: req.RequestID,
		Status:    protocol.StatusOK,
	}
}

// handleReplicate handles REPLICATE requests.
func (s *Server) handleReplicate(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		RequestID: req.RequestID,
	}

	if err := s.handler.HandleReplicate(req); err != nil {
		resp.Status = protocol.StatusError
		resp.Error = err.Error()
		return resp
	}

	resp.Status = protocol.StatusOK
	return resp
}

// handleStats handles STATS requests.
func (s *Server) handleStats(req *protocol.Request) *protocol.Response {
	stats := s.Stats()
	return &protocol.Response{
		RequestID: req.RequestID,
		Status:    protocol.StatusOK,
		Value:     []byte(fmt.Sprintf("connections:%d,requests:%d", stats.ActiveConns, stats.TotalRequests)),
	}
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

// Addr returns the server's address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}
