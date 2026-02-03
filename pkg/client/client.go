package client

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrRedirect   = errors.New("redirect")
	ErrClosed     = errors.New("client closed")
	ErrTimeout    = errors.New("request timeout")
	ErrMaxRetries = errors.New("max retries exceeded")
)

// Client is a cache client with connection pooling.
type Client struct {
	mu           sync.RWMutex
	pools        map[string]*connPool
	addrs        []string
	maxConns     int
	connTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxRetries   int
	nextReqID    uint64
	closed       bool
	authToken    string
	tlsConfig    *tls.Config
}

// connPool manages a pool of connections to a single node.
type connPool struct {
	mu        sync.Mutex
	addr      string
	conns     chan *poolConn
	maxSize   int
	timeout   time.Duration
	authToken string
	tlsConfig *tls.Config
}

// poolConn wraps a connection with buffered I/O.
type poolConn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	encoder *protocol.Encoder
	decoder *protocol.Decoder
}

// Config holds client configuration.
type Config struct {
	Addrs        []string
	MaxConns     int
	ConnTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxRetries   int
	AuthToken    string
	TLSConfig    *tls.Config
}

// New creates a new client.
func New(cfg Config) *Client {
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 10
	}
	if cfg.ConnTimeout == 0 {
		cfg.ConnTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	return &Client{
		pools:        make(map[string]*connPool),
		addrs:        cfg.Addrs,
		maxConns:     cfg.MaxConns,
		connTimeout:  cfg.ConnTimeout,
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
		maxRetries:   cfg.MaxRetries,
		authToken:    cfg.AuthToken,
		tlsConfig:    cfg.TLSConfig,
	}
}

// Get retrieves a value by key.
func (c *Client) Get(key []byte) ([]byte, error) {
	req := &protocol.Request{
		Command:   protocol.CmdGet,
		Key:       key,
		RequestID: atomic.AddUint64(&c.nextReqID, 1),
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}

	if resp.Status == protocol.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.Status != protocol.StatusOK {
		return nil, fmt.Errorf("get failed: %s", resp.Error)
	}

	return resp.Value, nil
}

// GetWithTTL retrieves a value and its TTL.
func (c *Client) GetWithTTL(key []byte) ([]byte, time.Duration, error) {
	req := &protocol.Request{
		Command:   protocol.CmdGet,
		Key:       key,
		RequestID: atomic.AddUint64(&c.nextReqID, 1),
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, 0, err
	}

	if resp.Status == protocol.StatusNotFound {
		return nil, 0, ErrNotFound
	}

	if resp.Status != protocol.StatusOK {
		return nil, 0, fmt.Errorf("get failed: %s", resp.Error)
	}

	ttl := time.Duration(resp.TTL) * time.Millisecond
	return resp.Value, ttl, nil
}

// Set stores a key-value pair.
func (c *Client) Set(key, value []byte) error {
	return c.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a key-value pair with TTL.
func (c *Client) SetWithTTL(key, value []byte, ttl time.Duration) error {
	req := &protocol.Request{
		Command:   protocol.CmdSet,
		Key:       key,
		Value:     value,
		TTL:       ttl.Milliseconds(),
		RequestID: atomic.AddUint64(&c.nextReqID, 1),
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("set failed: %s", resp.Error)
	}

	return nil
}

// Delete removes a key.
func (c *Client) Delete(key []byte) error {
	req := &protocol.Request{
		Command:   protocol.CmdDel,
		Key:       key,
		RequestID: atomic.AddUint64(&c.nextReqID, 1),
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}

	return nil
}

// Ping pings a specific node.
func (c *Client) Ping(addr string) error {
	req := &protocol.Request{
		Command:   protocol.CmdPing,
		RequestID: atomic.AddUint64(&c.nextReqID, 1),
	}

	resp, err := c.doRequest(addr, req)
	if err != nil {
		return err
	}

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Error)
	}

	return nil
}

// doWithRetry sends a request with retry logic.
func (c *Client) doWithRetry(req *protocol.Request) (*protocol.Response, error) {
	if c.closed {
		return nil, ErrClosed
	}

	var lastErr error

	for retry := 0; retry < c.maxRetries; retry++ {
		addr := c.pickServer()

		resp, err := c.doRequest(addr, req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.Status == protocol.StatusRedirect && resp.NodeAddr != "" {
			addr = resp.NodeAddr
			resp, err = c.doRequest(addr, req)
			if err != nil {
				lastErr = err
				continue
			}
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("%v", lastErr)
	}
	return nil, ErrMaxRetries
}

// doRequest sends a request to a specific address.
func (c *Client) doRequest(addr string, req *protocol.Request) (*protocol.Response, error) {
	pool := c.getPool(addr)

	pc, err := pool.get()
	if err != nil {
		return nil, err
	}

	pc.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	if err := pc.encoder.EncodeRequest(req); err != nil {
		pool.discard(pc)
		return nil, fmt.Errorf("encode request: %w", err)
	}

	if err := pc.writer.Flush(); err != nil {
		pool.discard(pc)
		return nil, fmt.Errorf("flush: %w", err)
	}

	pc.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	resp, err := pc.decoder.DecodeResponse()
	if err != nil {
		pool.discard(pc)
		return nil, fmt.Errorf("decode response: %w", err)
	}

	pool.put(pc)
	return resp, nil
}

// pickServer picks a server to send the request to.
func (c *Client) pickServer() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.addrs) == 0 {
		return ""
	}

	idx := int(atomic.AddUint64(&c.nextReqID, 1)) % len(c.addrs)
	return c.addrs[idx]
}

// getPool gets or creates a connection pool for an address.
func (c *Client) getPool(addr string) *connPool {
	c.mu.RLock()
	pool, ok := c.pools[addr]
	c.mu.RUnlock()

	if ok {
		return pool
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	pool, ok = c.pools[addr]
	if ok {
		return pool
	}

	pool = &connPool{
		addr:      addr,
		conns:     make(chan *poolConn, c.maxConns),
		maxSize:   c.maxConns,
		timeout:   c.connTimeout,
		authToken: c.authToken,
		tlsConfig: c.tlsConfig,
	}
	c.pools[addr] = pool

	return pool
}

// get gets a connection from the pool.
func (p *connPool) get() (*poolConn, error) {
	select {
	case pc := <-p.conns:
		return pc, nil
	default:
	}

	return p.dial()
}

// dial creates a new connection with optional TLS and authentication.
func (p *connPool) dial() (*poolConn, error) {
	var conn net.Conn
	var err error

	if p.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: p.timeout}, "tcp", p.addr, p.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", p.addr, p.timeout)
	}

	if err != nil {
		return nil, err
	}

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	pc := &poolConn{
		conn:    conn,
		reader:  reader,
		writer:  writer,
		encoder: protocol.NewEncoder(writer),
		decoder: protocol.NewDecoder(reader),
	}

	// Authenticate if token is set
	if p.authToken != "" {
		if err := p.authenticate(pc); err != nil {
			conn.Close()
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	return pc, nil
}

// authenticate sends an AUTH request.
func (p *connPool) authenticate(pc *poolConn) error {
	req := &protocol.Request{
		Command:   protocol.CmdAuth,
		Value:     []byte(p.authToken),
		RequestID: uint64(time.Now().UnixNano()),
	}

	if err := pc.encoder.EncodeRequest(req); err != nil {
		return err
	}
	if err := pc.writer.Flush(); err != nil {
		return err
	}

	pc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := pc.decoder.DecodeResponse()
	if err != nil {
		return err
	}

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("auth rejected: %s", resp.Error)
	}

	return nil
}

// put returns a connection to the pool.
func (p *connPool) put(pc *poolConn) {
	select {
	case p.conns <- pc:
	default:
		pc.conn.Close()
	}
}

// discard closes and discards a connection.
func (p *connPool) discard(pc *poolConn) {
	pc.conn.Close()
}

// Close closes the client and all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	for _, pool := range c.pools {
		close(pool.conns)
		for pc := range pool.conns {
			pc.conn.Close()
		}
	}

	return nil
}

// AddServer adds a server to the client.
func (c *Client) AddServer(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, s := range c.addrs {
		if s == addr {
			return
		}
	}
	c.addrs = append(c.addrs, addr)
}

// RemoveServer removes a server from the client.
func (c *Client) RemoveServer(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, s := range c.addrs {
		if s == addr {
			c.addrs = append(c.addrs[:i], c.addrs[i+1:]...)
			break
		}
	}

	if pool, ok := c.pools[addr]; ok {
		close(pool.conns)
		for pc := range pool.conns {
			pc.conn.Close()
		}
		delete(c.pools, addr)
	}
}

// Servers returns the list of servers.
func (c *Client) Servers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	servers := make([]string, len(c.addrs))
	copy(servers, c.addrs)
	return servers
}

// MGet retrieves multiple keys in a single pipelined request.
// Returns a slice of values in the same order as keys. Missing keys have nil value.
func (c *Client) MGet(keys [][]byte) ([][]byte, error) {
	if c.closed {
		return nil, ErrClosed
	}
	if len(keys) == 0 {
		return nil, nil
	}

	addr := c.pickServer()
	pool := c.getPool(addr)

	pc, err := pool.get()
	if err != nil {
		return nil, err
	}

	// Send all requests (pipelined)
	pc.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	for _, key := range keys {
		req := &protocol.Request{
			Command:   protocol.CmdGet,
			Key:       key,
			RequestID: atomic.AddUint64(&c.nextReqID, 1),
		}
		if err := pc.encoder.EncodeRequest(req); err != nil {
			pool.discard(pc)
			return nil, fmt.Errorf("encode request: %w", err)
		}
	}

	if err := pc.writer.Flush(); err != nil {
		pool.discard(pc)
		return nil, fmt.Errorf("flush: %w", err)
	}

	// Read all responses
	pc.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	values := make([][]byte, len(keys))
	for i := range keys {
		resp, err := pc.decoder.DecodeResponse()
		if err != nil {
			pool.discard(pc)
			return nil, fmt.Errorf("decode response: %w", err)
		}
		if resp.Status == protocol.StatusOK {
			values[i] = resp.Value
		}
		// StatusNotFound leaves values[i] as nil
	}

	pool.put(pc)
	return values, nil
}

// MSet stores multiple key-value pairs in a single pipelined request.
func (c *Client) MSet(pairs []KeyValue) error {
	if c.closed {
		return ErrClosed
	}
	if len(pairs) == 0 {
		return nil
	}

	addr := c.pickServer()
	pool := c.getPool(addr)

	pc, err := pool.get()
	if err != nil {
		return err
	}

	// Send all requests (pipelined)
	pc.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	for _, kv := range pairs {
		req := &protocol.Request{
			Command:   protocol.CmdSet,
			Key:       kv.Key,
			Value:     kv.Value,
			TTL:       kv.TTL.Milliseconds(),
			RequestID: atomic.AddUint64(&c.nextReqID, 1),
		}
		if err := pc.encoder.EncodeRequest(req); err != nil {
			pool.discard(pc)
			return fmt.Errorf("encode request: %w", err)
		}
	}

	if err := pc.writer.Flush(); err != nil {
		pool.discard(pc)
		return fmt.Errorf("flush: %w", err)
	}

	// Read all responses
	pc.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	for range pairs {
		resp, err := pc.decoder.DecodeResponse()
		if err != nil {
			pool.discard(pc)
			return fmt.Errorf("decode response: %w", err)
		}
		if resp.Status != protocol.StatusOK {
			pool.discard(pc)
			return fmt.Errorf("set failed: %s", resp.Error)
		}
	}

	pool.put(pc)
	return nil
}

// KeyValue represents a key-value pair for batch operations.
type KeyValue struct {
	Key   []byte
	Value []byte
	TTL   time.Duration
}

// Pipeline provides a way to send multiple commands and read responses later.
// This allows for maximum throughput by reducing round-trips.
type Pipeline struct {
	client   *Client
	pc       *poolConn
	pool     *connPool
	requests int
	err      error
}

// Pipeline creates a new pipeline for batch operations.
// Must call Exec() to send and Close() when done.
func (c *Client) Pipeline() (*Pipeline, error) {
	if c.closed {
		return nil, ErrClosed
	}

	addr := c.pickServer()
	pool := c.getPool(addr)

	pc, err := pool.get()
	if err != nil {
		return nil, err
	}

	pc.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))

	return &Pipeline{
		client: c,
		pc:     pc,
		pool:   pool,
	}, nil
}

// Get queues a GET command.
func (p *Pipeline) Get(key []byte) {
	if p.err != nil {
		return
	}
	req := &protocol.Request{
		Command:   protocol.CmdGet,
		Key:       key,
		RequestID: atomic.AddUint64(&p.client.nextReqID, 1),
	}
	if err := p.pc.encoder.EncodeRequest(req); err != nil {
		p.err = err
		return
	}
	p.requests++
}

// Set queues a SET command.
func (p *Pipeline) Set(key, value []byte) {
	p.SetWithTTL(key, value, 0)
}

// SetWithTTL queues a SET command with TTL.
func (p *Pipeline) SetWithTTL(key, value []byte, ttl time.Duration) {
	if p.err != nil {
		return
	}
	req := &protocol.Request{
		Command:   protocol.CmdSet,
		Key:       key,
		Value:     value,
		TTL:       ttl.Milliseconds(),
		RequestID: atomic.AddUint64(&p.client.nextReqID, 1),
	}
	if err := p.pc.encoder.EncodeRequest(req); err != nil {
		p.err = err
		return
	}
	p.requests++
}

// Delete queues a DELETE command.
func (p *Pipeline) Delete(key []byte) {
	if p.err != nil {
		return
	}
	req := &protocol.Request{
		Command:   protocol.CmdDel,
		Key:       key,
		RequestID: atomic.AddUint64(&p.client.nextReqID, 1),
	}
	if err := p.pc.encoder.EncodeRequest(req); err != nil {
		p.err = err
		return
	}
	p.requests++
}

// Exec flushes all queued commands and returns their responses.
func (p *Pipeline) Exec() ([]*protocol.Response, error) {
	if p.err != nil {
		p.pool.discard(p.pc)
		return nil, p.err
	}

	if p.requests == 0 {
		p.pool.put(p.pc)
		return nil, nil
	}

	if err := p.pc.writer.Flush(); err != nil {
		p.pool.discard(p.pc)
		return nil, fmt.Errorf("flush: %w", err)
	}

	p.pc.conn.SetReadDeadline(time.Now().Add(p.client.readTimeout))
	responses := make([]*protocol.Response, p.requests)
	for i := 0; i < p.requests; i++ {
		resp, err := p.pc.decoder.DecodeResponse()
		if err != nil {
			p.pool.discard(p.pc)
			return responses[:i], fmt.Errorf("decode response %d: %w", i, err)
		}
		responses[i] = resp
	}

	p.pool.put(p.pc)
	p.requests = 0
	return responses, nil
}

// Close discards the pipeline without executing.
func (p *Pipeline) Close() {
	if p.pc != nil {
		p.pool.discard(p.pc)
		p.pc = nil
	}
}
