// Package loadtest provides load testing utilities for AegisKV.
package loadtest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/protocol"
)

// AegisClient implements Client for AegisKV.
type AegisClient struct {
	addrs   []string
	pool    sync.Pool
	timeout time.Duration
}

type aegisConn struct {
	conn    net.Conn
	encoder *protocol.Encoder
	decoder *protocol.Decoder
}

// NewAegisClient creates a new AegisKV load test client.
func NewAegisClient(addrs []string) *AegisClient {
	c := &AegisClient{
		addrs:   addrs,
		timeout: 5 * time.Second,
	}
	c.pool = sync.Pool{
		New: func() interface{} {
			conn, err := c.dial()
			if err != nil {
				return nil
			}
			return conn
		},
	}
	return c
}

func (c *AegisClient) dial() (*aegisConn, error) {
	// Round-robin through addresses
	for _, addr := range c.addrs {
		conn, err := net.DialTimeout("tcp", addr, c.timeout)
		if err != nil {
			continue
		}
		return &aegisConn{
			conn:    conn,
			encoder: protocol.NewEncoder(conn),
			decoder: protocol.NewDecoder(conn),
		}, nil
	}
	return nil, fmt.Errorf("failed to connect to any node")
}

func (c *AegisClient) getConn() (*aegisConn, error) {
	if conn := c.pool.Get(); conn != nil {
		return conn.(*aegisConn), nil
	}
	return c.dial()
}

func (c *AegisClient) putConn(conn *aegisConn) {
	c.pool.Put(conn)
}

func (c *AegisClient) closeConn(conn *aegisConn) {
	if conn != nil && conn.conn != nil {
		conn.conn.Close()
	}
}

// Get retrieves a value from AegisKV.
func (c *AegisClient) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}

	req := &protocol.Request{
		Command:   protocol.CmdGet,
		Key:       []byte(key),
		RequestID: uint64(time.Now().UnixNano()),
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.conn.SetDeadline(deadline)
	} else {
		conn.conn.SetDeadline(time.Now().Add(c.timeout))
	}

	if err := conn.encoder.EncodeRequest(req); err != nil {
		c.closeConn(conn)
		return nil, err
	}

	resp, err := conn.decoder.DecodeResponse()
	if err != nil {
		c.closeConn(conn)
		return nil, err
	}

	c.putConn(conn)

	if resp.Status != protocol.StatusOK {
		if resp.Status == protocol.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get error: %s", resp.Error)
	}

	return resp.Value, nil
}

// Set stores a value in AegisKV.
func (c *AegisClient) Set(ctx context.Context, key string, value []byte) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}

	req := &protocol.Request{
		Command:   protocol.CmdSet,
		Key:       []byte(key),
		Value:     value,
		RequestID: uint64(time.Now().UnixNano()),
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.conn.SetDeadline(deadline)
	} else {
		conn.conn.SetDeadline(time.Now().Add(c.timeout))
	}

	if err := conn.encoder.EncodeRequest(req); err != nil {
		c.closeConn(conn)
		return err
	}

	resp, err := conn.decoder.DecodeResponse()
	if err != nil {
		c.closeConn(conn)
		return err
	}

	c.putConn(conn)

	if resp.Status != protocol.StatusOK {
		return fmt.Errorf("set error: %s", resp.Error)
	}

	return nil
}

// Close closes all connections.
func (c *AegisClient) Close() error {
	return nil
}

// RedisClient implements Client for Redis (using RESP protocol).
type RedisClient struct {
	addr    string
	pool    sync.Pool
	timeout time.Duration
}

// NewRedisClient creates a new Redis load test client.
func NewRedisClient(addr string) *RedisClient {
	c := &RedisClient{
		addr:    addr,
		timeout: 5 * time.Second,
	}
	c.pool = sync.Pool{
		New: func() interface{} {
			conn, err := net.DialTimeout("tcp", addr, c.timeout)
			if err != nil {
				return nil
			}
			return conn
		},
	}
	return c
}

func (c *RedisClient) getConn() (net.Conn, error) {
	if conn := c.pool.Get(); conn != nil {
		return conn.(net.Conn), nil
	}
	return net.DialTimeout("tcp", c.addr, c.timeout)
}

func (c *RedisClient) putConn(conn net.Conn) {
	c.pool.Put(conn)
}

// Get retrieves a value from Redis.
func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// RESP protocol: *2\r\n$3\r\nGET\r\n$<keylen>\r\n<key>\r\n
	cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		conn.Close()
		return nil, err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}

	c.putConn(conn)

	// Parse RESP response
	resp := string(buf[:n])
	if len(resp) > 0 && resp[0] == '$' {
		// Bulk string - parse length and extract value
		// $<len>\r\n<data>\r\n
		if resp[1] == '-' {
			return nil, nil // nil response
		}
		// Find the data after the length line
		for i := 1; i < len(resp); i++ {
			if resp[i] == '\n' && i+1 < len(resp) {
				end := len(resp)
				if end > 2 && resp[end-2:] == "\r\n" {
					end -= 2
				}
				return []byte(resp[i+1 : end]), nil
			}
		}
	}
	return nil, nil
}

// Set stores a value in Redis.
func (c *RedisClient) Set(ctx context.Context, key string, value []byte) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// RESP protocol: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<val>\r\n
	cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), string(value))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		conn.Close()
		return err
	}

	buf := make([]byte, 256)
	_, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		return err
	}

	c.putConn(conn)
	return nil
}

// Close closes all connections.
func (c *RedisClient) Close() error {
	return nil
}

// MemcachedClient implements Client for Memcached.
type MemcachedClient struct {
	addr    string
	pool    sync.Pool
	timeout time.Duration
}

// NewMemcachedClient creates a new Memcached load test client.
func NewMemcachedClient(addr string) *MemcachedClient {
	c := &MemcachedClient{
		addr:    addr,
		timeout: 5 * time.Second,
	}
	c.pool = sync.Pool{
		New: func() interface{} {
			conn, err := net.DialTimeout("tcp", addr, c.timeout)
			if err != nil {
				return nil
			}
			return conn
		},
	}
	return c
}

func (c *MemcachedClient) getConn() (net.Conn, error) {
	if conn := c.pool.Get(); conn != nil {
		return conn.(net.Conn), nil
	}
	return net.DialTimeout("tcp", c.addr, c.timeout)
}

func (c *MemcachedClient) putConn(conn net.Conn) {
	c.pool.Put(conn)
}

// Get retrieves a value from Memcached.
func (c *MemcachedClient) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// Memcached text protocol: get <key>\r\n
	cmd := fmt.Sprintf("get %s\r\n", key)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		conn.Close()
		return nil, err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}

	c.putConn(conn)

	resp := string(buf[:n])
	if resp == "END\r\n" {
		return nil, nil
	}
	// Parse VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
	// Simplified parsing - extract data between first \r\n and END
	for i := 0; i < len(resp); i++ {
		if i+1 < len(resp) && resp[i:i+2] == "\r\n" {
			start := i + 2
			for j := start; j < len(resp); j++ {
				if j+5 <= len(resp) && resp[j:j+5] == "\r\nEND" {
					return []byte(resp[start:j]), nil
				}
			}
		}
	}
	return nil, nil
}

// Set stores a value in Memcached.
func (c *MemcachedClient) Set(ctx context.Context, key string, value []byte) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// Memcached text protocol: set <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	cmd := fmt.Sprintf("set %s 0 0 %d\r\n%s\r\n", key, len(value), string(value))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		conn.Close()
		return err
	}

	buf := make([]byte, 256)
	_, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		return err
	}

	c.putConn(conn)
	return nil
}

// Close closes all connections.
func (c *MemcachedClient) Close() error {
	return nil
}
