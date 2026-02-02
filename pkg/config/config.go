// Package config holds the configuration for an AegisKV node.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// Config holds the configuration for an AegisKV node.
type Config struct {
	// Node configuration
	NodeID   string `json:"node_id"`
	BindAddr string `json:"bind_addr"`
	DataDir  string `json:"data_dir"`

	// Cluster configuration
	Seeds             []string `json:"addrs"`
	ReplicationFactor int      `json:"replication_factor"`
	NumShards         int      `json:"num_shards"`
	VirtualNodes      int      `json:"virtual_nodes"`

	// Memory configuration
	MaxMemoryMB   int64   `json:"max_memory_mb"`
	EvictionRatio float64 `json:"eviction_ratio"`
	ShardMaxBytes int64   `json:"shard_max_bytes"`

	// WAL configuration
	WALMode      string `json:"wal_mode"`
	WALDir       string `json:"wal_dir"`
	WALMaxSizeMB int64  `json:"wal_max_size_mb"`

	// Gossip configuration
	GossipBindAddr      string        `json:"gossip_bind_addr"`
	GossipAdvertiseAddr string        `json:"gossip_advertise_addr"`
	GossipInterval      time.Duration `json:"gossip_interval"`
	SuspectTimeout      time.Duration `json:"suspect_timeout"`
	DeadTimeout         time.Duration `json:"dead_timeout"`
	GossipSecret        string        `json:"gossip_secret"` // HMAC key for gossip message signing

	// Server configuration
	ClientAdvertiseAddr string        `json:"client_advertise_addr"`
	ReadTimeout         time.Duration `json:"read_timeout"`
	WriteTimeout        time.Duration `json:"write_timeout"`
	MaxConns            int           `json:"max_conns"`

	// Replication configuration
	ReplicationBatchSize    int           `json:"replication_batch_size"`
	ReplicationBatchTimeout time.Duration `json:"replication_batch_timeout"`
	ReplicationMaxRetries   int           `json:"replication_max_retries"`

	// Authentication
	AuthToken string `json:"auth_token"` // Shared secret. Empty = no auth.

	// TLS configuration
	TLSEnabled  bool   `json:"tls_enabled"`
	TLSCertFile string `json:"tls_cert_file"`
	TLSKeyFile  string `json:"tls_key_file"`
	TLSCAFile   string `json:"tls_ca_file"`

	// Health / metrics HTTP server
	HealthAddr string `json:"health_addr"`

	// Maintenance
	MaintenanceInterval time.Duration `json:"maintenance_interval"`

	// Rate limiting (per-connection)
	RateLimit float64 `json:"rate_limit"` // Max ops/sec per connection. 0 = unlimited.
	RateBurst int     `json:"rate_burst"` // Burst size for rate limiter.

	// Logging
	LogLevel string `json:"log_level"` // "debug", "info", "warn", "error"

	// Quorum consistency
	WriteQuorum int `json:"write_quorum"` // Min writes for success (0 = async, -1 = all)
	ReadQuorum  int `json:"read_quorum"`  // Min reads for consistency (0 = local only)

	// Graceful shutdown
	DrainTimeout time.Duration `json:"drain_timeout"` // Time to wait for in-flight requests

	// PID file
	PIDFile string `json:"pid_file"` // Path to PID file (empty = disabled)

	// Runtime TLS config (not serialized)
	TLSConfig *tls.Config `json:"-"`
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	hostname, _ := os.Hostname()

	return &Config{
		NodeID:   hostname,
		BindAddr: "0.0.0.0:7700",
		DataDir:  "./data",

		Seeds:             []string{},
		ReplicationFactor: 3,
		NumShards:         256,
		VirtualNodes:      100,

		MaxMemoryMB:   1024,
		EvictionRatio: 0.9,
		ShardMaxBytes: 64 * 1024 * 1024,

		WALMode:      "off",
		WALDir:       "./data/wal",
		WALMaxSizeMB: 64,

		GossipBindAddr: "0.0.0.0:7701",
		GossipInterval: time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    10 * time.Second,

		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxConns:     10000,

		ReplicationBatchSize:    100,
		ReplicationBatchTimeout: 10 * time.Millisecond,
		ReplicationMaxRetries:   3,

		HealthAddr:          ":7702",
		MaintenanceInterval: 10 * time.Second,
		LogLevel:            "info",
	}
}

// LoadFromFile loads configuration from a JSON file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadFromEnv loads configuration from environment variables.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("AEGIS_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv("AEGIS_BIND_ADDR"); v != "" {
		cfg.BindAddr = v
	}
	if v := os.Getenv("AEGIS_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("AEGIS_GOSSIP_ADDR"); v != "" {
		cfg.GossipBindAddr = v
	}
	if v := os.Getenv("AEGIS_WAL_MODE"); v != "" {
		cfg.WALMode = v
	}
	if v := os.Getenv("AEGIS_AUTH_TOKEN"); v != "" {
		cfg.AuthToken = v
	}
	if v := os.Getenv("AEGIS_GOSSIP_SECRET"); v != "" {
		cfg.GossipSecret = v
	}
	if v := os.Getenv("AEGIS_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	return cfg
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		c.NodeID, _ = os.Hostname()
	}
	if c.BindAddr == "" {
		c.BindAddr = "0.0.0.0:7700"
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 3
	}
	if c.NumShards <= 0 {
		c.NumShards = 256
	}
	if c.VirtualNodes <= 0 {
		c.VirtualNodes = 100
	}
	if c.MaxMemoryMB <= 0 {
		c.MaxMemoryMB = 1024
	}
	if c.EvictionRatio <= 0 || c.EvictionRatio > 1 {
		c.EvictionRatio = 0.9
	}
	if c.WALDir == "" || c.WALDir == "./data/wal" {
		c.WALDir = filepath.Join(c.DataDir, "wal")
	}
	if c.MaintenanceInterval <= 0 {
		c.MaintenanceInterval = 10 * time.Second
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	// Build TLS configuration if certificate files are provided
	if c.TLSCertFile != "" && c.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return err
		}
		c.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}
		// Add CA for mTLS if provided
		if c.TLSCAFile != "" {
			caCert, err := os.ReadFile(c.TLSCAFile)
			if err != nil {
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to parse TLS CA certificate")
			}
			c.TLSConfig.ClientCAs = pool
			c.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
			c.TLSConfig.RootCAs = pool
		}
		c.TLSEnabled = true
	}

	return nil
}

// GetWALMode returns the WAL mode as a types.WALMode.
func (c *Config) GetWALMode() types.WALMode {
	switch c.WALMode {
	case "write":
		return types.WALModeWrite
	case "fsync":
		return types.WALModeFsync
	default:
		return types.WALModeOff
	}
}

// SaveToFile saves the configuration to a JSON file.
func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// String returns a string representation of the config.
func (c *Config) String() string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data)
}
