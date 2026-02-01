package config

import (
	"encoding/json"
	"os"
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
	Seeds             []string `json:"seeds"`
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
	GossipAdvertiseAddr string        `json:"gossip_advertise_addr"` // For containers/NAT
	GossipInterval      time.Duration `json:"gossip_interval"`
	SuspectTimeout      time.Duration `json:"suspect_timeout"`
	DeadTimeout         time.Duration `json:"dead_timeout"`

	// Server configuration
	ClientAdvertiseAddr string        `json:"client_advertise_addr"` // Client-facing address for redirects
	ReadTimeout         time.Duration `json:"read_timeout"`
	WriteTimeout        time.Duration `json:"write_timeout"`
	MaxConns            int           `json:"max_conns"`

	// Replication configuration
	ReplicationBatchSize    int           `json:"replication_batch_size"`
	ReplicationBatchTimeout time.Duration `json:"replication_batch_timeout"`
	ReplicationMaxRetries   int           `json:"replication_max_retries"`
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
	return os.WriteFile(path, data, 0644)
}

// String returns a string representation of the config.
func (c *Config) String() string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data)
}
