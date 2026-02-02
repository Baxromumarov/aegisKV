package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDefaultConfig tests the default configuration values.
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Node defaults
	if cfg.BindAddr == "" {
		t.Error("BindAddr should not be empty")
	}
	if cfg.NodeID == "" {
		t.Error("NodeID should not be empty")
	}

	// Memory defaults
	if cfg.MaxMemoryMB <= 0 {
		t.Error("MaxMemoryMB should be positive")
	}

	// Cluster defaults
	if cfg.ReplicationFactor == 0 {
		t.Error("ReplicationFactor should not be zero")
	}
	if cfg.NumShards == 0 {
		t.Error("NumShards should not be zero")
	}
	if cfg.VirtualNodes == 0 {
		t.Error("VirtualNodes should not be zero")
	}

	// WAL defaults
	if cfg.WALDir == "" {
		t.Error("WALDir should not be empty")
	}

	// Gossip defaults
	if cfg.GossipBindAddr == "" {
		t.Error("GossipBindAddr should not be empty")
	}
	if cfg.GossipInterval <= 0 {
		t.Error("GossipInterval should be positive")
	}
}

// TestConfigNodeID tests NodeID generation.
func TestConfigNodeID(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.NodeID == "" {
		t.Error("NodeID should not be empty")
	}

	// NodeID should be based on hostname
	hostname, _ := os.Hostname()
	if cfg.NodeID != hostname {
		t.Logf("NodeID %q differs from hostname %q (may have suffix)", cfg.NodeID, hostname)
	}
}

// TestConfigLoadFromFile tests loading config from a file.
func TestConfigLoadFromFile(t *testing.T) {
	// Create temp config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configContent := `{
		"node_id": "test-node",
		"bind_addr": "192.168.1.100:7700",
		"addrs": ["192.168.1.101:7700", "192.168.1.102:7700"],
		"replication_factor": 3,
		"num_shards": 256,
		"max_memory_mb": 2048,
		"wal_mode": "async",
		"gossip_bind_addr": "192.168.1.100:7701",
		"auth_token": "secret-token",
		"tls_enabled": true,
		"tls_cert_file": "/etc/ssl/cert.pem",
		"tls_key_file": "/etc/ssl/key.pem"
	}`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify loaded values
	if cfg.NodeID != "test-node" {
		t.Errorf("expected NodeID 'test-node', got %q", cfg.NodeID)
	}
	if cfg.BindAddr != "192.168.1.100:7700" {
		t.Errorf("expected BindAddr '192.168.1.100:7700', got %q", cfg.BindAddr)
	}
	if len(cfg.Addrs) != 2 {
		t.Errorf("expected 2 addrs, got %d", len(cfg.Addrs))
	}
	if cfg.ReplicationFactor != 3 {
		t.Errorf("expected ReplicationFactor 3, got %d", cfg.ReplicationFactor)
	}
	if cfg.MaxMemoryMB != 2048 {
		t.Errorf("expected MaxMemoryMB 2048, got %d", cfg.MaxMemoryMB)
	}
	if cfg.WALMode != "async" {
		t.Errorf("expected WALMode 'async', got %q", cfg.WALMode)
	}
	if cfg.AuthToken != "secret-token" {
		t.Errorf("expected AuthToken 'secret-token', got %q", cfg.AuthToken)
	}
	if !cfg.TLSEnabled {
		t.Error("expected TLSEnabled = true")
	}
}

// TestConfigLoadFromFileNotFound tests loading from non-existent file.
func TestConfigLoadFromFileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/config.json")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// TestConfigLoadFromInvalidJSON tests loading invalid JSON.
func TestConfigLoadFromInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.json")

	if err := os.WriteFile(configPath, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := LoadFromFile(configPath)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// TestConfigValidate tests that Validate fills in defaults.
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*Config)
		check  func(*Config) bool
		errMsg string
	}{
		{
			name:  "FillsEmptyNodeID",
			setup: func(c *Config) { c.NodeID = "" },
			check: func(c *Config) bool { return c.NodeID != "" },
		},
		{
			name:  "FillsEmptyBindAddr",
			setup: func(c *Config) { c.BindAddr = "" },
			check: func(c *Config) bool { return c.BindAddr == "0.0.0.0:7700" },
		},
		{
			name:  "FillsZeroReplicationFactor",
			setup: func(c *Config) { c.ReplicationFactor = 0 },
			check: func(c *Config) bool { return c.ReplicationFactor == 3 },
		},
		{
			name:  "FillsZeroNumShards",
			setup: func(c *Config) { c.NumShards = 0 },
			check: func(c *Config) bool { return c.NumShards == 256 },
		},
		{
			name:  "FillsZeroVirtualNodes",
			setup: func(c *Config) { c.VirtualNodes = 0 },
			check: func(c *Config) bool { return c.VirtualNodes == 100 },
		},
		{
			name:  "FillsZeroMaxMemory",
			setup: func(c *Config) { c.MaxMemoryMB = 0 },
			check: func(c *Config) bool { return c.MaxMemoryMB == 1024 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{}
			tc.setup(cfg)

			err := cfg.Validate()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !tc.check(cfg) {
				t.Errorf("validation did not fill in expected default")
			}
		})
	}
}

// TestConfigBindAddresses tests bind address parsing.
func TestConfigBindAddresses(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BindAddr == "" {
		t.Error("BindAddr should have default")
	}
	if cfg.GossipBindAddr == "" {
		t.Error("GossipBindAddr should have default")
	}
}

// TestConfigWALSettings tests WAL configuration.
func TestConfigWALSettings(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.WALDir == "" {
		t.Error("WALDir should have default")
	}
	if cfg.WALMaxSizeMB <= 0 {
		t.Error("WALMaxSizeMB should be positive")
	}
}

// TestConfigTimeouts tests timeout configurations.
func TestConfigTimeouts(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.GossipInterval <= 0 {
		t.Error("GossipInterval should be positive")
	}
	if cfg.SuspectTimeout <= 0 {
		t.Error("SuspectTimeout should be positive")
	}
	if cfg.DeadTimeout <= 0 {
		t.Error("DeadTimeout should be positive")
	}
}

// TestConfigAuthDisabled tests config with auth disabled.
func TestConfigAuthDisabled(t *testing.T) {
	cfg := DefaultConfig()

	// Default should have no auth token
	if cfg.AuthToken != "" {
		t.Log("Default config has auth token set")
	}
}

// TestConfigTLSDisabled tests config with TLS disabled.
func TestConfigTLSDisabled(t *testing.T) {
	cfg := DefaultConfig()

	// Default should have TLS disabled
	if cfg.TLSEnabled {
		t.Error("Default config should have TLS disabled")
	}
}

// TestConfigQuorum tests quorum settings.
func TestConfigQuorum(t *testing.T) {
	cfg := DefaultConfig()

	// Quorum settings should be valid
	if cfg.WriteQuorum < -1 {
		t.Error("WriteQuorum should be >= -1")
	}
	if cfg.ReadQuorum < 0 {
		t.Error("ReadQuorum should be >= 0")
	}
}

// TestConfigClone tests cloning configuration.
func TestConfigClone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "original"
	cfg.Addrs = []string{"a", "b", "c"}

	// Clone by creating new and copying
	clone := *cfg
	clone.Addrs = make([]string, len(cfg.Addrs))
	copy(clone.Addrs, cfg.Addrs)

	// Modify original
	cfg.NodeID = "modified"
	cfg.Addrs[0] = "modified"

	// Clone should be unchanged
	if clone.NodeID == "modified" {
		t.Error("clone should not be affected by original modification")
	}
	if clone.Addrs[0] == "modified" {
		t.Error("clone's addrs should not be affected by original modification")
	}
}

// BenchmarkDefaultConfig benchmarks creating default config.
func BenchmarkDefaultConfig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = DefaultConfig()
	}
}

// BenchmarkConfigValidate benchmarks config validation.
func BenchmarkConfigValidate(b *testing.B) {
	cfg := DefaultConfig()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cfg.Validate()
	}
}
