package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/config"
	"github.com/baxromumarov/aegisKV/pkg/logger"
	"github.com/baxromumarov/aegisKV/pkg/node"
)

func main() {
	var (
		configFile      = flag.String("config", "", "Path to config file")
		nodeID          = flag.String("node-id", "", "Node ID (default: hostname)")
		bindAddr        = flag.String("bind", "0.0.0.0:7700", "Address to bind to")
		gossipAddr      = flag.String("gossip", "0.0.0.0:7701", "Address for gossip protocol")
		gossipAdvertise = flag.String("gossip-advertise", "", "Address to advertise for gossip (for containers/NAT)")
		clientAdvertise = flag.String("client-advertise", "", "Address to advertise for client redirects (for containers/NAT)")
		dataDir         = flag.String("data-dir", "./data", "Data directory")
		addrs           = flag.String("addrs", "", "Comma-separated list of seed nodes")
		replFactor      = flag.Int("replication-factor", 3, "Replication factor")
		numShards       = flag.Int("shards", 256, "Number of shards")
		walMode         = flag.String("wal", "off", "WAL mode: off, write, fsync")
		maxMemoryMB     = flag.Int64("max-memory", 1024, "Maximum memory in MB")

		// Security flags
		authToken    = flag.String("auth-token", "", "Shared authentication token for clients and replication")
		tlsCert      = flag.String("tls-cert", "", "Path to TLS certificate file")
		tlsKey       = flag.String("tls-key", "", "Path to TLS key file")
		tlsCA        = flag.String("tls-ca", "", "Path to TLS CA certificate for mTLS")
		gossipSecret = flag.String("gossip-secret", "", "Shared secret for gossip HMAC signing")

		// Health and observability
		healthAddr = flag.String("health-addr", "", "Address for HTTP health endpoints (e.g. :8080)")

		// Rate limiting
		rateLimitRPS = flag.Float64("rate-limit", 0, "Per-connection requests per second (0=disabled)")
		rateBurst    = flag.Int("rate-burst", 10, "Rate limit burst size")

		// Log level
		logLevel = flag.String("log-level", "info", "Log level: debug, info, warn, error")

		// Quorum
		writeQuorum = flag.Int("write-quorum", 0, "Write quorum (0=async, -1=all)")
		readQuorum  = flag.Int("read-quorum", 0, "Read quorum (0=local only)")

		// Graceful shutdown
		drainTimeout = flag.Duration("drain-timeout", 30*time.Second, "Time to wait for in-flight requests")

		// PID file
		pidFile = flag.String("pid-file", "", "Path to PID file")
	)

	flag.Parse()

	// Configure logger level
	switch strings.ToLower(*logLevel) {
	case "debug":
		logger.SetLevel(logger.LevelDebug)
	case "info":
		logger.SetLevel(logger.LevelInfo)
	case "warn", "warning":
		logger.SetLevel(logger.LevelWarn)
	case "error":
		logger.SetLevel(logger.LevelError)
	default:
		logger.SetLevel(logger.LevelInfo)
	}

	var cfg *config.Config
	var err error

	if *configFile != "" {
		cfg, err = config.LoadFromFile(*configFile)
		if err != nil {
			logger.Error("Failed to load config: %v", err)
			os.Exit(1)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Apply command-line overrides
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *bindAddr != "" {
		cfg.BindAddr = *bindAddr
	}
	if *gossipAddr != "" {
		cfg.GossipBindAddr = *gossipAddr
	}
	if *gossipAdvertise != "" {
		cfg.GossipAdvertiseAddr = *gossipAdvertise
	}
	if *clientAdvertise != "" {
		cfg.ClientAdvertiseAddr = *clientAdvertise
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *addrs != "" {
		cfg.Seeds = strings.Split(*addrs, ",")
	}
	if *replFactor != 3 {
		cfg.ReplicationFactor = *replFactor
	}
	if *numShards != 256 {
		cfg.NumShards = *numShards
	}
	if *walMode != "off" {
		cfg.WALMode = *walMode
	}
	if *maxMemoryMB != 1024 {
		cfg.MaxMemoryMB = *maxMemoryMB
	}

	// Security overrides
	if *authToken != "" {
		cfg.AuthToken = *authToken
	}
	if *tlsCert != "" {
		cfg.TLSCertFile = *tlsCert
	}
	if *tlsKey != "" {
		cfg.TLSKeyFile = *tlsKey
	}
	if *tlsCA != "" {
		cfg.TLSCAFile = *tlsCA
	}
	if *gossipSecret != "" {
		cfg.GossipSecret = *gossipSecret
	}

	// Health endpoint
	if *healthAddr != "" {
		cfg.HealthAddr = *healthAddr
	}

	// Rate limiting
	if *rateLimitRPS > 0 {
		cfg.RateLimit = *rateLimitRPS
		cfg.RateBurst = *rateBurst
	}

	// Quorum settings
	cfg.WriteQuorum = *writeQuorum
	cfg.ReadQuorum = *readQuorum

	// Graceful shutdown
	cfg.DrainTimeout = *drainTimeout

	// PID file
	cfg.PIDFile = *pidFile

	if err := cfg.Validate(); err != nil {
		logger.Error("Invalid configuration: %v", err)
		os.Exit(1)
	}

	n, err := node.New(cfg)
	if err != nil {
		logger.Error("Failed to create node: %v", err)
		os.Exit(1)
	}

	if err := n.Start(); err != nil {
		logger.Error("Failed to start node: %v", err)
		os.Exit(1)
	}

	logger.Info("AegisKV node %s started", n.NodeID())
	logger.Info("  Bind address: %s", cfg.BindAddr)
	logger.Info("  Gossip address: %s", cfg.GossipBindAddr)
	if cfg.GossipAdvertiseAddr != "" {
		logger.Info("  Gossip advertise: %s", cfg.GossipAdvertiseAddr)
	}
	logger.Info("  Data directory: %s", cfg.DataDir)
	logger.Info("  Shards: %d", cfg.NumShards)
	logger.Info("  Replication factor: %d", cfg.ReplicationFactor)
	logger.Info("  WAL mode: %s", cfg.WALMode)
	logger.Info("  Max memory: %d MB", cfg.MaxMemoryMB)
	if cfg.TLSCertFile != "" {
		logger.Info("  TLS: enabled")
	}
	if cfg.AuthToken != "" {
		logger.Info("  Auth: enabled")
	}
	if cfg.HealthAddr != "" {
		logger.Info("  Health endpoint: %s", cfg.HealthAddr)
	}

	// Write PID file
	if cfg.PIDFile != "" {
		if err := writePIDFile(cfg.PIDFile); err != nil {
			logger.Error("Failed to write PID file: %v", err)
		} else {
			defer removePIDFile(cfg.PIDFile)
			logger.Info("  PID file: %s", cfg.PIDFile)
		}
	}

	// Signal handling with SIGHUP for config reload
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	var configPath atomic.Value
	configPath.Store(*configFile)

	for {
		sig := <-sigCh

		switch sig {
		case syscall.SIGHUP:
			// Reload configuration
			cfgPath := configPath.Load().(string)
			if cfgPath == "" {
				logger.Warn("SIGHUP received but no config file specified, ignoring")
				continue
			}

			logger.Info("SIGHUP received, reloading config from %s", cfgPath)
			newCfg, err := config.LoadFromFile(cfgPath)
			if err != nil {
				logger.Error("Failed to reload config: %v", err)
				continue
			}

			// Apply hot-reloadable settings
			applyHotReload(n, newCfg)
			logger.Info("Configuration reloaded successfully")

		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("Received signal %v, initiating graceful shutdown...", sig)

			// Graceful drain with timeout
			drainDone := make(chan struct{})
			go func() {
				if err := n.Stop(); err != nil {
					logger.Error("Error during shutdown: %v", err)
				}
				close(drainDone)
			}()

			select {
			case <-drainDone:
				logger.Info("Graceful shutdown complete")
			case <-time.After(cfg.DrainTimeout):
				logger.Warn("Drain timeout exceeded (%v), forcing shutdown", cfg.DrainTimeout)
			}

			return
		}
	}
}

// writePIDFile writes the current process ID to a file.
func writePIDFile(path string) error {
	pid := os.Getpid()
	return os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0644)
}

// removePIDFile removes the PID file.
func removePIDFile(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove PID file: %v", err)
	}
}

// applyHotReload applies configuration changes that can be reloaded at runtime.
func applyHotReload(n *node.Node, newCfg *config.Config) {
	// Log level can be changed at runtime
	switch strings.ToLower(newCfg.LogLevel) {
	case "debug":
		logger.SetLevel(logger.LevelDebug)
	case "info":
		logger.SetLevel(logger.LevelInfo)
	case "warn", "warning":
		logger.SetLevel(logger.LevelWarn)
	case "error":
		logger.SetLevel(logger.LevelError)
	}

	// Rate limiting could be updated if server exposes SetRateLimit
	// (Future: add server.SetRateLimit(newCfg.RateLimit, newCfg.RateBurst))

	logger.Info("  Applied: log_level=%s", newCfg.LogLevel)
}

// version info (can be set at build time)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

func init() {
	// Print version if requested
	for _, arg := range os.Args[1:] {
		if arg == "-version" || arg == "--version" {
			fmt.Printf("AegisKV %s (built %s)\n", Version, BuildTime)
			os.Exit(0)
		}
	}
}
