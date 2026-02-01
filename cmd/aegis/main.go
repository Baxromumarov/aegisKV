package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
		seeds           = flag.String("seeds", "", "Comma-separated list of seed nodes")
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
	if *seeds != "" {
		cfg.Seeds = strings.Split(*seeds, ",")
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.Info("Received signal %v, shutting down...", sig)

	if err := n.Stop(); err != nil {
		logger.Error("Error during shutdown: %v", err)
	}

	logger.Info("Shutdown complete")
}
