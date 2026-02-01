package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/baxromumarov/aegisKV/pkg/config"
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
	)

	flag.Parse()

	var cfg *config.Config
	var err error

	if *configFile != "" {
		cfg, err = config.LoadFromFile(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

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

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	n, err := node.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	if err := n.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	fmt.Printf("AegisKV node %s started\n", n.NodeID())
	fmt.Printf("  Bind address: %s\n", cfg.BindAddr)
	fmt.Printf("  Gossip address: %s\n", cfg.GossipBindAddr)
	if cfg.GossipAdvertiseAddr != "" {
		fmt.Printf("  Gossip advertise: %s\n", cfg.GossipAdvertiseAddr)
	}
	fmt.Printf("  Data directory: %s\n", cfg.DataDir)
	fmt.Printf("  Shards: %d\n", cfg.NumShards)
	fmt.Printf("  Replication factor: %d\n", cfg.ReplicationFactor)
	fmt.Printf("  WAL mode: %s\n", cfg.WALMode)
	fmt.Printf("  Max memory: %d MB\n", cfg.MaxMemoryMB)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	fmt.Printf("\nReceived signal %v, shutting down...\n", sig)

	if err := n.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	fmt.Println("Shutdown complete")
}
