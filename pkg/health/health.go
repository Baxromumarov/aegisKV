// Package health provides HTTP health check endpoints.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// StatsFunc is a function that returns node statistics.
type StatsFunc func() map[string]any

// AdminInfo contains detailed cluster state for debugging.
type AdminInfo struct {
	Members []MemberInfo `json:"members"`
	Shards  []ShardInfo  `json:"shards"`
	State   NodeState    `json:"state"`
}

// MemberInfo contains information about a cluster member.
type MemberInfo struct {
	ID         string `json:"id"`
	Addr       string `json:"addr"`
	ClientAddr string `json:"client_addr"`
	State      string `json:"state"`
	LastSeen   string `json:"last_seen"`
}

// ShardInfo contains information about a shard.
type ShardInfo struct {
	ID        uint64   `json:"id"`
	State     string   `json:"state"`
	Primary   string   `json:"primary"`
	Followers []string `json:"followers"`
	Term      uint64   `json:"term"`
	Seq       uint64   `json:"seq"`
	IsPrimary bool     `json:"is_primary"`
}

// NodeState contains the current node state.
type NodeState struct {
	NodeID         string `json:"node_id"`
	Ready          bool   `json:"ready"`
	Draining       bool   `json:"draining"`
	PrimaryShards  int    `json:"primary_shards"`
	FollowerShards int    `json:"follower_shards"`
	TotalMembers   int    `json:"total_members"`
	AliveMembers   int    `json:"alive_members"`
}

// AdminFunc is a function that returns admin info for debugging.
type AdminFunc func() *AdminInfo

// Server is an HTTP server for health checks.
type Server struct {
	mu        sync.RWMutex
	addr      string
	nodeID    string
	statsFunc StatsFunc
	adminFunc AdminFunc
	server    *http.Server
	ready     bool
	draining  bool
}

// New creates a new health server.
func New(addr, nodeID string, statsFunc StatsFunc) *Server {
	return &Server{
		addr:      addr,
		nodeID:    nodeID,
		statsFunc: statsFunc,
		ready:     false,
	}
}

// Start starts the health server.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/live", s.handleLive)

	// Admin endpoints for debugging and testing
	mux.HandleFunc("/admin/members", s.handleAdminMembers)
	mux.HandleFunc("/admin/shards", s.handleAdminShards)
	mux.HandleFunc("/admin/state", s.handleAdminState)
	mux.HandleFunc("/admin/all", s.handleAdminAll)

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash the node
		}
	}()

	return nil
}

// Stop stops the health server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// SetReady sets the ready state.
func (s *Server) SetReady(ready bool) {
	s.mu.Lock()
	s.ready = ready
	s.mu.Unlock()
}

// handleHealth returns basic health status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "healthy",
		"node_id": s.nodeID,
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReady returns readiness status.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"status":  "not_ready",
			"node_id": s.nodeID,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ready",
		"node_id": s.nodeID,
	})
}

// handleStats returns detailed node statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	stats := map[string]any{
		"node_id": s.nodeID,
		"time":    time.Now().UTC().Format(time.RFC3339),
	}

	if s.statsFunc != nil {
		for k, v := range s.statsFunc() {
			stats[k] = v
		}
	}

	json.NewEncoder(w).Encode(stats)
}

// handleLive is a simple liveness probe.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// SetAdminFunc sets the admin info function.
func (s *Server) SetAdminFunc(fn AdminFunc) {
	s.mu.Lock()
	s.adminFunc = fn
	s.mu.Unlock()
}

// SetDraining sets the draining state.
func (s *Server) SetDraining(draining bool) {
	s.mu.Lock()
	s.draining = draining
	s.mu.Unlock()
}

// handleAdminMembers returns cluster member information.
func (s *Server) handleAdminMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.mu.RLock()
	adminFunc := s.adminFunc
	s.mu.RUnlock()

	if adminFunc == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "admin info not available",
		})
		return
	}

	info := adminFunc()
	if info == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "failed to get admin info",
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"members": info.Members,
		"count":   len(info.Members),
	})
}

// handleAdminShards returns shard information.
func (s *Server) handleAdminShards(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.mu.RLock()
	adminFunc := s.adminFunc
	s.mu.RUnlock()

	if adminFunc == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "admin info not available",
		})
		return
	}

	info := adminFunc()
	if info == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "failed to get admin info",
		})
		return
	}

	// Calculate summary
	primaryCount := 0
	followerCount := 0
	for _, shard := range info.Shards {
		if shard.IsPrimary {
			primaryCount++
		} else {
			followerCount++
		}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"shards":         info.Shards,
		"total":          len(info.Shards),
		"primary_count":  primaryCount,
		"follower_count": followerCount,
	})
}

// handleAdminState returns current node state.
func (s *Server) handleAdminState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.mu.RLock()
	adminFunc := s.adminFunc
	ready := s.ready
	draining := s.draining
	s.mu.RUnlock()

	if adminFunc == nil {
		// Return basic state if admin func not available
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":  s.nodeID,
			"ready":    ready,
			"draining": draining,
		})
		return
	}

	info := adminFunc()
	if info == nil {
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":  s.nodeID,
			"ready":    ready,
			"draining": draining,
		})
		return
	}

	// Use info.State but override with current health server values
	state := info.State
	state.Ready = ready
	state.Draining = draining

	json.NewEncoder(w).Encode(state)
}

// handleAdminAll returns all admin information in one call.
func (s *Server) handleAdminAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.mu.RLock()
	adminFunc := s.adminFunc
	ready := s.ready
	draining := s.draining
	s.mu.RUnlock()

	if adminFunc == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "admin info not available",
		})
		return
	}

	info := adminFunc()
	if info == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "failed to get admin info",
		})
		return
	}

	// Override with current health server values
	info.State.Ready = ready
	info.State.Draining = draining

	json.NewEncoder(w).Encode(info)
}
