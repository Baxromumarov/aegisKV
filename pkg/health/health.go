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
type StatsFunc func() map[string]interface{}

// Server is an HTTP server for health checks.
type Server struct {
	mu        sync.RWMutex
	addr      string
	nodeID    string
	statsFunc StatsFunc
	server    *http.Server
	ready     bool
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
	json.NewEncoder(w).Encode(map[string]interface{}{
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
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "not_ready",
			"node_id": s.nodeID,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ready",
		"node_id": s.nodeID,
	})
}

// handleStats returns detailed node statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	stats := map[string]interface{}{
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
