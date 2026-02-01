// Package tracing provides distributed tracing integration using OpenTelemetry.
// It enables request tracing across cache operations and cluster communication.
package tracing

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Tracer provides distributed tracing capabilities.
type Tracer struct {
	mu          sync.RWMutex
	enabled     bool
	serviceName string
	spans       map[string]*Span
	completed   []*Span
	maxSpans    int
	hooks       []Hook
}

// Config holds tracer configuration.
type Config struct {
	ServiceName   string
	Enabled       bool
	SampleRate    float64 // 0.0 to 1.0
	MaxSpans      int     // Maximum completed spans to keep
	ExporterType  string  // "console", "otlp", "jaeger", "zipkin"
	ExporterURL   string  // URL for OTLP/Jaeger/Zipkin exporter
	BatchSize     int     // Number of spans to batch before export
	BatchTimeout  time.Duration
	ResourceAttrs map[string]string
}

// DefaultConfig returns default tracer configuration.
func DefaultConfig() Config {
	return Config{
		ServiceName:  "aegiskv",
		Enabled:      true,
		SampleRate:   1.0,
		MaxSpans:     10000,
		ExporterType: "console",
		BatchSize:    512,
		BatchTimeout: 5 * time.Second,
		ResourceAttrs: map[string]string{
			"service.name": "aegiskv",
		},
	}
}

// Span represents a trace span.
type Span struct {
	TraceID    string
	SpanID     string
	ParentID   string
	Name       string
	StartTime  time.Time
	EndTime    time.Time
	Status     SpanStatus
	Attributes map[string]any
	Events     []SpanEvent
	Links      []SpanLink
	mu         sync.RWMutex
	tracer     *Tracer
	finished   bool
}

// SpanStatus represents the status of a span.
type SpanStatus int

const (
	StatusUnset SpanStatus = iota
	StatusOK
	StatusError
)

// String returns string representation of status.
func (s SpanStatus) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusError:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// SpanEvent represents an event within a span.
type SpanEvent struct {
	Name       string
	Timestamp  time.Time
	Attributes map[string]any
}

// SpanLink represents a link to another span.
type SpanLink struct {
	TraceID    string
	SpanID     string
	Attributes map[string]any
}

// Hook is called when spans start or end.
type Hook interface {
	OnStart(span *Span)
	OnEnd(span *Span)
}

// New creates a new tracer with the given configuration.
func New(cfg Config) *Tracer {
	if cfg.MaxSpans == 0 {
		cfg.MaxSpans = 10000
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "aegiskv"
	}

	return &Tracer{
		enabled:     cfg.Enabled,
		serviceName: cfg.ServiceName,
		spans:       make(map[string]*Span),
		completed:   make([]*Span, 0, cfg.MaxSpans),
		maxSpans:    cfg.MaxSpans,
	}
}

// Enabled returns whether tracing is enabled.
func (t *Tracer) Enabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.enabled
}

// SetEnabled enables or disables tracing.
func (t *Tracer) SetEnabled(enabled bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enabled = enabled
}

// AddHook adds a hook to be called on span events.
func (t *Tracer) AddHook(h Hook) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hooks = append(t.hooks, h)
}

// StartSpan creates a new span.
func (t *Tracer) StartSpan(
	ctx context.Context,
	name string,
	opts ...SpanOption,
) (context.Context, *Span) {
	t.mu.RLock()
	enabled := t.enabled
	t.mu.RUnlock()

	if !enabled {
		// Return a no-op span
		return ctx, &Span{Name: name, finished: true}
	}

	span := &Span{
		TraceID:    generateID(),
		SpanID:     generateID(),
		Name:       name,
		StartTime:  time.Now(),
		Status:     StatusUnset,
		Attributes: make(map[string]any),
		Events:     make([]SpanEvent, 0),
		Links:      make([]SpanLink, 0),
		tracer:     t,
	}

	// Check for parent span in context
	if parent := SpanFromContext(ctx); parent != nil {
		span.TraceID = parent.TraceID
		span.ParentID = parent.SpanID
	}

	// Apply options
	for _, opt := range opts {
		opt(span)
	}

	// Store active span
	t.mu.Lock()
	t.spans[span.SpanID] = span
	hooks := t.hooks
	t.mu.Unlock()

	// Call hooks
	for _, h := range hooks {
		h.OnStart(span)
	}

	return ContextWithSpan(ctx, span), span
}

// End finishes the span.
func (s *Span) End() {
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.finished = true
	s.EndTime = time.Now()
	s.mu.Unlock()

	if s.tracer == nil {
		return
	}

	s.tracer.mu.Lock()
	delete(s.tracer.spans, s.SpanID)

	// Keep completed spans up to max
	if len(s.tracer.completed) < s.tracer.maxSpans {
		s.tracer.completed = append(s.tracer.completed, s)
	}
	hooks := s.tracer.hooks
	s.tracer.mu.Unlock()

	// Call hooks
	for _, h := range hooks {
		h.OnEnd(s)
	}
}

// SetStatus sets the span status.
func (s *Span) SetStatus(status SpanStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
}

// SetAttribute sets a single attribute.
func (s *Span) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = make(map[string]any)
	}
	s.Attributes[key] = value
}

// SetAttributes sets multiple attributes.
func (s *Span) SetAttributes(attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = make(map[string]any)
	}
	for k, v := range attrs {
		s.Attributes[k] = v
	}
}

// AddEvent adds an event to the span.
func (s *Span) AddEvent(name string, attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// RecordError records an error on the span.
func (s *Span) RecordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Status = StatusError
	s.Events = append(s.Events, SpanEvent{
		Name:      "exception",
		Timestamp: time.Now(),
		Attributes: map[string]any{
			"exception.type":    fmt.Sprintf("%T", err),
			"exception.message": err.Error(),
		},
	})
}

// AddLink adds a link to another span.
func (s *Span) AddLink(traceID, spanID string, attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Links = append(s.Links, SpanLink{
		TraceID:    traceID,
		SpanID:     spanID,
		Attributes: attrs,
	})
}

// Duration returns the span duration.
func (s *Span) Duration() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

// SpanOption is a function that configures a span.
type SpanOption func(*Span)

// WithAttributes sets initial attributes on a span.
func WithAttributes(attrs map[string]any) SpanOption {
	return func(s *Span) {
		s.Attributes = attrs
	}
}

// WithLinks sets initial links on a span.
func WithLinks(links []SpanLink) SpanOption {
	return func(s *Span) {
		s.Links = links
	}
}

// Context key for spans
type spanContextKey struct{}

// ContextWithSpan returns a context with the span attached.
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, span)
}

// SpanFromContext extracts a span from context.
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(spanContextKey{}).(*Span); ok {
		return span
	}
	return nil
}

// Stats returns tracer statistics.
func (t *Tracer) Stats() TracerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return TracerStats{
		ActiveSpans:    len(t.spans),
		CompletedSpans: len(t.completed),
		Enabled:        t.enabled,
	}
}

// TracerStats contains tracer statistics.
type TracerStats struct {
	ActiveSpans    int
	CompletedSpans int
	Enabled        bool
}

// CompletedSpans returns a copy of completed spans.
func (t *Tracer) CompletedSpans() []*Span {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*Span, len(t.completed))
	copy(result, t.completed)
	return result
}

// Clear removes all completed spans.
func (t *Tracer) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = t.completed[:0]
}

// Simple ID generator (in production would use real trace IDs)
var idCounter uint64
var idMu sync.Mutex

func generateID() string {
	idMu.Lock()
	idCounter++
	id := idCounter
	idMu.Unlock()
	return fmt.Sprintf("%016x", id)
}

// Predefined span names for cache operations
const (
	SpanGet        = "cache.get"
	SpanSet        = "cache.set"
	SpanDelete     = "cache.delete"
	SpanReplicate  = "replication.sync"
	SpanGossip     = "gossip.broadcast"
	SpanQuorum     = "quorum.operation"
	SpanWALWrite   = "wal.write"
	SpanWALRecover = "wal.recover"
)

// Predefined attribute keys
const (
	AttrCacheKey     = "cache.key"
	AttrCacheHit     = "cache.hit"
	AttrCacheTTL     = "cache.ttl"
	AttrCacheSize    = "cache.size"
	AttrNodeID       = "node.id"
	AttrClusterSize  = "cluster.size"
	AttrReplicaCount = "replication.count"
	AttrQuorumSize   = "quorum.size"
	AttrWALSequence  = "wal.sequence"
	AttrErrorMessage = "error.message"
)

// TraceContext holds trace propagation context.
type TraceContext struct {
	TraceID  string
	SpanID   string
	ParentID string
	Sampled  bool
}

// Inject injects trace context into a carrier map.
func (tc TraceContext) Inject(carrier map[string]string) {
	carrier["traceparent"] = fmt.Sprintf(
		"00-%s-%s-%s",
		tc.TraceID,
		tc.SpanID,
		boolToSampled(tc.Sampled),
	)
}

// Extract extracts trace context from a carrier map.
func Extract(carrier map[string]string) (TraceContext, bool) {
	tp := carrier["traceparent"]
	if tp == "" {
		return TraceContext{}, false
	}

	var version, traceID, spanID, flags string
	_, err := fmt.Sscanf(
		tp,
		"%s-%s-%s-%s",
		&version,
		&traceID,
		&spanID,
		&flags,
	)
	if err != nil {
		return TraceContext{}, false
	}

	return TraceContext{
		TraceID: traceID,
		SpanID:  spanID,
		Sampled: flags == "01",
	}, true
}

func boolToSampled(b bool) string {
	if b {
		return "01"
	}
	return "00"
}

// NoopHook is a hook that does nothing.
type NoopHook struct{}

func (NoopHook) OnStart(_ *Span) {}
func (NoopHook) OnEnd(_ *Span)   {}

// LoggingHook logs span start and end.
type LoggingHook struct {
	Logger func(format string, args ...any)
}

func (h LoggingHook) OnStart(span *Span) {
	if h.Logger != nil {
		h.Logger(
			"span started: %s (trace=%s, span=%s)",
			span.Name,
			span.TraceID,
			span.SpanID,
		)
	}
}

func (h LoggingHook) OnEnd(span *Span) {
	if h.Logger != nil {
		h.Logger(
			"span ended: %s (trace=%s, span=%s, duration=%s, status=%s)",
			span.Name,
			span.TraceID,
			span.SpanID,
			span.Duration(),
			span.Status,
		)
	}
}
