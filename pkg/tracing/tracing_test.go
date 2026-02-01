package tracing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ServiceName != "aegiskv" {
		t.Errorf("expected service name 'aegiskv', got '%s'", cfg.ServiceName)
	}
	if !cfg.Enabled {
		t.Error("expected enabled to be true")
	}
	if cfg.SampleRate != 1.0 {
		t.Errorf("expected sample rate 1.0, got %f", cfg.SampleRate)
	}
	if cfg.MaxSpans != 10000 {
		t.Errorf("expected max spans 10000, got %d", cfg.MaxSpans)
	}
}

func TestNew(t *testing.T) {
	tracer := New(DefaultConfig())
	if tracer == nil {
		t.Fatal("New returned nil")
	}

	if !tracer.Enabled() {
		t.Error("tracer should be enabled")
	}
}

func TestNewWithEmptyConfig(t *testing.T) {
	tracer := New(Config{})

	if tracer.serviceName != "aegiskv" {
		t.Error("should default to 'aegiskv'")
	}
	if tracer.maxSpans != 10000 {
		t.Error("should default to 10000 max spans")
	}
}

func TestSetEnabled(t *testing.T) {
	tracer := New(DefaultConfig())

	tracer.SetEnabled(false)
	if tracer.Enabled() {
		t.Error("tracer should be disabled")
	}

	tracer.SetEnabled(true)
	if !tracer.Enabled() {
		t.Error("tracer should be enabled")
	}
}

func TestStartSpan(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	ctx, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	if span.Name != "test-operation" {
		t.Errorf("expected name 'test-operation', got '%s'", span.Name)
	}
	if span.TraceID == "" {
		t.Error("span should have TraceID")
	}
	if span.SpanID == "" {
		t.Error("span should have SpanID")
	}
	if span.StartTime.IsZero() {
		t.Error("span should have StartTime")
	}
}

func TestStartSpanDisabled(t *testing.T) {
	tracer := New(Config{Enabled: false})
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	span.End()

	if span.Name != "test-operation" {
		t.Error("no-op span should have name")
	}
	if !span.finished {
		t.Error("no-op span should be marked finished")
	}
}

func TestSpanEnd(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	time.Sleep(10 * time.Millisecond)
	span.End()

	if span.EndTime.IsZero() {
		t.Error("span should have EndTime after End()")
	}
	if span.Duration() < 10*time.Millisecond {
		t.Errorf("span duration too short: %v", span.Duration())
	}
}

func TestSpanDoubleEnd(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	span.End()
	firstEndTime := span.EndTime

	time.Sleep(5 * time.Millisecond)
	span.End()

	if span.EndTime != firstEndTime {
		t.Error("double End() should not change EndTime")
	}
}

func TestSpanSetStatus(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	if span.Status != StatusUnset {
		t.Error("initial status should be Unset")
	}

	span.SetStatus(StatusOK)
	if span.Status != StatusOK {
		t.Error("status should be OK")
	}

	span.SetStatus(StatusError)
	if span.Status != StatusError {
		t.Error("status should be Error")
	}
}

func TestSpanStatusString(t *testing.T) {
	if StatusUnset.String() != "UNSET" {
		t.Errorf("expected 'UNSET', got '%s'", StatusUnset.String())
	}
	if StatusOK.String() != "OK" {
		t.Errorf("expected 'OK', got '%s'", StatusOK.String())
	}
	if StatusError.String() != "ERROR" {
		t.Errorf("expected 'ERROR', got '%s'", StatusError.String())
	}
}

func TestSpanSetAttribute(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	span.SetAttribute("key", "value")
	span.SetAttribute("count", 42)

	if span.Attributes["key"] != "value" {
		t.Error("attribute 'key' not set correctly")
	}
	if span.Attributes["count"] != 42 {
		t.Error("attribute 'count' not set correctly")
	}
}

func TestSpanSetAttributes(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	span.SetAttributes(map[string]any{
		"key1": "value1",
		"key2": "value2",
	})

	if span.Attributes["key1"] != "value1" {
		t.Error("attribute 'key1' not set")
	}
	if span.Attributes["key2"] != "value2" {
		t.Error("attribute 'key2' not set")
	}
}

func TestSpanAddEvent(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	span.AddEvent("cache-miss", map[string]any{"key": "test-key"})

	if len(span.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(span.Events))
	}
	if span.Events[0].Name != "cache-miss" {
		t.Error("event name incorrect")
	}
	if span.Events[0].Attributes["key"] != "test-key" {
		t.Error("event attribute incorrect")
	}
}

func TestSpanRecordError(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	err := errors.New("test error")
	span.RecordError(err)

	if span.Status != StatusError {
		t.Error("status should be Error after RecordError")
	}
	if len(span.Events) != 1 {
		t.Fatal("expected 1 event for error")
	}
	if span.Events[0].Name != "exception" {
		t.Error("error event should be named 'exception'")
	}
	if span.Events[0].Attributes["exception.message"] != "test error" {
		t.Error("error message not recorded")
	}
}

func TestSpanRecordNilError(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	span.RecordError(nil)

	if len(span.Events) != 0 {
		t.Error("nil error should not add event")
	}
	if span.Status != StatusUnset {
		t.Error("nil error should not change status")
	}
}

func TestSpanAddLink(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	span.AddLink("trace123", "span456", map[string]any{"reason": "related"})

	if len(span.Links) != 1 {
		t.Fatal("expected 1 link")
	}
	if span.Links[0].TraceID != "trace123" {
		t.Error("link TraceID incorrect")
	}
	if span.Links[0].SpanID != "span456" {
		t.Error("link SpanID incorrect")
	}
}

func TestChildSpan(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	ctx, parentSpan := tracer.StartSpan(ctx, "parent-operation")
	defer parentSpan.End()

	_, childSpan := tracer.StartSpan(ctx, "child-operation")
	defer childSpan.End()

	if childSpan.TraceID != parentSpan.TraceID {
		t.Error("child should inherit parent TraceID")
	}
	if childSpan.ParentID != parentSpan.SpanID {
		t.Error("child ParentID should be parent SpanID")
	}
}

func TestSpanFromContext(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	if SpanFromContext(ctx) != nil {
		t.Error("should return nil when no span in context")
	}

	ctx, span := tracer.StartSpan(ctx, "test-operation")
	defer span.End()

	retrieved := SpanFromContext(ctx)
	if retrieved != span {
		t.Error("SpanFromContext should return the span")
	}
}

func TestTracerStats(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	stats := tracer.Stats()
	if stats.ActiveSpans != 0 {
		t.Errorf("expected 0 active spans, got %d", stats.ActiveSpans)
	}

	_, span1 := tracer.StartSpan(ctx, "span1")
	_, span2 := tracer.StartSpan(ctx, "span2")

	stats = tracer.Stats()
	if stats.ActiveSpans != 2 {
		t.Errorf("expected 2 active spans, got %d", stats.ActiveSpans)
	}

	span1.End()
	span2.End()

	stats = tracer.Stats()
	if stats.ActiveSpans != 0 {
		t.Errorf("expected 0 active spans after end, got %d", stats.ActiveSpans)
	}
	if stats.CompletedSpans != 2 {
		t.Errorf("expected 2 completed spans, got %d", stats.CompletedSpans)
	}
}

func TestCompletedSpans(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	span.End()

	completed := tracer.CompletedSpans()
	if len(completed) != 1 {
		t.Fatalf("expected 1 completed span, got %d", len(completed))
	}
	if completed[0].Name != "test-operation" {
		t.Error("completed span has wrong name")
	}
}

func TestClear(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	span.End()

	tracer.Clear()

	completed := tracer.CompletedSpans()
	if len(completed) != 0 {
		t.Error("Clear should remove all completed spans")
	}
}

func TestWithAttributes(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	attrs := map[string]any{"initial": "value"}
	_, span := tracer.StartSpan(ctx, "test-operation", WithAttributes(attrs))
	defer span.End()

	if span.Attributes["initial"] != "value" {
		t.Error("initial attribute not set")
	}
}

func TestWithLinks(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	links := []SpanLink{{TraceID: "t1", SpanID: "s1"}}
	_, span := tracer.StartSpan(ctx, "test-operation", WithLinks(links))
	defer span.End()

	if len(span.Links) != 1 {
		t.Fatal("links not set")
	}
	if span.Links[0].TraceID != "t1" {
		t.Error("link TraceID incorrect")
	}
}

type testHook struct {
	onStart func(*Span)
	onEnd   func(*Span)
}

func (h *testHook) OnStart(s *Span) {
	if h.onStart != nil {
		h.onStart(s)
	}
}

func (h *testHook) OnEnd(s *Span) {
	if h.onEnd != nil {
		h.onEnd(s)
	}
}

func TestAddHook(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	started := false
	ended := false

	hook := &testHook{
		onStart: func(s *Span) { started = true },
		onEnd:   func(s *Span) { ended = true },
	}
	tracer.AddHook(hook)

	_, span := tracer.StartSpan(ctx, "test-operation")
	if !started {
		t.Error("OnStart hook not called")
	}

	span.End()
	if !ended {
		t.Error("OnEnd hook not called")
	}
}

func TestTraceContextInject(t *testing.T) {
	tc := TraceContext{
		TraceID: "abc123",
		SpanID:  "def456",
		Sampled: true,
	}

	carrier := make(map[string]string)
	tc.Inject(carrier)

	if carrier["traceparent"] != "00-abc123-def456-01" {
		t.Errorf("unexpected traceparent: %s", carrier["traceparent"])
	}
}

func TestTraceContextInjectNotSampled(t *testing.T) {
	tc := TraceContext{
		TraceID: "abc123",
		SpanID:  "def456",
		Sampled: false,
	}

	carrier := make(map[string]string)
	tc.Inject(carrier)

	if carrier["traceparent"] != "00-abc123-def456-00" {
		t.Errorf("unexpected traceparent: %s", carrier["traceparent"])
	}
}

func TestNoopHook(t *testing.T) {
	hook := NoopHook{}
	hook.OnStart(nil)
	hook.OnEnd(nil)
}

func TestLoggingHook(t *testing.T) {
	logged := false

	hook := LoggingHook{
		Logger: func(format string, args ...any) {
			logged = true
		},
	}

	span := &Span{
		Name:    "test-span",
		TraceID: "trace1",
		SpanID:  "span1",
		Status:  StatusOK,
	}

	hook.OnStart(span)
	hook.OnEnd(span)

	if !logged {
		t.Error("logger should be called")
	}
}

func TestLoggingHookNilLogger(t *testing.T) {
	hook := LoggingHook{Logger: nil}
	hook.OnStart(&Span{})
	hook.OnEnd(&Span{})
}

func TestConcurrentSpans(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			_, span := tracer.StartSpan(ctx, "concurrent-operation")
			span.SetAttribute("id", id)
			span.AddEvent("event", nil)
			time.Sleep(time.Millisecond)
			span.End()
		}(i)
	}

	wg.Wait()

	stats := tracer.Stats()
	if stats.ActiveSpans != 0 {
		t.Errorf("expected 0 active spans, got %d", stats.ActiveSpans)
	}
	if stats.CompletedSpans != numGoroutines {
		t.Errorf("expected %d completed spans, got %d", numGoroutines, stats.CompletedSpans)
	}
}

func TestSpanDurationBeforeEnd(t *testing.T) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	_, span := tracer.StartSpan(ctx, "test-operation")
	time.Sleep(10 * time.Millisecond)

	dur := span.Duration()
	if dur < 10*time.Millisecond {
		t.Error("duration should be at least 10ms")
	}

	span.End()
}

func TestPredefinedConstants(t *testing.T) {
	if SpanGet == "" {
		t.Error("SpanGet should be defined")
	}
	if SpanSet == "" {
		t.Error("SpanSet should be defined")
	}
	if AttrCacheKey == "" {
		t.Error("AttrCacheKey should be defined")
	}
}

func BenchmarkStartSpan(b *testing.B) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tracer.StartSpan(ctx, "benchmark-operation")
		span.End()
	}
}

func BenchmarkStartSpanDisabled(b *testing.B) {
	tracer := New(Config{Enabled: false})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tracer.StartSpan(ctx, "benchmark-operation")
		span.End()
	}
}

func BenchmarkStartSpanParallel(b *testing.B) {
	tracer := New(DefaultConfig())
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, span := tracer.StartSpan(ctx, "benchmark-operation")
			span.End()
		}
	})
}

func BenchmarkSetAttributes(b *testing.B) {
	tracer := New(DefaultConfig())
	ctx := context.Background()
	_, span := tracer.StartSpan(ctx, "benchmark")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		span.SetAttribute("key", "value")
	}
	span.End()
}
