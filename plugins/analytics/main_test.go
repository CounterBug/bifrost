package analytics

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/plugins/analytics/sinks"
)

// TestSink is a mock sink for testing
type TestSink struct {
	mu         sync.Mutex
	events     []*APICallEvent
	batches    [][]sinks.Event
	sendErr    error
	batchErr   error
	closeErr   error
	closed     bool
	sendDelay  time.Duration
	batchDelay time.Duration
}

func NewTestSink() *TestSink {
	return &TestSink{
		events:  make([]*APICallEvent, 0),
		batches: make([][]sinks.Event, 0),
	}
}

func (s *TestSink) Name() string {
	return "test"
}

func (s *TestSink) Send(ctx context.Context, event sinks.Event) error {
	if s.sendDelay > 0 {
		time.Sleep(s.sendDelay)
	}
	if s.sendErr != nil {
		return s.sendErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := event.(*APICallEvent); ok {
		s.events = append(s.events, e.Clone())
	}
	return nil
}

func (s *TestSink) SendBatch(ctx context.Context, events []sinks.Event) error {
	if s.batchDelay > 0 {
		time.Sleep(s.batchDelay)
	}
	if s.batchErr != nil {
		return s.batchErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Clone events for safety
	cloned := make([]sinks.Event, len(events))
	for i, e := range events {
		if evt, ok := e.(*APICallEvent); ok {
			cloned[i] = evt.Clone()
		}
	}
	s.batches = append(s.batches, cloned)
	return nil
}

func (s *TestSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.closeErr
}

func (s *TestSink) GetEvents() []*APICallEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*APICallEvent, len(s.events))
	copy(result, s.events)
	return result
}

func (s *TestSink) GetBatches() [][]sinks.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches
}

func (s *TestSink) TotalEventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := len(s.events)
	for _, batch := range s.batches {
		count += len(batch)
	}
	return count
}

// TestLogger is a mock logger for testing
type TestLogger struct {
	mu       sync.Mutex
	messages []string
}

func NewTestLogger() *TestLogger {
	return &TestLogger{messages: make([]string, 0)}
}

func (l *TestLogger) Debug(format string, args ...any) {
	l.log("DEBUG", format, args...)
}

func (l *TestLogger) Info(format string, args ...any) {
	l.log("INFO", format, args...)
}

func (l *TestLogger) Warn(format string, args ...any) {
	l.log("WARN", format, args...)
}

func (l *TestLogger) Error(format string, args ...any) {
	l.log("ERROR", format, args...)
}

func (l *TestLogger) Fatal(format string, args ...any) {
	l.log("FATAL", format, args...)
}

func (l *TestLogger) SetLevel(level schemas.LogLevel) {
	// No-op for testing
}

func (l *TestLogger) SetOutputType(outputType schemas.LoggerOutputType) {
	// No-op for testing
}

func (l *TestLogger) log(level, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, level+": "+format)
}

func (l *TestLogger) GetMessages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]string, len(l.messages))
	copy(result, l.messages)
	return result
}

// createBifrostContextWithRequestID creates a BifrostContext with the request ID set via the parent context
// (reserved keys must be set on the parent context, not via SetValue)
func createBifrostContextWithRequestID(requestID string) *schemas.BifrostContext {
	parentCtx := context.WithValue(context.Background(), schemas.BifrostContextKeyRequestID, requestID)
	return schemas.NewBifrostContext(parentCtx, time.Now().Add(30*time.Second))
}

// Helper to create a test plugin with custom sink
func createTestPlugin(t *testing.T, sink sinks.Sink, config *Config) *AnalyticsPlugin {
	t.Helper()

	if config == nil {
		config = &Config{
			SinkType:        "stdout",
			BatchSize:       10,
			FlushIntervalMs: 100,
		}
	}

	// Always use stdout for initial creation, then replace with test sink
	config.SinkType = "stdout"

	logger := NewTestLogger()
	ctx := context.Background()

	plugin, err := Init(ctx, config, logger, nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Replace the sink directly for testing
	if sink != nil {
		plugin.SetSink(sink)
	}

	return plugin
}

func TestInit_NilConfig(t *testing.T) {
	ctx := context.Background()
	_, err := Init(ctx, nil, NewTestLogger(), nil)
	if err == nil {
		t.Error("Init(nil config) should return error")
	}
}

func TestInit_DefaultSinkType(t *testing.T) {
	ctx := context.Background()
	config := &Config{}
	logger := NewTestLogger()

	plugin, err := Init(ctx, config, logger, nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	// Default should be stdout
	if plugin.config.SinkType != "stdout" {
		t.Errorf("default SinkType = %s, want stdout", plugin.config.SinkType)
	}
}

func TestInit_DefaultBatchSettings(t *testing.T) {
	ctx := context.Background()
	config := &Config{SinkType: "stdout"}
	logger := NewTestLogger()

	plugin, err := Init(ctx, config, logger, nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	if plugin.config.BatchSize != 100 {
		t.Errorf("default BatchSize = %d, want 100", plugin.config.BatchSize)
	}
	if plugin.config.FlushIntervalMs != 1000 {
		t.Errorf("default FlushIntervalMs = %d, want 1000", plugin.config.FlushIntervalMs)
	}
}

func TestInit_InvalidSinkType(t *testing.T) {
	ctx := context.Background()
	config := &Config{SinkType: "nonexistent-sink-xyz"}
	logger := NewTestLogger()

	_, err := Init(ctx, config, logger, nil)
	if err == nil {
		t.Error("Init() with invalid sink type should return error")
	}
}

func TestInit_CustomLabels(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		SinkType:     "stdout",
		CustomLabels: []string{"tenant_id", "environment", "region"},
	}
	logger := NewTestLogger()

	plugin, err := Init(ctx, config, logger, nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	if len(plugin.customLabelKeys) != 3 {
		t.Errorf("customLabelKeys count = %d, want 3", len(plugin.customLabelKeys))
	}
}

func TestAnalyticsPlugin_GetName(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	if plugin.GetName() != "analytics" {
		t.Errorf("GetName() = %s, want analytics", plugin.GetName())
	}
}

func TestAnalyticsPlugin_TransportInterceptor(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	headers := map[string]string{"X-Test": "value"}
	body := map[string]any{"key": "value"}

	retHeaders, retBody, err := plugin.TransportInterceptor(ctx, "http://test.com", headers, body)
	if err != nil {
		t.Fatalf("TransportInterceptor() error = %v", err)
	}

	// Should return unchanged headers and body
	if retHeaders["X-Test"] != "value" {
		t.Error("TransportInterceptor should not modify headers")
	}
	if retBody["key"] != "value" {
		t.Error("TransportInterceptor should not modify body")
	}
}

func TestAnalyticsPlugin_PreHook_NilContext(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	req := &schemas.BifrostRequest{}
	retReq, shortCircuit, err := plugin.PreHook(nil, req)
	if err != nil {
		t.Fatalf("PreHook(nil context) error = %v", err)
	}
	if shortCircuit != nil {
		t.Error("PreHook should not short-circuit")
	}
	if retReq != req {
		t.Error("PreHook should return the same request")
	}
}

func TestAnalyticsPlugin_PreHook_SetsStartTime(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	req := &schemas.BifrostRequest{}

	before := time.Now()
	_, _, _ = plugin.PreHook(ctx, req)
	after := time.Now()

	// Verify start time was set in context
	startTime, ok := ctx.Value(startTimeKey).(time.Time)
	if !ok {
		t.Fatal("PreHook should set start time in context")
	}

	if startTime.Before(before) || startTime.After(after) {
		t.Errorf("start time %v should be between %v and %v", startTime, before, after)
	}
}

func TestAnalyticsPlugin_PostHook_NilContext(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	result := &schemas.BifrostResponse{}
	retResult, retErr, err := plugin.PostHook(nil, result, nil)
	if err != nil {
		t.Fatalf("PostHook(nil context) error = %v", err)
	}
	if retResult != result {
		t.Error("PostHook should return the same result")
	}
	if retErr != nil {
		t.Error("PostHook should return nil error")
	}
}

func TestAnalyticsPlugin_PostHook_NoRequestID(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, nil)
	defer plugin.Cleanup()

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	// No request ID set
	result := &schemas.BifrostResponse{}

	_, _, err := plugin.PostHook(ctx, result, nil)
	if err != nil {
		t.Fatalf("PostHook() error = %v", err)
	}

	// Wait a bit for async processing
	time.Sleep(50 * time.Millisecond)

	// Should not produce events without request ID
	if testSink.TotalEventCount() != 0 {
		t.Errorf("events count = %d, want 0 (no request ID)", testSink.TotalEventCount())
	}
}

func TestAnalyticsPlugin_PostHook_Success(t *testing.T) {
	testSink := NewTestSink()
	testLogger := NewTestLogger()

	// Create plugin with our test logger
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       1, // Small batch for immediate sending
		FlushIntervalMs: 10,
	}

	plugin, err := Init(context.Background(), config, testLogger, nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	plugin.SetSink(testSink)
	defer plugin.Cleanup()

	// Reserved keys must be set on the parent context before creating BifrostContext
	parentCtx := context.WithValue(context.Background(), schemas.BifrostContextKeyRequestID, "req-123")
	ctx := schemas.NewBifrostContext(parentCtx, time.Now().Add(30*time.Second))
	// Non-reserved keys can use SetValue
	ctx.SetValue(startTimeKey, time.Now().Add(-100*time.Millisecond))

	// Create a mock response with ExtraFields
	result := &schemas.BifrostResponse{
		ChatResponse: &schemas.BifrostChatResponse{
			ID:    "resp-123",
			Model: "gpt-4",
			Usage: &schemas.BifrostLLMUsage{
				PromptTokens:     100,
				CompletionTokens: 50,
				TotalTokens:      150,
			},
			ExtraFields: schemas.BifrostResponseExtraFields{
				RequestType:    schemas.ChatCompletionRequest,
				Provider:       schemas.OpenAI,
				ModelRequested: "gpt-4",
				Latency:        100,
			},
		},
	}

	_, _, err = plugin.PostHook(ctx, result, nil)
	if err != nil {
		t.Fatalf("PostHook() error = %v", err)
	}

	// Wait for async processing and batching
	time.Sleep(200 * time.Millisecond)

	// Check logger for errors/panics
	for _, msg := range testLogger.GetMessages() {
		t.Logf("Logger: %s", msg)
	}

	// Check stats to see if events were produced
	stats := plugin.Stats()
	t.Logf("Plugin stats: produced=%d, sent=%d, dropped=%d, errors=%d, pending=%d",
		stats["events_produced"], stats["events_sent"], stats["events_dropped"],
		stats["send_errors"], stats["pending_events"])

	if stats["events_produced"] < 1 {
		t.Errorf("expected events_produced >= 1, got %d", stats["events_produced"])
	}

	if testSink.TotalEventCount() < 1 {
		t.Errorf("expected at least 1 event, got %d", testSink.TotalEventCount())
	}
}

func TestAnalyticsPlugin_PostHook_Error(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:        "stdout",
		BatchSize:       1,
		FlushIntervalMs: 10,
	})
	defer plugin.Cleanup()

	ctx := createBifrostContextWithRequestID("req-error")
	ctx.SetValue(startTimeKey, time.Now().Add(-50*time.Millisecond))

	errType := "rate_limit_error"
	errCode := 429
	bifrostErr := &schemas.BifrostError{
		Type:       &errType,
		StatusCode: &errCode,
		Error: &schemas.ErrorField{
			Message: "Rate limit exceeded",
		},
		ExtraFields: schemas.BifrostErrorExtraFields{
			RequestType:    schemas.ChatCompletionRequest,
			Provider:       schemas.OpenAI,
			ModelRequested: "gpt-4",
		},
	}

	_, _, err := plugin.PostHook(ctx, nil, bifrostErr)
	if err != nil {
		t.Fatalf("PostHook() error = %v", err)
	}

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	if testSink.TotalEventCount() < 1 {
		t.Errorf("expected at least 1 event for error, got %d", testSink.TotalEventCount())
	}
}

func TestAnalyticsPlugin_Batching(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:        "stdout",
		BatchSize:       5,
		FlushIntervalMs: 1000, // Long interval to test batch size trigger
	})
	defer plugin.Cleanup()

	// Send 5 events to trigger batch
	for i := 0; i < 5; i++ {
		ctx := createBifrostContextWithRequestID("req-batch-" + string(rune('0'+i)))
		ctx.SetValue(startTimeKey, time.Now())

		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(ctx, result, nil)
	}

	// Wait for batch processing
	time.Sleep(200 * time.Millisecond)

	// Should have batched
	batches := testSink.GetBatches()
	if len(batches) < 1 {
		t.Errorf("expected at least 1 batch, got %d", len(batches))
	}
}

func TestAnalyticsPlugin_FlushInterval(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:        "stdout",
		BatchSize:       100, // Large batch size
		FlushIntervalMs: 50,  // Short flush interval
	})
	defer plugin.Cleanup()

	// Send 1 event (won't trigger batch size)
	ctx := createBifrostContextWithRequestID("req-flush")
	ctx.SetValue(startTimeKey, time.Now())

	result := &schemas.BifrostResponse{
		ChatResponse: &schemas.BifrostChatResponse{
			ExtraFields: schemas.BifrostResponseExtraFields{
				RequestType: schemas.ChatCompletionRequest,
				Provider:    schemas.OpenAI,
			},
		},
	}
	_, _, _ = plugin.PostHook(ctx, result, nil)

	// Wait for flush interval
	time.Sleep(150 * time.Millisecond)

	// Should have flushed due to interval
	if testSink.TotalEventCount() < 1 {
		t.Errorf("expected at least 1 event after flush interval, got %d", testSink.TotalEventCount())
	}
}

func TestAnalyticsPlugin_Cleanup(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:        "stdout",
		BatchSize:       100,
		FlushIntervalMs: 10000, // Long interval
	})

	// Send some events
	for i := 0; i < 3; i++ {
		ctx := createBifrostContextWithRequestID("req-cleanup-" + string(rune('0'+i)))
		ctx.SetValue(startTimeKey, time.Now())
		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(ctx, result, nil)
	}

	// Short wait for events to queue
	time.Sleep(50 * time.Millisecond)

	// Cleanup should flush remaining events
	err := plugin.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	// Verify sink was closed
	if !testSink.closed {
		t.Error("Cleanup() should close the sink")
	}

	// Should have flushed events
	if testSink.TotalEventCount() < 3 {
		t.Errorf("Cleanup should flush pending events, got %d", testSink.TotalEventCount())
	}
}

func TestAnalyticsPlugin_Stats(t *testing.T) {
	testSink := NewTestSink()
	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:        "stdout",
		BatchSize:       10,
		FlushIntervalMs: 50,
	})
	defer plugin.Cleanup()

	// Send some events
	for i := 0; i < 5; i++ {
		ctx := createBifrostContextWithRequestID("req-stats-" + string(rune('0'+i)))
		ctx.SetValue(startTimeKey, time.Now())
		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(ctx, result, nil)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	stats := plugin.Stats()
	if stats["events_produced"] < 5 {
		t.Errorf("events_produced = %d, want >= 5", stats["events_produced"])
	}
}

func TestAnalyticsPlugin_DropOnBackpressure(t *testing.T) {
	// Create a slow sink
	testSink := NewTestSink()
	testSink.batchDelay = 100 * time.Millisecond

	plugin := createTestPlugin(t, testSink, &Config{
		SinkType:           "stdout",
		BatchSize:          1,
		FlushIntervalMs:    10,
		DropOnBackpressure: true,
	})
	defer plugin.Cleanup()

	// Flood with events
	for i := 0; i < 100; i++ {
		ctx := createBifrostContextWithRequestID("req-flood")
		ctx.SetValue(startTimeKey, time.Now())
		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(ctx, result, nil)
	}

	// Some events should be dropped
	time.Sleep(50 * time.Millisecond)

	stats := plugin.Stats()
	// With backpressure, dropped count might be > 0
	// This is a probabilistic test
	t.Logf("Events produced: %d, dropped: %d", stats["events_produced"], stats["events_dropped"])
}

func TestAnalyticsPlugin_CustomLabels(t *testing.T) {
	// Capture the events with a buffer-based stdout sink
	var buf bytes.Buffer

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       1,
		FlushIntervalMs: 10,
		CustomLabels:    []string{"tenant_id", "environment"},
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	// Access the stdout sink and set a custom writer
	if stdoutSink, ok := plugin.sink.(*sinks.StdoutSink); ok {
		stdoutSink.SetWriter(&buf)
	}

	// Create context with custom labels (request ID must be set via parent context)
	reqCtx := createBifrostContextWithRequestID("req-labels")
	reqCtx.SetValue(startTimeKey, time.Now())
	reqCtx.SetValue(schemas.BifrostContextKey("tenant_id"), "tenant-123")
	reqCtx.SetValue(schemas.BifrostContextKey("environment"), "production")

	result := &schemas.BifrostResponse{
		ChatResponse: &schemas.BifrostChatResponse{
			ExtraFields: schemas.BifrostResponseExtraFields{
				RequestType: schemas.ChatCompletionRequest,
				Provider:    schemas.OpenAI,
			},
		},
	}
	_, _, _ = plugin.PostHook(reqCtx, result, nil)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	output := buf.String()
	if !strings.Contains(output, "tenant-123") {
		t.Error("output should contain tenant_id label value")
	}
	if !strings.Contains(output, "production") {
		t.Error("output should contain environment label value")
	}
}

func TestGetStringFromContext(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	ctx.SetValue(schemas.BifrostContextKey("test-key"), "test-value")

	val := getStringFromContext(ctx, schemas.BifrostContextKey("test-key"))
	if val != "test-value" {
		t.Errorf("getStringFromContext() = %s, want test-value", val)
	}

	// Test non-existent key
	val = getStringFromContext(ctx, schemas.BifrostContextKey("nonexistent"))
	if val != "" {
		t.Errorf("getStringFromContext(nonexistent) = %s, want empty", val)
	}

	// Test nil context
	val = getStringFromContext(nil, schemas.BifrostContextKey("test"))
	if val != "" {
		t.Errorf("getStringFromContext(nil) = %s, want empty", val)
	}
}

func TestGetIntFromContext(t *testing.T) {
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	ctx.SetValue(schemas.BifrostContextKey("int-key"), 42)

	val := getIntFromContext(ctx, schemas.BifrostContextKey("int-key"))
	if val != 42 {
		t.Errorf("getIntFromContext() = %d, want 42", val)
	}

	// Test non-existent key
	val = getIntFromContext(ctx, schemas.BifrostContextKey("nonexistent"))
	if val != 0 {
		t.Errorf("getIntFromContext(nonexistent) = %d, want 0", val)
	}

	// Test nil context
	val = getIntFromContext(nil, schemas.BifrostContextKey("test"))
	if val != 0 {
		t.Errorf("getIntFromContext(nil) = %d, want 0", val)
	}
}

// Async workers tests

func TestInit_DefaultAsyncWorkers(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		SinkType: "stdout",
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	// Default should be 1
	if plugin.config.AsyncWorkers != 1 {
		t.Errorf("default AsyncWorkers = %d, want 1", plugin.config.AsyncWorkers)
	}

	stats := plugin.Stats()
	if stats["async_workers"] != 1 {
		t.Errorf("Stats()[async_workers] = %d, want 1", stats["async_workers"])
	}
}

func TestInit_CustomAsyncWorkers(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		SinkType:     "stdout",
		AsyncWorkers: 4,
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Cleanup()

	if plugin.config.AsyncWorkers != 4 {
		t.Errorf("AsyncWorkers = %d, want 4", plugin.config.AsyncWorkers)
	}

	stats := plugin.Stats()
	if stats["async_workers"] != 4 {
		t.Errorf("Stats()[async_workers] = %d, want 4", stats["async_workers"])
	}
}

func TestAnalyticsPlugin_AsyncWorkers_ConcurrentBatches(t *testing.T) {
	// Use a slow sink to verify concurrent processing
	testSink := NewTestSink()
	testSink.batchDelay = 100 * time.Millisecond // Each batch takes 100ms

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       1, // Send batch after every event
		FlushIntervalMs: 10000,
		AsyncWorkers:    4, // 4 concurrent workers
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	plugin.SetSink(testSink)

	// Send 4 events quickly - they should all be processed concurrently
	for i := 0; i < 4; i++ {
		reqCtx := createBifrostContextWithRequestID("req-concurrent-" + string(rune('0'+i)))
		reqCtx.SetValue(startTimeKey, time.Now())

		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(reqCtx, result, nil)
	}

	// Wait for events to be queued and start processing
	time.Sleep(50 * time.Millisecond)

	// Measure time from here - all batches should be in-flight concurrently
	start := time.Now()

	// Wait for completion via cleanup (which waits for in-flight)
	plugin.Cleanup()
	elapsed := time.Since(start)

	// With 4 workers and 100ms per batch, 4 concurrent batches should complete in ~100-150ms
	// Without concurrency (1 worker), it would take ~400ms
	if elapsed > 200*time.Millisecond {
		t.Errorf("elapsed = %v, expected < 200ms (concurrent processing should be faster than 400ms serial)", elapsed)
	}

	// Verify all events were sent
	stats := plugin.Stats()
	if stats["events_sent"] != 4 {
		t.Errorf("events_sent = %d, want 4", stats["events_sent"])
	}
	if stats["batches_sent"] != 4 {
		t.Errorf("batches_sent = %d, want 4", stats["batches_sent"])
	}
}

func TestAnalyticsPlugin_AsyncWorkers_Backpressure(t *testing.T) {
	// Slow sink with limited workers to trigger backpressure
	testSink := NewTestSink()
	testSink.batchDelay = 50 * time.Millisecond

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       1,
		FlushIntervalMs: 10000,
		AsyncWorkers:    2, // Only 2 workers
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	plugin.SetSink(testSink)

	// Send events
	for i := 0; i < 4; i++ {
		reqCtx := createBifrostContextWithRequestID("req-bp-" + string(rune('0'+i)))
		reqCtx.SetValue(startTimeKey, time.Now())

		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(reqCtx, result, nil)
	}

	// Wait for events to be queued and processing to start
	time.Sleep(100 * time.Millisecond)

	// Check that batches_in_flight is being tracked
	stats := plugin.Stats()
	if _, ok := stats["batches_in_flight"]; !ok {
		t.Error("stats should include batches_in_flight")
	}

	plugin.Cleanup()

	// After cleanup, all should be sent
	stats = plugin.Stats()
	if stats["events_sent"] != 4 {
		t.Errorf("events_sent = %d, want 4", stats["events_sent"])
	}
	if stats["batches_in_flight"] != 0 {
		t.Errorf("batches_in_flight = %d, want 0 after cleanup", stats["batches_in_flight"])
	}
}

func TestAnalyticsPlugin_AsyncWorkers_GracefulShutdown(t *testing.T) {
	// Slow sink to have in-flight batches during shutdown
	testSink := NewTestSink()
	testSink.batchDelay = 20 * time.Millisecond

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       1,
		FlushIntervalMs: 10000,
		AsyncWorkers:    4,
	}

	plugin, err := Init(ctx, config, NewTestLogger(), nil)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	plugin.SetSink(testSink)

	// Send events
	for i := 0; i < 8; i++ {
		reqCtx := createBifrostContextWithRequestID("req-shutdown-" + string(rune('0'+i)))
		reqCtx.SetValue(startTimeKey, time.Now())

		result := &schemas.BifrostResponse{
			ChatResponse: &schemas.BifrostChatResponse{
				ExtraFields: schemas.BifrostResponseExtraFields{
					RequestType: schemas.ChatCompletionRequest,
					Provider:    schemas.OpenAI,
				},
			},
		}
		_, _, _ = plugin.PostHook(reqCtx, result, nil)
	}

	// Wait for all events to be queued and processed by batchWorker
	// PostHook spawns goroutines that queue to eventChan
	// Then batchWorker reads from channel and calls flush()
	time.Sleep(100 * time.Millisecond)

	// Now trigger shutdown - should wait for in-flight batches
	err = plugin.Cleanup()
	if err != nil {
		t.Errorf("Cleanup() error = %v", err)
	}

	// All events should have been sent
	stats := plugin.Stats()
	if stats["events_sent"] != 8 {
		t.Errorf("events_sent = %d, want 8 (graceful shutdown should wait for in-flight)", stats["events_sent"])
	}
	if stats["batches_in_flight"] != 0 {
		t.Errorf("batches_in_flight = %d, want 0", stats["batches_in_flight"])
	}
}

// Benchmark tests

func BenchmarkAnalyticsPlugin_PostHook(b *testing.B) {
	testSink := NewTestSink()

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       100,
		FlushIntervalMs: 1000,
	}

	plugin, _ := Init(ctx, config, NewTestLogger(), nil)
	plugin.SetSink(testSink)
	defer plugin.Cleanup()

	result := &schemas.BifrostResponse{
		ChatResponse: &schemas.BifrostChatResponse{
			ID:    "resp-bench",
			Model: "gpt-4",
			Usage: &schemas.BifrostLLMUsage{
				PromptTokens:     100,
				CompletionTokens: 50,
				TotalTokens:      150,
			},
			ExtraFields: schemas.BifrostResponseExtraFields{
				RequestType: schemas.ChatCompletionRequest,
				Provider:    schemas.OpenAI,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqCtx := createBifrostContextWithRequestID("req-bench")
		reqCtx.SetValue(startTimeKey, time.Now())
		_, _, _ = plugin.PostHook(reqCtx, result, nil)
	}
}

func BenchmarkAnalyticsPlugin_PreHook(b *testing.B) {
	testSink := NewTestSink()

	ctx := context.Background()
	config := &Config{
		SinkType:        "stdout",
		BatchSize:       100,
		FlushIntervalMs: 1000,
	}

	plugin, _ := Init(ctx, config, NewTestLogger(), nil)
	plugin.SetSink(testSink)
	defer plugin.Cleanup()

	req := &schemas.BifrostRequest{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqCtx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
		_, _, _ = plugin.PreHook(reqCtx, req)
	}
}
