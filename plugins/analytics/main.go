// Package analytics provides an analytics event tracking plugin for Bifrost.
// It captures API call metadata, timing, token usage, and costs, then streams
// events to configurable sinks (Kafka, webhooks, stdout, etc.).
package analytics

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/plugins/analytics/sinks"
)

const (
	PluginName = "analytics"

	// Context keys for timing
	startTimeKey schemas.BifrostContextKey = "bf-analytics-start-time"

	// Default configuration values
	defaultBatchSize      = 100
	defaultFlushInterval  = 1000 // milliseconds
	defaultChannelBuffer  = 10000
	defaultPoolPrewarmCnt = 1000
	defaultAsyncWorkers   = 1 // backwards compatible: synchronous by default
)

// Config holds the analytics plugin configuration
type Config struct {
	// SinkType specifies the event destination: "stdout", "webhook", "kafka"
	SinkType string `json:"sink_type"`

	// SinkConfig holds sink-specific configuration
	SinkConfig map[string]any `json:"sink_config"`

	// BatchSize is the number of events to batch before sending (default: 100)
	BatchSize int `json:"batch_size"`

	// FlushIntervalMs is the maximum time between flushes in milliseconds (default: 1000)
	FlushIntervalMs int `json:"flush_interval_ms"`

	// CustomLabels are context keys to extract as event labels
	CustomLabels []string `json:"custom_labels"`

	// DropOnBackpressure drops events when the channel is full instead of blocking
	DropOnBackpressure bool `json:"drop_on_backpressure"`

	// AsyncWorkers controls concurrent batch sends (default: 1 = synchronous)
	// Higher values increase throughput but events may arrive out-of-order.
	// Recommended: 4-8 for production workloads.
	AsyncWorkers int `json:"async_workers"`
}

// AnalyticsPlugin implements the schemas.Plugin interface for analytics event tracking
type AnalyticsPlugin struct {
	ctx    context.Context
	cancel context.CancelFunc

	config *Config
	sink   sinks.Sink
	logger schemas.Logger

	// Pricing for cost calculation
	pricingManager *modelcatalog.ModelCatalog

	// Event batching
	eventChan     chan *APICallEvent
	done          chan struct{}
	wg            sync.WaitGroup
	flushTicker   *time.Ticker
	pendingEvents []*APICallEvent
	pendingMu     sync.Mutex

	// Async batch sending
	sendSemaphore chan struct{} // limits concurrent batch sends
	inFlightWg    sync.WaitGroup // tracks in-flight batch sends for graceful shutdown

	// Object pooling for performance
	eventPool sync.Pool

	// Metrics
	eventsProduced  atomic.Int64
	eventsDropped   atomic.Int64
	eventsSent      atomic.Int64
	batchesSent     atomic.Int64
	sendErrors      atomic.Int64
	lastFlushTime   atomic.Int64
	batchesInFlight atomic.Int64
	customLabelKeys []schemas.BifrostContextKey
}

// Init creates a new analytics plugin with the given configuration
func Init(
	ctx context.Context,
	config *Config,
	logger schemas.Logger,
	pricingManager *modelcatalog.ModelCatalog,
) (*AnalyticsPlugin, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	if config.SinkType == "" {
		config.SinkType = "stdout"
	}

	// Apply defaults
	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.FlushIntervalMs <= 0 {
		config.FlushIntervalMs = defaultFlushInterval
	}
	if config.AsyncWorkers <= 0 {
		config.AsyncWorkers = defaultAsyncWorkers
	}

	// Create sink
	sink, err := sinks.Create(ctx, config.SinkType, config.SinkConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink %q: %w", config.SinkType, err)
	}

	pluginCtx, cancel := context.WithCancel(ctx)

	// Convert custom label strings to context keys
	customLabelKeys := make([]schemas.BifrostContextKey, len(config.CustomLabels))
	for i, label := range config.CustomLabels {
		customLabelKeys[i] = schemas.BifrostContextKey(label)
	}

	plugin := &AnalyticsPlugin{
		ctx:             pluginCtx,
		cancel:          cancel,
		config:          config,
		sink:            sink,
		logger:          logger,
		pricingManager:  pricingManager,
		eventChan:       make(chan *APICallEvent, defaultChannelBuffer),
		done:            make(chan struct{}),
		pendingEvents:   make([]*APICallEvent, 0, config.BatchSize),
		sendSemaphore:   make(chan struct{}, config.AsyncWorkers), // bounded concurrency
		customLabelKeys: customLabelKeys,
		eventPool: sync.Pool{
			New: func() interface{} {
				return &APICallEvent{
					Labels: make(map[string]string),
				}
			},
		},
	}

	// Prewarm the event pool
	for range defaultPoolPrewarmCnt {
		plugin.eventPool.Put(&APICallEvent{
			Labels: make(map[string]string),
		})
	}

	// Start background workers
	plugin.flushTicker = time.NewTicker(time.Duration(config.FlushIntervalMs) * time.Millisecond)

	plugin.wg.Add(1)
	go plugin.batchWorker()

	logger.Info("analytics plugin initialized with sink: %s, async_workers: %d", config.SinkType, config.AsyncWorkers)
	return plugin, nil
}

// batchWorker collects events and sends them in batches
func (p *AnalyticsPlugin) batchWorker() {
	defer p.wg.Done()

	for {
		select {
		case event := <-p.eventChan:
			p.pendingMu.Lock()
			p.pendingEvents = append(p.pendingEvents, event)
			shouldFlush := len(p.pendingEvents) >= p.config.BatchSize
			p.pendingMu.Unlock()

			if shouldFlush {
				p.flush()
			}

		case <-p.flushTicker.C:
			p.flush()

		case <-p.done:
			// Final flush on shutdown
			p.flush()
			return
		}
	}
}

// flush sends pending events to the sink using bounded async concurrency
func (p *AnalyticsPlugin) flush() {
	p.pendingMu.Lock()
	if len(p.pendingEvents) == 0 {
		p.pendingMu.Unlock()
		return
	}

	// Take ownership of pending events
	events := p.pendingEvents
	p.pendingEvents = make([]*APICallEvent, 0, p.config.BatchSize)
	p.pendingMu.Unlock()

	// Convert to sink.Event interface
	sinkEvents := make([]sinks.Event, len(events))
	for i, e := range events {
		sinkEvents[i] = e
	}

	// Acquire semaphore slot (blocks if all workers are busy)
	p.sendSemaphore <- struct{}{}
	p.inFlightWg.Add(1)
	p.batchesInFlight.Add(1)

	// Send batch asynchronously
	go func(events []*APICallEvent, sinkEvents []sinks.Event) {
		defer func() {
			<-p.sendSemaphore // release semaphore slot
			p.inFlightWg.Done()
			p.batchesInFlight.Add(-1)
		}()

		if err := p.sink.SendBatch(p.ctx, sinkEvents); err != nil {
			p.sendErrors.Add(1)
			p.logger.Warn("analytics: failed to send batch of %d events: %v", len(events), err)
		} else {
			p.eventsSent.Add(int64(len(events)))
			p.batchesSent.Add(1)
		}

		p.lastFlushTime.Store(time.Now().UnixMilli())

		// Return events to pool
		for _, event := range events {
			event.Reset()
			p.eventPool.Put(event)
		}
	}(events, sinkEvents)
}

// GetName returns the plugin name
func (p *AnalyticsPlugin) GetName() string {
	return PluginName
}

// TransportInterceptor is not used by this plugin
func (p *AnalyticsPlugin) TransportInterceptor(
	ctx *schemas.BifrostContext,
	url string,
	headers map[string]string,
	body map[string]any,
) (map[string]string, map[string]any, error) {
	return headers, body, nil
}

// PreHook captures the start time and request metadata
func (p *AnalyticsPlugin) PreHook(
	ctx *schemas.BifrostContext,
	req *schemas.BifrostRequest,
) (*schemas.BifrostRequest, *schemas.PluginShortCircuit, error) {
	if ctx == nil {
		return req, nil, nil
	}

	// Store start time for latency calculation
	ctx.SetValue(startTimeKey, time.Now())

	return req, nil, nil
}

// PostHook creates and sends the analytics event
func (p *AnalyticsPlugin) PostHook(
	ctx *schemas.BifrostContext,
	result *schemas.BifrostResponse,
	bifrostErr *schemas.BifrostError,
) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if ctx == nil {
		return result, bifrostErr, nil
	}

	// Get request ID
	requestID, _ := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
	if requestID == "" {
		return result, bifrostErr, nil
	}

	// Calculate latency
	var latencyMs int64
	if startTime, ok := ctx.Value(startTimeKey).(time.Time); ok {
		latencyMs = time.Since(startTime).Milliseconds()
	}

	// Build event asynchronously to avoid blocking the request path
	go p.buildAndSendEvent(ctx, requestID, latencyMs, result, bifrostErr)

	return result, bifrostErr, nil
}

// buildAndSendEvent constructs and queues an analytics event
func (p *AnalyticsPlugin) buildAndSendEvent(
	ctx *schemas.BifrostContext,
	requestID string,
	latencyMs int64,
	result *schemas.BifrostResponse,
	bifrostErr *schemas.BifrostError,
) {
	// Recover from panics to prevent silent failures
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("analytics: panic in buildAndSendEvent: %v", r)
		}
	}()

	event := p.eventPool.Get().(*APICallEvent)
	event.Reset()

	// Event identity
	event.EventID = uuid.New().String()
	event.EventType = EventTypeAPICall
	event.Timestamp = time.Now().UTC()
	event.RequestID = requestID

	// Check for fallback request
	if fallbackID, ok := ctx.Value(schemas.BifrostContextKeyFallbackRequestID).(string); ok && fallbackID != "" {
		event.ParentRequestID = requestID
		event.RequestID = fallbackID
		event.IsFallback = true
	}

	// Extract request type and metadata from response or error
	requestType, provider, model := bifrost.GetResponseFields(result, bifrostErr)
	event.RequestType = string(requestType)
	event.Provider = string(provider)
	event.Model = model
	event.IsStreaming = bifrost.IsStreamRequestType(requestType)

	// Key tracking
	event.SelectedKeyID = getStringFromContext(ctx, schemas.BifrostContextKeySelectedKeyID)
	event.SelectedKeyName = getStringFromContext(ctx, schemas.BifrostContextKeySelectedKeyName)
	event.VirtualKeyID = getStringFromContext(ctx, schemas.BifrostContextKey("bf-governance-virtual-key-id"))
	event.VirtualKeyName = getStringFromContext(ctx, schemas.BifrostContextKey("bf-governance-virtual-key-name"))

	// Fallback and retry tracking
	event.FallbackIndex = getIntFromContext(ctx, schemas.BifrostContextKeyFallbackIndex)
	event.NumberOfRetries = getIntFromContext(ctx, schemas.BifrostContextKeyNumberOfRetries)

	// Timing
	event.LatencyMs = latencyMs

	// Determine status and extract data
	if bifrostErr != nil {
		event.Status = "error"
		if bifrostErr.Type != nil {
			event.ErrorType = *bifrostErr.Type
		}
		if bifrostErr.StatusCode != nil {
			event.ErrorCode = *bifrostErr.StatusCode
		}
		if bifrostErr.Error != nil {
			event.ErrorMessage = bifrostErr.Error.Message
		}
	} else if result != nil {
		event.Status = "success"

		// Token usage
		p.extractTokenUsage(event, result)

		// Cache information
		if cacheDebug := result.GetExtraFields().CacheDebug; cacheDebug != nil {
			event.CacheHit = cacheDebug.CacheHit
			if cacheDebug.HitType != nil {
				event.CacheType = *cacheDebug.HitType
			}
			if cacheDebug.ProviderUsed != nil {
				event.CacheProvider = *cacheDebug.ProviderUsed
			}
			if cacheDebug.Similarity != nil {
				event.CacheSimilar = fmt.Sprintf("%.4f", *cacheDebug.Similarity)
			}
			if cacheDebug.CacheHit {
				event.EventType = EventTypeCacheHit
			}
		}

		// Cost calculation
		if p.pricingManager != nil {
			cost := p.pricingManager.CalculateCostWithCacheDebug(result)
			if cost > 0 {
				event.CostUSD = &cost
			}
		}

		// Time to first token for streaming (if available in extra fields)
		if extra := result.GetExtraFields(); extra.Latency > 0 && event.IsStreaming {
			// For streaming, Latency in extra fields often represents TTFT
			ttft := extra.Latency
			event.TimeToFirstToken = &ttft
		}
	}

	// Extract custom labels from context
	if len(p.customLabelKeys) > 0 {
		if event.Labels == nil {
			event.Labels = make(map[string]string)
		}
		for _, key := range p.customLabelKeys {
			if val := getStringFromContext(ctx, key); val != "" {
				event.Labels[string(key)] = val
			}
		}
	}

	// Queue the event
	p.eventsProduced.Add(1)

	if p.config.DropOnBackpressure {
		select {
		case p.eventChan <- event:
		default:
			// Channel full, drop event
			p.eventsDropped.Add(1)
			event.Reset()
			p.eventPool.Put(event)
		}
	} else {
		p.eventChan <- event
	}
}

// extractTokenUsage extracts token usage from different response types
func (p *AnalyticsPlugin) extractTokenUsage(event *APICallEvent, result *schemas.BifrostResponse) {
	var usage *schemas.BifrostLLMUsage

	switch {
	case result.TextCompletionResponse != nil && result.TextCompletionResponse.Usage != nil:
		usage = result.TextCompletionResponse.Usage
	case result.ChatResponse != nil && result.ChatResponse.Usage != nil:
		usage = result.ChatResponse.Usage
	case result.ResponsesResponse != nil && result.ResponsesResponse.Usage != nil:
		u := result.ResponsesResponse.Usage.ToBifrostLLMUsage()
		usage = u
	case result.EmbeddingResponse != nil && result.EmbeddingResponse.Usage != nil:
		usage = result.EmbeddingResponse.Usage
	case result.TranscriptionResponse != nil && result.TranscriptionResponse.Usage != nil:
		usage = &schemas.BifrostLLMUsage{}
		if result.TranscriptionResponse.Usage.InputTokens != nil {
			usage.PromptTokens = *result.TranscriptionResponse.Usage.InputTokens
		}
		if result.TranscriptionResponse.Usage.OutputTokens != nil {
			usage.CompletionTokens = *result.TranscriptionResponse.Usage.OutputTokens
		}
		if result.TranscriptionResponse.Usage.TotalTokens != nil {
			usage.TotalTokens = *result.TranscriptionResponse.Usage.TotalTokens
		}
	}

	if usage != nil {
		event.InputTokens = usage.PromptTokens
		event.OutputTokens = usage.CompletionTokens
		event.TotalTokens = usage.TotalTokens

		// Cache tokens from prompt tokens details (cache read tokens)
		if usage.PromptTokensDetails != nil {
			event.CacheReadInputTokens = usage.PromptTokensDetails.CachedTokens
		}
		// Cache tokens from completion tokens details (cache creation tokens)
		if usage.CompletionTokensDetails != nil {
			event.CacheCreationInputTokens = usage.CompletionTokensDetails.CachedTokens
		}
	}
}

// Cleanup gracefully shuts down the plugin
func (p *AnalyticsPlugin) Cleanup() error {
	p.logger.Info("analytics plugin shutting down...")

	// Stop the flush ticker
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}

	// Signal worker to stop
	close(p.done)

	// Wait for batch worker to finish (includes final flush)
	p.wg.Wait()

	// Wait for all in-flight batch sends to complete
	p.inFlightWg.Wait()

	// Close the sink
	if err := p.sink.Close(); err != nil {
		p.logger.Warn("analytics: error closing sink: %v", err)
	}

	// Cancel context
	p.cancel()

	p.logger.Info("analytics plugin shutdown complete. Events produced: %d, sent: %d, dropped: %d, errors: %d",
		p.eventsProduced.Load(), p.eventsSent.Load(), p.eventsDropped.Load(), p.sendErrors.Load())

	return nil
}

// Stats returns current plugin statistics
func (p *AnalyticsPlugin) Stats() map[string]int64 {
	return map[string]int64{
		"events_produced":    p.eventsProduced.Load(),
		"events_sent":        p.eventsSent.Load(),
		"events_dropped":     p.eventsDropped.Load(),
		"batches_sent":       p.batchesSent.Load(),
		"batches_in_flight":  p.batchesInFlight.Load(),
		"send_errors":        p.sendErrors.Load(),
		"last_flush_epoch":   p.lastFlushTime.Load(),
		"pending_events":     int64(len(p.eventChan)),
		"async_workers":      int64(p.config.AsyncWorkers),
	}
}

// SetSink replaces the sink - intended for testing only
func (p *AnalyticsPlugin) SetSink(sink sinks.Sink) {
	p.sink = sink
}

// Helper functions

func getStringFromContext(ctx *schemas.BifrostContext, key schemas.BifrostContextKey) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(key).(string); ok {
		return val
	}
	return ""
}

func getIntFromContext(ctx *schemas.BifrostContext, key schemas.BifrostContextKey) int {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(key).(int); ok {
		return val
	}
	return 0
}
