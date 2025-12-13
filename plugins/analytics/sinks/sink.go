// Package sinks provides event destination implementations for the analytics plugin.
// Sinks receive APICallEvents and deliver them to various backends like Kafka,
// webhooks, or stdout for debugging.
package sinks

import (
	"context"
	"fmt"
)

// Event represents the minimal interface for analytics events.
// This allows sinks to work with different event types.
type Event interface {
	JSON() ([]byte, error)
}

// Sink defines the interface for analytics event destinations.
// Implementations should be thread-safe as events may be sent concurrently.
type Sink interface {
	// Name returns the sink identifier (e.g., "kafka", "webhook", "stdout")
	Name() string

	// Send delivers a single event to the sink.
	// Implementations should be non-blocking where possible.
	Send(ctx context.Context, event Event) error

	// SendBatch delivers multiple events to the sink efficiently.
	// This is the preferred method for high-throughput scenarios.
	SendBatch(ctx context.Context, events []Event) error

	// Close gracefully shuts down the sink, flushing any pending events.
	Close() error
}

// Config holds common sink configuration options
type Config struct {
	// BufferSize is the number of events to buffer before flushing
	BufferSize int `json:"buffer_size"`

	// FlushIntervalMs is the maximum time between flushes in milliseconds
	FlushIntervalMs int `json:"flush_interval_ms"`

	// MaxRetries is the number of retry attempts for failed sends
	MaxRetries int `json:"max_retries"`

	// RetryBackoffMs is the initial backoff duration between retries
	RetryBackoffMs int `json:"retry_backoff_ms"`
}

// DefaultConfig returns sensible default sink configuration
func DefaultConfig() Config {
	return Config{
		BufferSize:      100,
		FlushIntervalMs: 1000,
		MaxRetries:      3,
		RetryBackoffMs:  100,
	}
}

// Factory creates sinks from configuration
type Factory func(ctx context.Context, config map[string]any) (Sink, error)

// registry holds registered sink factories
var registry = make(map[string]Factory)

// Register adds a sink factory to the registry
func Register(name string, factory Factory) {
	registry[name] = factory
}

// Create instantiates a sink by name from the registry
func Create(ctx context.Context, name string, config map[string]any) (Sink, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown sink type: %s", name)
	}
	return factory(ctx, config)
}

// Available returns the list of registered sink types
func Available() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
