// Package analytics provides an analytics event tracking plugin for Bifrost.
// It captures API call metadata, timing, token usage, and costs, then streams
// events to configurable sinks (Kafka, webhooks, stdout, etc.).
package analytics

import (
	"time"

	"github.com/bytedance/sonic"
)

// EventType represents the type of analytics event
type EventType string

const (
	EventTypeAPICall   EventType = "api_call"
	EventTypeError     EventType = "error"
	EventTypeCacheHit  EventType = "cache_hit"
	EventTypeRateLimit EventType = "rate_limit"
)

// APICallEvent represents a single API call event for analytics tracking.
// This is the primary event type produced by the analytics plugin.
type APICallEvent struct {
	// Event identity
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`

	// Request identity
	RequestID       string `json:"request_id"`
	ParentRequestID string `json:"parent_request_id,omitempty"` // For fallback requests

	// Request metadata
	Provider    string `json:"provider"`
	Model       string `json:"model"`
	RequestType string `json:"request_type"` // chat_completion, embedding, speech, etc.
	IsStreaming bool   `json:"is_streaming"`

	// Key tracking
	SelectedKeyID   string `json:"selected_key_id,omitempty"`
	SelectedKeyName string `json:"selected_key_name,omitempty"`
	VirtualKeyID    string `json:"virtual_key_id,omitempty"`
	VirtualKeyName  string `json:"virtual_key_name,omitempty"`

	// Fallback tracking
	FallbackIndex   int  `json:"fallback_index"`
	NumberOfRetries int  `json:"number_of_retries"`
	IsFallback      bool `json:"is_fallback"`

	// Timing (all in milliseconds)
	LatencyMs        int64  `json:"latency_ms"`
	TimeToFirstToken *int64 `json:"ttft_ms,omitempty"` // Streaming only

	// Token usage
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
	TotalTokens  int `json:"total_tokens,omitempty"`

	// Cache tokens (Anthropic, etc.)
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`

	// Cost tracking (USD)
	CostUSD *float64 `json:"cost_usd,omitempty"`

	// Result status
	Status       string `json:"status"` // success, error, cached
	ErrorType    string `json:"error_type,omitempty"`
	ErrorCode    int    `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`

	// Cache information
	CacheHit      bool   `json:"cache_hit"`
	CacheType     string `json:"cache_type,omitempty"`     // hash, semantic
	CacheSimilar  string `json:"cache_similar,omitempty"`  // similarity score for semantic cache
	CacheProvider string `json:"cache_provider,omitempty"` // redis, qdrant, weaviate

	// Custom labels (extensible metadata)
	Labels map[string]string `json:"labels,omitempty"`
}

// JSON returns the JSON representation of the event using sonic for performance
func (e *APICallEvent) JSON() ([]byte, error) {
	return sonic.Marshal(e)
}

// JSONString returns the JSON string representation of the event
func (e *APICallEvent) JSONString() (string, error) {
	data, err := e.JSON()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Reset clears all fields for object pool reuse
func (e *APICallEvent) Reset() {
	e.EventID = ""
	e.EventType = ""
	e.Timestamp = time.Time{}
	e.RequestID = ""
	e.ParentRequestID = ""
	e.Provider = ""
	e.Model = ""
	e.RequestType = ""
	e.IsStreaming = false
	e.SelectedKeyID = ""
	e.SelectedKeyName = ""
	e.VirtualKeyID = ""
	e.VirtualKeyName = ""
	e.FallbackIndex = 0
	e.NumberOfRetries = 0
	e.IsFallback = false
	e.LatencyMs = 0
	e.TimeToFirstToken = nil
	e.InputTokens = 0
	e.OutputTokens = 0
	e.TotalTokens = 0
	e.CacheCreationInputTokens = 0
	e.CacheReadInputTokens = 0
	e.CostUSD = nil
	e.Status = ""
	e.ErrorType = ""
	e.ErrorCode = 0
	e.ErrorMessage = ""
	e.CacheHit = false
	e.CacheType = ""
	e.CacheSimilar = ""
	e.CacheProvider = ""
	// Clear labels map but keep capacity
	for k := range e.Labels {
		delete(e.Labels, k)
	}
}

// Clone creates a deep copy of the event
func (e *APICallEvent) Clone() *APICallEvent {
	clone := &APICallEvent{
		EventID:                  e.EventID,
		EventType:                e.EventType,
		Timestamp:                e.Timestamp,
		RequestID:                e.RequestID,
		ParentRequestID:          e.ParentRequestID,
		Provider:                 e.Provider,
		Model:                    e.Model,
		RequestType:              e.RequestType,
		IsStreaming:              e.IsStreaming,
		SelectedKeyID:            e.SelectedKeyID,
		SelectedKeyName:          e.SelectedKeyName,
		VirtualKeyID:             e.VirtualKeyID,
		VirtualKeyName:           e.VirtualKeyName,
		FallbackIndex:            e.FallbackIndex,
		NumberOfRetries:          e.NumberOfRetries,
		IsFallback:               e.IsFallback,
		LatencyMs:                e.LatencyMs,
		InputTokens:              e.InputTokens,
		OutputTokens:             e.OutputTokens,
		TotalTokens:              e.TotalTokens,
		CacheCreationInputTokens: e.CacheCreationInputTokens,
		CacheReadInputTokens:     e.CacheReadInputTokens,
		Status:                   e.Status,
		ErrorType:                e.ErrorType,
		ErrorCode:                e.ErrorCode,
		ErrorMessage:             e.ErrorMessage,
		CacheHit:                 e.CacheHit,
		CacheType:                e.CacheType,
		CacheSimilar:             e.CacheSimilar,
		CacheProvider:            e.CacheProvider,
	}

	if e.TimeToFirstToken != nil {
		ttft := *e.TimeToFirstToken
		clone.TimeToFirstToken = &ttft
	}

	if e.CostUSD != nil {
		cost := *e.CostUSD
		clone.CostUSD = &cost
	}

	if len(e.Labels) > 0 {
		clone.Labels = make(map[string]string, len(e.Labels))
		for k, v := range e.Labels {
			clone.Labels[k] = v
		}
	}

	return clone
}
