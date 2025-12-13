package analytics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAPICallEvent_JSON(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	cost := 0.0025
	ttft := int64(150)

	event := &APICallEvent{
		EventID:         "evt-123",
		EventType:       EventTypeAPICall,
		Timestamp:       timestamp,
		RequestID:       "req-456",
		Provider:        "openai",
		Model:           "gpt-4",
		RequestType:     "chat_completion",
		IsStreaming:     false,
		SelectedKeyID:   "key-1",
		LatencyMs:       1250,
		TimeToFirstToken: &ttft,
		InputTokens:     100,
		OutputTokens:    50,
		TotalTokens:     150,
		CostUSD:         &cost,
		Status:          "success",
		Labels: map[string]string{
			"tenant_id": "tenant-abc",
			"env":       "production",
		},
	}

	data, err := event.JSON()
	if err != nil {
		t.Fatalf("JSON() error = %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON output is not valid JSON: %v", err)
	}

	// Verify key fields
	if parsed["event_id"] != "evt-123" {
		t.Errorf("event_id = %v, want evt-123", parsed["event_id"])
	}
	if parsed["provider"] != "openai" {
		t.Errorf("provider = %v, want openai", parsed["provider"])
	}
	if parsed["model"] != "gpt-4" {
		t.Errorf("model = %v, want gpt-4", parsed["model"])
	}
	if parsed["status"] != "success" {
		t.Errorf("status = %v, want success", parsed["status"])
	}
	if int(parsed["input_tokens"].(float64)) != 100 {
		t.Errorf("input_tokens = %v, want 100", parsed["input_tokens"])
	}

	// Verify labels
	labels, ok := parsed["labels"].(map[string]interface{})
	if !ok {
		t.Fatal("labels is not a map")
	}
	if labels["tenant_id"] != "tenant-abc" {
		t.Errorf("labels.tenant_id = %v, want tenant-abc", labels["tenant_id"])
	}
}

func TestAPICallEvent_JSONString(t *testing.T) {
	event := &APICallEvent{
		EventID:   "evt-123",
		EventType: EventTypeAPICall,
		Provider:  "anthropic",
		Model:     "claude-3",
		Status:    "success",
	}

	str, err := event.JSONString()
	if err != nil {
		t.Fatalf("JSONString() error = %v", err)
	}

	if len(str) == 0 {
		t.Error("JSONString() returned empty string")
	}

	// Should contain key fields
	if !contains(str, "evt-123") {
		t.Error("JSONString() should contain event_id")
	}
	if !contains(str, "anthropic") {
		t.Error("JSONString() should contain provider")
	}
}

func TestAPICallEvent_Reset(t *testing.T) {
	cost := 0.01
	ttft := int64(100)

	event := &APICallEvent{
		EventID:          "evt-123",
		EventType:        EventTypeAPICall,
		Timestamp:        time.Now(),
		RequestID:        "req-456",
		ParentRequestID:  "req-parent",
		Provider:         "openai",
		Model:            "gpt-4",
		RequestType:      "chat_completion",
		IsStreaming:      true,
		SelectedKeyID:    "key-1",
		SelectedKeyName:  "prod-key",
		VirtualKeyID:     "vk-1",
		VirtualKeyName:   "virtual-key",
		FallbackIndex:    2,
		NumberOfRetries:  3,
		IsFallback:       true,
		LatencyMs:        1000,
		TimeToFirstToken: &ttft,
		InputTokens:      100,
		OutputTokens:     50,
		TotalTokens:      150,
		CostUSD:          &cost,
		Status:           "success",
		ErrorType:        "rate_limit",
		ErrorCode:        429,
		ErrorMessage:     "Rate limited",
		CacheHit:         true,
		CacheType:        "semantic",
		Labels: map[string]string{
			"key1": "value1",
		},
	}

	event.Reset()

	// Verify all fields are reset
	if event.EventID != "" {
		t.Errorf("EventID not reset, got %s", event.EventID)
	}
	if event.EventType != "" {
		t.Errorf("EventType not reset, got %s", event.EventType)
	}
	if !event.Timestamp.IsZero() {
		t.Errorf("Timestamp not reset, got %v", event.Timestamp)
	}
	if event.RequestID != "" {
		t.Errorf("RequestID not reset, got %s", event.RequestID)
	}
	if event.Provider != "" {
		t.Errorf("Provider not reset, got %s", event.Provider)
	}
	if event.Model != "" {
		t.Errorf("Model not reset, got %s", event.Model)
	}
	if event.IsStreaming {
		t.Error("IsStreaming not reset")
	}
	if event.IsFallback {
		t.Error("IsFallback not reset")
	}
	if event.FallbackIndex != 0 {
		t.Errorf("FallbackIndex not reset, got %d", event.FallbackIndex)
	}
	if event.LatencyMs != 0 {
		t.Errorf("LatencyMs not reset, got %d", event.LatencyMs)
	}
	if event.TimeToFirstToken != nil {
		t.Errorf("TimeToFirstToken not reset, got %v", event.TimeToFirstToken)
	}
	if event.InputTokens != 0 {
		t.Errorf("InputTokens not reset, got %d", event.InputTokens)
	}
	if event.CostUSD != nil {
		t.Errorf("CostUSD not reset, got %v", event.CostUSD)
	}
	if event.Status != "" {
		t.Errorf("Status not reset, got %s", event.Status)
	}
	if event.CacheHit {
		t.Error("CacheHit not reset")
	}
	if len(event.Labels) != 0 {
		t.Errorf("Labels not cleared, got %v", event.Labels)
	}
}

func TestAPICallEvent_Clone(t *testing.T) {
	cost := 0.05
	ttft := int64(200)

	original := &APICallEvent{
		EventID:          "evt-original",
		EventType:        EventTypeAPICall,
		Timestamp:        time.Now(),
		RequestID:        "req-123",
		Provider:         "openai",
		Model:            "gpt-4",
		RequestType:      "chat_completion",
		IsStreaming:      true,
		LatencyMs:        500,
		TimeToFirstToken: &ttft,
		InputTokens:      200,
		OutputTokens:     100,
		TotalTokens:      300,
		CostUSD:          &cost,
		Status:           "success",
		CacheHit:         true,
		Labels: map[string]string{
			"env":    "prod",
			"region": "us-east",
		},
	}

	clone := original.Clone()

	// Verify values match
	if clone.EventID != original.EventID {
		t.Errorf("EventID mismatch: %s != %s", clone.EventID, original.EventID)
	}
	if clone.Provider != original.Provider {
		t.Errorf("Provider mismatch: %s != %s", clone.Provider, original.Provider)
	}
	if clone.LatencyMs != original.LatencyMs {
		t.Errorf("LatencyMs mismatch: %d != %d", clone.LatencyMs, original.LatencyMs)
	}
	if clone.InputTokens != original.InputTokens {
		t.Errorf("InputTokens mismatch: %d != %d", clone.InputTokens, original.InputTokens)
	}

	// Verify pointers are independent copies
	if clone.TimeToFirstToken == original.TimeToFirstToken {
		t.Error("TimeToFirstToken should be a new pointer")
	}
	if *clone.TimeToFirstToken != *original.TimeToFirstToken {
		t.Errorf("TimeToFirstToken value mismatch: %d != %d", *clone.TimeToFirstToken, *original.TimeToFirstToken)
	}

	if clone.CostUSD == original.CostUSD {
		t.Error("CostUSD should be a new pointer")
	}
	if *clone.CostUSD != *original.CostUSD {
		t.Errorf("CostUSD value mismatch: %f != %f", *clone.CostUSD, *original.CostUSD)
	}

	// Verify labels are independent
	if &clone.Labels == &original.Labels {
		t.Error("Labels should be a new map")
	}
	clone.Labels["new_key"] = "new_value"
	if _, exists := original.Labels["new_key"]; exists {
		t.Error("Modifying clone.Labels affected original.Labels")
	}
}

func TestAPICallEvent_Clone_NilPointers(t *testing.T) {
	original := &APICallEvent{
		EventID:  "evt-123",
		Provider: "openai",
		Status:   "success",
		// TimeToFirstToken and CostUSD are nil
		// Labels is nil
	}

	clone := original.Clone()

	if clone.TimeToFirstToken != nil {
		t.Error("TimeToFirstToken should be nil")
	}
	if clone.CostUSD != nil {
		t.Error("CostUSD should be nil")
	}
	if clone.Labels != nil {
		t.Error("Labels should be nil when original is nil")
	}
}

func TestEventType_Constants(t *testing.T) {
	// Verify event type constants exist and have expected values
	if EventTypeAPICall != "api_call" {
		t.Errorf("EventTypeAPICall = %s, want api_call", EventTypeAPICall)
	}
	if EventTypeError != "error" {
		t.Errorf("EventTypeError = %s, want error", EventTypeError)
	}
	if EventTypeCacheHit != "cache_hit" {
		t.Errorf("EventTypeCacheHit = %s, want cache_hit", EventTypeCacheHit)
	}
	if EventTypeRateLimit != "rate_limit" {
		t.Errorf("EventTypeRateLimit = %s, want rate_limit", EventTypeRateLimit)
	}
}

func TestAPICallEvent_JSON_EmptyEvent(t *testing.T) {
	event := &APICallEvent{}

	data, err := event.JSON()
	if err != nil {
		t.Fatalf("JSON() error on empty event = %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Empty event JSON is invalid: %v", err)
	}
}

func TestAPICallEvent_JSON_SpecialCharacters(t *testing.T) {
	event := &APICallEvent{
		EventID:      "evt-123",
		ErrorMessage: `Error with "quotes" and \backslashes\ and <tags>`,
		Labels: map[string]string{
			"special": `value with "quotes"`,
		},
	}

	data, err := event.JSON()
	if err != nil {
		t.Fatalf("JSON() error with special characters = %v", err)
	}

	// Verify it can be parsed back
	var parsed APICallEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON with special characters: %v", err)
	}

	if parsed.ErrorMessage != event.ErrorMessage {
		t.Errorf("ErrorMessage not preserved: got %q, want %q", parsed.ErrorMessage, event.ErrorMessage)
	}
}

// Benchmark tests

func BenchmarkAPICallEvent_JSON(b *testing.B) {
	cost := 0.0025
	event := &APICallEvent{
		EventID:      "evt-123",
		EventType:    EventTypeAPICall,
		Timestamp:    time.Now(),
		RequestID:    "req-456",
		Provider:     "openai",
		Model:        "gpt-4",
		RequestType:  "chat_completion",
		IsStreaming:  false,
		LatencyMs:    1250,
		InputTokens:  100,
		OutputTokens: 50,
		TotalTokens:  150,
		CostUSD:      &cost,
		Status:       "success",
		Labels: map[string]string{
			"tenant_id": "tenant-abc",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = event.JSON()
	}
}

func BenchmarkAPICallEvent_Reset(b *testing.B) {
	event := &APICallEvent{
		EventID:     "evt-123",
		Provider:    "openai",
		Model:       "gpt-4",
		InputTokens: 100,
		Labels: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event.Reset()
		// Restore some fields for next iteration
		event.EventID = "evt-123"
		event.Labels = map[string]string{"key1": "value1"}
	}
}

func BenchmarkAPICallEvent_Clone(b *testing.B) {
	cost := 0.05
	original := &APICallEvent{
		EventID:      "evt-123",
		Provider:     "openai",
		Model:        "gpt-4",
		InputTokens:  200,
		OutputTokens: 100,
		CostUSD:      &cost,
		Labels: map[string]string{
			"env":    "prod",
			"region": "us-east",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = original.Clone()
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
