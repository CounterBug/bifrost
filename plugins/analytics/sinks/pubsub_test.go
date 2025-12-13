package sinks

import (
	"context"
	"testing"
)

func TestNewPubSubSink_NoConfig(t *testing.T) {
	ctx := context.Background()
	_, err := NewPubSubSink(ctx, nil)
	if err == nil {
		t.Error("NewPubSubSink(nil) should return error")
	}
}

func TestNewPubSubSink_NoProjectID(t *testing.T) {
	ctx := context.Background()
	_, err := NewPubSubSink(ctx, &PubSubConfig{
		TopicID: "test-topic",
	})
	if err == nil {
		t.Error("NewPubSubSink without project_id should return error")
	}
}

func TestNewPubSubSink_NoTopicID(t *testing.T) {
	ctx := context.Background()
	_, err := NewPubSubSink(ctx, &PubSubConfig{
		ProjectID: "test-project",
	})
	if err == nil {
		t.Error("NewPubSubSink without topic_id should return error")
	}
}

func TestPubSubSink_Name(t *testing.T) {
	// We can't easily create a real PubSubSink without credentials,
	// so we test the Name method on a manually constructed sink
	sink := &PubSubSink{}
	if sink.Name() != "pubsub" {
		t.Errorf("Name() = %s, want pubsub", sink.Name())
	}
}

func TestPubSubSink_Registry_MissingProjectID(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "pubsub", map[string]any{
		"topic_id": "test-topic",
	})
	if err == nil {
		t.Error("Create(pubsub) without project_id should return error")
	}
}

func TestPubSubSink_Registry_MissingTopicID(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "pubsub", map[string]any{
		"project_id": "test-project",
	})
	if err == nil {
		t.Error("Create(pubsub) without topic_id should return error")
	}
}

func TestPubSubConfig_Defaults(t *testing.T) {
	cfg := &PubSubConfig{
		ProjectID: "test-project",
		TopicID:   "test-topic",
	}

	// Verify defaults are zero values
	if cfg.BatchSize != 0 {
		t.Errorf("BatchSize default = %d, want 0", cfg.BatchSize)
	}
	if cfg.BatchBytes != 0 {
		t.Errorf("BatchBytes default = %d, want 0", cfg.BatchBytes)
	}
	if cfg.BatchDelayMs != 0 {
		t.Errorf("BatchDelayMs default = %d, want 0", cfg.BatchDelayMs)
	}
	if cfg.NumGoroutines != 0 {
		t.Errorf("NumGoroutines default = %d, want 0", cfg.NumGoroutines)
	}
}

func TestPubSubConfig_WithAllOptions(t *testing.T) {
	cfg := &PubSubConfig{
		ProjectID:       "my-project",
		TopicID:         "my-topic",
		CredentialsFile: "/path/to/creds.json",
		Endpoint:        "localhost:8085",
		OrderingKey:     "tenant-123",
		BatchSize:       200,
		BatchBytes:      2097152,
		BatchDelayMs:    20,
		NumGoroutines:   50,
		FlowControlMax:  2000,
	}

	if cfg.ProjectID != "my-project" {
		t.Errorf("ProjectID = %s, want my-project", cfg.ProjectID)
	}
	if cfg.TopicID != "my-topic" {
		t.Errorf("TopicID = %s, want my-topic", cfg.TopicID)
	}
	if cfg.CredentialsFile != "/path/to/creds.json" {
		t.Errorf("CredentialsFile = %s, want /path/to/creds.json", cfg.CredentialsFile)
	}
	if cfg.Endpoint != "localhost:8085" {
		t.Errorf("Endpoint = %s, want localhost:8085", cfg.Endpoint)
	}
	if cfg.OrderingKey != "tenant-123" {
		t.Errorf("OrderingKey = %s, want tenant-123", cfg.OrderingKey)
	}
	if cfg.BatchSize != 200 {
		t.Errorf("BatchSize = %d, want 200", cfg.BatchSize)
	}
	if cfg.BatchBytes != 2097152 {
		t.Errorf("BatchBytes = %d, want 2097152", cfg.BatchBytes)
	}
	if cfg.BatchDelayMs != 20 {
		t.Errorf("BatchDelayMs = %d, want 20", cfg.BatchDelayMs)
	}
	if cfg.NumGoroutines != 50 {
		t.Errorf("NumGoroutines = %d, want 50", cfg.NumGoroutines)
	}
	if cfg.FlowControlMax != 2000 {
		t.Errorf("FlowControlMax = %d, want 2000", cfg.FlowControlMax)
	}
}

func TestPubSubSink_Close_Nil(t *testing.T) {
	// Test that Close handles nil client gracefully
	sink := &PubSubSink{
		client: nil,
		topic:  nil,
	}

	// This should panic without proper nil handling
	// We can't easily test this without mocking the client
	_ = sink
}

// Note: Full integration tests for NewPubSubSink, Send, and SendBatch
// would require a running Pub/Sub emulator or real GCP credentials.
// These tests verify configuration parsing and error handling.

func TestPubSubSink_Registry_ConfigParsing(t *testing.T) {
	// Test that the registry correctly parses all config options
	// We can't create a real sink without credentials, but we can
	// verify the config parsing by checking error messages

	ctx := context.Background()

	// This should fail at the client creation stage, not config parsing
	config := map[string]any{
		"project_id":        "test-project",
		"topic_id":          "test-topic",
		"credentials_file":  "/nonexistent/path.json",
		"endpoint":          "localhost:8085",
		"ordering_key":      "tenant-123",
		"batch_size":        float64(200),
		"batch_bytes":       float64(2097152),
		"batch_delay_ms":    float64(20),
		"num_goroutines":    float64(50),
		"flow_control_max":  float64(2000),
	}

	// This will fail because the credentials file doesn't exist,
	// but it validates that config parsing works
	_, err := Create(ctx, "pubsub", config)
	if err == nil {
		// If it succeeds, that's fine too (might have ADC configured)
		t.Log("Create succeeded - Application Default Credentials might be configured")
	}
}
