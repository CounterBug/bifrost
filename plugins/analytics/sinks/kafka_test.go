package sinks

import (
	"context"
	"testing"
)

func TestNewKafkaSink_NoConfig(t *testing.T) {
	ctx := context.Background()
	_, err := NewKafkaSink(ctx, nil)
	if err == nil {
		t.Error("NewKafkaSink(nil) should return error")
	}
}

func TestNewKafkaSink_NoBrokers(t *testing.T) {
	ctx := context.Background()
	_, err := NewKafkaSink(ctx, &KafkaConfig{
		Topic: "test-topic",
	})
	if err == nil {
		t.Error("NewKafkaSink without brokers should return error")
	}
}

func TestNewKafkaSink_NoTopic(t *testing.T) {
	ctx := context.Background()
	_, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers: []string{"localhost:9092"},
	})
	if err == nil {
		t.Error("NewKafkaSink without topic should return error")
	}
}

func TestNewKafkaSink_Valid(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	if err != nil {
		t.Fatalf("NewKafkaSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("NewKafkaSink() returned nil")
	}
	if sink.Name() != "kafka" {
		t.Errorf("Name() = %s, want kafka", sink.Name())
	}

	// Clean up
	_ = sink.Close()
}

func TestNewKafkaSink_FullConfig(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:      []string{"localhost:9092", "localhost:9093"},
		Topic:        "test-topic",
		BatchSize:    200,
		BatchTimeout: 2000,
		BatchBytes:   2097152,
		Compression:  "gzip",
		RequiredAcks: "all",
		Async:        true,
	})
	if err != nil {
		t.Fatalf("NewKafkaSink() error = %v", err)
	}
	defer sink.Close()

	if sink.topic != "test-topic" {
		t.Errorf("topic = %s, want test-topic", sink.topic)
	}
}

func TestNewKafkaSink_CompressionTypes(t *testing.T) {
	compressions := []string{"none", "gzip", "snappy", "lz4", "zstd", ""}

	ctx := context.Background()
	for _, comp := range compressions {
		t.Run("compression_"+comp, func(t *testing.T) {
			sink, err := NewKafkaSink(ctx, &KafkaConfig{
				Brokers:     []string{"localhost:9092"},
				Topic:       "test-topic",
				Compression: comp,
			})
			if err != nil {
				t.Fatalf("NewKafkaSink(compression=%s) error = %v", comp, err)
			}
			sink.Close()
		})
	}
}

func TestNewKafkaSink_RequiredAcks(t *testing.T) {
	acks := []string{"none", "0", "leader", "1", "all", "-1", ""}

	ctx := context.Background()
	for _, ack := range acks {
		t.Run("acks_"+ack, func(t *testing.T) {
			sink, err := NewKafkaSink(ctx, &KafkaConfig{
				Brokers:      []string{"localhost:9092"},
				Topic:        "test-topic",
				RequiredAcks: ack,
			})
			if err != nil {
				t.Fatalf("NewKafkaSink(acks=%s) error = %v", ack, err)
			}
			sink.Close()
		})
	}
}

func TestNewKafkaSink_SASL_Plain(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		SASLMechanism: "plain",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	if err != nil {
		t.Fatalf("NewKafkaSink(SASL=plain) error = %v", err)
	}
	sink.Close()
}

func TestNewKafkaSink_SASL_SCRAM256(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		SASLMechanism: "scram-sha-256",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	if err != nil {
		t.Fatalf("NewKafkaSink(SASL=scram-sha-256) error = %v", err)
	}
	sink.Close()
}

func TestNewKafkaSink_SASL_SCRAM512(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		SASLMechanism: "scram-sha-512",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	if err != nil {
		t.Fatalf("NewKafkaSink(SASL=scram-sha-512) error = %v", err)
	}
	sink.Close()
}

func TestNewKafkaSink_SASL_Invalid(t *testing.T) {
	ctx := context.Background()
	_, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		SASLMechanism: "invalid-mechanism",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	if err == nil {
		t.Error("NewKafkaSink with invalid SASL mechanism should return error")
	}
}

func TestNewKafkaSink_TLS(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:    []string{"localhost:9092"},
		Topic:      "test-topic",
		TLSEnabled: true,
	})
	if err != nil {
		t.Fatalf("NewKafkaSink(TLS=true) error = %v", err)
	}
	sink.Close()
}

func TestKafkaSink_Name(t *testing.T) {
	ctx := context.Background()
	sink, _ := NewKafkaSink(ctx, &KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	defer sink.Close()

	if sink.Name() != "kafka" {
		t.Errorf("Name() = %s, want kafka", sink.Name())
	}
}

func TestKafkaSink_Close_Nil(t *testing.T) {
	sink := &KafkaSink{
		writer: nil,
		topic:  "test",
	}

	err := sink.Close()
	if err != nil {
		t.Errorf("Close() with nil writer should not error, got: %v", err)
	}
}

func TestKafkaSink_Registry(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"brokers": []any{"localhost:9092", "localhost:9093"},
		"topic":   "analytics-events",
	}

	sink, err := Create(ctx, "kafka", config)
	if err != nil {
		t.Fatalf("Create(kafka) error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create(kafka) returned nil")
	}
	defer sink.Close()

	if sink.Name() != "kafka" {
		t.Errorf("Name() = %s, want kafka", sink.Name())
	}
}

func TestKafkaSink_Registry_StringBrokers(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"brokers": []string{"localhost:9092"},
		"topic":   "test-topic",
	}

	sink, err := Create(ctx, "kafka", config)
	if err != nil {
		t.Fatalf("Create(kafka) error = %v", err)
	}
	defer sink.Close()
}

func TestKafkaSink_Registry_FullConfig(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"brokers":        []any{"localhost:9092"},
		"topic":          "test-topic",
		"sasl_mechanism": "plain",
		"sasl_username":  "user",
		"sasl_password":  "pass",
		"tls_enabled":    true,
		"batch_size":     float64(200),
		"batch_timeout":  float64(2000),
		"batch_bytes":    float64(2097152),
		"compression":    "snappy",
		"required_acks":  "all",
		"async":          true,
	}

	sink, err := Create(ctx, "kafka", config)
	if err != nil {
		t.Fatalf("Create(kafka) with full config error = %v", err)
	}
	defer sink.Close()
}

func TestKafkaSink_Registry_MissingBrokers(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "kafka", map[string]any{
		"topic": "test-topic",
	})
	if err == nil {
		t.Error("Create(kafka) without brokers should return error")
	}
}

func TestKafkaSink_Registry_MissingTopic(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "kafka", map[string]any{
		"brokers": []any{"localhost:9092"},
	})
	if err == nil {
		t.Error("Create(kafka) without topic should return error")
	}
}

// Note: Integration tests for Send/SendBatch would require a running Kafka broker.
// These tests verify configuration parsing and object creation.

func TestBuildSASLMechanism_Plain(t *testing.T) {
	config := &KafkaConfig{
		SASLMechanism: "plain",
		SASLUsername:  "testuser",
		SASLPassword:  "testpass",
	}

	mechanism, err := buildSASLMechanism(config)
	if err != nil {
		t.Fatalf("buildSASLMechanism(plain) error = %v", err)
	}
	if mechanism == nil {
		t.Fatal("buildSASLMechanism(plain) returned nil")
	}
}

func TestBuildSASLMechanism_SCRAM256(t *testing.T) {
	config := &KafkaConfig{
		SASLMechanism: "scram-sha-256",
		SASLUsername:  "testuser",
		SASLPassword:  "testpass",
	}

	mechanism, err := buildSASLMechanism(config)
	if err != nil {
		t.Fatalf("buildSASLMechanism(scram-sha-256) error = %v", err)
	}
	if mechanism == nil {
		t.Fatal("buildSASLMechanism(scram-sha-256) returned nil")
	}
}

func TestBuildSASLMechanism_SCRAM512(t *testing.T) {
	config := &KafkaConfig{
		SASLMechanism: "scram-sha-512",
		SASLUsername:  "testuser",
		SASLPassword:  "testpass",
	}

	mechanism, err := buildSASLMechanism(config)
	if err != nil {
		t.Fatalf("buildSASLMechanism(scram-sha-512) error = %v", err)
	}
	if mechanism == nil {
		t.Fatal("buildSASLMechanism(scram-sha-512) returned nil")
	}
}

func TestBuildSASLMechanism_Invalid(t *testing.T) {
	config := &KafkaConfig{
		SASLMechanism: "unsupported",
		SASLUsername:  "testuser",
		SASLPassword:  "testpass",
	}

	_, err := buildSASLMechanism(config)
	if err == nil {
		t.Error("buildSASLMechanism(unsupported) should return error")
	}
}
