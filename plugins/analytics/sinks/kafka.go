package sinks

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// KafkaSink sends events to a Kafka topic
type KafkaSink struct {
	writer *kafka.Writer
	topic  string
}

// KafkaConfig holds configuration for the Kafka sink
type KafkaConfig struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`

	// Topic is the Kafka topic to publish events to
	Topic string `json:"topic"`

	// SASL authentication (optional)
	SASLMechanism string `json:"sasl_mechanism"` // "plain", "scram-sha-256", "scram-sha-512"
	SASLUsername  string `json:"sasl_username"`
	SASLPassword  string `json:"sasl_password"`

	// TLS configuration
	TLSEnabled bool `json:"tls_enabled"`

	// Batching configuration
	BatchSize    int `json:"batch_size"`     // Default: 100
	BatchTimeout int `json:"batch_timeout"`  // Milliseconds, default: 1000
	BatchBytes   int `json:"batch_bytes"`    // Default: 1048576 (1MB)

	// Compression: "none", "gzip", "snappy", "lz4", "zstd"
	Compression string `json:"compression"`

	// RequiredAcks: "none" (0), "leader" (1), "all" (-1)
	RequiredAcks string `json:"required_acks"`

	// Async enables asynchronous writes (fire-and-forget)
	Async bool `json:"async"`
}

// NewKafkaSink creates a new Kafka sink with the given configuration
func NewKafkaSink(ctx context.Context, config *KafkaConfig) (*KafkaSink, error) {
	if config == nil || len(config.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	// Build transport with SASL/TLS if configured
	transport := &kafka.Transport{}

	if config.SASLMechanism != "" {
		mechanism, err := buildSASLMechanism(config)
		if err != nil {
			return nil, err
		}
		transport.SASL = mechanism
	}

	if config.TLSEnabled {
		transport.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	// Determine compression codec
	compression := kafka.Compression(0) // none
	switch config.Compression {
	case "gzip":
		compression = kafka.Gzip
	case "snappy":
		compression = kafka.Snappy
	case "lz4":
		compression = kafka.Lz4
	case "zstd":
		compression = kafka.Zstd
	}

	// Determine required acks
	requiredAcks := kafka.RequireOne // default: leader only
	switch config.RequiredAcks {
	case "none", "0":
		requiredAcks = kafka.RequireNone
	case "all", "-1":
		requiredAcks = kafka.RequireAll
	}

	// Apply defaults
	batchSize := 100
	if config.BatchSize > 0 {
		batchSize = config.BatchSize
	}

	batchTimeout := time.Second
	if config.BatchTimeout > 0 {
		batchTimeout = time.Duration(config.BatchTimeout) * time.Millisecond
	}

	batchBytes := 1048576 // 1MB
	if config.BatchBytes > 0 {
		batchBytes = config.BatchBytes
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Transport:    transport,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		BatchBytes:   int64(batchBytes),
		Compression:  compression,
		RequiredAcks: requiredAcks,
		Async:        config.Async,
	}

	return &KafkaSink{
		writer: writer,
		topic:  config.Topic,
	}, nil
}

// buildSASLMechanism creates the appropriate SASL mechanism
func buildSASLMechanism(config *KafkaConfig) (sasl.Mechanism, error) {
	switch config.SASLMechanism {
	case "plain":
		return &plain.Mechanism{
			Username: config.SASLUsername,
			Password: config.SASLPassword,
		}, nil

	case "scram-sha-256":
		mechanism, err := scram.Mechanism(scram.SHA256, config.SASLUsername, config.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
		}
		return mechanism, nil

	case "scram-sha-512":
		mechanism, err := scram.Mechanism(scram.SHA512, config.SASLUsername, config.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %w", err)
		}
		return mechanism, nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", config.SASLMechanism)
	}
}

// Name returns the sink identifier
func (s *KafkaSink) Name() string {
	return "kafka"
}

// Send delivers a single event to Kafka
func (s *KafkaSink) Send(ctx context.Context, event Event) error {
	data, err := event.JSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Value: data,
		Time:  time.Now(),
	}

	return s.writer.WriteMessages(ctx, msg)
}

// SendBatch delivers multiple events to Kafka efficiently
func (s *KafkaSink) SendBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		data, err := event.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal event %d: %w", i, err)
		}
		messages[i] = kafka.Message{
			Value: data,
			Time:  time.Now(),
		}
	}

	return s.writer.WriteMessages(ctx, messages...)
}

// Close gracefully shuts down the Kafka writer
func (s *KafkaSink) Close() error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

func init() {
	Register("kafka", func(ctx context.Context, config map[string]any) (Sink, error) {
		cfg := &KafkaConfig{}

		// Parse brokers
		if v, ok := config["brokers"].([]any); ok {
			for _, b := range v {
				if s, ok := b.(string); ok {
					cfg.Brokers = append(cfg.Brokers, s)
				}
			}
		}
		if v, ok := config["brokers"].([]string); ok {
			cfg.Brokers = v
		}

		// Parse topic
		if v, ok := config["topic"].(string); ok {
			cfg.Topic = v
		}

		// Parse SASL config
		if v, ok := config["sasl_mechanism"].(string); ok {
			cfg.SASLMechanism = v
		}
		if v, ok := config["sasl_username"].(string); ok {
			cfg.SASLUsername = v
		}
		if v, ok := config["sasl_password"].(string); ok {
			cfg.SASLPassword = v
		}

		// Parse TLS
		if v, ok := config["tls_enabled"].(bool); ok {
			cfg.TLSEnabled = v
		}

		// Parse batching config
		if v, ok := config["batch_size"].(float64); ok {
			cfg.BatchSize = int(v)
		}
		if v, ok := config["batch_timeout"].(float64); ok {
			cfg.BatchTimeout = int(v)
		}
		if v, ok := config["batch_bytes"].(float64); ok {
			cfg.BatchBytes = int(v)
		}

		// Parse compression and acks
		if v, ok := config["compression"].(string); ok {
			cfg.Compression = v
		}
		if v, ok := config["required_acks"].(string); ok {
			cfg.RequiredAcks = v
		}

		// Parse async
		if v, ok := config["async"].(bool); ok {
			cfg.Async = v
		}

		return NewKafkaSink(ctx, cfg)
	})
}
