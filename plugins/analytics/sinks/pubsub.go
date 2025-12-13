package sinks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PubSubSink sends events to a Google Cloud Pub/Sub topic
type PubSubSink struct {
	client   *pubsub.Client
	topic    *pubsub.Topic
	grpcConn *grpc.ClientConn // Only set when using emulator endpoint

	// For collecting publish results
	mu      sync.Mutex
	results []*pubsub.PublishResult
}

// PubSubConfig holds configuration for the Pub/Sub sink
type PubSubConfig struct {
	// ProjectID is the GCP project ID (required)
	ProjectID string `json:"project_id"`

	// TopicID is the Pub/Sub topic ID (required)
	TopicID string `json:"topic_id"`

	// CredentialsFile path to a service account JSON file (optional)
	// If not provided, uses Application Default Credentials
	CredentialsFile string `json:"credentials_file"`

	// CredentialsJSON is the service account JSON content (optional)
	// Alternative to CredentialsFile for secret management
	CredentialsJSON string `json:"credentials_json"`

	// Endpoint allows overriding the default endpoint (useful for emulator testing)
	Endpoint string `json:"endpoint"`

	// OrderingKey enables message ordering within the same key (optional)
	// If set, all messages will use this ordering key
	OrderingKey string `json:"ordering_key"`

	// PublishSettings customization
	BatchSize      int `json:"batch_size"`       // Max messages per batch (default: 100)
	BatchBytes     int `json:"batch_bytes"`      // Max bytes per batch (default: 1MB)
	BatchDelayMs   int `json:"batch_delay_ms"`   // Max delay before publishing (default: 10ms)
	NumGoroutines  int `json:"num_goroutines"`   // Concurrent publish goroutines (default: 25)
	FlowControlMax int `json:"flow_control_max"` // Max outstanding messages (default: 1000)
}

// NewPubSubSink creates a new Pub/Sub sink with the given configuration
func NewPubSubSink(ctx context.Context, cfg *PubSubConfig) (*PubSubSink, error) {
	if cfg == nil {
		return nil, fmt.Errorf("pubsub config is required")
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("pubsub project_id is required")
	}
	if cfg.TopicID == "" {
		return nil, fmt.Errorf("pubsub topic_id is required")
	}

	// Build client options
	var opts []option.ClientOption

	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	} else if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	}

	var grpcConn *grpc.ClientConn
	if cfg.Endpoint != "" {
		// When using an emulator endpoint, use insecure gRPC credentials
		var err error
		grpcConn, err = grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to create grpc connection: %w", err)
		}
		opts = append(opts, option.WithGRPCConn(grpcConn))
		opts = append(opts, option.WithoutAuthentication())
	}

	// Create Pub/Sub client
	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		if grpcConn != nil {
			grpcConn.Close()
		}
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	// Get topic reference
	topic := client.Topic(cfg.TopicID)

	// Configure publish settings
	if cfg.BatchSize > 0 {
		topic.PublishSettings.CountThreshold = cfg.BatchSize
	}
	if cfg.BatchBytes > 0 {
		topic.PublishSettings.ByteThreshold = cfg.BatchBytes
	}
	if cfg.BatchDelayMs > 0 {
		topic.PublishSettings.DelayThreshold = time.Duration(cfg.BatchDelayMs) * time.Millisecond
	}
	if cfg.NumGoroutines > 0 {
		topic.PublishSettings.NumGoroutines = cfg.NumGoroutines
	}
	if cfg.FlowControlMax > 0 {
		topic.PublishSettings.FlowControlSettings.MaxOutstandingMessages = cfg.FlowControlMax
	}

	// Enable ordering if configured
	if cfg.OrderingKey != "" {
		topic.EnableMessageOrdering = true
	}

	return &PubSubSink{
		client:   client,
		topic:    topic,
		grpcConn: grpcConn,
		results:  make([]*pubsub.PublishResult, 0),
	}, nil
}

// Name returns the sink identifier
func (s *PubSubSink) Name() string {
	return "pubsub"
}

// Send delivers a single event to Pub/Sub
func (s *PubSubSink) Send(ctx context.Context, event Event) error {
	data, err := event.JSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &pubsub.Message{
		Data: data,
	}

	result := s.topic.Publish(ctx, msg)

	// Wait for the result synchronously for single sends
	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// SendBatch delivers multiple events to Pub/Sub efficiently
func (s *PubSubSink) SendBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Publish all messages asynchronously
	results := make([]*pubsub.PublishResult, len(events))

	for i, event := range events {
		data, err := event.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal event %d: %w", i, err)
		}

		msg := &pubsub.Message{
			Data: data,
		}

		results[i] = s.topic.Publish(ctx, msg)
	}

	// Wait for all publishes to complete
	var firstErr error
	failCount := 0

	for i, result := range results {
		_, err := result.Get(ctx)
		if err != nil {
			failCount++
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to publish message %d: %w", i, err)
			}
		}
	}

	if firstErr != nil {
		return fmt.Errorf("pubsub: %d of %d messages failed, first error: %w", failCount, len(events), firstErr)
	}

	return nil
}

// Close gracefully shuts down the Pub/Sub client
func (s *PubSubSink) Close() error {
	// Stop accepting new messages and flush pending
	s.topic.Stop()

	// Close the client
	err := s.client.Close()

	// Close the gRPC connection if it was created for emulator mode
	if s.grpcConn != nil {
		if grpcErr := s.grpcConn.Close(); grpcErr != nil && err == nil {
			err = grpcErr
		}
	}

	return err
}

func init() {
	Register("pubsub", func(ctx context.Context, config map[string]any) (Sink, error) {
		cfg := &PubSubConfig{}

		if v, ok := config["project_id"].(string); ok {
			cfg.ProjectID = v
		}
		if v, ok := config["topic_id"].(string); ok {
			cfg.TopicID = v
		}
		if v, ok := config["credentials_file"].(string); ok {
			cfg.CredentialsFile = v
		}
		if v, ok := config["credentials_json"].(string); ok {
			cfg.CredentialsJSON = v
		}
		if v, ok := config["endpoint"].(string); ok {
			cfg.Endpoint = v
		}
		if v, ok := config["ordering_key"].(string); ok {
			cfg.OrderingKey = v
		}
		if v, ok := config["batch_size"].(float64); ok {
			cfg.BatchSize = int(v)
		}
		if v, ok := config["batch_bytes"].(float64); ok {
			cfg.BatchBytes = int(v)
		}
		if v, ok := config["batch_delay_ms"].(float64); ok {
			cfg.BatchDelayMs = int(v)
		}
		if v, ok := config["num_goroutines"].(float64); ok {
			cfg.NumGoroutines = int(v)
		}
		if v, ok := config["flow_control_max"].(float64); ok {
			cfg.FlowControlMax = int(v)
		}

		return NewPubSubSink(ctx, cfg)
	})
}
