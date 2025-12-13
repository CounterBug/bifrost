package sinks

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
)

// KinesisSink sends events to an AWS Kinesis stream
type KinesisSink struct {
	client     *kinesis.Client
	streamName string
}

// KinesisConfig holds configuration for the Kinesis sink
type KinesisConfig struct {
	// StreamName is the Kinesis stream name (required)
	StreamName string `json:"stream_name"`

	// Region is the AWS region (required)
	Region string `json:"region"`

	// Credentials (optional - uses default credential chain if not provided)
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"` // For temporary credentials

	// Endpoint allows overriding the default endpoint (useful for LocalStack testing)
	Endpoint string `json:"endpoint"`

	// PartitionKeyField specifies which event field to use as partition key
	// If empty, a random UUID is used for each record
	PartitionKeyField string `json:"partition_key_field"`
}

// NewKinesisSink creates a new Kinesis sink with the given configuration
func NewKinesisSink(ctx context.Context, cfg *KinesisConfig) (*KinesisSink, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kinesis config is required")
	}
	if cfg.StreamName == "" {
		return nil, fmt.Errorf("kinesis stream_name is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("kinesis region is required")
	}

	// Build AWS config options
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	// Use explicit credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				cfg.SessionToken,
			),
		))
	}

	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build Kinesis client options
	var clientOpts []func(*kinesis.Options)
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *kinesis.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	client := kinesis.NewFromConfig(awsCfg, clientOpts...)

	return &KinesisSink{
		client:     client,
		streamName: cfg.StreamName,
	}, nil
}

// Name returns the sink identifier
func (s *KinesisSink) Name() string {
	return "kinesis"
}

// Send delivers a single event to Kinesis
func (s *KinesisSink) Send(ctx context.Context, event Event) error {
	data, err := event.JSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	input := &kinesis.PutRecordInput{
		StreamName:   aws.String(s.streamName),
		Data:         data,
		PartitionKey: aws.String(uuid.New().String()),
	}

	_, err = s.client.PutRecord(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put record to Kinesis: %w", err)
	}

	return nil
}

// SendBatch delivers multiple events to Kinesis efficiently using PutRecords
func (s *KinesisSink) SendBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Kinesis PutRecords has a limit of 500 records per request
	const maxBatchSize = 500

	for i := 0; i < len(events); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]

		if err := s.sendBatchInternal(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// sendBatchInternal sends a single batch (up to 500 records)
func (s *KinesisSink) sendBatchInternal(ctx context.Context, events []Event) error {
	records := make([]types.PutRecordsRequestEntry, len(events))
	timestamp := time.Now().UnixNano()

	for i, event := range events {
		data, err := event.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal event %d: %w", i, err)
		}

		// Use timestamp + index for better distribution across shards
		partitionKey := fmt.Sprintf("%d-%d", timestamp, i)

		records[i] = types.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(partitionKey),
		}
	}

	input := &kinesis.PutRecordsInput{
		StreamName: aws.String(s.streamName),
		Records:    records,
	}

	output, err := s.client.PutRecords(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put records to Kinesis: %w", err)
	}

	// Check for partial failures
	if output.FailedRecordCount != nil && *output.FailedRecordCount > 0 {
		return fmt.Errorf("kinesis: %d of %d records failed", *output.FailedRecordCount, len(records))
	}

	return nil
}

// Close gracefully shuts down the Kinesis client
func (s *KinesisSink) Close() error {
	// AWS SDK v2 clients don't require explicit cleanup
	return nil
}

func init() {
	Register("kinesis", func(ctx context.Context, config map[string]any) (Sink, error) {
		cfg := &KinesisConfig{}

		if v, ok := config["stream_name"].(string); ok {
			cfg.StreamName = v
		}
		if v, ok := config["region"].(string); ok {
			cfg.Region = v
		}
		if v, ok := config["access_key_id"].(string); ok {
			cfg.AccessKeyID = v
		}
		if v, ok := config["secret_access_key"].(string); ok {
			cfg.SecretAccessKey = v
		}
		if v, ok := config["session_token"].(string); ok {
			cfg.SessionToken = v
		}
		if v, ok := config["endpoint"].(string); ok {
			cfg.Endpoint = v
		}
		if v, ok := config["partition_key_field"].(string); ok {
			cfg.PartitionKeyField = v
		}

		return NewKinesisSink(ctx, cfg)
	})
}
