//go:build integration

package sinks

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func TestKinesisSink_Integration_SendBatch(t *testing.T) {
	ctx := context.Background()

	// Start LocalStack container
	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:3.0")
	if err != nil {
		t.Fatalf("failed to start localstack container: %v", err)
	}
	defer func() {
		if err := localstackContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate localstack container: %v", err)
		}
	}()

	// Get endpoint
	endpoint, err := localstackContainer.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		t.Fatalf("failed to get localstack endpoint: %v", err)
	}

	streamName := "test-analytics-stream"

	// Create Kinesis client for setup
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	kinesisClient := kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Create stream
	_, err = kinesisClient.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Wait for stream to become active
	waiter := kinesis.NewStreamExistsWaiter(kinesisClient)
	err = waiter.Wait(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}, 30*time.Second)
	if err != nil {
		t.Fatalf("stream did not become active: %v", err)
	}

	// Create sink
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      streamName,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		Endpoint:        endpoint,
	})
	if err != nil {
		t.Fatalf("failed to create kinesis sink: %v", err)
	}
	defer sink.Close()

	// Send events
	events := []Event{
		&testEvent{id: "kinesis-event-1", data: "kinesis data 1"},
		&testEvent{id: "kinesis-event-2", data: "kinesis data 2"},
		&testEvent{id: "kinesis-event-3", data: "kinesis data 3"},
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	// Get shard iterator
	shardIteratorOutput, err := kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("failed to get shard iterator: %v", err)
	}

	// Read records
	records, err := kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: shardIteratorOutput.ShardIterator,
		Limit:         aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("failed to get records: %v", err)
	}

	if len(records.Records) != len(events) {
		t.Errorf("got %d records, want %d", len(records.Records), len(events))
	}

	for i, record := range records.Records {
		var received map[string]any
		if err := json.Unmarshal(record.Data, &received); err != nil {
			t.Errorf("failed to unmarshal record %d: %v", i, err)
			continue
		}
		t.Logf("Received record %d: %s", i, string(record.Data))
	}
}

func TestKinesisSink_Integration_Send(t *testing.T) {
	ctx := context.Background()

	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:3.0")
	if err != nil {
		t.Fatalf("failed to start localstack container: %v", err)
	}
	defer func() {
		if err := localstackContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate localstack container: %v", err)
		}
	}()

	endpoint, err := localstackContainer.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		t.Fatalf("failed to get localstack endpoint: %v", err)
	}

	streamName := "test-single-stream"

	// Create Kinesis client for setup
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	kinesisClient := kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Create stream
	_, err = kinesisClient.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	waiter := kinesis.NewStreamExistsWaiter(kinesisClient)
	err = waiter.Wait(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}, 30*time.Second)
	if err != nil {
		t.Fatalf("stream did not become active: %v", err)
	}

	// Create sink
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      streamName,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		Endpoint:        endpoint,
	})
	if err != nil {
		t.Fatalf("failed to create kinesis sink: %v", err)
	}
	defer sink.Close()

	// Send single event
	event := &testEvent{id: "kinesis-single", data: "single kinesis data"}
	err = sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	// Get shard iterator
	shardIteratorOutput, err := kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("failed to get shard iterator: %v", err)
	}

	// Read records
	records, err := kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: shardIteratorOutput.ShardIterator,
		Limit:         aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("failed to get records: %v", err)
	}

	if len(records.Records) != 1 {
		t.Errorf("got %d records, want 1", len(records.Records))
	}

	if len(records.Records) > 0 {
		t.Logf("Received single record: %s", string(records.Records[0].Data))
	}
}

func TestKinesisSink_Integration_LargeBatch(t *testing.T) {
	ctx := context.Background()

	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:3.0")
	if err != nil {
		t.Fatalf("failed to start localstack container: %v", err)
	}
	defer func() {
		if err := localstackContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate localstack container: %v", err)
		}
	}()

	endpoint, err := localstackContainer.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		t.Fatalf("failed to get localstack endpoint: %v", err)
	}

	streamName := "test-large-batch-stream"

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	kinesisClient := kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	_, err = kinesisClient.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	waiter := kinesis.NewStreamExistsWaiter(kinesisClient)
	err = waiter.Wait(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}, 30*time.Second)
	if err != nil {
		t.Fatalf("stream did not become active: %v", err)
	}

	// Create sink
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      streamName,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		Endpoint:        endpoint,
	})
	if err != nil {
		t.Fatalf("failed to create kinesis sink: %v", err)
	}
	defer sink.Close()

	// Send a batch larger than Kinesis limit (500 records)
	// This tests the batching logic in SendBatch
	events := make([]Event, 600)
	for i := 0; i < 600; i++ {
		events[i] = &testEvent{
			id:   "large-batch-event-" + string(rune('0'+i%10)),
			data: "batch data",
		}
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() with large batch error = %v", err)
	}

	// Get shard iterator and read all records
	shardIteratorOutput, err := kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("failed to get shard iterator: %v", err)
	}

	// Read all records (may need multiple calls)
	totalRecords := 0
	shardIterator := shardIteratorOutput.ShardIterator
	for shardIterator != nil && totalRecords < len(events) {
		records, err := kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         aws.Int32(1000),
		})
		if err != nil {
			t.Fatalf("failed to get records: %v", err)
		}
		totalRecords += len(records.Records)
		shardIterator = records.NextShardIterator

		if len(records.Records) == 0 {
			break
		}
	}

	if totalRecords != len(events) {
		t.Errorf("got %d records, want %d", totalRecords, len(events))
	} else {
		t.Logf("Successfully sent and received %d records", totalRecords)
	}
}
