package sinks

import (
	"context"
	"testing"
)

func TestNewKinesisSink_NoConfig(t *testing.T) {
	ctx := context.Background()
	_, err := NewKinesisSink(ctx, nil)
	if err == nil {
		t.Error("NewKinesisSink(nil) should return error")
	}
}

func TestNewKinesisSink_NoStreamName(t *testing.T) {
	ctx := context.Background()
	_, err := NewKinesisSink(ctx, &KinesisConfig{
		Region: "us-east-1",
	})
	if err == nil {
		t.Error("NewKinesisSink without stream_name should return error")
	}
}

func TestNewKinesisSink_NoRegion(t *testing.T) {
	ctx := context.Background()
	_, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName: "test-stream",
	})
	if err == nil {
		t.Error("NewKinesisSink without region should return error")
	}
}

func TestNewKinesisSink_ValidWithCredentials(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      "test-stream",
		Region:          "us-east-1",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	if err != nil {
		t.Fatalf("NewKinesisSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("NewKinesisSink() returned nil")
	}
	if sink.Name() != "kinesis" {
		t.Errorf("Name() = %s, want kinesis", sink.Name())
	}
}

func TestNewKinesisSink_WithEndpoint(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      "test-stream",
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		Endpoint:        "http://localhost:4566", // LocalStack endpoint
	})
	if err != nil {
		t.Fatalf("NewKinesisSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("NewKinesisSink() returned nil")
	}
}

func TestNewKinesisSink_WithSessionToken(t *testing.T) {
	ctx := context.Background()
	sink, err := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      "test-stream",
		Region:          "us-west-2",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "FwoGZXIvYXdzEBYaDEXAMPLE",
	})
	if err != nil {
		t.Fatalf("NewKinesisSink() error = %v", err)
	}
	defer sink.Close()

	if sink.streamName != "test-stream" {
		t.Errorf("streamName = %s, want test-stream", sink.streamName)
	}
}

func TestKinesisSink_Name(t *testing.T) {
	ctx := context.Background()
	sink, _ := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      "test-stream",
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	})
	defer sink.Close()

	if sink.Name() != "kinesis" {
		t.Errorf("Name() = %s, want kinesis", sink.Name())
	}
}

func TestKinesisSink_Close(t *testing.T) {
	ctx := context.Background()
	sink, _ := NewKinesisSink(ctx, &KinesisConfig{
		StreamName:      "test-stream",
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	})

	err := sink.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestKinesisSink_Registry(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"stream_name":       "analytics-stream",
		"region":            "us-east-1",
		"access_key_id":     "AKIAIOSFODNN7EXAMPLE",
		"secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	sink, err := Create(ctx, "kinesis", config)
	if err != nil {
		t.Fatalf("Create(kinesis) error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create(kinesis) returned nil")
	}
	defer sink.Close()

	if sink.Name() != "kinesis" {
		t.Errorf("Name() = %s, want kinesis", sink.Name())
	}
}

func TestKinesisSink_Registry_FullConfig(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"stream_name":         "analytics-stream",
		"region":              "eu-west-1",
		"access_key_id":       "AKIAIOSFODNN7EXAMPLE",
		"secret_access_key":   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"session_token":       "FwoGZXIvYXdzEBYaDEXAMPLE",
		"endpoint":            "http://localhost:4566",
		"partition_key_field": "request_id",
	}

	sink, err := Create(ctx, "kinesis", config)
	if err != nil {
		t.Fatalf("Create(kinesis) with full config error = %v", err)
	}
	defer sink.Close()
}

func TestKinesisSink_Registry_MissingStreamName(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "kinesis", map[string]any{
		"region":            "us-east-1",
		"access_key_id":     "test",
		"secret_access_key": "test",
	})
	if err == nil {
		t.Error("Create(kinesis) without stream_name should return error")
	}
}

func TestKinesisSink_Registry_MissingRegion(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "kinesis", map[string]any{
		"stream_name":       "test-stream",
		"access_key_id":     "test",
		"secret_access_key": "test",
	})
	if err == nil {
		t.Error("Create(kinesis) without region should return error")
	}
}

// Note: Integration tests for Send/SendBatch would require a running Kinesis or LocalStack.
// These tests verify configuration parsing and object creation.
