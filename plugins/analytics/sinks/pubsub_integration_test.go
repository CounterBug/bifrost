//go:build integration

package sinks

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPubSubSink_Integration_SendBatch(t *testing.T) {
	ctx := context.Background()

	// Start Pub/Sub emulator container
	container, endpoint := startPubSubEmulator(t, ctx)
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate pubsub emulator: %v", err)
		}
	}()

	projectID := "test-project"
	topicID := "test-analytics-topic"
	subscriptionID := "test-subscription"

	// Create client for setup
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create grpc connection: %v", err)
	}
	defer conn.Close()

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	// Create topic
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create subscription for reading
	sub, err := client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}

	// Create sink
	sink, err := NewPubSubSink(ctx, &PubSubConfig{
		ProjectID: projectID,
		TopicID:   topicID,
		Endpoint:  endpoint,
	})
	if err != nil {
		t.Fatalf("failed to create pubsub sink: %v", err)
	}
	defer sink.Close()

	// Send events
	events := []Event{
		&testEvent{id: "pubsub-event-1", data: "pubsub data 1"},
		&testEvent{id: "pubsub-event-2", data: "pubsub data 2"},
		&testEvent{id: "pubsub-event-3", data: "pubsub data 3"},
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	// Receive and verify messages
	receivedCount := 0
	receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		var received map[string]any
		if err := json.Unmarshal(msg.Data, &received); err != nil {
			t.Errorf("failed to unmarshal message: %v", err)
		} else {
			t.Logf("Received message: %s", string(msg.Data))
			receivedCount++
		}
		msg.Ack()
		if receivedCount >= len(events) {
			cancel()
		}
	})

	if err != nil && err != context.Canceled {
		t.Logf("receive ended: %v", err)
	}

	if receivedCount != len(events) {
		t.Errorf("received %d messages, want %d", receivedCount, len(events))
	}
}

func TestPubSubSink_Integration_Send(t *testing.T) {
	ctx := context.Background()

	container, endpoint := startPubSubEmulator(t, ctx)
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate pubsub emulator: %v", err)
		}
	}()

	projectID := "test-project"
	topicID := "test-single-topic"
	subscriptionID := "test-single-sub"

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create grpc connection: %v", err)
	}
	defer conn.Close()

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	sub, err := client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}

	// Create sink
	sink, err := NewPubSubSink(ctx, &PubSubConfig{
		ProjectID: projectID,
		TopicID:   topicID,
		Endpoint:  endpoint,
	})
	if err != nil {
		t.Fatalf("failed to create pubsub sink: %v", err)
	}
	defer sink.Close()

	// Send single event
	event := &testEvent{id: "pubsub-single", data: "single pubsub data"}
	err = sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	// Receive message
	receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	received := false
	err = sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		t.Logf("Received single event: %s", string(msg.Data))
		received = true
		msg.Ack()
		cancel()
	})

	if !received {
		t.Error("did not receive the sent message")
	}
}

func TestPubSubSink_Integration_BatchSettings(t *testing.T) {
	ctx := context.Background()

	container, endpoint := startPubSubEmulator(t, ctx)
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate pubsub emulator: %v", err)
		}
	}()

	projectID := "test-project"
	topicID := "test-batch-settings-topic"
	subscriptionID := "test-batch-settings-sub"

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create grpc connection: %v", err)
	}
	defer conn.Close()

	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	sub, err := client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}

	// Create sink with custom batch settings
	sink, err := NewPubSubSink(ctx, &PubSubConfig{
		ProjectID:      projectID,
		TopicID:        topicID,
		Endpoint:       endpoint,
		BatchSize:      10,
		BatchBytes:     1024 * 1024,
		BatchDelayMs:   50,
		NumGoroutines:  4,
		FlowControlMax: 100,
	})
	if err != nil {
		t.Fatalf("failed to create pubsub sink with batch settings: %v", err)
	}
	defer sink.Close()

	// Send multiple events
	events := make([]Event, 20)
	for i := 0; i < 20; i++ {
		events[i] = &testEvent{
			id:   "batch-event-" + string(rune('0'+i%10)),
			data: "batch data",
		}
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	// Receive all messages
	receivedCount := 0
	receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		receivedCount++
		msg.Ack()
		if receivedCount >= len(events) {
			cancel()
		}
	})

	if receivedCount != len(events) {
		t.Errorf("received %d messages, want %d", receivedCount, len(events))
	} else {
		t.Logf("Successfully sent and received %d messages with custom batch settings", receivedCount)
	}
}

// startPubSubEmulator starts a Pub/Sub emulator container and returns the container and endpoint
func startPubSubEmulator(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
		WaitingFor:   wait.ForLog("Server started").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start pubsub emulator: %v", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "8085/tcp", "")
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get pubsub emulator endpoint: %v", err)
	}

	return container, endpoint
}
