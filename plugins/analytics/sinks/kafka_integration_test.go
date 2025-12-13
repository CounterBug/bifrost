//go:build integration

package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestKafkaSink_Integration_SendBatch(t *testing.T) {
	ctx := context.Background()

	// Start Kafka container using default settings
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster-1"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}()

	// Get broker address
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %v", err)
	}

	topicName := "test-analytics-events"

	// Create topic
	if err := createKafkaTopic(brokers[0], topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create sink
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:      brokers,
		Topic:        topicName,
		BatchSize:    10,
		BatchTimeout: 100,
		RequiredAcks: "all",
	})
	if err != nil {
		t.Fatalf("failed to create kafka sink: %v", err)
	}
	defer sink.Close()

	// Send events
	events := []Event{
		&testEvent{id: "event-1", data: "test data 1"},
		&testEvent{id: "event-2", data: "test data 2"},
		&testEvent{id: "event-3", data: "test data 3"},
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	// Consume and verify messages
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   time.Second,
	})
	defer reader.Close()

	// Read the messages we sent
	receivedCount := 0
	deadline := time.Now().Add(10 * time.Second)

	for receivedCount < len(events) && time.Now().Before(deadline) {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := reader.ReadMessage(ctxWithTimeout)
		cancel()
		if err != nil {
			continue
		}

		var received map[string]any
		if err := json.Unmarshal(msg.Value, &received); err != nil {
			t.Errorf("failed to unmarshal message: %v", err)
			continue
		}

		t.Logf("Received message: %s", string(msg.Value))
		receivedCount++
	}

	if receivedCount != len(events) {
		t.Errorf("received %d messages, want %d", receivedCount, len(events))
	}
}

func TestKafkaSink_Integration_Send(t *testing.T) {
	ctx := context.Background()

	// Start Kafka container using default settings
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster-2"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %v", err)
	}

	topicName := "test-single-events"

	// Create topic
	if err := createKafkaTopic(brokers[0], topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create sink
	sink, err := NewKafkaSink(ctx, &KafkaConfig{
		Brokers:      brokers,
		Topic:        topicName,
		RequiredAcks: "leader",
	})
	if err != nil {
		t.Fatalf("failed to create kafka sink: %v", err)
	}
	defer sink.Close()

	// Send single event
	event := &testEvent{id: "single-event", data: "single test data"}
	err = sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	// Verify message was sent
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   time.Second,
	})
	defer reader.Close()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(ctxWithTimeout)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	t.Logf("Received single event: %s", string(msg.Value))
}

func TestKafkaSink_Integration_Compression(t *testing.T) {
	ctx := context.Background()

	// Start Kafka container using default settings
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster-3"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %v", err)
	}

	compressionTypes := []string{"gzip", "snappy", "lz4", "zstd"}

	for _, compression := range compressionTypes {
		t.Run(compression, func(t *testing.T) {
			topicName := "test-compression-" + compression

			// Create topic
			if err := createKafkaTopic(brokers[0], topicName, 1); err != nil {
				t.Fatalf("failed to create topic: %v", err)
			}

			sink, err := NewKafkaSink(ctx, &KafkaConfig{
				Brokers:     brokers,
				Topic:       topicName,
				Compression: compression,
			})
			if err != nil {
				t.Fatalf("failed to create kafka sink with %s compression: %v", compression, err)
			}
			defer sink.Close()

			event := &testEvent{id: "compressed-event", data: "compressed test data for " + compression}
			err = sink.Send(ctx, event)
			if err != nil {
				t.Fatalf("Send() with %s compression error = %v", compression, err)
			}

			t.Logf("Successfully sent message with %s compression", compression)
		})
	}
}

// createKafkaTopic creates a topic with the given name and number of partitions
func createKafkaTopic(broker, topic string, numPartitions int) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafkago.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

// testEvent is a simple event implementation for testing
type testEvent struct {
	id   string
	data string
}

func (e *testEvent) JSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"event_id": e.id,
		"data":     e.data,
	})
}

func (e *testEvent) JSONString() (string, error) {
	data, err := e.JSON()
	if err != nil {
		return "", err
	}
	return string(data), nil
}
