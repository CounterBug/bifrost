package sinks

import (
	"context"
	"errors"
	"testing"
)

// MockEvent implements the Event interface for testing
type MockEvent struct {
	Data      string
	ShouldErr bool
}

func (m *MockEvent) JSON() ([]byte, error) {
	if m.ShouldErr {
		return nil, errors.New("mock JSON error")
	}
	return []byte(`{"data":"` + m.Data + `"}`), nil
}

// MockSink implements the Sink interface for testing
type MockSink struct {
	name       string
	events     []Event
	batches    [][]Event
	closed     bool
	sendErr    error
	batchErr   error
	closeErr   error
}

func NewMockSink(name string) *MockSink {
	return &MockSink{
		name:    name,
		events:  make([]Event, 0),
		batches: make([][]Event, 0),
	}
}

func (m *MockSink) Name() string {
	return m.name
}

func (m *MockSink) Send(ctx context.Context, event Event) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.events = append(m.events, event)
	return nil
}

func (m *MockSink) SendBatch(ctx context.Context, events []Event) error {
	if m.batchErr != nil {
		return m.batchErr
	}
	m.batches = append(m.batches, events)
	return nil
}

func (m *MockSink) Close() error {
	m.closed = true
	return m.closeErr
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BufferSize != 100 {
		t.Errorf("BufferSize = %d, want 100", cfg.BufferSize)
	}
	if cfg.FlushIntervalMs != 1000 {
		t.Errorf("FlushIntervalMs = %d, want 1000", cfg.FlushIntervalMs)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.RetryBackoffMs != 100 {
		t.Errorf("RetryBackoffMs = %d, want 100", cfg.RetryBackoffMs)
	}
}

func TestRegisterAndCreate(t *testing.T) {
	// Register a test sink
	testSinkName := "test-sink-register"
	Register(testSinkName, func(ctx context.Context, config map[string]any) (Sink, error) {
		return NewMockSink(testSinkName), nil
	})

	// Verify it's in the available list
	available := Available()
	found := false
	for _, name := range available {
		if name == testSinkName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Registered sink %s not found in Available()", testSinkName)
	}

	// Create the sink
	ctx := context.Background()
	sink, err := Create(ctx, testSinkName, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create() returned nil sink")
	}
	if sink.Name() != testSinkName {
		t.Errorf("sink.Name() = %s, want %s", sink.Name(), testSinkName)
	}
}

func TestCreate_UnknownSink(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "unknown-sink-type-xyz", nil)
	if err == nil {
		t.Error("Create() should return error for unknown sink type")
	}
}

func TestCreate_FactoryError(t *testing.T) {
	testSinkName := "test-sink-error"
	expectedErr := errors.New("factory error")

	Register(testSinkName, func(ctx context.Context, config map[string]any) (Sink, error) {
		return nil, expectedErr
	})

	ctx := context.Background()
	_, err := Create(ctx, testSinkName, nil)
	if err == nil {
		t.Error("Create() should return error when factory fails")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Create() error = %v, want %v", err, expectedErr)
	}
}

func TestAvailable(t *testing.T) {
	// Register a few test sinks
	Register("available-test-1", func(ctx context.Context, config map[string]any) (Sink, error) {
		return NewMockSink("available-test-1"), nil
	})
	Register("available-test-2", func(ctx context.Context, config map[string]any) (Sink, error) {
		return NewMockSink("available-test-2"), nil
	})

	available := Available()
	if len(available) < 2 {
		t.Errorf("Available() returned %d sinks, expected at least 2", len(available))
	}
}

func TestMockSink_Send(t *testing.T) {
	sink := NewMockSink("mock")
	ctx := context.Background()
	event := &MockEvent{Data: "test"}

	err := sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if len(sink.events) != 1 {
		t.Errorf("events count = %d, want 1", len(sink.events))
	}
}

func TestMockSink_SendBatch(t *testing.T) {
	sink := NewMockSink("mock")
	ctx := context.Background()
	events := []Event{
		&MockEvent{Data: "event1"},
		&MockEvent{Data: "event2"},
		&MockEvent{Data: "event3"},
	}

	err := sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	if len(sink.batches) != 1 {
		t.Errorf("batches count = %d, want 1", len(sink.batches))
	}
	if len(sink.batches[0]) != 3 {
		t.Errorf("batch[0] count = %d, want 3", len(sink.batches[0]))
	}
}

func TestMockSink_Close(t *testing.T) {
	sink := NewMockSink("mock")

	if sink.closed {
		t.Error("sink should not be closed initially")
	}

	err := sink.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if !sink.closed {
		t.Error("sink should be closed after Close()")
	}
}

func TestMockSink_SendError(t *testing.T) {
	sink := NewMockSink("mock")
	sink.sendErr = errors.New("send failed")
	ctx := context.Background()

	err := sink.Send(ctx, &MockEvent{Data: "test"})
	if err == nil {
		t.Error("Send() should return error")
	}
}

func TestMockSink_BatchError(t *testing.T) {
	sink := NewMockSink("mock")
	sink.batchErr = errors.New("batch failed")
	ctx := context.Background()

	err := sink.SendBatch(ctx, []Event{&MockEvent{Data: "test"}})
	if err == nil {
		t.Error("SendBatch() should return error")
	}
}

func TestMockEvent_JSON(t *testing.T) {
	event := &MockEvent{Data: "hello"}
	data, err := event.JSON()
	if err != nil {
		t.Fatalf("JSON() error = %v", err)
	}

	expected := `{"data":"hello"}`
	if string(data) != expected {
		t.Errorf("JSON() = %s, want %s", string(data), expected)
	}
}

func TestMockEvent_JSONError(t *testing.T) {
	event := &MockEvent{Data: "test", ShouldErr: true}
	_, err := event.JSON()
	if err == nil {
		t.Error("JSON() should return error when ShouldErr is true")
	}
}
