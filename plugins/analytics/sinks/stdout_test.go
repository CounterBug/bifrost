package sinks

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestNewStdoutSink_Default(t *testing.T) {
	sink := NewStdoutSink(nil)

	if sink == nil {
		t.Fatal("NewStdoutSink(nil) returned nil")
	}
	if sink.Name() != "stdout" {
		t.Errorf("Name() = %s, want stdout", sink.Name())
	}
}

func TestNewStdoutSink_WithConfig(t *testing.T) {
	config := &StdoutConfig{
		Pretty: true,
		Output: "stderr",
	}
	sink := NewStdoutSink(config)

	if sink == nil {
		t.Fatal("NewStdoutSink() returned nil")
	}
	if !sink.pretty {
		t.Error("pretty should be true")
	}
}

func TestStdoutSink_Send(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	event := &MockEvent{Data: "test-event"}

	err := sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "[analytics]") {
		t.Errorf("output should contain [analytics] prefix, got: %s", output)
	}
	if !strings.Contains(output, "test-event") {
		t.Errorf("output should contain event data, got: %s", output)
	}
	if !strings.HasSuffix(output, "\n") {
		t.Error("output should end with newline")
	}
}

func TestStdoutSink_Send_JSONError(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	event := &MockEvent{Data: "test", ShouldErr: true}

	err := sink.Send(ctx, event)
	if err == nil {
		t.Error("Send() should return error when JSON marshaling fails")
	}
}

func TestStdoutSink_SendBatch(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

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

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Errorf("SendBatch() should produce 3 lines, got %d", len(lines))
	}

	for i, line := range lines {
		if !strings.Contains(line, "[analytics]") {
			t.Errorf("line %d should contain [analytics] prefix", i)
		}
	}
}

func TestStdoutSink_SendBatch_Empty(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	err := sink.SendBatch(ctx, []Event{})
	if err != nil {
		t.Fatalf("SendBatch() with empty slice should not error, got: %v", err)
	}

	if buf.Len() != 0 {
		t.Error("SendBatch() with empty slice should produce no output")
	}
}

func TestStdoutSink_SendBatch_JSONError(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	events := []Event{
		&MockEvent{Data: "good"},
		&MockEvent{Data: "bad", ShouldErr: true},
	}

	err := sink.SendBatch(ctx, events)
	if err == nil {
		t.Error("SendBatch() should return error when JSON marshaling fails")
	}
}

func TestStdoutSink_Close(t *testing.T) {
	sink := NewStdoutSink(nil)
	err := sink.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestStdoutSink_SetWriter(t *testing.T) {
	sink := NewStdoutSink(nil)

	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	sink.SetWriter(buf1)
	ctx := context.Background()

	_ = sink.Send(ctx, &MockEvent{Data: "first"})
	if buf1.Len() == 0 {
		t.Error("first buffer should have content")
	}

	sink.SetWriter(buf2)
	_ = sink.Send(ctx, &MockEvent{Data: "second"})
	if buf2.Len() == 0 {
		t.Error("second buffer should have content")
	}
	if !strings.Contains(buf2.String(), "second") {
		t.Error("second buffer should contain 'second'")
	}
}

func TestStdoutSink_Registry(t *testing.T) {
	// The stdout sink should be auto-registered via init()
	ctx := context.Background()
	sink, err := Create(ctx, "stdout", nil)
	if err != nil {
		t.Fatalf("Create(stdout) error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create(stdout) returned nil")
	}
	if sink.Name() != "stdout" {
		t.Errorf("Name() = %s, want stdout", sink.Name())
	}
}

func TestStdoutSink_Registry_WithConfig(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{
		"pretty": true,
		"output": "stderr",
	}

	sink, err := Create(ctx, "stdout", config)
	if err != nil {
		t.Fatalf("Create(stdout) error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create(stdout) returned nil")
	}
}

func TestStdoutSink_ConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	done := make(chan bool)

	// Spawn multiple goroutines writing concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				_ = sink.Send(ctx, &MockEvent{Data: "concurrent"})
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify output contains expected number of lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1000 {
		t.Errorf("Expected 1000 lines, got %d", len(lines))
	}
}

// Benchmark tests

func BenchmarkStdoutSink_Send(b *testing.B) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	event := &MockEvent{Data: "benchmark-event"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sink.Send(ctx, event)
	}
}

func BenchmarkStdoutSink_SendBatch(b *testing.B) {
	var buf bytes.Buffer
	sink := NewStdoutSink(nil)
	sink.SetWriter(&buf)

	ctx := context.Background()
	events := make([]Event, 100)
	for i := range events {
		events[i] = &MockEvent{Data: "batch-event"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = sink.SendBatch(ctx, events)
	}
}
