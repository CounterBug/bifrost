package sinks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewWebhookSink_NoConfig(t *testing.T) {
	_, err := NewWebhookSink(nil)
	if err == nil {
		t.Error("NewWebhookSink(nil) should return error")
	}
}

func TestNewWebhookSink_NoURL(t *testing.T) {
	_, err := NewWebhookSink(&WebhookConfig{})
	if err == nil {
		t.Error("NewWebhookSink with empty URL should return error")
	}
}

func TestNewWebhookSink_Valid(t *testing.T) {
	sink, err := NewWebhookSink(&WebhookConfig{
		URL: "http://localhost:8080/events",
	})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}
	if sink == nil {
		t.Fatal("NewWebhookSink() returned nil")
	}
	if sink.Name() != "webhook" {
		t.Errorf("Name() = %s, want webhook", sink.Name())
	}
}

func TestNewWebhookSink_CustomConfig(t *testing.T) {
	sink, err := NewWebhookSink(&WebhookConfig{
		URL:             "http://localhost:8080/events",
		TimeoutMs:       10000,
		MaxConnsPerHost: 50,
		Headers: map[string]string{
			"Authorization": "Bearer token123",
		},
	})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}
	if sink.timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", sink.timeout)
	}
}

func TestWebhookSink_Send(t *testing.T) {
	var receivedBody []byte
	var receivedContentType string
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		receivedContentType = r.Header.Get("Content-Type")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	event := &MockEvent{Data: "webhook-test"}

	err = sink.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if atomic.LoadInt32(&requestCount) != 1 {
		t.Errorf("expected 1 request, got %d", requestCount)
	}
	if receivedContentType != "application/json" {
		t.Errorf("Content-Type = %s, want application/json", receivedContentType)
	}
	if string(receivedBody) != `{"data":"webhook-test"}` {
		t.Errorf("body = %s, want %s", string(receivedBody), `{"data":"webhook-test"}`)
	}
}

func TestWebhookSink_Send_WithHeaders(t *testing.T) {
	var receivedAuth string
	var receivedCustom string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedCustom = r.Header.Get("X-Custom-Header")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{
		URL: server.URL,
		Headers: map[string]string{
			"Authorization":   "Bearer secret-token",
			"X-Custom-Header": "custom-value",
		},
	})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	err = sink.Send(ctx, &MockEvent{Data: "test"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("Authorization = %s, want Bearer secret-token", receivedAuth)
	}
	if receivedCustom != "custom-value" {
		t.Errorf("X-Custom-Header = %s, want custom-value", receivedCustom)
	}
}

func TestWebhookSink_Send_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	err = sink.Send(ctx, &MockEvent{Data: "test"})
	if err == nil {
		t.Error("Send() should return error on 5xx response")
	}
}

func TestWebhookSink_Send_ClientError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	err = sink.Send(ctx, &MockEvent{Data: "test"})
	if err == nil {
		t.Error("Send() should return error on 4xx response")
	}
}

func TestWebhookSink_Send_JSONError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	err = sink.Send(ctx, &MockEvent{Data: "test", ShouldErr: true})
	if err == nil {
		t.Error("Send() should return error when JSON marshaling fails")
	}
}

func TestWebhookSink_SendBatch(t *testing.T) {
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	events := []Event{
		&MockEvent{Data: "event1"},
		&MockEvent{Data: "event2"},
		&MockEvent{Data: "event3"},
	}

	err = sink.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}

	// Verify it's a JSON array
	var parsed []map[string]interface{}
	if err := json.Unmarshal(receivedBody, &parsed); err != nil {
		t.Fatalf("received body is not valid JSON array: %v", err)
	}
	if len(parsed) != 3 {
		t.Errorf("expected 3 events in array, got %d", len(parsed))
	}
}

func TestWebhookSink_SendBatch_Empty(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	err = sink.SendBatch(ctx, []Event{})
	if err != nil {
		t.Fatalf("SendBatch() with empty slice should not error, got: %v", err)
	}

	if atomic.LoadInt32(&requestCount) != 0 {
		t.Error("SendBatch() with empty slice should not make HTTP request")
	}
}

func TestWebhookSink_SendBatch_JSONError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	events := []Event{
		&MockEvent{Data: "good"},
		&MockEvent{Data: "bad", ShouldErr: true},
	}

	err = sink.SendBatch(ctx, events)
	if err == nil {
		t.Error("SendBatch() should return error when JSON marshaling fails")
	}
}

func TestWebhookSink_SendBatch_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sink, err := NewWebhookSink(&WebhookConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	ctx := context.Background()
	events := []Event{&MockEvent{Data: "test"}}

	err = sink.SendBatch(ctx, events)
	if err == nil {
		t.Error("SendBatch() should return error on server error")
	}
}

func TestWebhookSink_Close(t *testing.T) {
	sink, err := NewWebhookSink(&WebhookConfig{URL: "http://localhost:8080"})
	if err != nil {
		t.Fatalf("NewWebhookSink() error = %v", err)
	}

	err = sink.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestWebhookSink_Registry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx := context.Background()
	config := map[string]any{
		"url": server.URL,
		"headers": map[string]any{
			"X-Test": "value",
		},
		"timeout_ms":        float64(5000),
		"max_conns_per_host": float64(50),
	}

	sink, err := Create(ctx, "webhook", config)
	if err != nil {
		t.Fatalf("Create(webhook) error = %v", err)
	}
	if sink == nil {
		t.Fatal("Create(webhook) returned nil")
	}
	if sink.Name() != "webhook" {
		t.Errorf("Name() = %s, want webhook", sink.Name())
	}
}

func TestWebhookSink_Registry_MissingURL(t *testing.T) {
	ctx := context.Background()
	_, err := Create(ctx, "webhook", map[string]any{})
	if err == nil {
		t.Error("Create(webhook) without URL should return error")
	}
}

// Benchmark tests

func BenchmarkWebhookSink_Send(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, _ := NewWebhookSink(&WebhookConfig{URL: server.URL})
	ctx := context.Background()
	event := &MockEvent{Data: "benchmark"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sink.Send(ctx, event)
	}
}

func BenchmarkWebhookSink_SendBatch(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, _ := NewWebhookSink(&WebhookConfig{URL: server.URL})
	ctx := context.Background()

	events := make([]Event, 100)
	for i := range events {
		events[i] = &MockEvent{Data: "batch"}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sink.SendBatch(ctx, events)
	}
}
