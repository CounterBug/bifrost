package sinks

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/valyala/fasthttp"
)

// WebhookSink sends events to an HTTP endpoint
type WebhookSink struct {
	client  *fasthttp.Client
	url     string
	headers map[string]string
	timeout time.Duration
}

// WebhookConfig holds configuration for the webhook sink
type WebhookConfig struct {
	// URL is the HTTP endpoint to send events to
	URL string `json:"url"`

	// Headers are additional HTTP headers to include in requests
	Headers map[string]string `json:"headers"`

	// TimeoutMs is the request timeout in milliseconds (default: 5000)
	TimeoutMs int `json:"timeout_ms"`

	// MaxConnsPerHost limits concurrent connections (default: 100)
	MaxConnsPerHost int `json:"max_conns_per_host"`
}

// NewWebhookSink creates a new webhook sink with the given configuration
func NewWebhookSink(config *WebhookConfig) (*WebhookSink, error) {
	if config == nil || config.URL == "" {
		return nil, fmt.Errorf("webhook URL is required")
	}

	timeout := 5 * time.Second
	if config.TimeoutMs > 0 {
		timeout = time.Duration(config.TimeoutMs) * time.Millisecond
	}

	maxConns := 100
	if config.MaxConnsPerHost > 0 {
		maxConns = config.MaxConnsPerHost
	}

	client := &fasthttp.Client{
		MaxConnsPerHost:     maxConns,
		ReadTimeout:         timeout,
		WriteTimeout:        timeout,
		MaxIdleConnDuration: 30 * time.Second,
	}

	return &WebhookSink{
		client:  client,
		url:     config.URL,
		headers: config.Headers,
		timeout: timeout,
	}, nil
}

// Name returns the sink identifier
func (s *WebhookSink) Name() string {
	return "webhook"
}

// Send delivers a single event to the webhook endpoint
func (s *WebhookSink) Send(ctx context.Context, event Event) error {
	data, err := event.JSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(s.url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")

	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	req.SetBody(data)

	if err := s.client.DoTimeout(req, resp, s.timeout); err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode(), string(resp.Body()))
	}

	return nil
}

// SendBatch delivers multiple events to the webhook endpoint as a JSON array
func (s *WebhookSink) SendBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Build JSON array manually for efficiency
	var buf bytes.Buffer
	buf.WriteByte('[')

	for i, event := range events {
		if i > 0 {
			buf.WriteByte(',')
		}
		data, err := event.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal event %d: %w", i, err)
		}
		buf.Write(data)
	}

	buf.WriteByte(']')

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(s.url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")

	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	req.SetBody(buf.Bytes())

	if err := s.client.DoTimeout(req, resp, s.timeout); err != nil {
		return fmt.Errorf("webhook batch request failed: %w", err)
	}

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode(), string(resp.Body()))
	}

	return nil
}

// Close releases resources
func (s *WebhookSink) Close() error {
	// fasthttp.Client doesn't need explicit cleanup
	return nil
}

func init() {
	Register("webhook", func(ctx context.Context, config map[string]any) (Sink, error) {
		cfg := &WebhookConfig{}

		if v, ok := config["url"].(string); ok {
			cfg.URL = v
		}
		if v, ok := config["headers"].(map[string]any); ok {
			cfg.Headers = make(map[string]string)
			for k, val := range v {
				if s, ok := val.(string); ok {
					cfg.Headers[k] = s
				}
			}
		}
		if v, ok := config["timeout_ms"].(float64); ok {
			cfg.TimeoutMs = int(v)
		}
		if v, ok := config["max_conns_per_host"].(float64); ok {
			cfg.MaxConnsPerHost = int(v)
		}

		return NewWebhookSink(cfg)
	})
}
