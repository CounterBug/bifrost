package sinks

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// StdoutSink writes events to stdout (or a configured writer) for debugging and testing.
type StdoutSink struct {
	writer io.Writer
	mu     sync.Mutex
	pretty bool
}

// StdoutConfig holds configuration for the stdout sink
type StdoutConfig struct {
	// Pretty enables indented JSON output (slower, but more readable)
	Pretty bool `json:"pretty"`

	// Output specifies the output destination: "stdout" or "stderr"
	Output string `json:"output"`
}

// NewStdoutSink creates a new stdout sink with the given configuration
func NewStdoutSink(config *StdoutConfig) *StdoutSink {
	writer := os.Stdout
	if config != nil && config.Output == "stderr" {
		writer = os.Stderr
	}

	pretty := false
	if config != nil {
		pretty = config.Pretty
	}

	return &StdoutSink{
		writer: writer,
		pretty: pretty,
	}
}

// Name returns the sink identifier
func (s *StdoutSink) Name() string {
	return "stdout"
}

// Send writes a single event to stdout
func (s *StdoutSink) Send(ctx context.Context, event Event) error {
	data, err := event.JSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = fmt.Fprintf(s.writer, "[analytics] %s\n", string(data))
	return err
}

// SendBatch writes multiple events to stdout
func (s *StdoutSink) SendBatch(ctx context.Context, events []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		data, err := event.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}
		if _, err := fmt.Fprintf(s.writer, "[analytics] %s\n", string(data)); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for stdout sink
func (s *StdoutSink) Close() error {
	return nil
}

// SetWriter allows changing the output writer (useful for testing)
func (s *StdoutSink) SetWriter(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer = w
}

func init() {
	Register("stdout", func(ctx context.Context, config map[string]any) (Sink, error) {
		cfg := &StdoutConfig{}
		if v, ok := config["pretty"].(bool); ok {
			cfg.Pretty = v
		}
		if v, ok := config["output"].(string); ok {
			cfg.Output = v
		}
		return NewStdoutSink(cfg), nil
	})
}
