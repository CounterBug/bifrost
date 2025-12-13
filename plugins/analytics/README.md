# Bifrost Analytics Plugin

A high-performance analytics plugin for [Bifrost](https://github.com/maximhq/bifrost) that captures API call metadata, timing, token usage, and costs, then streams events to configurable sinks.

## Features

- **Real-time event streaming** - Capture every API call with minimal latency impact
- **Multiple sink support** - Send events to Kafka, Kinesis, Pub/Sub, webhooks, or stdout
- **Batching & buffering** - Configurable batch sizes and flush intervals for optimal throughput
- **Token & cost tracking** - Automatic extraction of token usage and cost calculation
- **Cache analytics** - Track cache hits, types, and similarity scores
- **Custom labels** - Add tenant IDs, environment tags, or any custom metadata
- **Backpressure handling** - Optional event dropping when buffers are full
- **Object pooling** - Memory-efficient event handling for high-throughput scenarios

## Installation

Add the plugin to your Bifrost configuration:

```go
import (
    "github.com/maximhq/bifrost/plugins/analytics"
)

// Initialize the plugin
plugin, err := analytics.Init(ctx, &analytics.Config{
    SinkType:        "kafka",
    SinkConfig:      kafkaConfig,
    BatchSize:       100,
    FlushIntervalMs: 1000,
}, logger, pricingManager)

// Register with Bifrost
bifrost.RegisterPlugin(plugin)
```

## Configuration

### Core Options

| Option               | Type     | Default    | Description                                                              |
|----------------------|----------|------------|--------------------------------------------------------------------------|
| `SinkType`           | string   | `"stdout"` | Sink type: `stdout`, `webhook`, `kafka`, `kinesis`, `pubsub`             |
| `SinkConfig`         | map      | `nil`      | Sink-specific configuration                                              |
| `BatchSize`          | int      | `100`      | Events per batch before flushing                                         |
| `FlushIntervalMs`    | int      | `1000`     | Max milliseconds between flushes                                         |
| `DropOnBackpressure` | bool     | `false`    | Drop events when buffer is full instead of blocking                      |
| `CustomLabels`       | []string | `nil`      | Context keys to extract as event labels                                  |
| `AsyncWorkers`       | int      | `1`        | Concurrent batch sends (1 = synchronous, 4-8 recommended for production) |

## Sink Configurations

### 1. Stdout (Development/Debugging)

```go
config := &analytics.Config{
    SinkType: "stdout",
    SinkConfig: map[string]any{
        "pretty": true,      // Pretty-print JSON
        "output": "stderr",  // "stdout" or "stderr"
    },
}
```

### 2. Webhook (HTTP POST)

```go
config := &analytics.Config{
    SinkType: "webhook",
    SinkConfig: map[string]any{
        "url": "https://analytics.example.com/events",
        "headers": map[string]string{
            "Authorization": "Bearer your-token",
            "X-Source":      "bifrost",
        },
        "timeout_ms":        5000,   // Request timeout
        "max_conns_per_host": 100,   // Connection pool size
    },
}
```

### 3. Apache Kafka

```go
config := &analytics.Config{
    SinkType: "kafka",
    SinkConfig: map[string]any{
        // Required
        "brokers": []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
        "topic":   "bifrost-analytics",

        // Authentication (optional)
        "sasl_mechanism": "scram-sha-256",  // "plain", "scram-sha-256", "scram-sha-512"
        "sasl_username":  "analytics-user",
        "sasl_password":  "secret",
        "tls_enabled":    true,

        // Performance tuning
        "batch_size":    200,       // Messages per batch
        "batch_timeout": 500,       // Max ms to wait for batch
        "batch_bytes":   1048576,   // Max bytes per batch (1MB)
        "compression":   "snappy",  // "none", "gzip", "snappy", "lz4", "zstd"
        "required_acks": "leader",  // "none", "leader", "all"
        "async":         false,     // Fire-and-forget mode
    },
}
```

### 4. AWS Kinesis

```go
config := &analytics.Config{
    SinkType: "kinesis",
    SinkConfig: map[string]any{
        // Required
        "stream_name": "bifrost-analytics-stream",
        "region":      "us-east-1",

        // Authentication (optional - uses default credential chain if not provided)
        "access_key_id":     "AKIAIOSFODNN7EXAMPLE",
        "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "session_token":     "FwoGZXIvYXdzEBYaD...",  // For temporary credentials

        // Optional
        "endpoint":            "http://localhost:4566",  // LocalStack endpoint
        "partition_key_field": "request_id",             // Field to use as partition key
    },
}
```

**AWS Credential Chain:** If credentials are not provided, the SDK uses the default chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM role for EC2/ECS/Lambda

### 5. Google Cloud Pub/Sub

```go
config := &analytics.Config{
    SinkType: "pubsub",
    SinkConfig: map[string]any{
        // Required
        "project_id": "my-gcp-project",
        "topic_id":   "bifrost-analytics",

        // Authentication (optional - uses ADC if not provided)
        "credentials_file": "/path/to/service-account.json",
        // OR
        "credentials_json": `{"type": "service_account", ...}`,

        // Optional
        "endpoint":     "localhost:8085",  // Pub/Sub emulator
        "ordering_key": "tenant-123",      // Enable message ordering

        // Performance tuning
        "batch_size":        100,    // Max messages per batch
        "batch_bytes":       1048576, // Max bytes per batch
        "batch_delay_ms":    10,     // Max delay before publishing
        "num_goroutines":    25,     // Concurrent publish goroutines
        "flow_control_max":  1000,   // Max outstanding messages
    },
}
```

**GCP Authentication:** If credentials are not provided, uses Application Default Credentials:
1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. Default service account (GCE, GKE, Cloud Run, etc.)
3. User credentials from `gcloud auth application-default login`

## Event Schema

Each analytics event contains:

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "api_call",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "request_id": "req-abc123",
  "parent_request_id": "",
  "is_fallback": false,

  "request_type": "chat_completion",
  "provider": "openai",
  "model": "gpt-4",
  "is_streaming": false,

  "selected_key_id": "key-123",
  "selected_key_name": "production-key",
  "virtual_key_id": "vk-456",
  "virtual_key_name": "tenant-a-key",

  "fallback_index": 0,
  "number_of_retries": 0,

  "status": "success",
  "error_type": "",
  "error_code": 0,
  "error_message": "",

  "latency_ms": 1250,
  "time_to_first_token": 150,

  "input_tokens": 500,
  "output_tokens": 150,
  "total_tokens": 650,
  "cache_read_input_tokens": 100,
  "cache_creation_input_tokens": 0,

  "cache_hit": true,
  "cache_type": "semantic",
  "cache_provider": "qdrant",
  "cache_similarity": "0.9523",

  "cost_usd": 0.0195,

  "labels": {
    "tenant_id": "tenant-123",
    "environment": "production"
  }
}
```

### Event Types

| Type         | Description                       |
|--------------|-----------------------------------|
| `api_call`   | Standard API request              |
| `error`      | Request that resulted in an error |
| `cache_hit`  | Request served from cache         |
| `rate_limit` | Request that was rate limited     |

## Custom Labels

Extract values from the request context as event labels:

```go
config := &analytics.Config{
    SinkType:     "kafka",
    CustomLabels: []string{"tenant_id", "environment", "region", "user_id"},
    // ...
}
```

Set labels in your request handler:

```go
ctx.SetValue(schemas.BifrostContextKey("tenant_id"), "tenant-123")
ctx.SetValue(schemas.BifrostContextKey("environment"), "production")
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Bifrost Gateway                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Request ──► PreHook ──► LLM Provider ──► PostHook ──► Response│
│                 │                              │                │
│                 │      ┌───────────────────────┘                │
│                 │      │                                        │
│                 ▼      ▼                                        │
│            ┌─────────────────┐                                  │
│            │ Analytics Plugin │                                 │
│            ├─────────────────┤                                  │
│            │  Event Builder   │◄── Async goroutine              │
│            │        │         │                                 │
│            │        ▼         │                                 │
│            │   Event Pool     │◄── Object pooling               │
│            │        │         │                                 │
│            │        ▼         │                                 │
│            │  Event Channel   │◄── Buffered (10,000)            │
│            │        │         │                                 │
│            │        ▼         │                                 │
│            │  Batch Worker    │◄── Background goroutine         │
│            │        │         │                                 │
│            │        ▼         │                                 │
│            │    Semaphore     │◄── Bounded concurrency          │
│            │   ┌───┴───┐      │                                 │
│            │   ▼       ▼      │                                 │
│            │ Send    Send     │◄── AsyncWorkers goroutines      │
│            │   └───┬───┘      │                                 │
│            │       ▼          │                                 │
│            │     Sink         │                                 │
│            └───────┬──────────┘                                 │
│                    │                                            │
└────────────────────┼────────────────────────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   External Systems      │
        ├─────────────────────────┤
        │ • Kafka                 │
        │ • AWS Kinesis           │
        │ • Google Pub/Sub        │
        │ • HTTP Webhook          │
        │ • Stdout (debug)        │
        └─────────────────────────┘
```

## Performance Considerations

### Recommended Production Settings

```go
config := &analytics.Config{
    SinkType:           "kafka",
    BatchSize:          500,          // Larger batches for throughput
    FlushIntervalMs:    100,          // Lower latency
    DropOnBackpressure: true,         // Don't block on slow consumers
    AsyncWorkers:       4,            // Concurrent batch sends
    SinkConfig: map[string]any{
        "brokers":      []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
        "topic":        "bifrost-analytics",
        "compression":  "lz4",        // Fast compression
        "required_acks": "leader",    // Balance durability/speed
        "async":        true,         // Fire-and-forget
    },
}
```

### Async Workers

The `AsyncWorkers` setting controls how many batch sends can run concurrently:

| Setting       | Behavior                                 | Use Case                    |
|---------------|------------------------------------------|-----------------------------|
| `1` (default) | Synchronous - batches sent one at a time | Low-volume, strict ordering |
| `4-8`         | Bounded async - up to N concurrent sends | Production workloads        |

**Trade-offs:**
- **Higher values**: Increased throughput, but events may arrive out-of-order at the sink
- **Lower values**: Strict ordering within batches, but potential bottleneck under load

**Note:** Most analytics sinks (Kafka, Kinesis, Pub/Sub) handle out-of-order delivery gracefully. Use partition keys for ordering guarantees within a partition.

### Monitoring

Access plugin statistics:

```go
stats := plugin.Stats()
// Returns:
// {
//   "events_produced":   12500,
//   "events_sent":       12480,
//   "events_dropped":    20,
//   "batches_sent":      125,
//   "batches_in_flight": 2,
//   "send_errors":       0,
//   "last_flush_epoch":  1705312200000,
//   "pending_events":    45,
//   "async_workers":     4,
// }
```

| Metric              | Description                                      |
|---------------------|--------------------------------------------------|
| `events_produced`   | Total events created                             |
| `events_sent`       | Events successfully delivered to sink            |
| `events_dropped`    | Events dropped due to backpressure               |
| `batches_sent`      | Total batches sent                               |
| `batches_in_flight` | Batches currently being sent (0 to AsyncWorkers) |
| `send_errors`       | Failed batch sends                               |
| `pending_events`    | Events waiting in channel                        |
| `async_workers`     | Configured concurrent workers                    |

## Testing

### Unit Tests

```bash
go test ./... -v
```

### With Coverage

```bash
go test ./... -cover
```

### Integration Tests

Integration tests use [testcontainers-go](https://golang.testcontainers.org/) to automatically spin up real instances of Kafka, Kinesis (via LocalStack), and Pub/Sub (via emulator). **Docker must be running**.

```bash
# Run all integration tests (requires Docker)
go test ./... -tags=integration -v

# Run specific sink integration tests
go test ./sinks -tags=integration -run Kafka -v
go test ./sinks -tags=integration -run Kinesis -v
go test ./sinks -tags=integration -run PubSub -v
```

**What's tested:**
- **Kafka**: Single/batch sends, compression (gzip, snappy, lz4, zstd)
- **Kinesis**: Single/batch sends, large batches (>500 records)
- **Pub/Sub**: Single/batch sends, custom batch settings

**Note:** Integration tests take longer (~30-60s each) as they start real containers.

## License

This plugin is part of the Bifrost project.
