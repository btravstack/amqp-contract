# OpenTelemetry Observability

amqp-contract provides built-in OpenTelemetry instrumentation for observing your AMQP messaging operations. The telemetry is **lazy-loaded** and **zero-overhead** when OpenTelemetry is not installed.

## Overview

The telemetry implementation follows [OpenTelemetry semantic conventions for messaging](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/), providing:

- **Distributed tracing** - Track messages across services
- **Metrics** - Monitor publish/consume rates and latencies
- **Automatic context propagation** - Spans are automatically linked

## Installation

Install the OpenTelemetry API and SDK:

```bash
pnpm add @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/auto-instrumentations-node
```

::: tip Lazy Loading
If `@opentelemetry/api` is not installed, amqp-contract gracefully degrades with zero performance overhead. You can add observability later without code changes.
:::

## Traces

### Publish Spans

When publishing messages, spans are created with the following attributes:

| Attribute                                    | Description              | Example         |
| -------------------------------------------- | ------------------------ | --------------- |
| `messaging.system`                           | Always `rabbitmq`        | `rabbitmq`      |
| `messaging.destination.name`                 | Exchange name            | `orders`        |
| `messaging.destination.kind`                 | Always `exchange`        | `exchange`      |
| `messaging.operation`                        | Always `publish`         | `publish`       |
| `messaging.rabbitmq.destination.routing_key` | Routing key (if present) | `order.created` |
| `messaging.message.id`                       | Message ID (if set)      | `uuid-123`      |
| `messaging.message.body.size`                | Payload size in bytes    | `256`           |

Span name format: `{exchange_name} publish`

### Consume Spans

When processing messages, spans are created with the following attributes:

| Attribute                                 | Description                 | Example            |
| ----------------------------------------- | --------------------------- | ------------------ |
| `messaging.system`                        | Always `rabbitmq`           | `rabbitmq`         |
| `messaging.destination.name`              | Queue name                  | `order-processing` |
| `messaging.destination.kind`              | Always `queue`              | `queue`            |
| `messaging.operation`                     | Always `process`            | `process`          |
| `messaging.rabbitmq.message.delivery_tag` | Delivery tag                | `1`                |
| `amqp.consumer.name`                      | Consumer name from contract | `processOrder`     |

Span name format: `{queue_name} process`

### Error Handling

When errors occur, spans are marked with:

- `error.type` - Error class name (e.g., `RetryableError`, `NonRetryableError`)
- Status code set to `ERROR`
- Exception recorded with stack trace

## Metrics

The following metrics are automatically collected:

### Counters

| Metric                           | Description                  | Unit        |
| -------------------------------- | ---------------------------- | ----------- |
| `amqp.client.messages.published` | Number of messages published | `{message}` |
| `amqp.worker.messages.consumed`  | Number of messages consumed  | `{message}` |

### Histograms

| Metric                         | Description                    | Unit |
| ------------------------------ | ------------------------------ | ---- |
| `amqp.client.publish.duration` | Duration of publish operations | `ms` |
| `amqp.worker.process.duration` | Duration of message processing | `ms` |

### Common Attributes

All metrics include these attributes:

- `messaging.system` - Always `rabbitmq`
- `messaging.destination.name` - Exchange or queue name
- `success` - Boolean indicating operation success

## Configuration Example

Here's a complete setup with the OpenTelemetry Node.js SDK:

```typescript
// tracing.ts
import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";
import { PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";

const sdk = new NodeSDK({
  serviceName: "my-service",
  traceExporter: new OTLPTraceExporter({
    url: "http://localhost:4318/v1/traces",
  }),
  metricReader: new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({
      url: "http://localhost:4318/v1/metrics",
    }),
    exportIntervalMillis: 10000,
  }),
  instrumentations: [getNodeAutoInstrumentations()],
});

// Start before importing application code
sdk.start();

// Graceful shutdown
process.on("SIGTERM", () => {
  sdk
    .shutdown()
    .then(() => console.log("Tracing terminated"))
    .catch((error) => console.error("Error terminating tracing", error))
    .finally(() => process.exit(0));
});
```

Then import this file at your application entry point:

```typescript
// main.ts
import "./tracing.js";

import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
// ... rest of your application
```

## Custom TelemetryProvider

For advanced use cases, you can provide a custom `TelemetryProvider`:

```typescript
import type { TelemetryProvider } from "@amqp-contract/core";

const customTelemetryProvider: TelemetryProvider = {
  getTracer: () => myCustomTracer,
  getPublishCounter: () => myPublishCounter,
  getConsumeCounter: () => myConsumeCounter,
  getPublishLatencyHistogram: () => myPublishLatencyHistogram,
  getConsumeLatencyHistogram: () => myConsumeLatencyHistogram,
};

// Use in client
const client = await TypedAmqpClient.create({
  contract,
  connection,
  telemetry: customTelemetryProvider,
}).getOrElse((e) => {
  throw e;
});

// Use in worker
const worker = await TypedAmqpWorker.create({
  contract,
  connection,
  handlers,
  telemetry: customTelemetryProvider,
}).getOrElse((e) => {
  throw e;
});
```

## Best Practices

### 1. Use Sampling in Production

For high-throughput systems, configure trace sampling to reduce overhead:

```typescript
import { ParentBasedSampler, TraceIdRatioBasedSampler } from "@opentelemetry/sdk-trace-base";

const sdk = new NodeSDK({
  sampler: new ParentBasedSampler({
    root: new TraceIdRatioBasedSampler(0.1), // Sample 10% of traces
  }),
  // ...
});
```

### 2. Add Custom Attributes

Add business context to spans:

```typescript
import { trace } from "@opentelemetry/api";

// In your handler
const processOrder = ({ payload }) => {
  const span = trace.getActiveSpan();
  span?.setAttribute("order.id", payload.orderId);
  span?.setAttribute("order.amount", payload.amount);

  return fromPromise(process(payload), (e) => new RetryableError("Failed", e)).map(() => undefined);
};
```

### 3. Monitor Key Metrics

Set up alerts for:

- High `amqp.worker.process.duration` - Slow message processing
- Low `success` rate on `amqp.client.messages.published` - Publishing failures
- High error rate on consume spans - Handler failures

### 4. Correlate with HTTP Traces

When messages originate from HTTP requests, the trace context is automatically propagated, allowing you to see the full request flow across services.

## Visualization

The telemetry data can be visualized in any OpenTelemetry-compatible backend:

- **Jaeger** - Distributed tracing
- **Zipkin** - Distributed tracing
- **Prometheus + Grafana** - Metrics dashboards
- **Datadog** - Full observability platform
- **New Relic** - Full observability platform
- **Honeycomb** - Observability for distributed systems

## Related

- [Logging](/guide/logging) - Structured logging for publish/consume operations
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [OpenTelemetry Messaging Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
- [Worker Usage](/guide/worker-usage) - Error handling and retry strategies
- [Client Usage](/guide/client-usage) - Publishing messages
