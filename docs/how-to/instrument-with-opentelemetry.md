---
title: Instrument with OpenTelemetry - amqp-contract
description: Enable built-in tracing and metrics, propagate trace context across the broker, and supply a custom telemetry provider.
---

# Instrument with OpenTelemetry

Instrumentation is built in and lazy-loaded. With `@opentelemetry/api` absent it costs nothing; install it and spans and metrics start flowing with no code change.

## Turn it on

```bash
pnpm add @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/auto-instrumentations-node
```

Set the SDK up before anything else imports the library:

```typescript
// tracing.ts — import this first
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { NodeSDK } from "@opentelemetry/sdk-node";

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: "http://localhost:4318/v1/traces" }),
  instrumentations: [getNodeAutoInstrumentations()],
});

sdk.start();
```

```typescript
// index.ts
import "./tracing.js";
import { TypedAmqpClient } from "@amqp-contract/client";
```

There is no `telemetry: true` flag. Detection is the presence of `@opentelemetry/api`.

## Know what you get

Spans follow the [OpenTelemetry messaging conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/).

**Publish spans** are named `{exchange} publish` and carry `messaging.system` (`rabbitmq`), `messaging.destination.name`, `messaging.destination.kind` (`exchange`), `messaging.operation` (`publish`), `messaging.rabbitmq.destination.routing_key`, `messaging.message.id` and `messaging.message.body.size`.

**Consume spans** are named `{queue} process` and carry `messaging.system`, `messaging.destination.name`, `messaging.destination.kind` (`queue`), `messaging.operation` (`process`), `messaging.rabbitmq.message.delivery_tag` and `amqp.consumer.name`.

On failure a span records the exception, sets status `ERROR`, and sets `error.type` to the error class — so `RetryableError` and `NonRetryableError` are distinguishable in your backend, which makes "how much of our error rate is permanent?" a query rather than a guess.

Metrics:

| Metric                           | Type      | Unit        |
| -------------------------------- | --------- | ----------- |
| `amqp.client.messages.published` | Counter   | `{message}` |
| `amqp.worker.messages.consumed`  | Counter   | `{message}` |
| `amqp.client.publish.duration`   | Histogram | ms          |
| `amqp.worker.process.duration`   | Histogram | ms          |

All carry `messaging.system`, `messaging.destination.name` and a `success` boolean.

## Connect traces across the broker

Publish and consume spans are not linked automatically — a message crossing a broker breaks the in-process context. To join them, carry W3C trace context in a header.

Stamp it on the way out with a publish interceptor, and resume it on the way in with worker middleware. Both hook points are in [add middleware](/how-to/add-middleware#propagate-a-trace-across-services); the instrumentation here is what the resumed context attaches to.

Without this you still get per-service spans and metrics, just not one trace spanning producer and consumer.

## Sample in production

Tracing every message is rarely affordable:

```typescript
import { ParentBasedSampler, TraceIdRatioBasedSampler } from "@opentelemetry/sdk-trace-base";

const sdk = new NodeSDK({
  sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(0.1) }),
  // …
});
```

`ParentBasedSampler` keeps a trace whole: once an upstream service samples a trace, this one honours that decision instead of re-rolling and truncating it.

Metrics are unaffected by sampling, so rates and latencies stay accurate at any sampling ratio.

## Add your own attributes

The library's spans are not extension points. To attach business context, start your own span inside the handler:

```typescript
import { trace } from "@opentelemetry/api";

const tracer = trace.getTracer("orders");

processOrder: ({ payload }) =>
  fromPromise(
    tracer.startActiveSpan("process-order", async (span) => {
      span.setAttribute("order.id", payload.orderId);
      try {
        await handleOrder(payload);
      } finally {
        span.end();
      }
    }),
    qualifyRetryable("processing failed"),
  ).map(() => undefined),
```

It nests under the consume span automatically.

## Supply a custom telemetry provider

To route telemetry somewhere other than OpenTelemetry, or to disable it explicitly, pass a `TelemetryProvider`:

```typescript
import type { TelemetryProvider } from "@amqp-contract/core";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  telemetry: myProvider,
}).get();
```

Omitting it uses the default provider, which attempts to load OpenTelemetry and no-ops if it is absent.

## Watch the right signals

`amqp.worker.process.duration` rising while `amqp.worker.messages.consumed` stays flat means handlers are slowing down — usually a downstream dependency.

Consumed falling below published means the queue is growing. Compare against broker-side queue depth, which is not something this library reports.

`success=false` on `amqp.worker.messages.consumed`, split by `error.type`, is the most useful single chart: a rise in `NonRetryableError` means bad data arriving, while a rise in `RetryableError` means infrastructure trouble.

## Where next

- [Add logging](/how-to/add-logging) — per-message detail traces do not carry.
- [Add middleware](/how-to/add-middleware) — trace propagation.
- [Tune performance](/how-to/tune-performance) — acting on what the metrics show.
