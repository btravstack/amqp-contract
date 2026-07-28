---
title: Add logging - amqp-contract
description: Wire a Logger into the client and worker, adapt Pino or Winston, and know what the library logs.
---

# Add logging

The library emits structured logs through a small framework-agnostic interface. Nothing is logged until you supply a logger.

## Wire in a logger

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  logger,
}).get();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  urls: ["amqp://localhost"],
  logger,
}).get();
```

The interface is four methods, each taking a message and an optional structured context:

```typescript
import type { Logger, LoggerContext } from "@amqp-contract/core";

type LoggerContext = Record<string, unknown> & { error?: unknown };

type Logger = {
  debug(message: string, context?: LoggerContext): void;
  info(message: string, context?: LoggerContext): void;
  warn(message: string, context?: LoggerContext): void;
  error(message: string, context?: LoggerContext): void;
};
```

## Adapt Pino

Pino takes the object first and the message second, so the arguments swap:

```typescript
import pino from "pino";
import type { Logger } from "@amqp-contract/core";

const base = pino();

const logger: Logger = {
  debug: (message, context) => base.debug(context ?? {}, message),
  info: (message, context) => base.info(context ?? {}, message),
  warn: (message, context) => base.warn(context ?? {}, message),
  error: (message, context) => base.error(context ?? {}, message),
};
```

## Adapt Winston

Winston's signature matches directly:

```typescript
import winston from "winston";
import type { Logger } from "@amqp-contract/core";

const base = winston.createLogger({ transports: [new winston.transports.Console()] });

const logger: Logger = {
  debug: (message, context) => base.debug(message, context),
  info: (message, context) => base.info(message, context),
  warn: (message, context) => base.warn(message, context),
  error: (message, context) => base.error(message, context),
};
```

## Log to the console

For a script or a local run:

```typescript
const logger: Logger = {
  debug: (m, c) => console.debug(m, c ?? ""),
  info: (m, c) => console.info(m, c ?? ""),
  warn: (m, c) => console.warn(m, c ?? ""),
  error: (m, c) => console.error(m, c ?? ""),
};
```

## Get a per-message logger in handlers

The library's logger covers its own operations, not your handler's. For a logger carrying per-message correlation, build one in `createContext`:

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  createContext: (info) => ({
    log: base.child({
      handler: info.handlerName,
      correlationId: info.rawMessage.properties.correlationId,
    }),
  }),
  handlers: {
    processOrder: ({ payload }, _raw, { context }) => {
      context.log.info({ orderId: payload.orderId }, "processing");
      return OkAsync(undefined);
    },
  },
  urls: ["amqp://localhost"],
}).get();
```

See [add middleware](/how-to/add-middleware#inject-dependencies-per-message).

## Know what gets logged

The client logs one thing: a successful publish, at `info`, with `publisherName`, `exchange`, `routingKey` and `compressed`.

The worker is where the useful output is:

| Level   | Covers                                                                                                                                                                                                      |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `info`  | Successful consume; message published for retry (with `retryCount`, and `delayMs` under `ttl-backoff`); sending to DLQ                                                                                      |
| `warn`  | Retrying a message; retry disabled in `none` mode; consumer cancelled by the server; **queue has no DLX — message will be lost on nack**                                                                    |
| `error` | Payload or header validation failed; decompression failed; error processing message; non-retryable error going straight to DLQ; max retries exceeded; retry publish failed because the write buffer is full |

Two of these deserve alerts rather than dashboards. `Queue does not have DLX configured - message will be lost on nack` means you are losing data. `Failed to publish message for retry (write buffer full)` means retries are being dropped under load.

This is also where dead-letter failure reasons live. Messages nacked directly carry no `x-last-error` header, so the log line is the only record of _why_ — see [retry failed messages](/how-to/retry-failed-messages#inspect-retry-state).

## Choose between logging and tracing

Logging answers "what happened to this message"; the built-in OpenTelemetry instrumentation answers "where did the time go across services". They are complementary, and the library emits both independently — see [instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry).

## Where next

- [Instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry) — spans and metrics.
- [Troubleshoot](/how-to/troubleshoot) — diagnosing from these log lines.
