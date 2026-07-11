# Logging

amqp-contract includes an optional, framework-agnostic logging abstraction. When you provide a `Logger` instance, both client and worker emit structured log messages for publish/consume operations, retries, errors, and more.

## Logger Interface

The `Logger` type is exported from `@amqp-contract/core`:

```typescript
import type { Logger, LoggerContext } from "@amqp-contract/core";
```

```typescript
type LoggerContext = Record<string, unknown> & {
  error?: unknown;
};

type Logger = {
  debug(message: string, context?: LoggerContext): void;
  info(message: string, context?: LoggerContext): void;
  warn(message: string, context?: LoggerContext): void;
  error(message: string, context?: LoggerContext): void;
};
```

Each method receives a human-readable `message` and an optional `context` object containing structured data relevant to the log entry.

## Usage with Client

Pass a `logger` to `TypedAmqpClient.create()`:

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  logger, // [!code highlight]
}).getOrThrow();
```

## Usage with Worker

Pass a `logger` to `TypedAmqpWorker.create()`:

```typescript
import { TypedAmqpWorker } from "@amqp-contract/worker";

const worker = await TypedAmqpWorker.create({
  contract,
  urls: ["amqp://localhost"],
  handlers,
  logger, // [!code highlight]
}).getOrThrow();
```

## What Gets Logged

### Client

| Level  | Message                          | Context                                                 |
| ------ | -------------------------------- | ------------------------------------------------------- |
| `info` | `Message published successfully` | `publisherName`, `exchange`, `routingKey`, `compressed` |

### Worker

#### Consume Lifecycle

| Level   | Message                                  | Context                              |
| ------- | ---------------------------------------- | ------------------------------------ |
| `info`  | `Message consumed successfully`          | `consumerName`, `queueName`          |
| `warn`  | `Consumer cancelled by server`           | `consumerName`, `queueName`          |
| `warn`  | `Failed to cancel consumer during close` | `consumerTag`, `error`               |
| `error` | `{field} validation failed`              | `consumerName`, `queueName`, `error` |
| `error` | `Failed to decompress message`           | `consumerName`, `queueName`, `error` |

#### Error Handling

| Level   | Message                                           | Context                                           |
| ------- | ------------------------------------------------- | ------------------------------------------------- |
| `error` | `Error processing message`                        | `consumerName`, `queueName`, `errorType`, `error` |
| `error` | `Non-retryable error, sending to DLQ immediately` | `consumerName`, `errorType`, `error`              |

#### Retry — Immediate-Requeue Mode

| Level   | Message                                                         | Context                                                          |
| ------- | --------------------------------------------------------------- | ---------------------------------------------------------------- |
| `info`  | `Message published for retry`                                   | `queueName`, `retryCount`                                        |
| `warn`  | `Failed to parse message for retry, using original buffer`      | `queueName`, `error`                                             |
| `warn`  | `Retrying message (immediate-requeue mode)`                     | `consumerName`, `queueName`, `retryCount`, `maxRetries`, `error` |
| `error` | `Max retries exceeded, sending to DLQ (immediate-requeue mode)` | `consumerName`, `queueName`, `retryCount`, `maxRetries`, `error` |
| `error` | `Failed to publish message for retry (write buffer full)`       | `queueName`, `retryCount`                                        |

#### Retry — TTL-Backoff Mode

| Level   | Message                                                    | Context                                                                     |
| ------- | ---------------------------------------------------------- | --------------------------------------------------------------------------- |
| `info`  | `Message published for retry`                              | `queueName`, `retryCount`, `delayMs`                                        |
| `warn`  | `Failed to parse message for retry, using original buffer` | `queueName`, `error`                                                        |
| `warn`  | `Retrying message (ttl-backoff mode)`                      | `consumerName`, `queueName`, `retryCount`, `maxRetries`, `delayMs`, `error` |
| `error` | `Max retries exceeded, sending to DLQ (ttl-backoff mode)`  | `consumerName`, `queueName`, `retryCount`, `maxRetries`, `error`            |
| `error` | `Failed to publish message for retry (write buffer full)`  | `queueName`, `retryCount`, `delayMs`                                        |
| `error` | `Queue does not have TTL-backoff infrastructure`           | `consumerName`, `queueName`                                                 |

#### Retry — None Mode (No retry)

| Level  | Message                                      | Context                 |
| ------ | -------------------------------------------- | ----------------------- |
| `warn` | `Retry disabled (none mode), sending to DLQ` | `consumerName`, `error` |

#### Dead-Letter Queue

| Level  | Message                                                             | Context                    |
| ------ | ------------------------------------------------------------------- | -------------------------- |
| `warn` | `Queue does not have DLX configured - message will be lost on nack` | `queueName`                |
| `info` | `Sending message to DLQ`                                            | `queueName`, `deliveryTag` |

## Integration Examples

### Pino

```typescript
import pino from "pino";
import type { Logger } from "@amqp-contract/core";

const pinoLogger = pino({ name: "amqp" });

const logger: Logger = {
  debug: (message, context) => pinoLogger.debug(context, message),
  info: (message, context) => pinoLogger.info(context, message),
  warn: (message, context) => pinoLogger.warn(context, message),
  error: (message, context) => pinoLogger.error(context, message),
};
```

### Winston

```typescript
import winston from "winston";
import type { Logger } from "@amqp-contract/core";

const winstonLogger = winston.createLogger({
  level: "debug",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
});

const logger: Logger = {
  debug: (message, context) => winstonLogger.debug(message, context),
  info: (message, context) => winstonLogger.info(message, context),
  warn: (message, context) => winstonLogger.warn(message, context),
  error: (message, context) => winstonLogger.error(message, context),
};
```

### Console

```typescript
import type { Logger } from "@amqp-contract/core";

const logger: Logger = {
  debug: (message, context) => console.debug(message, context),
  info: (message, context) => console.info(message, context),
  warn: (message, context) => console.warn(message, context),
  error: (message, context) => console.error(message, context),
};
```

## Logging vs OpenTelemetry

Logging and [OpenTelemetry observability](/guide/opentelemetry-observability) serve complementary purposes:

| Concern          | Logging                                         | OpenTelemetry                                         |
| ---------------- | ----------------------------------------------- | ----------------------------------------------------- |
| **Purpose**      | Human-readable operational messages             | Structured traces, metrics, and context propagation   |
| **When to use**  | Debugging, audit trails, operational monitoring | Distributed tracing, performance dashboards, alerting |
| **Overhead**     | Minimal — synchronous string formatting         | Slightly higher — span creation, metric recording     |
| **Dependencies** | None — bring your own logger                    | Requires `@opentelemetry/api`                         |

Both can be enabled simultaneously — logging provides immediate human-readable output while OpenTelemetry provides deep observability across services.

## Related

- [OpenTelemetry Observability](/guide/opentelemetry-observability) — Distributed tracing and metrics
- [Worker Usage](/guide/worker-usage) — Error handling and retry strategies
- [Client Usage](/guide/client-usage) — Publishing messages
