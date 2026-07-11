# @amqp-contract/worker

**Type-safe AMQP worker for consuming messages using amqp-contract with AsyncResult/Result error handling.**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/worker.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/worker)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/worker.svg)](https://www.npmjs.com/package/@amqp-contract/worker)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/api/worker)**

## Installation

```bash
pnpm add @amqp-contract/worker
```

## Features

- ✅ **Type-safe message consumption** — Handlers are fully typed based on your contract
- ✅ **Automatic validation** — Messages are validated before reaching your handlers
- ✅ **Automatic retry mechanism** — Built-in immediate or exponential backoff retry mechanisms
- ✅ **Prefetch configuration** — Control message flow with per-consumer prefetch settings
- ✅ **Automatic reconnection** — Built-in connection management with failover support

## Usage

### Basic Usage

```typescript
import { TypedAmqpWorker, RetryableError } from "@amqp-contract/worker";
import type { Logger } from "@amqp-contract/core";
import { fromPromise, type AsyncResult } from "unthrown";
import { contract } from "./contract";

// Optional: Create a logger implementation
const logger: Logger = {
  debug: (message, context) => console.debug(message, context),
  info: (message, context) => console.info(message, context),
  warn: (message, context) => console.warn(message, context),
  error: (message, context) => console.error(message, context),
};

// Create worker from contract with handlers (automatically connects and starts consuming)
const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        console.log("Processing order:", payload.orderId);

        // Your business logic here
        return fromPromise(
          Promise.all([processPayment(payload), updateInventory(payload)]),
          (error) => new RetryableError("Order processing failed", error),
        ).map(() => undefined);
      },
    },
    urls: ["amqp://localhost"],
    logger, // Optional: logs message consumption and errors
  })
).get();

// Worker is already consuming messages

// Clean up when needed
// await worker.close();
```

### Advanced Features

For advanced features like prefetch configuration and **automatic retry**, see the [Worker Usage Guide](https://btravstack.github.io/amqp-contract/guide/worker-usage).

#### Retry configuration

Retry is configured at the queue level in your contract definition. Add `retry` to your queue definition:

```typescript
import { defineQueue, defineExchange, defineContract } from "@amqp-contract/contract";

const dlx = defineExchange("orders-dlx");

// Configure retry at queue level
const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: dlx },
  retry: {
    mode: "ttl-backoff",
    maxRetries: 3, // Retry up to 3 times (default: 3)
    initialDelayMs: 1000, // Start with 1 second delay (default: 1000)
    maxDelayMs: 30000, // Max 30 seconds between retries (default: 30000)
    backoffMultiplier: 2, // Double the delay each time (default: 2)
    jitter: true, // Add randomness to prevent thundering herd (default: true)
  },
});
```

Then use `RetryableError` in your handlers:

```typescript
import { TypedAmqpWorker, RetryableError } from "@amqp-contract/worker";
import { fromPromise, type AsyncResult } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) =>
        // If this fails with RetryableError, message is automatically retried
        fromPromise(
          processPayment(payload),
          (error) => new RetryableError("Payment failed", error),
        ).map(() => undefined),
    },
    urls: ["amqp://localhost"],
  })
).get();
```

See the [Error Handling and Retry](https://btravstack.github.io/amqp-contract/guide/worker-usage#error-handling-and-retry) section in the guide for complete details.

## Defining Handlers Externally

You can define handlers outside of the worker creation using `defineHandler` and `defineHandlers` for better code organization. See the [Worker API documentation](https://btravstack.github.io/amqp-contract/api/worker) for details.

## Error Handling

Worker handlers return `AsyncResult<void, HandlerError>` for explicit error handling:

```typescript
import { RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { Err, fromPromise, type AsyncResult } from "unthrown";

handlers: {
  processOrder: ({ payload }) => {
    // Validation errors - non-retryable
    if (payload.amount <= 0) {
      return Err(new NonRetryableError("Invalid amount")).toAsync();
    }

    // Transient errors - retryable
    return fromPromise(process(payload), (error) => new RetryableError("Processing failed", error))
      .map(() => undefined);
  },
}
```

**Error Types:**

Worker defines error classes:

- `TechnicalError` - Runtime failures (parsing, processing)
- `MessageValidationError` - Message fails schema validation
- `RetryableError` - Signals that the error is transient and should be retried
- `NonRetryableError` - Signals permanent failure, message is sent to DLQ (if configured) or dropped

## API

For complete API documentation, see the [Worker API Reference](https://btravstack.github.io/amqp-contract/api/worker).

## Documentation

📖 **[Read the full documentation →](https://btravstack.github.io/amqp-contract)**

## License

MIT
