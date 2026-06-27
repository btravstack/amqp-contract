# Worker Usage

Learn how to use the type-safe AMQP worker to consume messages.

## Recommended pattern: `defineHandler`

The library encourages defining handlers with `defineHandler` (or `defineHandlers` for a batch). It pulls full type inference from the contract — payload type, headers type, and (for RPCs) the response type — and gives you a single point to test the handler in isolation.

```typescript
import {
  TypedAmqpWorker,
  defineHandler,
  RetryableError,
  NonRetryableError,
} from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { contract } from "./contract";

const processOrder = defineHandler(contract, "processOrder", ({ payload }) =>
  fromPromise(saveOrder(payload), (error) => new RetryableError("Database unavailable", error)).map(
    () => undefined,
  ),
);

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: { processOrder },
    urls: ["amqp://localhost"],
  })
).unwrap();

console.log("✅ Worker ready!");
```

The worker connects and starts consuming all queues defined in the contract.

> See [Error Model](./error-model.md) for the difference between `RetryableError` and `NonRetryableError`, and [Retry Strategies](./retry-strategies.md) for how the queue's retry mode interacts with handler errors.

## Inline handlers (quick scripts)

For one-file demos, you can inline the handler. The signature and types are identical; you just lose the named, externally-testable function:

```typescript
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { contract } from "./contract";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        console.log("Processing:", payload.orderId);
        return Ok(undefined).toAsync();
      },
      notifyOrder: ({ payload }) => {
        console.log("Notifying:", payload.orderId);
        return Ok(undefined).toAsync();
      },
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

In production code, prefer `defineHandler` so handler logic lives in its own module and can be unit-tested.

## Message Handlers

Handlers receive validated, fully-typed messages with `{ payload, headers }`:

```typescript
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        // Payload is fully typed!
        console.log(payload.orderId); // ✅ string
        console.log(payload.amount); // ✅ number
        console.log(payload.items); // ✅ array

        for (const item of payload.items) {
          console.log(`${item.productId}: ${item.quantity}`);
        }
        return Ok(undefined).toAsync();
      },
    },
    connection,
  })
).unwrap();
```

### Type Safety

The worker enforces:

- ✅ **Required handlers** - All consumers must have handlers
- ✅ **Message validation** - Validated before reaching handlers
- ✅ **Type inference** - Fully typed parameters

```typescript
// ❌ TypeScript error: missing handler
const workerResult = (await TypedAmqpWorker.create({
  contract,
  handlers: {
    notifyOrder: ({ payload }) => { ... },
    // Missing processOrder handler!
  },
  urls: ['amqp://localhost'],
})).unwrap();

// ✅ All handlers present
const worker = (await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => { ... },
    notifyOrder: ({ payload }) => { ... },
  },
  urls: ['amqp://localhost'],
})).unwrap();

console.log('✅ All handlers present');
```

## Defining Handlers Externally

For better organization, define handlers separately. The library provides two types of handlers:

### Safe Handlers (Recommended)

Safe handlers return `AsyncResult<void, HandlerError>` for explicit error handling:

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { Err, fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { contract } from "./contract";

const processOrderHandler = defineHandler(contract, "processOrder", ({ payload }) =>
  fromPromise(saveToDatabase(payload), (error) => new RetryableError("Database error", error)).map(
    () => undefined,
  ),
);

// Non-retryable errors go directly to DLQ
const validateOrderHandler = defineHandler(contract, "validateOrder", ({ payload }) => {
  if (payload.amount <= 0) {
    return Err(new NonRetryableError("Invalid order amount")).toAsync();
  }
  return Ok(undefined).toAsync();
});
```

### Multiple Handlers

```typescript
import { defineHandlers, RetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { contract } from "./contract";

// Safe handlers (recommended) - for async operations use fromPromise
const handlers = defineHandlers(contract, {
  processOrder: ({ payload }) =>
    fromPromise(
      processPayment(payload),
      (error) => new RetryableError("Payment failed", error),
    ).map(() => undefined),
  notifyOrder: ({ payload }) =>
    fromPromise(sendEmail(payload), (error) => new RetryableError("Email failed", error)).map(
      () => undefined,
    ),
});

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers,
    urls: ["amqp://localhost"],
  })
).unwrap();
```

### Benefits

External handler definitions provide several advantages:

- **Better Organization**: Separate handler logic from worker setup code
- **Reusability**: Share handlers across multiple workers or test them independently
- **Type Safety**: Full TypeScript type checking at definition time
- **Testability**: Test handlers in isolation before integrating with workers
- **Maintainability**: Easier to modify and refactor handler logic
- **Explicit Error Control**: Safe handlers force explicit error handling

### Example: Organized Handler Module

Create a dedicated module for handlers with explicit error handling:

```typescript
// handlers/order-handlers.ts
import { defineHandler, defineHandlers, RetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult } from "unthrown";
import { orderContract } from "../contract";
import { processPayment } from "../services/payment";
import { sendEmail } from "../services/email";

export const processOrderHandler = defineHandler(orderContract, "processOrder", ({ payload }) =>
  fromPromise(processPayment(payload), (error) => new RetryableError("Payment failed", error)).map(
    () => undefined,
  ),
);

export const notifyOrderHandler = defineHandler(orderContract, "notifyOrder", ({ payload }) =>
  fromPromise(sendEmail(payload), (error) => new RetryableError("Email failed", error)).map(
    () => undefined,
  ),
);

// Export all handlers together
export const orderHandlers = defineHandlers(orderContract, {
  processOrder: processOrderHandler,
  notifyOrder: notifyOrderHandler,
});
```

```typescript
// worker.ts
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { orderContract } from "./contract";
import { orderHandlers } from "./handlers/order-handlers";

const worker = (
  await TypedAmqpWorker.create({
    contract: orderContract,
    handlers: orderHandlers,
    urls: ["amqp://localhost"],
  })
).unwrap();
```

## Starting Consumers

### Automatic Consumption

By default, `TypedAmqpWorker.create` automatically starts all consumers defined in the contract:

```typescript
const worker = (await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => { ... },
    notifyOrder: ({ payload }) => { ... },
  },
  connection,
})).unwrap();
// Worker is already consuming messages from all queues
console.log('Worker ready, waiting for messages...');
```

## Message Acknowledgment

### Automatic Acknowledgment

By default, messages are automatically acknowledged after successful processing:

```typescript
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        console.log("Processing:", payload.orderId);
        // Message is automatically acked after this handler completes
        return Ok(undefined).toAsync();
      },
    },
    connection,
  })
).unwrap();
```

### Manual Acknowledgment

For more control over acknowledgment, use the raw message parameter and error types:

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult, type Result } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: defineHandler(contract, "processOrder", ({ payload }, rawMessage) => {
        // Access raw AMQP message properties if needed
        console.log("Delivery tag:", rawMessage.fields.deliveryTag);

        return fromPromise(
          processOrder(payload),
          (error) => new RetryableError("Processing failed", error), // Failure - will retry
        ).map(() => undefined); // Success - message will be acked
      }),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

**Acknowledgment behavior:**

- Handler returns `Ok(undefined)` → Message is acknowledged
- Handler returns `Err(RetryableError)` → Message is nacked and retried
- Handler returns `Err(NonRetryableError)` → Message is sent to DLQ (if configured) or dropped

## Graceful Shutdown

Properly close the worker on shutdown:

```typescript
async function shutdown() {
  console.log("Shutting down...");
  await worker.close();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

## Complete Example

```typescript
import { TypedAmqpWorker, defineHandlers, RetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult } from "unthrown";
import { contract } from "./contract";

async function main() {
  const worker = (
    await TypedAmqpWorker.create({
      contract,
      handlers: defineHandlers(contract, {
        processOrder: ({ payload }) => {
          console.log(`Processing order ${payload.orderId}`);

          return fromPromise(
            Promise.all([saveToDatabase(payload), sendConfirmation(payload.customerId)]),
          )
            .map(() => undefined)
            .mapErr((error) => {
              console.error("Processing failed:", error);
              return new RetryableError("Order processing failed", error);
            });
        },

        notifyOrder: ({ payload }) => {
          console.log(`Sending notification for ${payload.orderId}`);
          return fromPromise(
            sendEmail(payload),
            (error) => new RetryableError("Email failed", error),
          ).map(() => undefined);
        },
      }),
      urls: ["amqp://localhost"],
    })
  ).unwrap();

  console.log("✅ Worker ready!");

  // Graceful shutdown
  const shutdown = async () => {
    console.log("Shutting down...");
    await worker.close();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch(console.error);
```

## Advanced Features

### Prefetch Configuration

Control the number of unacknowledged messages a consumer can have at once. This helps manage memory usage and processing rate.

Use the tuple syntax `[handler, options]` to configure prefetch per-handler:

```typescript
import { fromPromise, Ok, type AsyncResult } from "unthrown";
import { RetryableError } from "@amqp-contract/worker";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: [
        ({ payload }) =>
          fromPromise(
            saveToDatabase(payload),
            (error) => new RetryableError("Failed to save order", error),
          ).map(() => {
            console.log("Order:", payload.orderId);
            return undefined;
          }),
        { prefetch: 10 }, // Process up to 10 messages concurrently
      ],
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

### Default Consumer Options

If you want to apply a common consumer configuration across all handlers, use `defaultConsumerOptions` when creating the worker:

```typescript
import { fromPromise, Ok, type AsyncResult } from "unthrown";
import { RetryableError } from "@amqp-contract/worker";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) =>
        fromPromise(
          processOrder(payload),
          (error) => new RetryableError("Processing failed", error),
        ).map(() => undefined),
    },
    urls: ["amqp://localhost"],
    defaultConsumerOptions: {
      prefetch: 10,
    },
  })
).unwrap();
```

`defaultConsumerOptions` are applied to every consumer handler. When a handler is defined with tuple syntax, per-handler options override these defaults.

### Handler Configuration Patterns

Three configuration patterns are supported:

1. **Simple handler** - No options

```typescript
handlers: {
  processOrder: ({ payload }) => {
    // Single message processing
    return Ok(undefined).toAsync();
  };
}
```

2. **Handler with prefetch** - Control concurrency

```typescript
handlers: {
  processOrder: [
    ({ payload }) => {
      // Single message processing with prefetch
      return Ok(undefined).toAsync();
    },
    { prefetch: 10 },
  ];
}
```

## Best Practices

1. **Handle Errors** - Always wrap business logic in try-catch
2. **Use Prefetch** - Limit concurrent messages with `prefetch` option to control memory usage
3. **Graceful Shutdown** - Properly close connections to finish processing in-flight messages
4. **Idempotency** - Handlers should be safe to retry since messages may be redelivered
5. **Dead Letters** - Configure DLQ to collect and process failed messages

## Error Handling and Retry

The worker supports automatic retry with two different strategies, configured at the **queue level** in the contract:

1. **Immediate-Requeue Mode** - Requeues failed messages immediately (no wait queues)
2. **TTL-Backoff Mode** - Uses TTL + wait queue pattern for exponential backoff

### Retry Strategies {#retry-strategies}

#### Immediate-Requeue Mode (Recommended)

A simpler mode that requeues failed messages immediately (no wait queues):

```typescript
import { defineQueue, defineExchange, defineContract } from "@amqp-contract/contract";
import { TypedAmqpWorker, RetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult } from "unthrown";

// 1. Define queue with immediate-requeue retry
const dlx = defineExchange("orders-dlx");
const ordersQueue = defineQueue("orders", {
  type: "quorum", // Default queue type
  deadLetter: {
    exchange: dlx,
    routingKey: "orders.failed",
  },
  retry: { mode: "immediate-requeue", maxRetries: 3 }, // Dead-letter after 3 retry attempts
});

// 2. Worker automatically uses queue's retry configuration
const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) =>
        fromPromise(
          processPayment(payload),
          (error) => new RetryableError("Payment failed", error),
        ).map(() => undefined),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

**How Immediate-Requeue works:**

- For quorum queues, messages are requeued with `nack(requeue=true)`, and the worker tracks delivery count via the native RabbitMQ `x-delivery-count` header.
- For classic queues, messages are re-published on the same queue, and the worker tracks delivery count via a custom `x-retry-count` header.
- When count exceeds `maxRetries`, the message is automatically dead-lettered (if DLX is configured) or dropped.
- No wait queues or TTL management needed.

**Best for:**

- Simpler architecture requirements
- When immediate retries are acceptable
- Avoiding head-of-queue blocking issues

**Limitation:** No exponential backoff — retries are immediate.

#### TTL-Backoff Mode

This mode provides exponential backoff using RabbitMQ's TTL. **Wait queues and bindings are automatically generated** when you use `defineContract`:

```typescript
import {
  defineQueue,
  defineExchange,
  defineContract,
  defineConsumer,
  defineMessage,
} from "@amqp-contract/contract";
import { TypedAmqpWorker, RetryableError } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult } from "unthrown";
import { z } from "zod";

// 1. Define queue with TTL-backoff retry - infrastructure auto-generated
const dlx = defineExchange("orders-dlx");
const ordersQueue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: {
    mode: "ttl-backoff",
    maxRetries: 3, // Maximum retry attempts (default: 3)
    initialDelayMs: 1000, // Initial delay before first retry (default: 1000ms)
    maxDelayMs: 30000, // Maximum delay between retries (default: 30000ms)
    backoffMultiplier: 2, // Exponential backoff multiplier (default: 2)
    jitter: true, // Add random jitter to prevent thundering herd (default: true)
  },
});
const orderMessage = defineMessage(z.object({ orderId: z.string(), amount: z.number() }));

// 2. defineContract auto-extracts exchanges, queues and creates wait queue + bindings
const contract = defineContract({
  consumers: {
    processOrder: defineConsumer(ordersQueue, orderMessage),
  },
});

// 3. Worker automatically uses queue's retry configuration
const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) =>
        fromPromise(
          processPayment(payload),
          (error) => new RetryableError("Payment failed", error),
        ).map(() => undefined),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

**How TTL-Backoff works:**

1. **Message is acknowledged** - The worker acks the original message
2. **Published to wait queue** - Message is republished to a wait queue with a TTL
3. **Wait in queue** - Message sits in the wait queue for the calculated delay
4. **Dead-lettered back** - After TTL expires, message is automatically routed back to the main queue
5. **Retry processing** - Worker processes the message again
6. **Repeat or DLQ** - Process repeats until success or max retries reached, then sent to Dead Letter Queue (DLQ) if any configured, or dropped

**Best for:** When you need configurable delays between retries to give downstream services time to recover.

**Limitation:** Potential head-of-queue blocking when messages have mixed TTLs.

#### Accessing Queue Properties

When TTL-backoff retry is configured, `defineQueue` returns a wrapper object containing the infrastructure. Use `extractQueue()` to access the underlying queue definition:

```typescript
import { extractQueue } from "@amqp-contract/contract";

const ordersQueue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "ttl-backoff", maxRetries: 3 },
});

// Access queue name
const queueName = extractQueue(ordersQueue).name; // "orders"
```

#### Comparing Retry Modes

| Feature                | TTL-Backoff                      | Immediate-Requeue |
| ---------------------- | -------------------------------- | ----------------- |
| Retry delays           | Configurable exponential backoff | Immediate         |
| Architecture           | Wait queues + Headers exchanges  | No wait queues    |
| Head-of-queue blocking | Possible with mixed TTLs         | None              |

### Exponential Backoff

With TTL-backoff mode, retry delays increase exponentially to give downstream services time to recover:

```typescript
// With default settings (initialDelayMs: 1000, backoffMultiplier: 2):
// Attempt 1: 1000ms delay
// Attempt 2: 2000ms delay
// Attempt 3: 4000ms delay
// After 3 attempts: Message sent to DLQ
```

**With jitter enabled** (default), a random factor (50-100% of calculated delay) is added to prevent all retried messages from hitting the system simultaneously.

### Dead Letter Exchange Configuration

A Dead Letter Exchange (DLX) can be configured at the queue level, to which failed messages will be sent (after all retry attempts, if any configured) instead of being dropped:

```typescript
import {
  defineQueue,
  defineExchange,
  defineContract,
  defineConsumer,
  defineMessage,
} from "@amqp-contract/contract";
import { z } from "zod";

// Define the Dead Letter Exchange
const dlxExchange = defineExchange("orders-dlx");

// Define the Dead Letter Queue
const dlq = defineQueue("orders-dlq");

// Define your message schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number().positive(),
  }),
);

// Define your main queue with deadLetter and retry configuration
const ordersQueue = defineQueue("orders", {
  deadLetter: {
    exchange: dlxExchange,
    routingKey: "orders.failed",
  },
  retry: { mode: "immediate-requeue", maxRetries: 3 }, // Or ttl-backoff
});

// Compose the contract - exchanges, queues, bindings auto-extracted
const contract = defineContract({
  consumers: {
    processOrder: defineConsumer(ordersQueue, orderMessage),
  },
  // ... publishers
});
```

### Retry Error Classes

The library provides two error classes for explicit error signaling:

#### RetryableError

Use `RetryableError` for transient failures that may succeed on retry:

```typescript
import { RetryableError, defineHandler } from "@amqp-contract/worker";
import { fromPromise, Ok, type AsyncResult } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: [
        defineHandler(contract, "processOrder", ({ payload }) =>
          fromPromise(
            externalApiCall(payload),
            (error) =>
              // Explicitly signal this should be retried
              new RetryableError("External API temporarily unavailable", error),
          ).map(() => undefined),
        ),
        {
          retry: {
            maxRetries: 5,
            initialDelayMs: 2000,
          },
        },
      ],
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

#### NonRetryableError

Use `NonRetryableError` for permanent failures that should NOT be retried:

```typescript
import { NonRetryableError, RetryableError, defineHandler } from "@amqp-contract/worker";
import { Err, fromPromise, Ok, type AsyncResult, type Result } from "unthrown";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: defineHandler(contract, "processOrder", ({ payload }) => {
        // Validation errors should not be retried
        if (payload.amount <= 0) {
          return Err(new NonRetryableError("Invalid order amount")).toAsync();
        }
        return fromPromise(
          processPayment(payload),
          (error) => new RetryableError("Payment failed", error),
        ).map(() => undefined);
      }),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

**NonRetryableError behavior:**

- Message is immediately sent to DLQ (if configured)
- No retry attempts are made
- Use for validation errors, business rule violations, or permanent failures

#### Using Safe Handlers for Better Error Control

For the most explicit error handling, use safe handlers that return `AsyncResult<Result>`:

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { Err, fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { match } from "ts-pattern";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: defineHandler(contract, "processOrder", ({ payload }) => {
        // Validation - non-retryable
        if (payload.amount <= 0) {
          return Err(new NonRetryableError("Invalid amount")).toAsync();
        }

        // Qualify the rejection at the boundary into a modeled HandlerError.
        return fromPromise(processPayment(payload), (error) =>
          match(error)
            .when(
              (e) => e instanceof PaymentDeclinedError,
              () => new NonRetryableError("Payment declined", error),
            )
            .otherwise(() => new RetryableError("Payment failed", error)),
        ).map(() => undefined);
      }),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

**When to use which error type:**

| Error Type          | Use Case                                            | Behavior                                   |
| ------------------- | --------------------------------------------------- | ------------------------------------------ |
| `RetryableError`    | Transient failures (network, rate limits, timeouts) | Retry based on queue's retry configuration |
| `NonRetryableError` | Permanent failures (validation, business rules)     | Send to DLQ (if configured) or drop        |
| Any other error     | Unexpected failures                                 | Retry based on queue's retry configuration |

**Note:** Retry is configured at the queue level. **All errors except `NonRetryableError` are retried** according to the queue's retry configuration.

### Monitoring Retry Headers

The worker adds headers to track retry information:

- `x-retry-count` - Number of times this message has been retried
- `x-last-error` - Error message from the last failed attempt
- `x-first-failure-timestamp` - Timestamp of the first failure

These headers can be useful for monitoring and debugging:

```typescript
// Example: Log retry information (requires custom message access)
// Note: Standard handlers don't expose raw message properties
// This is for illustration of what the worker tracks internally
```

### Best Practices for Retry

1. **Configure appropriate delays** - Start with 1-2 seconds, max out at 30-60 seconds
2. **Use jitter** - Keep jitter enabled (default) to prevent thundering herd
3. **Set reasonable max retries** - 3-5 retries is usually sufficient
4. **Configure DLX on all queues** - Ensures proper retry behavior and DLQ routing
5. **Make handlers idempotent** - Messages may be processed multiple times
6. **Monitor DLQ** - Set up alerts for messages reaching the DLQ
7. **Handle transient vs permanent failures** - Use retry for transient failures (network issues, rate limits), handle permanent failures (validation errors) before throwing

### Example: Complete Retry Setup

```typescript
import { TypedAmqpWorker, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import {
  defineContract,
  defineQueue,
  defineExchange,
  definePublisher,
  defineConsumer,
  defineMessage,
} from "@amqp-contract/contract";
import { Err, fromPromise, Ok, type AsyncResult, type Result } from "unthrown";
import { z } from "zod";

// Define exchanges
const mainExchange = defineExchange("orders");
const dlxExchange = defineExchange("orders-dlx");

// Define message schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// Define queue with retry configuration at the queue level
const ordersQueue = defineQueue("orders", {
  deadLetter: {
    exchange: dlxExchange,
    routingKey: "orders.failed",
  },
  retry: {
    mode: "ttl-backoff",
    maxRetries: 3,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    jitter: true,
  },
});
const dlq = defineQueue("orders-dlq");

// defineContract auto-extracts exchanges, queues, and creates wait queue + retry bindings for TTL-backoff
const contract = defineContract({
  consumers: {
    processOrder: defineConsumer(ordersQueue, orderMessage),
  },
  publishers: {
    orderCreated: definePublisher(mainExchange, orderMessage, {
      routingKey: "order.created",
    }),
  },
});

// Worker automatically uses queue's retry configuration
const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        // Validate before processing (don't retry validation errors)
        if (!payload.amount || payload.amount <= 0) {
          return Err(new NonRetryableError("Invalid order amount")).toAsync();
        }

        // Process with external service (retry on failure based on queue config)
        return fromPromise(
          Promise.all([
            paymentService.charge(payload),
            inventoryService.reserve(payload),
            notificationService.send(payload),
          ]),
        )
          .map(() => undefined)
          .mapErr((error) => new RetryableError("Order processing failed", error));
      },
    },
    urls: ["amqp://localhost"],
  })
).unwrap();

console.log("✅ Worker ready with retry enabled!");
```

## Best Practices

1. **Handle Errors** - Always wrap business logic in try-catch
2. **Use Prefetch** - Limit concurrent messages with `prefetch` option to control memory usage
3. **Graceful Shutdown** - Properly close connections to finish processing in-flight messages
4. **Idempotency** - Handlers should be safe to retry since messages may be redelivered
5. **Dead Letters** - Configure DLQ to collect and process failed messages

## Next Steps

- Learn about [Client Usage](/guide/client-usage)
- Explore [Defining Contracts](/guide/defining-contracts)
- Check out [Examples](/examples/)
