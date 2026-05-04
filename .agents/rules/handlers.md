# Handler Patterns

## Handler Signature

Handlers receive `({ payload, headers }, rawMessage)` and return `ResultAsync<void, HandlerError>`:

```typescript
import { okAsync, ResultAsync } from "neverthrow";
import { RetryableError, NonRetryableError } from "@amqp-contract/worker";

// Handler signature: (message, rawMessage) => ResultAsync<void, HandlerError>
const handler = ({ payload }, rawMessage) => {
  console.log(payload.orderId);
  return okAsync(undefined);
};

// For async operations, use ResultAsync.fromPromise(promise, errorMapper)
const asyncHandler = ({ payload }) =>
  ResultAsync.fromPromise(
    processPayment(payload),
    (error) => new RetryableError("Payment failed", error),
  ).map(() => undefined);
```

## Handler Parameters

1. **`message`**: Object containing `{ payload, headers }`
   - `payload`: Validated message data (typed from schema)
   - `headers`: Validated headers (if schema defines them)

2. **`rawMessage`**: Raw AMQP `ConsumeMessage` with full metadata
   - `fields.deliveryTag`, `fields.routingKey`, `fields.exchange`
   - `properties.messageId`, `properties.timestamp`, etc.

## Using defineHandler

Use `defineHandler` for all new code to get full type inference from the contract:

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { errAsync, okAsync, ResultAsync } from "neverthrow";

const processOrderHandler = defineHandler(contract, "processOrder", ({ payload }) =>
  ResultAsync.fromPromise(
    processPayment(payload.orderId),
    (error) => new RetryableError("Payment service unavailable", error),
  ).map(() => undefined),
);

// For permanent failures
const validateOrderHandler = defineHandler(contract, "validateOrder", ({ payload }) => {
  if (payload.amount < 1) {
    return errAsync(new NonRetryableError("Invalid amount"));
  }
  return okAsync(undefined);
});
```

## Error Types

- **`RetryableError`**: Transient failures — message retried
- **`NonRetryableError`**: Permanent failures — message sent to DLQ (if configured) or dropped
- Both extend `HandlerError` base class
- Factory functions: `retryable()`, `nonRetryable()` (shorthand)
- Type guards: `isRetryableError()`, `isNonRetryableError()`, `isHandlerError()`

```typescript
// Conditional error handling
({ payload }) =>
  ResultAsync.fromPromise(process(payload), (error) => {
    if (error instanceof ValidationError) {
      return new NonRetryableError("Invalid data");
    }
    return new RetryableError("Temporary failure", error);
  }).map(() => undefined);
```

## Handler Options

```typescript
const handlers = {
  processOrder: [
    processOrderHandler,
    { prefetch: 10 }, // Process up to 10 messages concurrently
  ],
};
```

## neverthrow API Reference

This project uses [neverthrow](https://github.com/supermacro/neverthrow) for functional error handling.

### ResultAsync<A, E> Key Methods

| Method                                 | Description                                                    | Example                                                      |
| -------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------ |
| `okAsync(value)` / `errAsync(error)`   | Create a resolved ResultAsync                                  | `okAsync(undefined)` / `errAsync(new Error("x"))`            |
| `ResultAsync.fromPromise(promise, fn)` | Convert Promise to ResultAsync; `fn` maps the rejection reason | `ResultAsync.fromPromise(fetch(url), (e) => new TechErr(e))` |
| `.map(f)`                              | Transform Ok value                                             | `.map(() => undefined)`                                      |
| `.mapErr(f)`                           | Transform Error value                                          | `.mapErr((e) => new RetryableError(e))`                      |
| `.andThen(f)`                          | Chain with another Result/ResultAsync                          | `.andThen((v) => okAsync(v))`                                |
| `await resultAsync`                    | Resolves to a `Result<T, E>` (does not throw on `Err`)         | `const r = await future; if (r.isErr()) { ... }`             |

### Result<Ok, Error> Key Methods

| Method                 | Description                          | Example                             |
| ---------------------- | ------------------------------------ | ----------------------------------- |
| `ok(value)`            | Create success                       | `ok(undefined)`                     |
| `err(error)`           | Create failure                       | `err(new RetryableError("failed"))` |
| `.isOk()` / `.isErr()` | Type guards                          | `if (result.isOk()) { ... }`        |
| `.map(f)`              | Transform Ok                         | `result.map(x => x * 2)`            |
| `.mapErr(f)`           | Transform Error                      | `result.mapErr(e => new Error(e))`  |
| `.getOr(default)`      | Extract with fallback                | `result.getOr(0)`                   |
| `.match(okFn, errFn)`  | Pattern match (positional callbacks) | `result.match(v => v, () => 0)`     |

### Common Patterns

```typescript
// Simple sync handler
({ payload }) => okAsync(undefined);

// Async with error mapping
({ payload }) =>
  ResultAsync.fromPromise(
    asyncOperation(payload),
    (error) => new RetryableError("Failed", error),
  ).map(() => undefined);
```

## Worker Package Exports

```typescript
// Worker class
export { TypedAmqpWorker } from "@amqp-contract/worker";

// Handler definition
export { defineHandler, defineHandlers } from "@amqp-contract/worker";

// Error classes and factory functions
export {
  RetryableError,
  NonRetryableError,
  TechnicalError,
  MessageValidationError,
  // Factory functions (shorthand)
  retryable,
  nonRetryable,
  // Type guards
  isRetryableError,
  isNonRetryableError,
  isHandlerError,
} from "@amqp-contract/worker";

// Types
export type {
  HandlerError,
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlers,
  WorkerInferConsumedMessage,
} from "@amqp-contract/worker";

// Retry types and helpers (from contract package)
export type { TtlBackoffRetryOptions, ImmediateRequeueRetryOptions } from "@amqp-contract/contract";
export { extractQueue } from "@amqp-contract/contract";
```
