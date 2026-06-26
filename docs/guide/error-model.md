# Error Model

amqp-contract uses [`unthrown`](https://github.com/btravstack/unthrown)'s `AsyncResult<T, E>` everywhere — there are no thrown exceptions in the public API, no `try/catch` to remember. Errors are values you propagate, transform, and inspect. unthrown adds a third **`Defect`** channel for genuinely unexpected failures, so `match` has an `ok` / `err` / `defect` shape.

This page lists every error type the library can produce, where it surfaces, and what you should do with it.

## Error hierarchy

```
Error
├── HandlerError                     (worker-side, returned by handlers)
│   ├── RetryableError               — go through queue retry mode
│   └── NonRetryableError            — straight to DLQ, skip retry
├── TechnicalError                   (any AMQP / framework failure)
└── MessageValidationError           (Standard Schema validation issue)

RpcCancelledError                    (client-side RPC, worker shut down)
RpcTimeoutError                      (client-side RPC, deadline elapsed)
```

## Handler errors

Returned by your handler functions. They live in `@amqp-contract/worker`.

### `RetryableError`

The failure is transient. The queue's [retry mode](./retry-strategies.md) decides what happens next. If the queue has no retry config or `mode: "none"`, the message is sent to the DLQ.

```ts
import { RetryableError } from "@amqp-contract/worker";

({ payload }) =>
  fromPromise(
    callExternalApi(payload),
    (error) => new RetryableError("API unavailable", error),
  ).map(() => undefined);
```

### `NonRetryableError`

The failure is permanent. The message bypasses the retry mode entirely and goes to the DLQ (or is dropped if no DLX is configured).

```ts
import { NonRetryableError } from "@amqp-contract/worker";

({ payload }) => {
  if (payload.amount < 0) {
    return err(new NonRetryableError("Negative amount")).toAsync();
  }
  // ...
};
```

### Factory functions and type guards

```ts
import {
  retryable,
  nonRetryable,
  isRetryableError,
  isNonRetryableError,
  isHandlerError,
} from "@amqp-contract/worker";

// Shorthand factories
retryable("API unavailable", error); // === new RetryableError(...)
nonRetryable("Invalid input", error); // === new NonRetryableError(...)

// Discriminate when handling
if (isRetryableError(err)) {
  /* ... */
}
if (isNonRetryableError(err)) {
  /* ... */
}
if (isHandlerError(err)) {
  /* either kind */
}
```

## Framework errors

### `TechnicalError`

Any failure of the AMQP transport itself: connection lost, channel closed, broker rejected an assert, etc. Returned from `@amqp-contract/core` operations. Carries an optional `cause` chain.

```ts
import { TechnicalError } from "@amqp-contract/core";

const result = await client.publish("orderCreated", { orderId: "1" });
result.match({
  ok: () => console.log("ok"),
  err: (err) => {
    if (err instanceof TechnicalError) {
      // err.cause holds the original amqplib / amqp-connection-manager error
    }
  },
  defect: (cause) => {
    throw cause;
  },
});
```

### `MessageValidationError`

A Standard Schema validation failed (incoming payload, incoming headers, or RPC response shape). Carries the source identifier (consumer/publisher name) and the schema's `issues` array.

On the worker side, validation failures route directly to the DLQ via `nack(requeue=false)` — they never enter the retry pipeline because retrying a malformed payload cannot succeed. The message body is preserved exactly as the broker delivered it; the worker does not republish, so it does not stamp diagnostic headers like `x-last-error` on this path. The error details (consumer name, schema `issues`) live in the worker's logs. See [Retry Strategies → Inspecting retry state](./retry-strategies.md#inspecting-retry-state) for the full breakdown of which DLQ paths add headers.

On the client side (publisher input or RPC response validation), `MessageValidationError` is returned via `err(...)` from `publish()` / `call()` so you can decide how to react before sending.

## Client-side RPC errors

### `RpcTimeoutError`

The reply did not arrive within the configured `timeoutMs` (or the server-side default). The pending call is cleared and the future resolves to `err(RpcTimeoutError)`.

### `RpcCancelledError`

The client was closed (`client.close()`) while a call was still pending. All in-flight calls fail with this error so callers don't hang.

```ts
import { RpcTimeoutError, RpcCancelledError } from "@amqp-contract/client";

const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 });
result.match({
  ok: (response) => /* ... */,
  err: (err) => {
    if (err instanceof RpcTimeoutError) /* retry, or fall back */;
    if (err instanceof RpcCancelledError) /* shutting down */;
    if (err instanceof MessageValidationError) /* response shape wrong */;
    if (err instanceof TechnicalError) /* transport problem */;
  },
  defect: (cause) => {
    throw cause;
  },
});
```

## Why not just throw?

Two reasons:

1. **Async errors that don't reject Promises silently.** A handler that throws synchronously inside a AsyncResult chain would normally crash the consume loop. Returning `err(...)` makes failure a value the worker can route deterministically (DLQ, retry, ack).

2. **Type-safe error union.** `AsyncResult<T, MyError | OtherError>` lets TypeScript force you to handle every variant via `.match({ ok, err, defect })`. A thrown `unknown` gives no such guarantees.

## Defensive guards

The worker still wraps the consume callback in `try/catch` so a buggy handler that throws synchronously cannot leave a message neither acked nor nacked: the worker logs the error and nacks with `requeue=false` (DLQ if configured). Don't rely on it — return `err(...).toAsync()` instead.

## See also

- [Retry Strategies](./retry-strategies.md) — how `RetryableError` interacts with queue-level retry modes.
- [Worker Usage](./worker-usage.md) — handler signatures and `defineHandler`.
- [Client Usage](./client-usage.md) — publish/RPC return types.
