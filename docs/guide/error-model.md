# Error Model

amqp-contract uses [`unthrown`](https://github.com/btravstack/unthrown)'s `AsyncResult<T, E>` everywhere — there are no thrown exceptions in the public API, no `try/catch` to remember. Errors are values you propagate, transform, and inspect. unthrown adds a third **`Defect`** channel for genuinely unexpected failures, so `match` has an `ok` / `err` / `defect` shape.

This page lists every error type the library can produce, where it surfaces, and what you should do with it.

## Getting the value out

The safe way to consume a result is `.match({ ok, err, defect })` — it forces you to handle every channel.

`unthrown` 4 makes `.unwrap()` **type-gated**: it compiles only on a result whose error channel is empty (`E = never`), so `Ok(x).unwrap()` works but `(await client.publish(...)).unwrap()` on a fallible result is a compile error. When you genuinely want to throw on failure (a script, a test, an example), use `.unwrapOrElse()` — it returns the value on `Ok`, runs your callback on `Err` (throw to surface it), and rethrows a `Defect`'s cause:

```ts
// throws on Err (and rethrows a Defect) — the escape hatch, not the default
const client = await TypedAmqpClient.create({ contract, urls: ["amqp://localhost"] }).unwrapOrElse(
  (e) => {
    throw e;
  },
);
```

Prefer `.match()` / `.recover()` / `.orElse()` in real code; reach for the `.unwrapOrElse((e) => { throw e })` form only where throwing is acceptable.

## Error hierarchy

```
Error
├── HandlerError                     (worker-side, returned by handlers)
│   ├── RetryableError               — go through queue retry mode
│   └── NonRetryableError            — straight to DLQ, skip retry
├── RpcError<code, data>             (typed business error declared on an RPC)
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
import { Err } from "unthrown";

({ payload }) => {
  if (payload.amount < 0) {
    return Err(new NonRetryableError("Negative amount")).toAsync();
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

On the client side (publisher input or RPC response validation), `MessageValidationError` is returned via `Err(...)` from `publish()` / `call()` so you can decide how to react before sending.

## Typed RPC errors (`RpcError`)

An RPC can declare its business failures in the contract, alongside the request and response schemas. Each error code maps to a `defineMessage(...)` validating the error's `data` payload:

```ts
import { defineMessage, defineQueue, defineRpc } from "@amqp-contract/contract";
import { z } from "zod";

const getOrder = defineRpc(defineQueue("rpc.get-order"), {
  request: defineMessage(z.object({ orderId: z.string() })),
  response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
  errors: {
    ORDER_NOT_FOUND: defineMessage(z.object({ orderId: z.string() })),
  },
});
```

Declared errors widen both ends of the RPC:

**Worker side** — the handler's error channel becomes `HandlerError | RpcError<code, data>`. Return one with the `rpcError` factory:

```ts
import { rpcError } from "@amqp-contract/worker";
import { Err, Ok } from "unthrown";

const handlers = defineHandlers(contract, {
  getOrder: ({ payload }) => {
    const order = orders.get(payload.orderId);
    if (!order) {
      return Err(rpcError("ORDER_NOT_FOUND", { orderId: payload.orderId })).toAsync();
    }
    return Ok({ orderId: order.id, status: order.status }).toAsync();
  },
});
```

A returned `RpcError` is the RPC's _business-failure channel_, not a processing failure: the worker validates `data` against the declared schema, publishes an error reply to the caller, and **acks the request — business errors are never retried**. Only `RetryableError` / `NonRetryableError` enter the retry/DLQ pipeline.

**Client side** — the `call()` error union gains the declared `RpcError<code, data>` members. Discriminate with `isRpcError` and narrow on `code`:

```ts
import { isRpcError } from "@amqp-contract/client";

const result = await client.call("getOrder", { orderId: "42" }, { timeoutMs: 5_000 });
if (result.isErr() && isRpcError(result.error)) {
  // result.error.code is "ORDER_NOT_FOUND"; result.error.data is { orderId: string }
  console.log(`Order ${result.error.data.orderId} not found`);
}
```

Error `data` is validated twice: on the worker before the reply is published, and again on the client when it arrives — the same double-validation contract as responses.

### Contract enforcement at runtime

The type system prevents undeclared codes, but a cast (or two services running different contract versions) can bypass it. The runtime holds the line:

- **Worker**: an undeclared code or `data` failing its schema is a contract violation — the reply is not published and the request routes to the DLQ as a `NonRetryableError`. The caller times out.
- **Client**: an error reply whose code the local contract doesn't declare resolves to `Err(TechnicalError)`; error data failing its schema resolves to `Err(MessageValidationError)`.

### Wire format

Success replies are unchanged. An error reply is marked by the `x-amqp-contract-error-code` AMQP header (exported as `RPC_ERROR_CODE_HEADER` from `@amqp-contract/core`) carrying the code, with a `{ message, data }` JSON body. RPCs that declare no `errors` behave exactly as before.

## Client-side RPC errors

### `RpcTimeoutError`

The reply did not arrive within the configured `timeoutMs` (or the server-side default). The pending call is cleared and the future resolves to `Err(RpcTimeoutError)`.

### `RpcCancelledError`

The client was closed (`client.close()`) while a call was still pending. All in-flight calls fail with this error so callers don't hang.

```ts
import { RpcTimeoutError, RpcCancelledError } from "@amqp-contract/client";

const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 });
result.match({
  ok: (response) => /* ... */,
  err: (err) => {
    if (isRpcError(err)) /* declared business error — narrow on err.code */;
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

1. **Async errors that don't reject Promises silently.** A handler that throws synchronously inside a AsyncResult chain would normally crash the consume loop. Returning `Err(...)` makes failure a value the worker can route deterministically (DLQ, retry, ack).

2. **Type-safe error union.** `AsyncResult<T, MyError | OtherError>` lets TypeScript force you to handle every variant via `.match({ ok, err, defect })`. A thrown `unknown` gives no such guarantees.

## Defensive guards

The worker still wraps the consume callback in `try/catch` so a buggy handler that throws synchronously cannot leave a message neither acked nor nacked: the worker logs the error and nacks with `requeue=false` (DLQ if configured). Don't rely on it — return `Err(...).toAsync()` instead.

## See also

- [Retry Strategies](./retry-strategies.md) — how `RetryableError` interacts with queue-level retry modes.
- [Worker Usage](./worker-usage.md) — handler signatures and `defineHandler`.
- [Client Usage](./client-usage.md) — publish/RPC return types.
