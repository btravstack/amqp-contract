# Handler Patterns

This project uses [unthrown](https://github.com/btravstack/unthrown) for explicit, value-based error handling. Handlers return an `AsyncResult<T, E>` rather than throwing or using `async/await`. unthrown adds a third **`Defect`** channel for unexpected throws alongside `Ok` / `Err`.

## Regular consumer handler

A consumer handler receives `({ payload, headers }, rawMessage)` and returns `AsyncResult<void, HandlerError>`:

```typescript
import { fromPromise, OkAsync } from "unthrown";
import { RetryableError, NonRetryableError } from "@amqp-contract/worker";

// Sync OK case — lift a sync Result into an AsyncResult with .toAsync()
const handler = ({ payload }, rawMessage) => {
  console.log(payload.orderId);
  return OkAsync(undefined);
};

// Async case — fromPromise REQUIRES the qualify mapper as the second arg
const asyncHandler = ({ payload }) =>
  fromPromise(processPayment(payload), (error) => new RetryableError("Payment failed", error)).map(
    () => undefined,
  );
```

### Parameters

1. **`message`** — `{ payload, headers }`
   - `payload`: validated against the message's payload schema
   - `headers`: validated against the message's optional headers schema (otherwise `undefined`)
2. **`rawMessage`** — the raw amqplib `ConsumeMessage` (e.g. `msg.fields.deliveryTag`, `msg.properties.messageId`)

## RPC handler

`defineRpc` creates a request-reply slot. RPC handlers return `AsyncResult<TResponse, HandlerError | WorkerInferRpcErrors<...>>` — the worker validates the response against the RPC's response schema and publishes it back to the caller's `replyTo` with the same `correlationId`.

All handlers (consumer and RPC) receive a third `helpers` argument — `{ context, errors }`. `context` is seeded by `createContext` and accumulated by the middleware chain (`TypedAmqpWorker.create({ createContext, middleware: composeMiddleware(...) })`); `errors` carries typed constructors for the RPC's declared errors (`ErrAsync(errors.CODE({ ... }))`), empty for consumers. Middleware `next({ payload })` substitutes the payload with re-validation before the handler. See `packages/worker/src/middleware.ts`, `packages/worker/src/worker.ts` (`runHandler`), and [docs/guide/middleware-and-interceptors.md](../../docs/guide/middleware-and-interceptors.md).

When the RPC declares an `errors` map (`defineRpc(queue, { request, response, errors })`), the handler may also return `Err(rpcError(code, data))` for a declared code — the worker validates `data` against the declared schema, publishes an error reply (marked by the `RPC_ERROR_CODE_HEADER` header), and **acks the request**; typed business errors never enter the retry/DLQ pipeline. Undeclared codes or invalid error data are contract violations routed to the DLQ. See `packages/worker/src/worker.ts` (`publishRpcErrorReply`) and [docs/guide/error-model.md](../../docs/guide/error-model.md#typed-rpc-errors-rpcerror).

You can define RPC handlers either with `defineHandler` / `defineHandlers` (overloaded against `InferRpcNames<TContract>`, and `validateHandlerTargetExists` checks both `contract.consumers` and `contract.rpcs`) or inline inside `TypedAmqpWorker.create({ handlers: { … } })`. The inline `handlers` parameter is typed against `WorkerInferHandlers<TContract>`, so each name (consumer or RPC) gets the correct signature inferred:

```typescript
import { fromPromise, OkAsync } from "unthrown";
import { TypedAmqpWorker, RetryableError } from "@amqp-contract/worker";

const result = await TypedAmqpWorker.create({
  contract,
  handlers: {
    // Regular consumer — `payload` typed from the consumer's message schema
    processOrder: ({ payload }) => OkAsync(undefined),

    // RPC handler — must return the typed response payload
    calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),

    // RPC with async work
    lookupUser: ({ payload }) =>
      fromPromise(
        db.users.findById(payload.userId),
        (error) => new RetryableError("DB unavailable", error),
      ).map((user) => ({ id: user.id, name: user.name })),
  },
  urls: ["amqp://localhost"],
});
```

The matching client-side call (`match` has three branches; `errCases` takes an exhaustive matcher over the error union):

```typescript
import { tag } from "unthrown";

const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 });
result.match({
  ok: (value) => console.log(value.sum), // 5
  errCases: (matcher) =>
    matcher.with(
      tag("@amqp-contract/MessageValidationError"),
      tag("@amqp-contract/RpcTimeoutError"),
      tag("@amqp-contract/RpcCancelledError"),
      (error) => console.error(error),
    ),
  // transport failures (TechnicalError) surface here as defects, not modeled errors
  defect: (cause) => console.error(cause),
});
```

RPC error semantics worth knowing:

- **Missing `replyTo` / `correlationId`** on the inbound message → `NonRetryableError`. The request is `nack`ed without requeue, so it routes to the queue's DLQ if configured (poison messages stay visible for inspection rather than being silently ack'd).
- **Response fails the response schema** → `NonRetryableError` (handler returned the wrong shape; retrying won't help).
- **Client-side timeout** → call resolves to `Err(RpcTimeoutError)`; pending state is cleared. If a reply still arrives, it's logged at `warn` and counted via `recordLateRpcReply` (telemetry hook for tuning) — it's not retried.
- **Client closed mid-call** → call resolves to `Err(RpcCancelledError)`.

## Using `defineHandler` / `defineHandlers`

Use `defineHandler` (single) or `defineHandlers` (object) for full type inference and a runtime check that the name exists in the contract. Both helpers accept consumer **and** RPC names — they're overloaded against `InferConsumerNames` and `InferRpcNames`, and `validateHandlerTargetExists` inspects both `contract.consumers` and `contract.rpcs` (an unknown name throws _"Handler target X not found in contract"_).

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { ErrAsync, fromPromise, OkAsync } from "unthrown";

const processOrderHandler = defineHandler(contract, "processOrder", ({ payload }) =>
  fromPromise(
    processPayment(payload.orderId),
    (error) => new RetryableError("Payment service unavailable", error),
  ).map(() => undefined),
);

// Permanent failures use NonRetryableError → DLQ, never retried
const validateOrderHandler = defineHandler(contract, "validateOrder", ({ payload }) => {
  if (payload.amount < 1) {
    return ErrAsync(new NonRetryableError("Invalid amount"));
  }
  return OkAsync(undefined);
});
```

## Error types

`HandlerError` is the **union type alias** `RetryableError | NonRetryableError` (not a base class — `instanceof HandlerError` does not work). Handlers can only legally return one of those two error types. The other errors below are produced by the framework around handlers.

### Returned from handlers

| Error               | Behaviour                                                                       |
| ------------------- | ------------------------------------------------------------------------------- |
| `RetryableError`    | Transient. Worker requeues per the queue's `retry` mode (immediate or backoff). |
| `NonRetryableError` | Permanent. Worker `nack`s without requeue, sending to DLQ if configured.        |

Helpers and type guards: `retryable()`, `nonRetryable()` factory functions; `isRetryableError`, `isNonRetryableError`, `isHandlerError` for narrowing.

### Raised by the framework around handlers

| Error                    | When                                                                                                                                                                                                                                |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MessageValidationError` | Inbound payload/headers failed schema validation **before** the handler ran. Routes to DLQ — never retried. A modeled `Err` in `E`.                                                                                                 |
| `TechnicalError`         | Transport-level failure (connection, channel, broker). Surfaced as a **`Defect`** (its `cause`), **not** a modeled `Err` — handle it in the `defect` arm of `match` (or `recoverDefect` / `tapDefect`), never in the error matcher. |

### Client-side (returned from `client.publish` / `client.call`)

| Error                    | When                                                                                                                                                                                     |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MessageValidationError` | Outbound payload failed the request/publisher schema before the message hit the broker. A modeled `Err`.                                                                                 |
| `TechnicalError`         | Publish/transport failed at the broker (channel buffer full, connection lost, etc.). Surfaced as a **`Defect`** (its `cause`), never a modeled `Err` — handle it in the `defect` arm.    |
| `RpcTimeoutError`        | RPC call's `timeoutMs` elapsed before a reply arrived. Pending state is cleared. A reply that arrives later is logged at `warn` and counted via `recordLateRpcReply` (it isn't retried). |
| `RpcCancelledError`      | RPC was in flight when `client.close()` was called. All pending calls fail with this so callers don't hang.                                                                              |

`publish()` returns `AsyncResult<void, MessageValidationError>` (a transport failure is a `Defect`, not in `E`).
`call()` returns `AsyncResult<TResponse, MessageValidationError | RpcTimeoutError | RpcCancelledError>` (plus any declared `RpcError`s; a transport failure is a `Defect`, not in `E`).

```typescript
// Conditional error mapping inside fromPromise's qualify
({ payload }) =>
  fromPromise(process(payload), (error) => {
    if (error instanceof ValidationError) return new NonRetryableError("Invalid data");
    return new RetryableError("Temporary failure", error);
  }).map(() => undefined);
```

## Per-handler options

Handler entries accept an `[handler, options]` tuple to override `defaultConsumerOptions` for a single consumer:

```typescript
const handlers = {
  processOrder: [processOrderHandler, { prefetch: 10 }],
};
```

## unthrown API quick reference

For the authoritative API read unthrown's type definitions; the subset this project uses:

`AsyncResult<T, E>` (async; `await` resolves to a `Result<T, E>`):

| Method                          | Description                                                                                                                                                                                                                                                                                                  |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `OkAsync(value)`                | Lift a successful sync `Result` into an `AsyncResult`                                                                                                                                                                                                                                                        |
| `ErrAsync(error)`               | Lift a failed sync `Result` into an `AsyncResult`                                                                                                                                                                                                                                                            |
| `fromPromise(promise, qualify)` | Wrap a `Promise`; `qualify(cause, defect)` maps the rejection to `E \| defect(cause)` (call the `defect` callback for unexpected failures). Required.                                                                                                                                                        |
| `fromSafePromise(promise)`      | Wrap a `Promise` asserted not to fail in a modeled way (rejection → `Defect`).                                                                                                                                                                                                                               |
| `.map(f)` / `.mapErrCases(m)`   | Transform the OK value / the error. `mapErrCases` takes an exhaustive matcher callback listing every modeled error case: `.mapErrCases((matcher) => matcher.with(tag("@amqp-contract/MessageValidationError"), (error) => …))` (a `TechnicalError` is a defect — recover it with `.recoverDefect`, not here) |
| `.flatMap(f)`                   | Chain another `Result` / `AsyncResult` (was `.andThen` in neverthrow)                                                                                                                                                                                                                                        |
| `.flatMapErrCases(m)`           | Recover from an error with another `Result` / `AsyncResult`; takes the same exhaustive matcher callback as `mapErrCases`                                                                                                                                                                                     |
| `.tap(f)` / `.tapErrCases(m)`   | Side effect on OK / error without changing the value (was `.andTee` / `.orTee`). `tapErrCases` takes the same matcher callback (its branch must be sync)                                                                                                                                                     |
| `await asyncResult`             | Resolves to a `Result<T, E>` — no exception, even on `Err`                                                                                                                                                                                                                                                   |

`Result<T, E>` (sync; a union of `Ok` / `Err` / `Defect`):

| Method / function                                               | Description                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ok(value)` / `Err(error)`                                      | Construct a successful / failed `Result`                                                                                                                                                                                                                                                                            |
| `r.isOk()` / `r.isErr()` / `r.isDefect()`                       | **Preferred** narrowing form — the methods narrow `this` (unthrown 0.2.0+), so `if (r.isErr()) r.error` works. Standalone `isOk(r)` / `isErr(r)` / `isDefect(r)` functions narrow identically but aren't used here.                                                                                                 |
| `.match({ ok, errCases, defect })`                              | Pattern match with three branches (positional `match(okFn, errFn)` is **not** supported). `errCases` takes the exhaustive matcher over the **modeled** errors: `errCases: (matcher) => matcher.with(tag("@amqp-contract/MessageValidationError"), (error) => …)`; a `TechnicalError` arrives in the `defect` branch |
| `r.match({ ok, defect, errCases: (m) => m.with(tag("…"), …) })` | Exhaustive per-tag dispatch on a tagged-error union's `_tag` via the error matcher (replaces the removed `matchTags`)                                                                                                                                                                                               |
| `.getOr(default)`                                               | Extract the value or fall back                                                                                                                                                                                                                                                                                      |
| `.getOrThrow()` / `.getErr()`                                   | Throw on the wrong variant; re-throws a `Defect`'s cause. Use sparingly.                                                                                                                                                                                                                                            |

## Public exports

For the authoritative list, read [`packages/worker/src/index.ts`](../../packages/worker/src/index.ts). What's currently re-exported:

- Classes: `TypedAmqpWorker`, `RetryableError`, `NonRetryableError`, `MessageValidationError` (the error classes are unthrown `TaggedError`s). `HandlerError` is a **type** (`RetryableError | NonRetryableError`), not a class.
- Factories / guards: `retryable`, `nonRetryable`, `isRetryableError`, `isNonRetryableError`, `isHandlerError`
- Helpers: `defineHandler`, `defineHandlers` (both accept consumer **and** RPC names)
- Types: `CreateWorkerOptions`, `ConsumerOptions`, `WorkerConsumedMessage`, `WorkerInferConsumedMessage`, `WorkerInferConsumerHandler`, `WorkerInferConsumerHandlerEntry`, `WorkerInferConsumerHeaders`, `WorkerInferHandlers` (consumers ∪ rpcs), `WorkerInferRpcConsumedMessage`, `WorkerInferRpcHandler`, `WorkerInferRpcHandlerEntry`, `WorkerInferRpcHeaders`, `WorkerInferRpcRequest`, `WorkerInferRpcResponse`
