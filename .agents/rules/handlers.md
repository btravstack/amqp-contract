# Handler Patterns

This project uses [neverthrow](https://github.com/supermacro/neverthrow) for explicit, value-based error handling. Handlers return a `ResultAsync<T, E>` rather than throwing or using `async/await`.

## Regular consumer handler

A consumer handler receives `({ payload, headers }, rawMessage)` and returns `ResultAsync<void, HandlerError>`:

```typescript
import { okAsync, ResultAsync } from "neverthrow";
import { RetryableError, NonRetryableError } from "@amqp-contract/worker";

// Sync OK case
const handler = ({ payload }, rawMessage) => {
  console.log(payload.orderId);
  return okAsync(undefined);
};

// Async case — fromPromise REQUIRES the error mapper as the second arg
const asyncHandler = ({ payload }) =>
  ResultAsync.fromPromise(
    processPayment(payload),
    (error) => new RetryableError("Payment failed", error),
  ).map(() => undefined);
```

### Parameters

1. **`message`** — `{ payload, headers }`
   - `payload`: validated against the message's payload schema
   - `headers`: validated against the message's optional headers schema (otherwise `undefined`)
2. **`rawMessage`** — the raw amqplib `ConsumeMessage` (e.g. `msg.fields.deliveryTag`, `msg.properties.messageId`)

## RPC handler

`defineRpc` creates a request-reply slot. Handlers return `ResultAsync<TResponse, HandlerError>` — the worker validates the response against the RPC's response schema and publishes it back to the caller's `replyTo` with the same `correlationId`.

```typescript
import { okAsync, ResultAsync } from "neverthrow";
import { defineHandler, RetryableError } from "@amqp-contract/worker";

const calculateHandler = defineHandler(contract, "calculate", ({ payload }) =>
  okAsync({ sum: payload.a + payload.b }),
);

const lookupUserHandler = defineHandler(contract, "lookupUser", ({ payload }) =>
  ResultAsync.fromPromise(
    db.users.findById(payload.userId),
    (error) => new RetryableError("DB unavailable", error),
  ).map((user) => ({ id: user.id, name: user.name })),
);
```

The matching client-side call:

```typescript
const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 });
if (result.isOk()) {
  console.log(result.value.sum); // 5
}
```

RPC error semantics worth knowing:

- **Missing `replyTo` / `correlationId`** on the inbound message → `NonRetryableError` (request goes to DLQ — retrying can't recover the lost reply path).
- **Response fails the response schema** → `NonRetryableError` (handler returned the wrong shape; retrying won't help).
- **Client-side timeout** → call resolves to `err(RpcTimeoutError)`; pending state is cleared so a late reply is dropped silently.
- **Client closed mid-call** → call resolves to `err(RpcCancelledError)`.

## Using `defineHandler` / `defineHandlers`

Use `defineHandler` (single) or `defineHandlers` (object) for full type inference from the contract. Both also validate at construction time that the name exists in `contract.consumers` ∪ `contract.rpcs`:

```typescript
import { defineHandler, RetryableError, NonRetryableError } from "@amqp-contract/worker";
import { errAsync, okAsync, ResultAsync } from "neverthrow";

const processOrderHandler = defineHandler(contract, "processOrder", ({ payload }) =>
  ResultAsync.fromPromise(
    processPayment(payload.orderId),
    (error) => new RetryableError("Payment service unavailable", error),
  ).map(() => undefined),
);

// Permanent failures use NonRetryableError → DLQ, never retried
const validateOrderHandler = defineHandler(contract, "validateOrder", ({ payload }) => {
  if (payload.amount < 1) {
    return errAsync(new NonRetryableError("Invalid amount"));
  }
  return okAsync(undefined);
});
```

## Error types

### Worker-side (returned from handlers)

| Error                    | Behaviour                                                                        |
| ------------------------ | -------------------------------------------------------------------------------- |
| `RetryableError`         | Transient. Worker requeues per the queue's `retry` mode (immediate or backoff).  |
| `NonRetryableError`      | Permanent. Worker `nack`s without requeue, sending to DLQ if configured.         |
| `MessageValidationError` | Inbound payload/headers failed schema validation. Routes to DLQ — never retried. |
| `TechnicalError`         | Transport-level failure (connection, channel, broker). Returned by core helpers. |

Helpers and type guards: `retryable()`, `nonRetryable()` factory functions; `isRetryableError`, `isNonRetryableError`, `isHandlerError` for narrowing. Both `RetryableError` and `NonRetryableError` extend the `HandlerError` union type.

### Client-side (returned from `client.publish` / `client.call`)

| Error                    | When you get it                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `MessageValidationError` | Outbound payload failed the request/publisher schema before the message hit the broker.                         |
| `TechnicalError`         | Publish failed at the broker (channel buffer full, connection lost, etc.).                                      |
| `RpcTimeoutError`        | RPC call's `timeoutMs` elapsed before a reply arrived. Pending state cleared; a late reply is dropped silently. |
| `RpcCancelledError`      | RPC was in flight when `client.close()` was called. All pending calls fail with this so callers don't hang.     |

`publish()` returns `ResultAsync<void, TechnicalError | MessageValidationError>`.
`call()` returns `ResultAsync<TResponse, TechnicalError | MessageValidationError | RpcTimeoutError | RpcCancelledError>`.

```typescript
// Conditional error mapping inside fromPromise
({ payload }) =>
  ResultAsync.fromPromise(process(payload), (error) => {
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

## neverthrow API quick reference

`ResultAsync<T, E>`:

| Method                                 | Description                                                           |
| -------------------------------------- | --------------------------------------------------------------------- |
| `okAsync(value)` / `errAsync(error)`   | Construct a resolved `ResultAsync`                                    |
| `ResultAsync.fromPromise(promise, fn)` | Wrap a `Promise`; `fn` maps the rejection reason. Mapper is required. |
| `.map(f)`                              | Transform the OK value                                                |
| `.mapErr(f)`                           | Transform the error                                                   |
| `.andThen(f)`                          | Chain another `Result` / `ResultAsync`                                |
| `.orElse(f)`                           | Recover from an error with another `Result` / `ResultAsync`           |
| `.andTee(f)` / `.orTee(f)`             | Side effect on OK / error without changing the value                  |
| `await resultAsync`                    | Resolves to a `Result<T, E>` — no exception, even on `Err`            |

`Result<T, E>` (sync):

| Method                 | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| `ok(value)`            | Construct a successful `Result`                                             |
| `err(error)`           | Construct a failed `Result`                                                 |
| `.isOk()` / `.isErr()` | Narrowing type guards (read `.value` / `.error` after)                      |
| `.match(okFn, errFn)`  | Positional pattern match (boxed-style `{ Ok, Error }` is **not** supported) |
| `.unwrapOr(default)`   | Extract the value or fall back                                              |
| `._unsafeUnwrap()`     | Throw on `Err`. Use sparingly — prefer matching.                            |

## Public exports

For the authoritative list of what's exported from `@amqp-contract/worker`, read [`packages/worker/src/index.ts`](../../packages/worker/src/index.ts). Notable types: `WorkerInferHandlers<TContract>` (full handlers object — covers `consumers` ∪ `rpcs`), `WorkerInferConsumerHandler`, `WorkerInferRpcHandler`, `WorkerInferConsumedMessage`, `WorkerInferRpcConsumedMessage`. The legacy `WorkerInferConsumerHandlers` alias is `@deprecated` and still re-exported for one cycle — new code should use `WorkerInferHandlers`.
