---
title: Error model - amqp-contract
description: Every error type the library produces, where it surfaces, its tag, and the RPC error wire format.
---

# Error model

Every error type, where it appears, and what to do with it. For the reasoning behind the three channels, see [errors as values](/explanation/errors-as-values).

## Channels

| Channel             | Contains                                              | Handled by                                             |
| ------------------- | ----------------------------------------------------- | ------------------------------------------------------ |
| `ok`                | The success value                                     | `ok` branch, `.map`, `.get()`                          |
| Modeled error (`E`) | Anticipated domain failures                           | `errCases` matcher, `.mapErrCases`, `.recoverErrCases` |
| Defect              | Unexpected failures — always a `TechnicalError` cause | `defect` branch, `.tapDefect`, `.recoverDefect`        |

## Error types

```
HandlerError                  worker-side, returned by handlers (a union type)
├── RetryableError            → queue retry mode
└── NonRetryableError         → dead-letter, skipping retries

MessageValidationError        Standard Schema validation failed
RpcError<code, data>          declared business error on an RPC
RpcTimeoutError               client-side: no reply in time
RpcCancelledError             client-side: client closed mid-call
TechnicalError                transport/framework failure — always a defect cause, never in E
```

All are `TaggedError`s, so they carry a namespaced `_tag` for exhaustive dispatch. `Error.name` stays bare.

| Type                     | Tag                                     | Exported from                                           |
| ------------------------ | --------------------------------------- | ------------------------------------------------------- |
| `RetryableError`         | `@amqp-contract/RetryableError`         | `@amqp-contract/worker`                                 |
| `NonRetryableError`      | `@amqp-contract/NonRetryableError`      | `@amqp-contract/worker`                                 |
| `MessageValidationError` | `@amqp-contract/MessageValidationError` | `@amqp-contract/core`, re-exported by client and worker |
| `RpcError`               | `@amqp-contract/RpcError`               | `@amqp-contract/core`, re-exported by client and worker |
| `RpcTimeoutError`        | `@amqp-contract/RpcTimeoutError`        | `@amqp-contract/client`                                 |
| `RpcCancelledError`      | `@amqp-contract/RpcCancelledError`      | `@amqp-contract/client`                                 |
| `TechnicalError`         | `@amqp-contract/TechnicalError`         | `@amqp-contract/core`, re-exported by client and worker |

## Error channel per operation

| Operation                | Returns                                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------------------- |
| `TypedAmqpClient.create` | `AsyncResult<TypedAmqpClient, never>`                                                                   |
| `client.publish`         | `AsyncResult<void, MessageValidationError>`                                                             |
| `client.call`            | `AsyncResult<TResponse, MessageValidationError \| RpcTimeoutError \| RpcCancelledError \| RpcError<…>>` |
| `client.close`           | `AsyncResult<void, never>`                                                                              |
| `TypedAmqpWorker.create` | `AsyncResult<TypedAmqpWorker, never>`                                                                   |
| `worker.close`           | `AsyncResult<void, never>`                                                                              |
| Consumer handler         | `AsyncResult<void, HandlerError>`                                                                       |
| RPC handler              | `AsyncResult<TResponse, HandlerError \| RpcError<…>>`                                                   |

An empty channel (`never`) means every failure is a defect, which is why `.get()` compiles on `create` and `close` but not on `publish`.

The client exports the unions by name: `PublishError` (an alias of `MessageValidationError`) and `CallError` (the full `call()` union). For the error union of one specific RPC — declared errors included — use `ClientInferCallError<typeof contract, "getOrder">`.

## Handler errors

### `RetryableError`

The failure may not recur. The queue's [retry mode](/how-to/retry-failed-messages) decides what happens. With no retry config, or `mode: "none"`, the message is dead-lettered.

```typescript
import { RetryableError } from "@amqp-contract/worker";

fromPromise(callApi(payload), (cause) => new RetryableError("API unavailable", cause));
```

### `NonRetryableError`

The failure is permanent. The message bypasses the retry mode and is dead-lettered.

```typescript
import { NonRetryableError } from "@amqp-contract/worker";
import { ErrAsync } from "unthrown";

ErrAsync(new NonRetryableError("negative amount"));
```

### Factories and guards

```typescript
import {
  isHandlerError,
  isNonRetryableError,
  isRetryableError,
  nonRetryable,
  qualifyNonRetryable,
  qualifyRetryable,
  retryable,
} from "@amqp-contract/worker";

retryable("API unavailable", cause); // new RetryableError(…)
nonRetryable("invalid input", cause); // new NonRetryableError(…)

// Prebuilt `fromPromise` mappers
fromPromise(callApi(payload), qualifyRetryable("API unavailable"));
fromPromise(chargeCard(payload), qualifyNonRetryable("card declined"));

isRetryableError(err);
isNonRetryableError(err);
isHandlerError(err); // either
```

`HandlerError` is a union type, not a class — use `isHandlerError`, not `instanceof`.

## `MessageValidationError`

A Standard Schema validation failed. Carries the source identifier (publisher or consumer name) and the schema's `issues` array.

**On the client**, returned as a modeled `Err` from `publish()` and `call()`, so you can react before anything is sent.

**On the worker**, validation failures dead-letter the message via `nack(requeue=false)` and never enter the retry pipeline — retrying a malformed payload cannot succeed. The body is preserved exactly as delivered; because the worker does not republish, no diagnostic headers are added. Details are in the logs.

Validated: publisher payloads, consumer payloads, consumer headers, RPC requests, RPC responses, and RPC error data. **Not** validated: headers on publish.

`isMessageValidationError(err)` is the type guard, exported from `@amqp-contract/core` and re-exported by client and worker.

## `TechnicalError`

Any failure of the transport or framework: connection lost, channel closed, a rejected assert, a publish that never reached the broker, a compression or JSON-parse failure, or a schema validator that threw.

These are unexpected, so they surface through the **defect** channel, never as a modeled `Err`. The `TechnicalError` is the defect's `cause`, and carries its own `cause` chain to the underlying amqplib error.

```typescript
import { TechnicalError } from "@amqp-contract/core";
import { P } from "unthrown";

result.match({
  ok: () => {},
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) => {}),
  defect: (cause) => {
    if (cause instanceof TechnicalError) {
      // cause.cause is the original amqplib / amqp-connection-manager error
    }
  },
});
```

Since 3.0 it is not part of any operation's `E`, so it never appears in an error matcher. See [upgrade](/how-to/upgrade#_2-4-x-→-3-0).

`isTechnicalError(cause)` is the type guard — an alternative to `instanceof` in the snippet above — exported from `@amqp-contract/core` and re-exported by client and worker.

## Typed RPC errors

Declared in the contract alongside request and response:

```typescript
const getOrder = defineRpc(defineQueue("rpc.get-order"), {
  request: defineMessage(z.object({ orderId: z.string() })),
  response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
  errors: {
    ORDER_NOT_FOUND: { data: z.object({ orderId: z.string() }) },
  },
});
```

**Worker side** — the handler's error channel becomes `HandlerError | RpcError<code, data>`:

```typescript
import { rpcError } from "@amqp-contract/worker";

ErrAsync(rpcError("ORDER_NOT_FOUND", { orderId }));
// or, typed and autocompleted:
ErrAsync(errors.ORDER_NOT_FOUND({ orderId }));
```

A returned `RpcError` is a _business outcome_: the worker validates its `data`, publishes an error reply, and **acknowledges the request**. Declared errors are never retried.

**Client side** — `call()`'s error union gains the declared members:

```typescript
import { isRpcError } from "@amqp-contract/client";

if (result.isErr() && isRpcError(result.error)) {
  result.error.code; // "ORDER_NOT_FOUND"
  result.error.data; // { orderId: string }
  result.error.message;
}
```

Error data is validated twice — on the worker before publishing, on the client on arrival.

### Runtime enforcement

| Situation                                    | Result                                                                             |
| -------------------------------------------- | ---------------------------------------------------------------------------------- |
| Worker returns an undeclared code            | No reply published; request dead-lettered as `NonRetryableError`; caller times out |
| Worker's error data fails its schema         | Same                                                                               |
| Worker's response fails the response schema  | Reply dropped; caller times out                                                    |
| Client receives an undeclared code           | Resolves to a **defect** (`TechnicalError` cause)                                  |
| Client's error data fails its schema         | Resolves to `Err(MessageValidationError)`                                          |
| Request missing `replyTo` or `correlationId` | Dead-lettered; never answered                                                      |

### Wire format

Success replies are unchanged. An error reply is marked by the `x-amqp-contract-error-code` header — exported as `RPC_ERROR_CODE_HEADER` from `@amqp-contract/core` — carrying the code, with a `{ message, data }` JSON body. RPCs declaring no `errors` are unaffected.

## Client-side RPC errors

### `RpcTimeoutError`

No reply within `timeoutMs` (or the server-side default). The pending call is cleared. Also what you observe when a reply was dropped for failing its schema.

### `RpcCancelledError`

The client was closed while the call was in flight. All pending calls fail with this rather than hanging.

## Extracting values

`.get()` compiles only when `E = never`. It still **panics on a defect**, rethrowing the cause — `Result<T, never>` does not mean "cannot throw", it means "has no errors you were supposed to handle".

```typescript
const client = await TypedAmqpClient.create({ contract, urls }).get();
```

`.getOrThrow()` is the escape hatch on a fallible result: returns the value on `Ok`, throws the `Err` value, rethrows a defect's cause. Intended for scripts, tests and examples.

```typescript
await client.publish("orderCreated", order).getOrThrow();
```

`.getOrElse(f)` is the non-throwing cousin, computing a fallback from the error.

Prefer `.match()`, `.recoverErrCases()` and `.flatMapErrCases()` in application code.

## Defensive guards

The worker wraps handler invocation in `try`/`catch` so a handler that throws cannot leave a message neither acked nor nacked — it is logged and dead-lettered. Treat this as a bug net, not an interface: a thrown error has lost the classification that would have let it retry.

## Where next

- [Errors as values](/explanation/errors-as-values) — why it is shaped this way.
- [Retry failed messages](/how-to/retry-failed-messages) — what handler errors trigger.
- [Use request/reply](/how-to/use-request-reply) — RPC recipes.
