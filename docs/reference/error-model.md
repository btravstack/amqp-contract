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
PublishError                  the broker side of a publish failed (timeout, nack, channel closed)
ConnectionError               the broker could not be reached at create()
RpcError<code, data>          declared business error on an RPC
RpcTimeoutError               client-side: no reply in time
RpcCancelledError             client-side: client closed mid-call
TechnicalError                transport/framework failure — always a defect cause, never in E
```

All are `TaggedError`s, so they carry a namespaced `_tag` for exhaustive dispatch. `Error.name` stays bare, and the stack's first line reads `Name: message`. Every class exposes its tag as a static — `P.tag(PublishError.tag)`, `P.tag(RetryableError.tag)` — instead of the raw string.

| Type                     | Tag                                     | Exported from                                           |
| ------------------------ | --------------------------------------- | ------------------------------------------------------- |
| `RetryableError`         | `@amqp-contract/RetryableError`         | `@amqp-contract/worker`                                 |
| `NonRetryableError`      | `@amqp-contract/NonRetryableError`      | `@amqp-contract/worker`                                 |
| `MessageValidationError` | `@amqp-contract/MessageValidationError` | `@amqp-contract/core`, re-exported by client and worker |
| `RpcError`               | `@amqp-contract/RpcError`               | `@amqp-contract/core`, re-exported by client and worker |
| `PublishError`           | `@amqp-contract/PublishError`           | `@amqp-contract/core`, re-exported by client and worker |
| `RpcTimeoutError`        | `@amqp-contract/RpcTimeoutError`        | `@amqp-contract/client`                                 |
| `RpcCancelledError`      | `@amqp-contract/RpcCancelledError`      | `@amqp-contract/client`                                 |
| `TechnicalError`         | `@amqp-contract/TechnicalError`         | `@amqp-contract/core`, re-exported by client and worker |
| `ConnectionError`        | `@amqp-contract/ConnectionError`        | `@amqp-contract/core`, re-exported by client and worker |

## Error channel per operation

| Operation                | Returns                                                                                                                 |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `TypedAmqpClient.create` | `AsyncResult<TypedAmqpClient, ConnectionError>`                                                                         |
| `client.publish`         | `AsyncResult<void, MessageValidationError \| PublishError>`                                                             |
| `client.call`            | `AsyncResult<TResponse, MessageValidationError \| PublishError \| RpcTimeoutError \| RpcCancelledError \| RpcError<…>>` |
| `client.close`           | `AsyncResult<void, never>`                                                                                              |
| `TypedAmqpWorker.create` | `AsyncResult<TypedAmqpWorker, ConnectionError>`                                                                         |
| `worker.close`           | `AsyncResult<void, never>`                                                                                              |
| Consumer handler         | `AsyncResult<void, HandlerError>`                                                                                       |
| RPC handler              | `AsyncResult<TResponse, HandlerError \| RpcError<…>>`                                                                   |

An empty channel (`never`) means every failure is a defect, which is why `.get()` compiles on `close` but not on `publish` — nor on `create`, whose channel carries the broker's own failure.

The client exports the unions by name: `ClientPublishError` (`MessageValidationError | PublishError`, the full `publish()` union) and `CallError` (the full `call()` union). For the error union of one specific RPC — declared errors included — use `ClientInferCallError<typeof contract, "getOrder">`.

## Handler errors

### `RetryableError`

The failure may not recur. The queue's [retry mode](/how-to/retry-failed-messages) decides what happens. With no retry config, or `mode: "none"`, the message is dead-lettered. From an **RPC** handler it always dead-letters the request: RPC requests are never retried, since the caller stopped waiting long before a backoff would end.

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

### Qualifiers

```typescript
import { qualifyNonRetryable, qualifyRetryable } from "@amqp-contract/worker";

// Prebuilt `fromPromise` mappers
fromPromise(callApi(payload), qualifyRetryable("API unavailable"));
fromPromise(chargeCard(payload), qualifyNonRetryable("card declined"));
```

Construct the errors directly where you need one outside a `fromPromise`
boundary: `new RetryableError(message, cause)` / `new NonRetryableError(message, cause)`.

### Narrowing

Prefer the error matcher, which is exhaustive:

```typescript
result.match({
  ok: () => {},
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/RetryableError"), (e) => e)
      .with(P.tag("@amqp-contract/NonRetryableError"), (e) => e),
  defect: (cause) => cause,
});
```

`HandlerError` is a union type, not a class, so there is no
`instanceof HandlerError`. For an ad-hoc runtime check use
`err instanceof RetryableError || err instanceof NonRetryableError`.

## `MessageValidationError`

A Standard Schema validation failed. Carries the source identifier (publisher or consumer name) and the schema's `issues` array.

**On the client**, returned as a modeled `Err` from `publish()` and `call()`, so you can react before anything is sent.

**On the worker**, it is modeled too, not a defect: the consume span records `MessageValidationError` as its exception and the consume metric counts a failure. The message is dead-lettered via `nack(requeue=false)` and never enters the retry pipeline — retrying a malformed payload cannot succeed. The body is preserved exactly as delivered; because the worker does not republish, no diagnostic headers are added. Details are in the logs. A body the worker cannot even decode (unknown `contentEncoding`, corrupt stream, over the 16 MiB `maxMessageBytes` cap) is a `TechnicalError` defect instead, dead-lettered the same way.

Validated: publisher payloads, consumer payloads, consumer headers, RPC requests, RPC responses, and RPC error data. **Not** validated: headers on publish.

`isMessageValidationError(err)` is the type guard, exported from `@amqp-contract/core` and re-exported by client and worker.

## `PublishError`

The broker side of a publish failed. Returned as a modeled `Err` from `publish()` and `call()` (and from core's `AmqpClient.publish` / `sendToQueue`), with a `reason`:

| `reason`           | What core observed                                                             |
| ------------------ | ------------------------------------------------------------------------------ |
| `"timeout"`        | The message sat buffered past `publishTimeoutMs` — the broker was unreachable. |
| `"nacked"`         | The broker refused the message (`basic.nack`).                                 |
| `"channel-closed"` | The channel closed before the message was confirmed.                           |

A full write buffer is **not** one of them: on the confirm channel it is only reported after the broker confirmed the message, so the publish answers `Ok` (logged at `debug`) rather than invite a duplicate republish.

`target` names where the message was going, and `cause` carries the underlying rejection. **Modeled, not a defect**: a broker that is down, overloaded or refusing a message is an operational condition a publisher is expected to handle — buffer, retry, shed load, answer 503. A failure core cannot classify (an unencodable payload, an unknown rejection) stays a `TechnicalError` defect.

## `ConnectionError`

The broker could not be reached when `create()` dialed it: refused, unresolvable, unauthorized, or still not ready when `connectTimeoutMs` elapsed. `cause` is the last failed dial reported by amqp-connection-manager (e.g. `ECONNREFUSED`, `ACCESS_REFUSED`), named in the message too; when no dial failed outright, it is the connect-timeout error. The first failed dial is also logged at `warn`, so a wrong URL shows up at once rather than when the timeout expires.

**Modeled, not a defect.** It is the anticipated failure of dialing a broker — a wrong URL, a rotated credential, a cluster that has not come up — every one an operator's business rather than a bug, and the one thing a start-up path most wants to branch on:

```typescript
import { P } from "unthrown";

const started = await TypedAmqpWorker.create({ contract, handlers, urls }).match({
  ok: (worker) => worker,
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/ConnectionError"), (error) => {
      logger.error({ error }, "broker unreachable");
      process.exitCode = 1;
      return undefined;
    }),
  defect: (cause) => {
    logger.error({ cause }, "bug while starting up");
    process.exitCode = 70;
    return undefined;
  },
});
```

`isConnectionError(error)` is the type guard, exported from `@amqp-contract/core` and re-exported by client and worker. A connection LOST later is not this: mid-publish it is a `PublishError` (`"timeout"` or `"channel-closed"`), mid-consume a `TechnicalError` defect, since no caller asked for it and none can act on it.

Neither is a **topology the broker refuses**. If the connection succeeds but the contract's exchanges, queues or bindings cannot be declared — `406 PRECONDITION_FAILED` on a mismatched queue, a missing exchange, a permission the credentials lack — `create()` answers a **defect** carrying a `TechnicalError`. The dial is an operator's business; a topology the broker rejects is a broken contract, which is a bug. It fails at once rather than at the connect timeout, and only on the first connect: a reconnect has no caller left to fail, so it is logged and retried as before.

## `TechnicalError`

Any unexpected failure of the transport or framework: a rejected assert, a consumer that could not start, a compression or JSON-parse failure, an over-cap message, an unclassifiable publish rejection, or a schema validator that threw. (A publish the broker side refused is a `PublishError`, not this.)

These are unexpected, so they surface through the **defect** channel, never as a modeled `Err`. The `TechnicalError` is the defect's `cause`, and carries its own `cause` chain to the underlying amqplib error.

```typescript
import { TechnicalError } from "@amqp-contract/core";
import { P } from "unthrown";

result.match({
  ok: () => {},
  errCases: (matcher) =>
    matcher.with(
      P.tag("@amqp-contract/MessageValidationError"),
      P.tag("@amqp-contract/PublishError"),
      (error) => {},
    ),
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
const rpcDlx = defineExchange("rpc-dlx");

const getOrder = defineRpc(defineQueue("rpc.get-order", { deadLetter: { exchange: rpcDlx } }), {
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
| `replyTo` refused by `rpc.allowReplyTo`      | Dead-lettered with the reason logged; never answered                               |
| Handler returns `RetryableError`             | Dead-lettered, even on a queue with a `retry` config — RPC requests never retry    |

### Wire format

Success replies are unchanged. An error reply is marked by the `x-amqp-contract-error-code` header — exported as `RPC_ERROR_CODE_HEADER` from `@amqp-contract/core` — carrying the code, with a `{ message, data }` JSON body. RPCs declaring no `errors` are unaffected.

## Client-side RPC errors

### `RpcTimeoutError`

No reply within `timeoutMs` (or the server-side default). The pending call is cleared. Also what you observe when a reply was dropped for failing its schema. The request itself was published with `expiration = timeoutMs`, so if no worker picked it up in time the broker drops it rather than letting it be answered for nobody.

### `RpcCancelledError`

The client was closed while the call was in flight. All pending calls fail with this rather than hanging.

## Extracting values

`.get()` compiles only when `E = never`. It still **panics on a defect**, rethrowing the cause — `Result<T, never>` does not mean "cannot throw", it means "has no errors you were supposed to handle".

```typescript
const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();
await client.close().get();
```

`.getOrThrow()` is the escape hatch on a fallible result: returns the value on `Ok`, throws the `Err` value, rethrows a defect's cause. Intended for scripts, tests and examples — `create` is the common one, since its `ConnectionError` is a real error channel.

```typescript
const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();
```

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
