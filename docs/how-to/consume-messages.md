---
title: Consume messages - amqp-contract
description: Write worker handlers, do async work inside them, control concurrency with prefetch, reach raw AMQP properties, and shut down cleanly.
---

# Consume messages

Recipes for the consuming side. For why handlers return results instead of throwing, see [errors as values](/explanation/errors-as-values).

## Start a worker

```typescript
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { contract } from "./contract.js";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: (_, { payload }) => {
      console.log(payload.orderId);
      return OkAsync(undefined);
    },
  },
  urls: ["amqp://localhost"],
}).get();
```

Creating the worker declares the contract's topology against the broker and starts consuming every queue in it. There is no separate `start()`.

Every consumer in the contract must have a handler. Omit one and the object does not typecheck — and worker creation fails before any connection is acquired, so a missing handler surfaces immediately rather than as a silently unconsumed queue.

## Do async work in a handler

Handlers return `AsyncResult`, not `Promise`, so `async`/`await` is not available. Lift the promise with `fromPromise` and say what a rejection means:

```typescript
import { fromPromise } from "unthrown";

processOrder: ({ retryable }, { payload }) =>
  fromPromise(
    saveOrder(payload),
    (cause) => retryable("database unavailable", cause),
  ).map(() => undefined),
```

The `.map(() => undefined)` discards the promise's value: a consumer handler must resolve to `void`.

`retryable` and `nonRetryable` come off the helpers record, so the routing decision needs no import. The `RetryableError` / `NonRetryableError` classes are still exported for a handler that would rather construct them itself.

The mapper is required. For the two common shapes there are prebuilt mappers:

```typescript
import { qualifyRetryable, qualifyNonRetryable } from "@amqp-contract/worker";

fromPromise(callApi(payload), qualifyRetryable("API unavailable"));
fromPromise(chargeCard(payload), qualifyNonRetryable("card permanently declined"));
```

Choosing between the two is [the retry model](/explanation/the-retry-model); the short version is that `NonRetryableError` means "this will fail identically tomorrow".

## Chain several async steps

```typescript
processOrder: (_, { payload }) =>
  fromPromise(saveOrder(payload), qualifyRetryable("save failed"))
    .flatMap((order) => fromPromise(notify(order), qualifyRetryable("notify failed")))
    .map(() => undefined),
```

`flatMap` sequences dependent steps and short-circuits on the first failure. For independent steps, run them together:

```typescript
processOrder: (_, { payload }) =>
  fromPromise(
    Promise.all([saveOrder(payload), sendConfirmation(payload.customerId)]),
    qualifyRetryable("order processing failed"),
  ).map(() => undefined),
```

## Branch before doing any work

Return `ErrAsync` directly when you can decide up front:

```typescript
import { ErrAsync } from "unthrown";

processOrder: ({ nonRetryable }, { payload }) => {
  if (payload.amount <= 0) {
    return ErrAsync(nonRetryable("non-positive amount"));
  }
  return fromPromise(saveOrder(payload), qualifyRetryable("save failed")).map(() => undefined);
},
```

## Read validated headers

Headers declared in the message's headers schema arrive validated and typed alongside the payload:

```typescript
processOrder: (_, { payload, headers }) => {
  console.log(headers.eventSource, headers.eventVersion);
  return OkAsync(undefined);
},
```

## Reach the raw AMQP message

`raw`, in the helpers record, is the underlying delivery — delivery tag, raw headers, redelivery flag:

```typescript
processOrder: ({ raw }, { payload }) => {
  console.log(raw.fields.deliveryTag, raw.fields.redelivered);
  console.log(raw.properties.correlationId);
  return OkAsync(undefined);
},
```

Use it for infrastructure concerns. Anything that is part of the contract belongs in the payload or the typed headers.

## Move handlers into their own modules

Inline handlers are fine for a single file. Beyond that, `declareHandler` gives a named, separately testable function with full inference from the contract:

```typescript
// handlers/process-order.ts
import { declareHandler, qualifyRetryable } from "@amqp-contract/worker";
import { fromPromise } from "unthrown";
import { contract } from "../contract.js";

export const processOrder = declareHandler(contract, "processOrder", (_, { payload }) =>
  fromPromise(saveOrder(payload), qualifyRetryable("database unavailable")).map(() => undefined),
);
```

```typescript
import { processOrder } from "./handlers/process-order.js";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: { processOrder },
  urls: ["amqp://localhost"],
}).get();
```

`declareHandlers(contract, { … })` does the same for a whole batch at once, which is what you want when handlers stay in one module.

Note that `declareHandler`'s return type is a handler _entry_ — either the function or a `[function, options]` tuple — so the value it gives back is not directly callable. To get a standalone function you can invoke in a unit test, type it with `WorkerInferConsumerHandler` instead:

```typescript
import type { WorkerInferConsumerHandler } from "@amqp-contract/worker";

export const processOrder: WorkerInferConsumerHandler<typeof contract, "processOrder"> = (
  _,
  { payload },
) => fromPromise(saveOrder(payload), qualifyRetryable("database unavailable")).map(() => undefined);
```

This is still fully inferred from the contract, and it can be passed to `handlers` unchanged. Most handler testing is better done against a real broker — see [test with RabbitMQ](/how-to/test-with-rabbitmq).

## Control concurrency

Prefetch caps how many unacknowledged messages a consumer holds. Use the tuple form to set it per handler:

```typescript
handlers: {
  processOrder: [
    (_, { payload }) => fromPromise(save(payload), qualifyRetryable("save failed")).map(() => undefined),
    { prefetch: 10 },
  ],
},
```

Or set a default for every consumer:

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  handlers: { … },
  urls: ["amqp://localhost"],
  defaultConsumerOptions: { prefetch: 10 },
}).get();
```

Per-handler options override the default. Picking a number is covered in [tune performance](/how-to/tune-performance#prefetch).

::: warning Prefetch tuples and middleware context
The tuple form does not carry middleware context types through to the handler — `helpers.context` widens and reading a field the middleware injected fails to compile. If you need both, set concurrency with `defaultConsumerOptions` and keep the handler in its plain function form.
:::

## Shut down without dropping messages

```typescript
const shutdown = async () => {
  await worker.close().get();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

`close()` stops accepting new deliveries and waits for in-flight handlers before tearing the channel down. The `.get()` is required — without it a failure to close is discarded.

The drain is bounded by `drainTimeoutMs` (default 30 000 ms, exported as `DEFAULT_DRAIN_TIMEOUT_MS`), so a hung handler cannot wedge shutdown — on timeout the channel closes anyway and the un-acked deliveries are redelivered by the broker ([at-least-once semantics](/explanation/delivery-guarantees)). Pass `null` to wait forever:

```typescript
await worker.close({ drainTimeoutMs: 5_000 }).get(); // cap the drain at 5s
await worker.close({ drainTimeoutMs: null }).get(); // wait for every in-flight handler
```

## Know how a return value routes the message

| Handler returns               | Message                                             |
| ----------------------------- | --------------------------------------------------- |
| `OkAsync(undefined)`          | Acknowledged                                        |
| `ErrAsync(RetryableError)`    | Handed to the queue's retry mode                    |
| `ErrAsync(NonRetryableError)` | Dead-lettered, bypassing retries                    |
| Throws                        | Logged and dead-lettered by the worker's safety net |

Do not rely on that last row. A thrown error has already lost the classification that would have let it be retried, so the worker can only assume the worst. Wrapping a handler body in `try`/`catch` to convert exceptions yourself is unnecessary — `fromPromise`'s mapper is the supported place to make that decision.

## Where next

- [Retry failed messages](/how-to/retry-failed-messages) — configuring what happens after a `RetryableError`.
- [Use request/reply](/how-to/use-request-reply) — handlers that return a value.
- [Add middleware](/how-to/add-middleware) — auth, tracing and DI without touching handlers.
- [Test with RabbitMQ](/how-to/test-with-rabbitmq) — integration tests against a real broker.
