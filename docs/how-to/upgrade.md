---
title: Upgrade - amqp-contract
description: Migration notes for each major version, including the 3.0 safe defaults (prefetch, publish timeout, dead-letter exchanges), the defect-channel change and the unthrown v5 matcher renames.
---

# Upgrade

All six `@amqp-contract/*` packages version together, so upgrade them in lockstep. This page lists the changes that need action; the full history is in the [releases](https://github.com/btravstack/amqp-contract/releases) and each package's `CHANGELOG.md`.

## 2.4.x → 3.0

Two independent breaking changes land together — `unthrown` v5 and the defect-channel move — plus three **safe defaults** that change runtime behaviour. Expect to touch every site that inspects a result.

::: danger Read this first: prefetch changed and nothing will tell you
Of everything on this page, [consumers prefetch 10 by default](#consumers-prefetch-10-by-default) is the only change with **no compile error and no startup failure**. Your build stays green, your tests stay green, and your throughput profile changes. If you read one section, read that one.
:::

### Consumers prefetch 10 by default

**What breaks:** nothing, visibly. Consumers previously ran with no `basic.qos` at all, which is AMQP's _unlimited_ — the broker pushed the entire ready backlog into a single consumer. They now prefetch **10**.

**Why it was unsafe:** an unbounded consumer holds the whole backlog in its own memory, and every one of those messages is unacked, so a crash redelivers all of it at once. Throughput was also concentrated in whichever replica connected first; idle peers got nothing. Redelivery on a crash is not specific to prefetch — see [Delivery guarantees](/explanation/delivery-guarantees).

**The exact edit** — none, if 10 works for you. Otherwise tune it, or opt back out explicitly:

```diff
  const worker = await TypedAmqpWorker.create({
    contract,
    urls,
    handlers,
+   // fast handlers, throughput-bound: raise it
+   defaultConsumerOptions: { prefetch: 100 },
  }).get();
```

```diff
  const worker = await TypedAmqpWorker.create({
    contract,
    urls,
    handlers,
+   // the pre-3.0 behaviour, stated out loud
+   defaultConsumerOptions: { prefetch: "unbounded" },
  }).get();
```

Per-handler tuples still override the default: `processOrder: [handler, { prefetch: 1 }]`. `"unbounded"` rather than `0` — AMQP's `0` means _unlimited_, which reads at a call site as its opposite.

**Prefetch is not a tuning knob on top of a concurrency limit — it _is_ the concurrency limit.** The worker starts each handler without awaiting the previous one, so the only thing that ever bounded in-flight handlers was how many messages the broker had pushed. That was unlimited; it is now 10. Three ways that shows up:

1. **I/O-bound handlers collapse in throughput.** A handler that spends its time on an HTTP call or a database write previously ran at whatever concurrency the backlog allowed — hundreds. It now runs 10 at a time. Nothing errors; the queue just drains slower. The fix is one line: `defaultConsumerOptions: { prefetch: N }`.
2. **RPC servers are consumers too, and this is the case that misleads.** A worker serving an RPC now handles at most 10 calls concurrently; the 11th waits for a handler to finish. If handler duration is anywhere near the caller's `timeoutMs`, callers start timing out under load that worked before — and the symptom appears on the client, pointing away from the cause. Raise the server's prefetch, not the client's timeout. (Client-side RPC is unaffected: the reply consumer runs `noAck`, and RabbitMQ ignores QoS for no-ack consumers.)
3. **A handler that waits on another message from its own queue now deadlocks.** Once 10 such handlers are parked, the broker will not deliver the message they are waiting for. This was already an anti-pattern — it deadlocks under any bounded prefetch — but it worked under unlimited and stops working here.

Handlers that assumed they could see the whole backlog are the other ones to re-measure. Picking a number is covered in [tune performance](/how-to/tune-performance#prefetch).

### Consumed queues need a dead-letter exchange

**What breaks:** `defineContract` throws for any queue reachable from `consumers` or `rpcs` that has neither `deadLetter` nor `onPoison: "drop"`. This one is loud — it fails at import time.

**Why it was unsafe:** a queue with no DLX loses every message its consumer rejects. `nack(requeue: false)` drops it and nothing records that it existed.

**Before you reach for the obvious fix:** you **cannot add `deadLetter` to a queue that already exists on the broker**. It becomes the `x-dead-letter-exchange` argument, which is part of the queue's identity, so the worker's redeclaration fails with `PRECONDITION_FAILED - inequivalent arg` — a 406 at deploy time rather than a define-time error. The three routes out (new queue and migrate, broker policy, or accept the loss) are tabulated in [troubleshoot → if the queue already exists in production](/how-to/troubleshoot#if-the-queue-already-exists-in-production). Pick the route there first, then make the contract edit below match it.

On a queue that does not exist yet, the DLX costs nothing:

```diff
+ import { defineQueueBinding } from "@amqp-contract/contract";
+
- const orderQueue = defineQueue("order-processing");
+ const ordersDlx = defineExchange("orders-dlx");
+ const orderDlq = defineQueue("order-processing-dlq");
+ const orderQueue = defineQueue("order-processing", {
+   deadLetter: { exchange: ordersDlx },
+ });

  export const contract = defineContract({
    publishers: { orderCreated },
    consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
+   queues: { orderDlq },
+   bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
  });
```

**Add the dead-letter queue and the binding, not just the exchange.** The `deadLetter` pointer satisfies _this_ check; a second check, [below](#a-dead-letter-exchange-needs-something-bound-to-it), rejects the exchange it names if nothing is bound there. Declaring the DLQ at the top level of `defineContract` is the [standalone topology](/how-to/define-a-contract#declare-standalone-topology) pattern; the DLQ itself is not consumed, so it needs no DLX of its own.

Or state that dropping is deliberate — correct for a metrics firehose, a lie anywhere else, and the required declaration when the dead-lettering lives in a broker policy the contract cannot see:

```diff
- const metricsQueue = defineQueue("metrics-ingest");
+ const metricsQueue = defineQueue("metrics-ingest", { onPoison: "drop" });
```

Only _consumed_ queues are checked. A dead-letter queue you declare but do not consume needs neither. If you do consume your DLQ, it needs `onPoison: "drop"` — a DLQ cannot dead-letter to itself.

### A dead-letter exchange needs something bound to it

**What breaks:** `defineContract` throws for any queue whose `deadLetter` exchange has nothing bound to it in the contract's binding graph. Loud, at import time, like the check above. Unlike it, this one applies to **every** queue the contract declares, consumed or not — a dead letter is discarded regardless of who consumes the source queue.

```
Queue "order-processing" dead-letters to exchange "orders-dlx" (topic), but nothing
there can receive them: its dead-lettered messages keep their original routing key.
Nothing is bound to "orders-dlx". RabbitMQ discards a message routed to zero queues, …
```

**This is not a new restriction — it is the discovery of an existing data-loss path.** If your contract trips this, those messages are already being discarded, today, in production. RabbitMQ drops a message that matches no binding, and a dead letter is an ordinary publish: an exchange with nothing bound loses exactly the messages you added it to keep. Nothing reports it. Worse, the worker logs `Sending message to DLQ` at `info` as it happens, so the loss reads as a successful hand-off in the one place you would look. The check moves that from silent runtime loss to a define-time error; it does not create the loss.

**The fix:** declare the dead-letter queue and bind it — two lines, on the same shape as the check above, and the full pattern is at [define a contract → declare standalone topology](/how-to/define-a-contract#declare-standalone-topology):

```diff
+ const orderDlq = defineQueue("order-processing-dlq");

  export const contract = defineContract({
    consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
+   queues: { orderDlq },
+   bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
  });
```

**On a direct DLX, bind the real routing key.** `#` is a _topic_ wildcard. A direct exchange has no wildcards, so it treats `#` as the literal routing key `#` and a dead letter arriving under any other key matches nothing — measured against RabbitMQ 4.2: the same `#` binding on a topic DLX receives the dead letter, on a direct DLX it receives nothing (`tests/src/__tests__/dlx-routability.spec.ts`). This is the one case the check cannot catch for you: with no `deadLetter.routingKey` set, the key a dead letter arrives under is the message's original key, which is not knowable at define time, so any binding at all satisfies the check. Bind the key the message will actually carry — the queue's `deadLetter.routingKey` if it sets one, otherwise every key the source queue can receive:

```typescript
import { defineExchange, defineQueue, defineQueueBinding } from "@amqp-contract/contract";

const paymentsDlx = defineExchange("payments-dlx", { type: "direct" });
const paymentsDlq = defineQueue("payments-dlq");

const paymentsQueue = defineQueue("payments", {
  deadLetter: { exchange: paymentsDlx, routingKey: "payments.dead" },
});

// The same key the dead letter arrives under — not "#", which a direct
// exchange matches literally and therefore never.
const paymentsDlqBinding = defineQueueBinding(paymentsDlq, paymentsDlx, {
  routingKey: "payments.dead",
});
```

**One more key that is not what you expect:** on a queue with `retry: { mode: "ttl-backoff" }`, a retried message re-enters through the wait queue carrying the **queue name** as its routing key, so a binding on the publisher's key stops matching after the first retry. Setting an explicit `deadLetter.routingKey` and binding that sidesteps both this and the direct-exchange trap above.

**The opt-out** is `externalConsumers: true`, for a dead-letter queue that another service or your IaC owns. It is an assertion that the binding exists on the broker and someone else guarantees it, so use it only when that is true — it disables the check for that queue permanently:

```typescript
import { defineExchange, defineQueue } from "@amqp-contract/contract";

const inventoryDlx = defineExchange("inventory-dlx");
const inventoryCommands = defineQueue("inventory-commands", {
  deadLetter: { exchange: inventoryDlx, externalConsumers: true },
});
```

**Not checked:** a dead-letter exchange supplied through the raw `arguments` passthrough. It names an exchange as a bare string rather than an `ExchangeDefinition`, and the contract need not declare that exchange at all, so its bindings are not knowable and the queue is skipped. You get no error and no protection:

```typescript
import { defineQueue } from "@amqp-contract/contract";

// Skipped by the check — verify this exchange's bindings on the broker yourself.
const legacyQueue = defineQueue("legacy-processing", {
  arguments: { "x-dead-letter-exchange": "legacy-dlx" },
});
```

**If the queue already exists in production:** adding the DLQ binding is subject to the same constraint as adding `deadLetter` in the first place — see [troubleshoot → if the queue already exists in production](/how-to/troubleshoot#if-the-queue-already-exists-in-production), and in particular the warning there about matching the existing exchange type and routing key before you declare. Declaring `orders-dlx` with a type that differs from the live one fails with a 406 at startup; declaring it with the right type and the wrong key fails silently, which is the failure this whole check exists to prevent.

### Channels set a 30s publish timeout

**What breaks:** a publish issued while the broker is unreachable used to buffer indefinitely with a promise that never settled. It now fails after **30 seconds**. Code that awaited a publish through an outage and never came back will now come back — with a failure.

**Why it was unsafe:** an unsettled promise is invisible. Requests pile up behind it with no error, no metric, and no timeout of their own.

**The timeout arrives as a `Defect`, not a modelled error.** `TypedAmqpClient.publish` models only `MessageValidationError`; every transport failure, the new timeout included, is routed to the **defect** channel. So it does **not** appear in `errCases`, and a `defect` arm you believed unreachable now fires during any outage longer than 30s. `.get()` and `.getOrThrow()` both panic on it. If you want publish failures observed rather than thrown, the `defect` branch is where they arrive:

```typescript
import { P } from "unthrown";

const result = await client.publish("sendEmail", payload);

result.match({
  ok: () => {},
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) => {
      log.error({ error }, "invalid payload, never sent");
    }),
  defect: (cause) => {
    // Reachable since 3.0: the 30s publish timeout lands here.
    log.error({ cause }, "publish failed");
  },
});
```

**A timed-out publish may still have reached the broker.** The timeout splices the message out of the unconfirmed set and rejects the promise; it does not tell the broker to forget it. So a retry in response to this failure can duplicate. AMQP is at-least-once regardless, but this is a new way to reach it — make the retry idempotent, or accept the duplicate.

**The exact edit** — none, unless 30s is wrong for you:

```diff
  const client = await TypedAmqpClient.create({
    contract,
    urls,
+   publishTimeoutMs: 10_000,
  }).get();
```

```diff
  const client = await TypedAmqpClient.create({
    contract,
    urls,
+   // restore the pre-3.0 unbounded buffering
+   publishTimeoutMs: null,
  }).get();
```

`TypedAmqpWorker` takes the same option for its retry republishes and RPC replies. `publishTimeoutMs` wins over `channelOptions.publishTimeout` if you set both.

### `unthrown` v5: error handling takes a matcher

`unthrown` is a **peer dependency**, so bump your own copy:

```bash
pnpm add unthrown@^5
```

`match`'s error key is renamed and now takes an exhaustive matcher rather than a single callback:

```diff
+ import { P } from "unthrown";

  result.match({
    ok: () => {/* … */},
-   err: (error) => {/* … */},
+   errCases: (matcher) =>
+     matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) => {/* … */}),
    defect: (cause) => { throw cause; },
  });
```

The bare error combinators gain a `*Cases` suffix and the same matcher shape:

| Before           | After                              |
| ---------------- | ---------------------------------- |
| `.mapErr(f)`     | `.mapErrCases((matcher) => …)`     |
| `.flatMapErr(f)` | `.flatMapErrCases((matcher) => …)` |
| `.tapErr(f)`     | `.tapErrCases((matcher) => …)`     |
| `.recoverErr(f)` | `.recoverErrCases((matcher) => …)` |

Return the **un-terminated** builder — `unthrown` calls `.exhaustive()` for you, so a missing case is a compile error at the call site. `.with(P._, handler)` is the catch-all when you genuinely want uniform handling.

The matcher and its patterns are built into `unthrown` — `match`, `P` and `P.tag` all come from the `unthrown` root export, and it has zero runtime dependencies. If you carried `ts-pattern` only for `unthrown`, **remove it**.

::: tip Tracking the 3.0 betas?
`unthrown` `5.0.0-beta.9` folded the standalone `tag` export into the pattern namespace as `P.tag`. If you pinned an earlier beta, swap `import { tag }` for `import { P }` and prefix the call sites — the pattern's type and runtime behaviour are unchanged.
:::

### `TechnicalError` moved to the defect channel

Infrastructure and transport failures — connection, publish, consume, cancel, close, compression, JSON parse, and thrown or rejected schema validators — are unexpected, so they now surface as a **defect** whose `cause` is a `TechnicalError`, never as a modeled `Err`.

Only anticipated domain failures remain in `E`: `MessageValidationError`, `RpcError`, `RpcTimeoutError`, `RpcCancelledError`, and the worker's `RetryableError` / `NonRetryableError`.

Matching `P.tag("@amqp-contract/TechnicalError")` in an error matcher no longer typechecks. Move it to the `defect` arm:

```diff
  result.match({
    ok: () => {/* … */},
    errCases: (matcher) =>
-     matcher.with(
-       P.tag("@amqp-contract/TechnicalError"),
-       P.tag("@amqp-contract/MessageValidationError"),
-       (error) => {/* … */},
-     ),
+     matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) => {/* … */}),
    defect: (cause) => {
+     // a TechnicalError arrives here now
      throw cause;
    },
  });
```

`.recoverDefect(…)` and `.tapDefect(…)` are the combinator equivalents.

Error channels narrow accordingly. `client.publish(...)` is now `AsyncResult<void, MessageValidationError>`, and `client.call(...)` drops `TechnicalError` from its union.

### `create()` and `close()` need `.get()`

Their modeled channel is now empty (`E = never`), and `.getOrThrow()` is gated to a _non-empty_ error channel, so it no longer compiles on them:

```diff
- const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();
+ const client = await TypedAmqpClient.create({ contract, urls }).get();
```

A failed `create()` still throws — `.get()` panics on a defect, rethrowing the underlying `TechnicalError`. `.getOrThrow()` on `publish(...)` / `call(...)` is unaffected; those still carry a modeled `E`.

### Implementation-side builders are `declare*`

Contract authoring keeps `define*`; implementation-side APIs are renamed to make the contract boundary visible (see the [glossary](/reference/glossary#declare-define)):

| Before             | After               |
| ------------------ | ------------------- |
| `defineHandler`    | `declareHandler`    |
| `defineHandlers`   | `declareHandlers`   |
| `defineMiddleware` | `declareMiddleware` |

`defineRpc` error-map entries are now `{ data: schema, message? }` instead of `defineMessage(schema)`, and the testing fixture wait options renamed `{ nbEvents, timeout }` → `{ count, timeoutMs }`.

### Queues are uniform; retry topology is derived

`defineQueue` now always returns a plain `QueueDefinition`, whatever its retry mode. Deleted: `extractQueue`, `QueueEntry`, `isQueueWithTtlBackoffInfrastructure`, `QueueWithTtlBackoffInfrastructure`, `TtlBackoffRetryInfrastructure` — if you called `extractQueue(entry)`, use the queue definition directly.

```diff
- const queue = extractQueue(orderQueue);
- console.log(queue.name);
+ console.log(orderQueue.name);
```

TTL-backoff infrastructure is no longer stored in the contract (so `contract.exchanges` contains only _your_ exchanges) — it is derived at topology-setup time, and the single shared wait queue is replaced by **one wait queue per distinct backoff delay** (`{queue}-wait-{delayMs}ms`, queue-level TTL, dead-lettering back to the origin queue). This fixes head-of-line blocking: a 60-second retry can no longer delay a 1-second retry queued behind it.

**Broker migration:** the old `{queue}-wait` queue and the `wait-exchange`/`retry-exchange` exchanges become unused. Let the old wait queue drain (its in-flight retries still dead-letter back correctly), then delete all three. Retried deliveries now arrive with the queue name as `fields.routingKey`; the original key is preserved in the `x-original-routing-key` header.

### Topic binding patterns are checked against the publisher

A `defineEventConsumer` routing-key override that can never match its publisher's routing key is now a compile error (a readable one, not a bare `never`):

```ts
// Publisher routing key: "order.created"
defineEventConsumer(orderCreated, queue, { routingKey: "user.*" });
// Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'
```

For JS callers, `definePublisher`, `defineQueueBinding`, and `defineExchangeBinding` on direct/topic exchanges now **throw at define time** when the routing key is missing or empty instead of silently defaulting to `""`.

### Renamed and relocated exports

| Before                                      | After                                              |
| ------------------------------------------- | -------------------------------------------------- |
| `ConsumerOptions` (core)                    | `AmqpConsumeOptions`                               |
| `PublishOptions` (core)                     | `AmqpPublishOptions`                               |
| `_internal_*` on the core root              | `@amqp-contract/core/internal`                     |
| `defineEventPublisher`'s `arguments` option | `bindingArguments` (it always configured bindings) |

The worker's and client's own `ConsumerOptions` / `PublishOptions` (the ones you use with `Typed*`) are unchanged. Builder-result brands are now `unique symbol`s — invisible in hovers and no longer forgeable; code that referenced `__brand` structurally must stop.

### Core signatures follow the options-object convention

Exported functions across the btravstack family now take at most two positional arguments with everything else in a trailing options object — no positional booleans ([Deno style guide](https://docs.deno.com/runtime/contributing/style_guide/#exported-functions%3A-max-2-args%2C-put-the-rest-into-an-options-object)). This only affects the low-level `AmqpClient` and the testing fixture; the typed client/worker surface already conformed:

```diff
- amqpClient.nack(msg, false, true);
+ amqpClient.nack(msg, { requeue: true });

- amqpClient.publish(exchange, routingKey, content, options);
+ amqpClient.publish({ exchange, routingKey }, content, options);

- publishMessage("orders-x", "order.created", payload);
+ publishMessage({ exchange: "orders-x", routingKey: "order.created" }, payload);
```

`AmqpClient.publish` / `sendToQueue` also now return `AsyncResult<void, never>` instead of `AsyncResult<boolean, never>`: a full channel write buffer is triaged once, inside core, as a defect — downstream code no longer checks a boolean.

### Suggested order

1. Bump `unthrown` and the six packages together.
2. Run `pnpm typecheck` and work through the errors — nearly all of this is compiler-visible (`extractQueue` deletions, renamed types, `declare*` renames, signature changes).
3. Fix `create()` / `close()` extraction first; it is mechanical.
4. Then convert each `match` / `*Err` site, moving `TechnicalError` handling into `defect` as you go.
5. Resolve the `defineContract` dead-letter throws — both of them: the missing `deadLetter` pointer, and the exchange it names having nothing bound. Decide the broker route (new queue, policy, or accepted loss) _before_ editing the contract, since a live queue cannot take a `deadLetter`. Check each DLX's type on the broker before binding: `#` routes everything on a topic exchange and nothing on a direct one.
6. Decide prefetch deliberately for every worker. It is the one change the compiler will not raise, so make it a review item rather than a discovery in production.
7. Deploy workers before deleting the old `{queue}-wait` queue and `wait-exchange`/`retry-exchange` from the broker.

## 2.3.x → 2.4.x

Upgrades `unthrown` to `4.1.0`:

```bash
pnpm add unthrown@^4.1
```

Two operator families are renamed. The old names still work but are deprecated:

| Deprecated            | Use instead                                        |
| --------------------- | -------------------------------------------------- |
| `.orElse(f)`          | `.flatMapErr(f)`                                   |
| `.recover(f)`         | `.recoverErr(f)`                                   |
| `.unwrap()`           | `.get()` — or `.getOrThrow()` on a fallible result |
| `.unwrapErr()`        | `.getErr()`                                        |
| `.unwrapOr(fallback)` | `.getOr(fallback)`                                 |
| `.unwrapOrElse(f)`    | `.getOrElse(f)`                                    |

No amqp-contract API changes.

## 2.2.x → 2.3.x

Upgrades `unthrown` to `4.0.0`:

```bash
pnpm add unthrown@^4
```

**`.unwrap()` is type-gated.** It compiles only when the error channel is empty. On a fallible result it is now a compile error:

```diff
- const client = (await TypedAmqpClient.create({ contract, urls })).unwrap();
+ const client = await TypedAmqpClient.create({ contract, urls }).unwrapOrElse((e) => {
+   throw e;
+ });
```

**`TaggedError` reserves `message`.** Only relevant if you define your own subclasses: a `message` field in the payload is rejected. Use `override message = "…"` and keep the payload for structured fields.

## 2.1.x → 2.2.x

Upgrades `unthrown` to `3.0.0`. The public surface is unchanged. Action is needed only if you write `qualify` mappers that route unexpected failures to the defect channel — the standalone `Defect` constructor is gone, replaced by a callback:

```diff
- import { fromPromise, Defect } from "unthrown";
- fromPromise(work(), (cause) => isExpected(cause) ? new MyError(cause) : Defect(cause));
+ import { fromPromise } from "unthrown";
+ fromPromise(work(), (cause, defect) => isExpected(cause) ? new MyError(cause) : defect(cause));
```

Mappers that only return a modeled error are unaffected.

## 2.0.x → 2.1.x

Upgrades `unthrown` to `2.0.0`, which is additive. No changes required.

## 1.x → 2.0

Upgrades `unthrown` to `1.0.0`, which renames the value constructors: **`ok` → `Ok`, `err` → `Err`, `defect` → `Defect`**. The lowercase forms are removed.

```diff
- import { ok, err } from "unthrown";
- return ok(undefined).toAsync();
+ import { Ok, Err } from "unthrown";
+ return Ok(undefined).toAsync();
```

`match` handler keys stay lowercase — they are case branches, not constructors.

## 0.x → 1.0

Replaces `neverthrow` with `unthrown`, which keeps errors-as-values but adds the defect channel.

| neverthrow (0.x)                     | unthrown (1.x)                                             |
| ------------------------------------ | ---------------------------------------------------------- |
| `ResultAsync<T, E>`                  | `AsyncResult<T, E>`                                        |
| `result.match(okFn, errFn)`          | `result.match({ ok, err, defect })`                        |
| `.andThen` / `.andTee` / `.orTee`    | `.flatMap` / `.tap` / `.tapErr`                            |
| `okAsync(v)` / `errAsync(e)`         | `ok(v).toAsync()` / `err(e).toAsync()`                     |
| `ResultAsync.fromPromise(p, mapper)` | `fromPromise(p, qualify)` — free function, mapper required |
| `._unsafeUnwrap()`                   | `.unwrap()`                                                |
| `error instanceof HandlerError`      | `isHandlerError(error)` — now a union type                 |

The constructors here are the 1.x lowercase forms. Going straight to 2.0+? Use `Ok` / `Err`. Going straight to 3.0? `match`'s `err` key is `errCases` and takes a matcher, and `OkAsync(v)` / `ErrAsync(e)` replace the `.toAsync()` lifts.

Error classes became `TaggedError`s with namespaced tags (`"@amqp-contract/MessageValidationError"`) for exhaustive dispatch. Their `Error.name` and constructors are unchanged.

## Where next

- [Error model](/reference/error-model) — the current error surface in full.
- [Errors as values](/explanation/errors-as-values) — why the defect channel exists.
