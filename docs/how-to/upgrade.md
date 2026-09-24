---
title: Upgrade - amqp-contract
description: Migration notes for each major version, including the 3.0 safe defaults (prefetch, publish timeout, dead-letter exchanges, a 16 MiB message cap), role-scoped topology, the modeled PublishError, the defect-channel change and the unthrown v5 matcher renames.
---

# Upgrade

All six `@amqp-contract/*` packages version together, so upgrade them in lockstep. This page lists the changes that need action; the full history is in the [releases](https://github.com/btravstack/amqp-contract/releases) and each package's `CHANGELOG.md`.

## 2.4.x → 3.0

Two independent breaking changes land together — `unthrown` v5 and the defect-channel move — plus a handler-signature swap, three **safe defaults** that change runtime behaviour, and a runtime hardening pass: a publish the broker does not take is a modeled `PublishError`, the client and the worker each declare only their own slice of the topology over separate connections, inbound messages are capped at 16 MiB, and RPC servers answer only allowed addresses and never retry. Expect to touch every site that inspects a result, and every handler leaf.

::: danger Read this first: four changes will not tell you
Most of this page is a compile error or a startup failure. These are neither — your build and your tests stay green while behaviour changes:

- [Consumers prefetch 10 by default](#consumers-prefetch-10-by-default) — your throughput profile changes. If you read one section, read this one.
- [The client and the worker each declare only their own topology](#the-client-and-the-worker-each-declare-only-their-own-topology) — a standalone queue or exchange neither role reaches is no longer declared by anyone.
- [Inbound messages are capped at 16 MiB](#inbound-messages-are-capped-at-16-mib) — a larger message is dead-lettered instead of handled.
- [RPC servers reply only to allowed addresses and never retry](#rpc-servers-reply-only-to-allowed-addresses-and-never-retry) — a request with a custom `replyTo`, or whose handler returns `RetryableError`, is dead-lettered.

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
  }).getOrThrow();
```

```diff
  const worker = await TypedAmqpWorker.create({
    contract,
    urls,
    handlers,
+   // the pre-3.0 behaviour, stated out loud
+   defaultConsumerOptions: { prefetch: "unbounded" },
  }).getOrThrow();
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

**On a direct DLX, bind the real routing key.** `#` is a _topic_ wildcard. A direct exchange has no wildcards, so it treats `#` as the literal routing key `#` and a dead letter arriving under any other key matches nothing — measured against RabbitMQ 4.2: the same `#` binding on a topic DLX receives the dead letter, on a direct DLX it receives nothing (`tests/src/__tests__/dlx-routability.spec.ts`). The dead-letter check itself cannot see this (with no `deadLetter.routingKey` set, the key a dead letter arrives under is not knowable at define time, so any binding satisfies it), so `defineQueueBinding` closes it instead: a `#` or `*` segment on a direct exchange throws at define time. Bind the key the message will actually carry — the queue's `deadLetter.routingKey` if it sets one, otherwise every key the source queue can receive:

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

**The timeout arrives as a modeled `PublishError`** with `reason: "timeout"`, in `errCases` — see [a publish the broker does not take is a `PublishError`](#a-publish-the-broker-does-not-take-is-a-publisherror). `.getOrThrow()` throws it; an exhaustive matcher has to handle it:

```typescript
import { P } from "unthrown";

const result = await client.publish("sendEmail", payload);

result.match({
  ok: () => {},
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/MessageValidationError"), (error) => {
        log.error({ error }, "invalid payload, never sent");
      })
      .with(P.tag("@amqp-contract/PublishError"), (error) => {
        // Reachable since 3.0: the 30s publish timeout lands here.
        log.error({ reason: error.reason }, "publish failed");
      }),
  defect: (cause) => {
    log.error({ cause }, "bug while publishing");
  },
});
```

**A timed-out publish may still have reached the broker.** The timeout splices the message out of the unconfirmed set and rejects the promise; it does not tell the broker to forget it. So a retry in response to this failure can duplicate. AMQP is at-least-once regardless, but this is a new way to reach it — make the retry idempotent, or accept the duplicate. [Delivery guarantees](/explanation/delivery-guarantees) covers this and every other source of duplication in one place.

**The exact edit** — none, unless 30s is wrong for you:

```diff
  const client = await TypedAmqpClient.create({
    contract,
    urls,
+   publishTimeoutMs: 10_000,
  }).getOrThrow();
```

```diff
  const client = await TypedAmqpClient.create({
    contract,
    urls,
+   // restore the pre-3.0 unbounded buffering
+   publishTimeoutMs: null,
  }).getOrThrow();
```

`TypedAmqpWorker` takes the same option for its retry republishes and RPC replies: a retry republish that times out requeues the original, a reply that times out dead-letters the request. `publishTimeoutMs` wins over `channelOptions.publishTimeout` if you set both.

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

Infrastructure and transport failures — consume, cancel, close, compression, JSON parse, a publish failure core cannot classify, and thrown or rejected schema validators — are unexpected, so they now surface as a **defect** whose `cause` is a `TechnicalError`, never as a modeled `Err`.

Only anticipated failures remain in `E`: `MessageValidationError`, `RpcError`, `RpcTimeoutError`, `RpcCancelledError`, the worker's `RetryableError` / `NonRetryableError` — and the two broker failures a caller is expected to handle: `ConnectionError` when `create()` cannot reach it ([the broker is a modeled failure](#the-broker-is-a-modeled-failure)) and `PublishError` when it does not take a publish ([below](#a-publish-the-broker-does-not-take-is-a-publisherror)). A connection **lost** while consuming is still a defect.

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

Error channels change accordingly: `client.publish(...)` is `AsyncResult<void, MessageValidationError | PublishError>`, and `client.call(...)` swaps `TechnicalError` for `PublishError` in its union.

### `close()` needs `.get()`, and `create()` keeps `.getOrThrow()`

`close()`'s modeled channel is now empty (`E = never`), and `.getOrThrow()` is gated to a _non-empty_ error channel, so it no longer compiles there:

```diff
- await client.close().getOrThrow();
+ await client.close().get();
```

`create()` goes the other way: it carries a modeled `ConnectionError` (see [the broker is a modeled failure](#the-broker-is-a-modeled-failure) below), so `.getOrThrow()` is what it takes — or triage the error, which is the point of modeling it. `.getOrThrow()` on `publish(...)` / `call(...)` is unaffected; those still carry a modeled `E`.

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
| Core implementation helpers (list below)    | `@amqp-contract/core/internal`                     |
| `defineEventPublisher`'s `arguments` option | `bindingArguments` (it always configured bindings) |

The core implementation helpers are `setupAmqpTopology`, `safeJsonParse`, `technicalDefect`, `startPublishSpan`, `startConsumeSpan`, `endSpanSuccess`, `endSpanError`, `recordPublishMetric`, `recordConsumeMetric`, `recordLateRpcReply` and the `ConnectionLease` type. Nothing was removed, but `/internal` carries no semver guarantee.

The worker's and client's own `ConsumerOptions` / `PublishOptions` (the ones you use with `Typed*`) are unchanged. `TopologyMode`, `ConnectionSource`, `PublishError`, `DEFAULT_MAX_MESSAGE_BYTES` and the telemetry types stay on the core root. Builder-result brands are now `unique symbol`s — invisible in hovers and no longer forgeable; code that referenced `__brand` structurally must stop.

The contract package moves its cross-package runtime helpers the same way: `extractConsumer`, `isBridgedPublisherConfig`, `isCommandConsumerConfig`, `isEventConsumerResult`, `isEventPublisherConfig`, `deriveTtlBackoffInfrastructure`, `ttlBackoffBaseDelay` and `ttlBackoffWaitQueueName` are exported from `@amqp-contract/contract/internal`, and their root exports are deprecated aliases.

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

`AmqpClient.publish` / `sendToQueue` also now return `AsyncResult<void, PublishError>` instead of `AsyncResult<boolean, never>`: the channel's outcome is classified once, inside core, and downstream code no longer checks a boolean. A `false` from the channel — the write buffer is full, reported only after the broker confirmed the message — is success, logged at `debug`.

### Handlers take helpers first, message second

**What breaks:** every handler that reads its payload. The leaf's parameters
swapped — the `{ context, errors, raw }` helpers record is the **first**
argument now, the validated `{ payload, headers }` message the second, and the
raw amqplib delivery moved from a third parameter into `raw` on the helpers.

**Why:** oRPC is the reference shape for this family, being the most widely used
of the three transports a `@btravstack/*` application composes — a developer
arriving here has more likely seen `({ errors, context }, input)` than either of
the others. The mint and compose calls already agreed across the three; the leaf
a developer types by hand did not, and it is the one they relearn per transport.
`@temporal-contract`'s activity leaf moved with it.

**The exact edit:**

```diff
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
-     processOrder: ({ payload }) => save(payload),
+     processOrder: ({ input: { payload } }) => save(payload),
-     handleFailed: ({ payload }, rawMessage) => log(rawMessage.properties.headers),
+     handleFailed: ({ raw, input: { payload } }) => log(raw.properties.headers),
-     getOrder: ({ payload }, _raw, { errors }) => lookup(payload, errors),
+     getOrder: ({ errors, input: { payload } }) => lookup(payload, errors),
    },
  }).getOrThrow();
```

A handler that needs none of the helpers still names the position:
`({ input: { payload } }) => ...`. One that reads neither is just `() => ...`.

The message is on the helpers record too, so a handler can be written from one
destructuring instead — `({ errors, input }) => ...`. That is oRPC's own
shape: `ProcedureHandlerOptions` carries `input` and the handler still takes it
positionally, and both spellings are the same call.

**Two additions worth adopting while you are here.** The helpers record also
carries `retryable` and `nonRetryable`, so the routing decision no longer needs
an import:

```diff
- import { RetryableError } from "@amqp-contract/worker";
-
- processOrder: ({ payload }) =>
-   fromPromise(save(payload), (cause) => new RetryableError("database unavailable", cause)),
+ processOrder: ({ retryable, input: { payload } }) =>
+   fromPromise(save(payload), (cause) => retryable("database unavailable", cause)),
```

The classes stay exported and `new RetryableError(...)` keeps working — this is
the same value by a shorter route.

**What the compiler does and does not catch:** a handler that reads its payload
fails to compile, because the first parameter is the helpers record now. One
that ignores its message keeps compiling with a parameter whose name lies —
grep the handlers object for a leaf whose first parameter is not `_` or a
helpers destructuring.

### The broker is a modeled failure

`TypedAmqpWorker.create` and `TypedAmqpClient.create` report an unreachable
broker as a typed `Err` — `ConnectionError`, exported from all three packages —
where it used to arrive as a `Defect` carrying a `TechnicalError`.

**Why:** a refused connection, a rotated credential, a cluster that has not come
up yet — every one of them is an operator's business and the anticipated
failure of dialing a broker, which is the definition of the `Err` channel here.
The defect channel keeps its meaning: the failures nobody anticipated. Before
this, a start-up path that wanted to turn "broker down" into an exit code had to
recover EVERY defect to reach it, which also swallowed genuine bugs raised
during start-up.

**The exact edit** — handle the channel, or unwrap it:

```diff
- const worker = await TypedAmqpWorker.create({ contract, handlers, urls }).get();
+ const worker = await TypedAmqpWorker.create({ contract, handlers, urls }).getOrThrow();
```

```diff
+ const started = await TypedAmqpWorker.create({ contract, handlers, urls }).match({
+   ok: (worker) => worker,
+   errCases: (matcher) =>
+     matcher.with(P.tag("@amqp-contract/ConnectionError"), (error) => {
+       logger.error({ error }, "broker unreachable");
+       process.exitCode = 1;
+       return undefined;
+     }),
+   defect: (cause) => {
+     logger.error({ error: cause }, "bug during start-up");
+     process.exitCode = 70;
+     return undefined;
+   },
+ });
```

A blanket `.recoverDefect(...)` that existed only to move this failure onto the
`Err` channel can go — that is what this change is for.

### A publish the broker does not take is a `PublishError`

**What breaks:** every exhaustive matcher over a `publish()` or `call()` result — it fails to compile until it handles the new case.

`client.publish(...)`, `client.call(...)` and core's `AmqpClient.publish` / `sendToQueue` report a broker-side failure as a modeled `PublishError` on the `E` channel, with a `reason`:

- `"timeout"` — the message sat buffered past `publishTimeoutMs` (the broker was unreachable);
- `"nacked"` — the broker refused it (`basic.nack`);
- `"channel-closed"` — the channel closed before the message was confirmed.

**Why:** a broker that is down, overloaded or refusing a message is an operating condition a publisher is expected to handle — buffer, retry, shed load, answer 503 — not a bug. A failure core cannot classify (an unencodable payload, an unknown rejection) stays a defect with a `TechnicalError` cause. A full write buffer is **not** a failure: on the confirm channel it is only reported after the broker confirmed the message, so the publish answers `Ok` instead of inviting a duplicate republish.

**The exact edit** — add the case, or group it with the others:

```diff
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/MessageValidationError"), (error) => {/* … */})
+     .with(P.tag(PublishError.tag), (error) => {/* error.reason */}),
```

In a handler that publishes, map it to `RetryableError` so a broker hiccup goes through the retry pipeline, and drop any `.recoverDefect(...)` that existed to catch a failed publish — see [share connections](/how-to/share-connections#publish-from-inside-a-handler). `PublishError` is exported from core and re-exported by client and worker.

::: tip Tracking the 3.0 betas?
The client's interceptor error union exported as `PublishError` (a type alias, `MessageValidationError`) is renamed **`ClientPublishError`** (`MessageValidationError | PublishError`), freeing the name for the error class. `CallError` gains `PublishError`.
:::

### RPC requests expire, and are metered on their own

`client.call(...)` publishes its request with `expiration` set to the call's `timeoutMs`, so a request no worker picked up before the caller gave up is dropped by the broker instead of being answered for nobody. A `publishOptions.expiration` you pass still wins.

The round trip is recorded on its own histogram, **`amqp.client.rpc.duration`**, instead of `amqp.client.publish.duration`, and RPC calls no longer increment `amqp.client.messages.published` — a slow handler no longer reads as a slow broker. Move RPC latency dashboards and alerts to the new metric. A custom `TelemetryProvider` records it by implementing the optional `getRpcCallLatencyHistogram`.

### RPC servers reply only to allowed addresses and never retry

**What breaks:** nothing at compile time. Two kinds of RPC request that used to be served are now dead-lettered, with the reason logged:

- **A `replyTo` other than direct reply-to.** By default the worker replies only to `amq.rabbitmq.reply-to…`, which is what `client.call()` uses, so a forged request cannot make it publish into an arbitrary queue. If your callers use their own reply queues, allow them:

  ```diff
    const worker = await TypedAmqpWorker.create({
      contract,
      handlers,
      urls,
  +   rpc: { allowReplyTo: (replyTo) => replyTo.startsWith("replies.") },
    }).getOrThrow();
  ```

- **A `RetryableError` from an RPC handler**, even on a queue with a `retry` config. The caller waits on a `timeoutMs` far shorter than most backoffs, so a retry re-ran the handler for nobody. Return a declared `RpcError` for a failure the caller should see.

### The client and the worker each declare only their own topology

**What breaks:** nothing visibly, unless a contract carries standalone `queues` or `exchanges` that no publisher and no consumer reaches. Before, the client and the worker both declared the entire contract on connect. Now each declares its role's slice:

- **The client** declares its publishers' exchanges, everything they route to (exchange-to-exchange bindings, transitively), every queue reachable that way with its binding, and the RPC request queues. So a message published before any worker started is retained, not confirmed and dropped. Those queues get the worker's exact arguments, but none of its infrastructure: no dead-letter exchange, no retry wait queues. Exclusive queues are never declared by the client.
- **The worker** declares the queues it consumes with their bindings and retry wait queues, the exchanges those bind to, and their dead-letter exchanges with whatever those route to (the DLQs).

A queue or exchange that neither slice reaches — an audit queue bound to an exchange this service never publishes to, say — is declared by nobody. Declare it where it is owned, or run the low-level setup yourself; `setupAmqpTopology` now lives on `@amqp-contract/core/internal`:

```typescript
import { setupAmqpTopology } from "@amqp-contract/core/internal";
import { connect } from "amqplib";

const connection = await connect("amqp://localhost");
const channel = await connection.createChannel();
await setupAmqpTopology(channel, contract); // declares every resource of the contract
await connection.close();
```

**New: `topology`** on `TypedAmqpClient.create` and `TypedAmqpWorker.create` (and core's `AmqpClient`) says what to do with the slice on every (re)connect: `"assert"` (the default, as before) declares it, `"passive"` only checks it exists and fails `create()` if something is missing — for credentials without configure permission — and `"none"` touches nothing, for topology provisioned elsewhere.

### Clients and workers no longer share a connection

**What breaks:** nothing at compile time; a process that runs a client and a worker against the same URLs now opens **two** TCP connections where it opened one. The process-wide pool is partitioned — clients share among themselves, workers among themselves. RabbitMQ blocks a publishing connection under a memory or disk alarm, and a consumer sharing it would stop acking with it. Check the broker's connection limits if you run many processes.

To share one connection deliberately, own it and pass it as **`connection`** instead of `urls` (exactly one of the two). It is borrowed, never closed by the client or worker:

```typescript
import amqp from "amqp-connection-manager";

const connection = amqp.connect(["amqp://localhost"]);
const client = await TypedAmqpClient.create({ contract, connection }).getOrThrow();
const worker = await TypedAmqpWorker.create({ contract, handlers, connection }).getOrThrow();
```

See [share connections](/how-to/share-connections).

### Inbound messages are capped at 16 MiB

**What breaks:** nothing at compile time. The worker now refuses any inbound body over **16 MiB** (`DEFAULT_MAX_MESSAGE_BYTES`, RabbitMQ 4's own default `max_message_size`) — a plain body as it arrives, a compressed one while it inflates, so a few-KB "zip bomb" never materialises. An over-cap message is dead-lettered on first delivery, like any other unparseable payload. If you legitimately send larger messages, raise it:

```diff
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers,
    urls,
+   maxMessageBytes: 64 * 1024 * 1024,
  }).getOrThrow();
```

The client decodes RPC replies through the same codec, so a reply carrying a `contentEncoding` is decompressed rather than failing to parse.

::: tip Tracking the 3.0 betas?
The betas capped only decompressed output, at 64 MiB, under the option `maxDecompressedBytes`. That name still works as a deprecated alias of `maxMessageBytes`.
:::

### The retry path is stricter

Behaviour changes in the worker's retry handling, none of which needs an edit:

- **A retry publish the broker does not take requeues the original.** When the retry copy fails with `PublishError`, the original is `nack`ed with `requeue: true`, its retry headers unchanged — never dead-lettered (or dropped, on an `onPoison: "drop"` queue) for a broker hiccup. The log line is `Publish for retry failed; requeueing the original for redelivery`.
- **Malformed retry headers count as 0.** An `x-retry-count` or `x-delivery-count` that is not a non-negative integer no longer bypasses the retry budget; a malformed `x-first-failure-timestamp` or `x-original-routing-key` is replaced; and a retry is only published to a declared wait queue — anything else is dead-lettered with the reason logged.
- **`x-last-error` is truncated to 1024 characters**, so an error carrying a stack or a payload dump can no longer exceed the broker's `frame_max` on every retry.
- **An inbound payload that fails its schema is a modeled `MessageValidationError`** on the worker as on the client: the consume span records it as its exception and the consume metric counts a failure. It is still dead-lettered on first delivery and never retried.
- Retry routing logs one decision line — `Retrying message (requeue)`, `Retrying message (republish)` or `Sending to DLQ: <reason>`. Update log-based alerts that matched the old per-mode wording.

### Trace context propagates by itself

Publish injects the active OpenTelemetry context into the message headers, through the propagator your SDK registered, and the consumer runs each delivery inside the context extracted from them — with the consume span active in `createContext`, middleware and the handler. One trace spans producer, broker and consumer with no code of yours. If you followed the old recipe — a publish interceptor stamping `traceparent` and a worker middleware resuming it — delete both; a hand-stamped `traceparent` is overwritten by the injected one anyway. See [instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry).

### Smaller additions

- **`isConnected()`** on `TypedAmqpClient`, `TypedAmqpWorker` and core's `AmqpClient`: whether the broker connection is up right now, for a readiness probe ([run in production](/how-to/run-in-production#wire-health-checks)).
- **Static `.tag` on every error class** — `P.tag(PublishError.tag)`, `P.tag(RetryableError.tag)` — instead of the raw `"@amqp-contract/…"` string. Each class's stack now starts with its own `Name: message`.
- **Named handler types**: `ConsumerHandler<TPayload, THeaders?, TContext?>`, `RpcHandler<TRequest, TResponse, TErrors?, THeaders?, TContext?>` and their `…Entry` forms (with the `[handler, options]` tuple). Handler type errors now name the resolved message instead of the whole contract type, and you can type a handler by its payload directly.
- **Diagnosable `ConnectionError`**: its `cause` is the last failed dial (`ECONNREFUSED`, `ACCESS_REFUSED`), and the first failed dial is logged at `warn`.

### Quorum queues with immediate-requeue retry declare `x-delivery-limit`

**What breaks:** workers and clients fail at startup against a quorum queue that **already exists on the broker** and has `retry: { mode: "immediate-requeue" }`. `defineQueue` now adds the queue argument `x-delivery-limit: maxRetries + 1`, and RabbitMQ refuses to redeclare a queue with arguments that differ from the live one:

```
PRECONDITION_FAILED - inequivalent arg 'x-delivery-limit' for queue 'order-processing'
in vhost '/': received the value '4' of type 'byte' but current is none
```

**Why:** RabbitMQ 4 caps quorum redeliveries at `x-delivery-limit`, 20 by default, and dead-letters past it by itself. With `maxRetries` of 20 or more, the broker dead-lettered the message (reason `delivery_limit`) before the worker's retry budget ran out, so the configured budget was never reached. Setting the limit one above `maxRetries` keeps the worker in charge.

**The fix** depends on what the live queue was declared with:

- **No `x-delivery-limit` argument** (the common case — the old default): recreate the queue so it is declared with the new argument. Drain it first; queue arguments cannot be changed in place. The contract always sends the argument for these queues, so there is no way to match a queue declared without one.
- **An explicit `x-delivery-limit` already in `arguments`**: nothing changes — `defineQueue` keeps your value — as long as it is at least `maxRetries + 1` (or negative, RabbitMQ's "unlimited"). A lower value is now rejected at define time; raise it (which again means recreating the queue) or lower `maxRetries`.

```typescript
import { defineExchange, defineQueue } from "@amqp-contract/contract";

const ordersDlx = defineExchange("orders-dlx");

// Already declared with x-delivery-limit 10: kept as-is, since 10 >= maxRetries + 1.
// A value below maxRetries + 1 is rejected at define time.
const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 },
  arguments: { "x-delivery-limit": 10 },
});
```

Classic queues, `ttl-backoff` retry and queues with no retry are unaffected.

### AsyncAPI: schemas convert natively; `schemaConverters` has its own type

**What changes:** `@amqp-contract/asyncapi` converts any schema implementing Standard JSON Schema (`~standard.jsonSchema` — current Zod 4 and ArkType releases) by itself, and that takes precedence over `schemaConverters`. The converters are now only a fallback, for libraries without it such as Valibot. Three things can need action:

- **Generated output differs slightly** for Zod and ArkType payloads, because the schema's own converter is used instead of oRPC's: Zod adds a `pattern` to `z.string().datetime()`, and ArkType's `$schema` marker is dropped. If CI diffs a committed `asyncapi.json`, regenerate and commit it once.
- **`schemaConverters` is typed `SchemaConverter[]`**, a structural type exported by `@amqp-contract/asyncapi`, instead of `@orpc/openapi`'s `ConditionalSchemaConverter[]`. The oRPC converters still satisfy it, so passing them compiles unchanged; code that _names_ `ConditionalSchemaConverter` should switch to `SchemaConverter`.
- **`@orpc/openapi` is no longer a dependency** of the package. If your own code imports from it, add it to your `package.json` yourself.

**The fix** for a Zod- or ArkType-only contract is to delete the converter (and `@orpc/zod` / `@orpc/arktype`, if nothing else uses them); keep the converter for Valibot:

```diff
- import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
- const generator = new AsyncAPIGenerator({
-   schemaConverters: [new ZodToJsonSchemaConverter()],
- });
+ const generator = new AsyncAPIGenerator();
```

The generator also takes a new `vhost` option for the channel bindings, defaulting to the previously hardcoded `"/"` — no action unless your broker uses another virtual host. See [generate AsyncAPI](/how-to/generate-asyncapi).

### Suggested order

1. Bump `unthrown` and the six packages together.
2. Run `pnpm typecheck` and work through the errors — nearly all of this is compiler-visible (`extractQueue` deletions, renamed types, `declare*` renames, helpers moved to `/internal`, signature changes).
3. Fix `create()` / `close()` extraction first; it is mechanical.
4. Then convert each `match` / `*Err` site, moving `TechnicalError` handling into `defect` as you go — except at `create()` and around `publish()` / `call()`, where broker failures moved the other way, into `errCases` as `ConnectionError` and `PublishError`. Replace any `recoverDefect` that existed to catch a failed publish with a `PublishError` case.
5. Resolve the `defineContract` dead-letter throws — both of them: the missing `deadLetter` pointer, and the exchange it names having nothing bound. Decide the broker route (new queue, policy, or accepted loss) _before_ editing the contract, since a live queue cannot take a `deadLetter`. Check each DLX's type on the broker before binding: `#` routes everything on a topic exchange, and on a direct one it is rejected at define time — bind the real key there.
6. Unwrap `create()` with `.getOrThrow()`, or triage its `ConnectionError` —
   and delete any blanket defect-recovery that existed to reach it.
7. Swap every handler leaf to `(helpers, message)` — the compiler names the
   ones that read their payload; grep for the rest.
8. Decide prefetch deliberately for every worker. The compiler will not raise it, and of the [silent changes](#_2-4-x-→-3-0) it is the one every worker meets, so make it a review item rather than a discovery in production.
9. Deploy workers before deleting the old `{queue}-wait` queue and `wait-exchange`/`retry-exchange` from the broker.
10. Plan a recreate for every existing quorum queue with `immediate-requeue` retry: it now [declares `x-delivery-limit`](#quorum-queues-with-immediate-requeue-retry-declare-x-delivery-limit) and fails to redeclare against the old one.
11. List the standalone `queues` / `exchanges` in each contract that no publisher or consumer reaches, and [declare them yourself](#the-client-and-the-worker-each-declare-only-their-own-topology) before relying on them.
12. Check the largest message each worker receives against the [16 MiB cap](#inbound-messages-are-capped-at-16-mib), and every RPC caller that sets its own `replyTo` against the [allowlist](#rpc-servers-reply-only-to-allowed-addresses-and-never-retry).
13. Delete the trace-propagation interceptor and middleware, if you wrote them: [propagation is built in](#trace-context-propagates-by-itself).
14. Expect roughly twice the broker connections from a process that runs both a client and a worker ([separate pools](#clients-and-workers-no-longer-share-a-connection)); check the broker's connection limits.

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

::: warning Historical mapping
This table records what 1.0 shipped. Later sections supersede several rows — `ok(v).toAsync()`, `.unwrap()` and the `err` match key no longer exist in current unthrown. If you are going from 0.x straight to the latest release, use the current equivalents instead:

| 1.x (this table)                       | Current                                                                |
| -------------------------------------- | ---------------------------------------------------------------------- |
| `ok(v).toAsync()` / `err(e).toAsync()` | `OkAsync(v)` / `ErrAsync(e)`                                           |
| `.unwrap()`                            | `.getOrThrow()` (or `.get()` when the error channel is `never`)        |
| `result.match({ ok, err, defect })`    | `result.match({ ok, errCases: (matcher) => matcher.with(…), defect })` |
| `.tapErr`                              | `.tapErrCases((matcher) => matcher.with(…))`                           |

:::

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
