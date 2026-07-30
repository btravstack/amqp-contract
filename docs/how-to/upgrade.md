---
title: Upgrade - amqp-contract
description: Migration notes for each major version, including the 3.0 defect-channel change and the unthrown v5 matcher renames.
---

# Upgrade

All six `@amqp-contract/*` packages version together, so upgrade them in lockstep. This page lists the changes that need action; the full history is in the [releases](https://github.com/btravstack/amqp-contract/releases) and each package's `CHANGELOG.md`.

## 2.4.x → 3.0

Two independent breaking changes land together. Expect to touch every site that inspects a result.

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
5. Deploy workers before deleting the old `{queue}-wait` queue and `wait-exchange`/`retry-exchange` from the broker.

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
