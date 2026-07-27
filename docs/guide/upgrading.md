---
title: Upgrading - amqp-contract
description: Migration notes for major versions of amqp-contract, including the neverthrow to unthrown transition and the Ok/Err constructor rename.
---

# Upgrading

All six `@amqp-contract/*` packages version together, so upgrade them in lockstep. This page summarizes the changes that require action; the full history lives in the [GitHub Releases](https://github.com/btravstack/amqp-contract/releases) and each package's `CHANGELOG.md`.

## 2.4.x → 3.0

**`TechnicalError` moved from the modeled error channel to the defect channel.** Infrastructure/transport failures (connection, publish, consume, cancel, close, compression/decompression, JSON parse, and thrown/rejected schema validators) are unexpected, so they now surface as an unthrown **`Defect`** (with a `TechnicalError` as the defect's `cause`, still exported for logging and `instanceof` checks) — never as a modeled `Err`. Only anticipated _domain_ failures remain in `E`: `MessageValidationError`, `RpcError`, `RpcTimeoutError`, `RpcCancelledError`, and the worker's `RetryableError` / `NonRetryableError`.

This is **breaking** for any call site that matched `TechnicalError` as a modeled error:

- **Matching `tag("@amqp-contract/TechnicalError")` in an error matcher** (`match`'s `errCases`, `mapErrCases`, `flatMapErrCases`, `tapErrCases`, `recoverErrCases`, `flatTapErrCases`) no longer type-checks — drop that case and handle the failure in the **`defect`** arm of `.match(...)`, or with `.recoverDefect(...)` / `.tapDefect(...)`. The defect's `cause` is the `TechnicalError` instance.

  ```diff
    result.match({
      ok: () => {/* ... */},
  -   errCases: (matcher) =>
  -     matcher.with(
  -       tag("@amqp-contract/TechnicalError"),
  -       tag("@amqp-contract/MessageValidationError"),
  -       (error) => {/* ... */},
  -     ),
  +   errCases: (matcher) =>
  +     matcher.with(tag("@amqp-contract/MessageValidationError"), (error) => {/* ... */}),
  -   defect: (cause) => { throw cause; },
  +   defect: (cause) => {
  +     // a TechnicalError arrives here now
  +     throw cause;
  +   },
    });
  ```

- **Error channels narrow.** `client.publish(...)` is now `AsyncResult<void, MessageValidationError>`; `client.call(...)` drops `TechnicalError` from its union (leaving `MessageValidationError | RpcTimeoutError | RpcCancelledError | <declared RpcErrors>`).

- **`create()` / `close()` (and the `AmqpClient` operations) now have an empty modeled channel (`E = never`).** `.getOrThrow()` is type-gated to a _non-empty_ error channel, so it no longer compiles on them — extract their success with **`.get()`** instead (a failed one still panics, rethrowing the underlying `TechnicalError`). `.getOrThrow()` on `publish(...)` / `call(...)` is unaffected — those still carry a modeled `E`.

  ```diff
  - const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();
  + const client = await TypedAmqpClient.create({ contract, urls }).get();
  ```

## 2.3.x → 2.4.x

Upgrades [`unthrown`](https://github.com/btravstack/unthrown) to `4.1.0`. Since `unthrown` is a **peer dependency**, bump your own copy to `^4.1.0`:

```bash
pnpm add unthrown@^4.1
```

unthrown 4.1 **renames** two operator families for consistency; the old names still work but are deprecated and will be removed in the next unthrown major:

| Deprecated (unthrown ≤ 4.0 name) | Use instead                                                 |
| -------------------------------- | ----------------------------------------------------------- |
| `.orElse(f)`                     | `.flatMapErr(f)`                                            |
| `.recover(f)`                    | `.recoverErr(f)`                                            |
| `.unwrap()`                      | `.get()` — or `.getOrThrow()` to throw on a fallible result |
| `.unwrapErr()`                   | `.getErr()`                                                 |
| `.unwrapOr(fallback)`            | `.getOr(fallback)`                                          |
| `.unwrapOrElse(f)`               | `.getOrElse(f)`                                             |

No amqp-contract API changes — but all documentation and examples now use the new names, so snippets like the throw-on-failure escape hatch read `.getOrThrow()`.

## 2.2.x → 2.3.x

Upgrades [`unthrown`](https://github.com/btravstack/unthrown) to `4.0.0`. Since `unthrown` is a **peer dependency**, bump your own copy to `^4.0.0`:

```bash
pnpm add unthrown@^4
```

amqp-contract's own API shape is unchanged, but unthrown 4 changes two things you may rely on:

**1. `.unwrap()` is type-gated.** It now compiles only on a result whose error channel is empty (`E = never`). Calling `.unwrap()` on a fallible result — e.g. `(await client.publish(...)).unwrap()` or `(await TypedAmqpClient.create(...)).unwrap()` — is now a **compile error**. Prefer `.match()`; to keep "throw on failure", unwrap with `.unwrapOrElse()`:

```diff
- const client = (await TypedAmqpClient.create({ contract, urls })).unwrap();
+ const client = await TypedAmqpClient.create({ contract, urls }).unwrapOrElse((e) => {
+   throw e;
+ });
```

See [Getting the value out](./error-model.md#getting-the-value-out).

**2. `TaggedError` reserves `message`.** Only relevant if you define your own `TaggedError` subclasses: a `message` field in the payload is now rejected. Move it to `override message = "…"` (or assign `this.message` in the constructor) and keep the payload for structured fields. amqp-contract's own error classes already do this.

## 2.1.x → 2.2.x

Upgrades [`unthrown`](https://github.com/btravstack/unthrown) to `3.0.0`. amqp-contract's own public surface is unchanged. Action is only needed if you author `qualify` mappers that intentionally route unexpected failures to the `Defect` channel:

- The standalone `Defect` constructor is no longer exported.
- `qualify` now receives a `defect` callback as its second argument: `(cause, defect) => E | defect(cause)`.

```diff
- import { fromPromise, Defect } from "unthrown";
- fromPromise(work(), (cause) => isExpected(cause) ? new MyError(cause) : Defect(cause));
+ import { fromPromise } from "unthrown";
+ fromPromise(work(), (cause, defect) => isExpected(cause) ? new MyError(cause) : defect(cause));
```

Mappers that only return a modeled error (the common case) are unaffected. If you depend on `unthrown` directly, bump it to `^3.0.0` so a single copy is shared.

## 2.0.x → 2.1.x

Upgrades `unthrown` to `2.0.0`, which is additive (adds the `AsyncResult` value namespace, `flatTapErr`, `isResult`). No code changes required.

## 1.x → 2.0

Upgrades `unthrown` to `1.0.0`, which renames the value constructors — **`ok` → `Ok`, `err` → `Err`, `defect` → `Defect`** (the lowercase forms are removed). Update every call site that constructs results directly:

```diff
- import { ok, err } from "unthrown";
- return ok(undefined).toAsync();
- return err(new RetryableError("...")).toAsync();
+ import { Ok, Err } from "unthrown";
+ return Ok(undefined).toAsync();
+ return Err(new RetryableError("...")).toAsync();
```

The `.match({ ok, err, defect })` handler keys stay lowercase — those are case branches, not constructors. Everything else (`fromPromise`, `fromSafePromise`, `.unwrap()`, `.isOk()`, …) is unchanged.

## 0.x → 1.0

Replaces `neverthrow` with [`unthrown`](https://github.com/btravstack/unthrown) across all packages. unthrown keeps the errors-as-values model but adds a third **`Defect`** channel for unexpected failures. If you consume the `Result` / `AsyncResult` values returned by the client, worker, or core, update your call sites:

| neverthrow (amqp-contract 0.x)             | unthrown (amqp-contract 1.x)                                 |
| ------------------------------------------ | ------------------------------------------------------------ |
| `ResultAsync<T, E>`                        | `AsyncResult<T, E>`                                          |
| `result.match(okFn, errFn)`                | `result.match({ ok, err, defect })`                          |
| `.andThen` / `.andTee` / `.orTee`          | `.flatMap` / `.tap` / `.tapErr`                              |
| `okAsync(v)` / `errAsync(e)`               | `ok(v).toAsync()` / `err(e).toAsync()`                       |
| `ResultAsync.fromPromise(p, mapper)`       | `fromPromise(p, qualify)` (free function, mapper required)   |
| `._unsafeUnwrap()` / `._unsafeUnwrapErr()` | `.unwrap()` / `.unwrapErr()`                                 |
| `error instanceof HandlerError`            | `isHandlerError(error)` (`HandlerError` is now a union type) |

The table shows the constructors as they were on amqp-contract 1.x (lowercase `ok` / `err`); if you're upgrading straight to 2.0+, use the capitalized `Ok` / `Err` forms from the [1.x → 2.0](#_1-x-→-2-0) section instead.

Error classes (`RetryableError`, `MessageValidationError`, …) became `TaggedError`s with namespaced tags (e.g. `"@amqp-contract/MessageValidationError"`) for exhaustive dispatch via the error matcher (`result.match({ ok, defect, errCases: (matcher) => matcher.with(tag("@amqp-contract/MessageValidationError"), …) })`); their `Error.name` and constructors are unchanged. (`TechnicalError` is also a `TaggedError`, but as of [3.0](#_2-4-x-→-3-0) it surfaces through the **defect** channel, not the error matcher — see that section.)

See the [Error Model guide](/guide/error-model) for the full picture of how results flow through the API.
