---
title: Upgrading - amqp-contract
description: Migration notes for major versions of amqp-contract, including the neverthrow to unthrown transition and the Ok/Err constructor rename.
---

# Upgrading

All six `@amqp-contract/*` packages version together, so upgrade them in lockstep. This page summarizes the changes that require action; the full history lives in the [GitHub Releases](https://github.com/btravstack/amqp-contract/releases) and each package's `CHANGELOG.md`.

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

| neverthrow (0.x)                           | unthrown (1.0+)                                              |
| ------------------------------------------ | ------------------------------------------------------------ |
| `ResultAsync<T, E>`                        | `AsyncResult<T, E>`                                          |
| `result.match(okFn, errFn)`                | `result.match({ ok, err, defect })`                          |
| `.andThen` / `.andTee` / `.orTee`          | `.flatMap` / `.tap` / `.tapErr`                              |
| `okAsync(v)` / `errAsync(e)`               | `ok(v).toAsync()` / `err(e).toAsync()`                       |
| `ResultAsync.fromPromise(p, mapper)`       | `fromPromise(p, qualify)` (free function, mapper required)   |
| `._unsafeUnwrap()` / `._unsafeUnwrapErr()` | `.unwrap()` / `.unwrapErr()`                                 |
| `error instanceof HandlerError`            | `isHandlerError(error)` (`HandlerError` is now a union type) |

Error classes (`TechnicalError`, `RetryableError`, …) became `TaggedError`s with namespaced tags (e.g. `"@amqp-contract/TechnicalError"`) for exhaustive dispatch via `matchTags`; their `Error.name` and constructors are unchanged.

See the [Error Model guide](/guide/error-model) for the full picture of how results flow through the API.
