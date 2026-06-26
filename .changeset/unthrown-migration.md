---
"@amqp-contract/contract": major
---

**BREAKING:** Replace the `neverthrow` dependency with [`unthrown`](https://github.com/btravstack/unthrown) for value-based error handling across all packages.

`unthrown` keeps the errors-as-values model but adds a third **`Defect`** channel for unexpected failures, and renames/​reshapes several APIs. If you consume the `Result` / `AsyncResult` values returned by the client, worker, and core, you will need to update your call sites.

### What changed

- **`ResultAsync` → `AsyncResult`.** All async-returning methods (`publish`, `call`, `create`, `close`, handler return types, …) now return `unthrown`'s `AsyncResult<T, E>`.
- **`.match()` is now boxed and has three channels.** `result.match(okFn, errFn)` → `result.match({ ok, err, defect })`. The extra `defect` branch handles unexpected throws.
- **`.andThen` → `.flatMap`**, **`.andTee` → `.tap`**, **`.orTee` → `.tapErr`** (`.map`, `.mapErr`, `.orElse` are unchanged).
- **No `okAsync` / `errAsync`.** Build async results with `ok(value).toAsync()` / `err(error).toAsync()`.
- **No static `ResultAsync.fromPromise` / `Result.fromThrowable`.** Use the free functions `fromPromise(promise, qualify)`, `fromSafePromise(promise)`, and `fromThrowable(fn, qualify)`. The `qualify` mapper returns `E | Defect`.
- **`._unsafeUnwrap()` → `.unwrap()`**, **`._unsafeUnwrapErr()` → `.unwrapErr()`** (these now throw `UnwrapError` on the wrong variant, and re-throw the original cause on a `Defect`).
- **`.isOk()` / `.isErr()` / `.isDefect()` narrow** like neverthrow's did (they guard `this`); standalone `isOk(result)` / `isErr(result)` / `isDefect(result)` functions are also available.
- **Error classes are now `TaggedError`s.** `TechnicalError`, `MessageValidationError`, `RetryableError`, `NonRetryableError`, `RpcTimeoutError`, and `RpcCancelledError` each carry a `_tag` for exhaustive dispatch via `matchTags`. The tags are **namespaced** — `"@amqp-contract/TechnicalError"`, `"@amqp-contract/RetryableError"`, etc. — so they don't collide with other libraries' tags in a shared `matchTags`. The human-facing `Error.name` is kept bare (`"TechnicalError"`, `"RetryableError"`, …), so stack traces and `.name` checks are unaffected. Their positional constructors are unchanged.
- **`HandlerError` is now a tagged-union type, not an abstract class.** It is `RetryableError | NonRetryableError`. Replace `error instanceof HandlerError` with `isHandlerError(error)`.
