---
"@amqp-contract/contract": minor
---

Upgrade [`unthrown`](https://github.com/btravstack/unthrown) (and `@unthrown/vitest`) to `2.0.0`.

unthrown 2.0 is **additive** relative to 1.x — it adds an `AsyncResult` value namespace (`AsyncResult.fromPromise` / `.all` / …), a `flatTapErr` combinator, and an `isResult` guard. None of amqp-contract's public API changed: the `Result` / `AsyncResult` types you receive from the client and worker keep the same shape (2.0's types are a superset of 1.x's), and `Ok` / `Err` / `fromPromise` / `matchTags` / `TaggedError` / `.isOk()` / `.match()` are unchanged.

No code changes are required to adopt this. If you import `unthrown` directly alongside amqp-contract, bump your own dependency to `^2.0.0` so a single copy is shared.
