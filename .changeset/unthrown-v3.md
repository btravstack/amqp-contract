---
"@amqp-contract/contract": minor
---

Upgrade [`unthrown`](https://github.com/btravstack/unthrown) (and `@unthrown/vitest`) to `3.0.0`.

amqp-contract's own public surface is unchanged — the `Result` / `AsyncResult` you receive from the client and worker keep the same shape and methods, and no source changes were needed.

unthrown 3.0 does change how a **defect** is produced inside a `qualify` mapper, which matters only if you author handlers that intentionally route an unexpected failure to the `Defect` channel:

- The standalone `Defect` constructor is no longer exported.
- `qualify` now receives a second argument — a `defect` callback — so its signature is `(cause, defect) => E | defect(cause)`.

```diff
- import { fromPromise, Defect } from "unthrown";
- fromPromise(work(), (cause) => isExpected(cause) ? new MyError(cause) : Defect(cause));
+ import { fromPromise } from "unthrown";
+ fromPromise(work(), (cause, defect) => isExpected(cause) ? new MyError(cause) : defect(cause));
```

Mappers that only return a modeled error (the common case — e.g. `(cause) => new RetryableError("…", cause)`) are unaffected.
