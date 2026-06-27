---
"@amqp-contract/contract": major
---

**BREAKING:** Upgrade [`unthrown`](https://github.com/btravstack/unthrown) to `1.0.0`.

unthrown 1.0 renames the value constructors — **`ok` → `Ok`, `err` → `Err`, `defect` → `Defect`** (the lowercase forms are removed). All packages now depend on `unthrown@1.0.0`, so consumers that build `Result` / `AsyncResult` values directly must update their call sites:

```diff
- import { ok, err } from "unthrown";
- return ok(undefined).toAsync();
- return err(new RetryableError("...")).toAsync();
+ import { Ok, Err } from "unthrown";
+ return Ok(undefined).toAsync();
+ return Err(new RetryableError("...")).toAsync();
```

Everything else is unchanged: the `.match({ ok, err, defect })` handler keys stay lowercase (they're case branches, not constructors), and `fromPromise` / `fromSafePromise` / `fromThrowable` / `all` / `allAsync` / `TaggedError(tag, { name })` / `.isOk()` / `.toAsync()` / `.unwrap()` keep the same signatures.
