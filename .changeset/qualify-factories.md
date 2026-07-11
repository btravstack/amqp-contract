---
"@amqp-contract/worker": minor
---

Add `qualifyRetryable(message)` / `qualifyNonRetryable(message)` qualifier factories: they build the `fromPromise` mapper instead of hand-writing `(e) => retryable(message, e)` — the most re-introduced mistake in handler code. `fromPromise(work(), qualifyRetryable("upstream failed"))`.
