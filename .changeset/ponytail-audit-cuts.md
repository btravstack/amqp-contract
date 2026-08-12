---
"@amqp-contract/asyncapi": minor
"@amqp-contract/client": minor
"@amqp-contract/contract": minor
"@amqp-contract/core": minor
"@amqp-contract/testing": minor
"@amqp-contract/worker": minor
---

Remove over-engineered surface and collapse duplicated definitions.

**Breaking — removed exports**

- `@amqp-contract/worker`: `isRetryableError`, `isNonRetryableError`, `isHandlerError`, `retryable`, `nonRetryable`. The guards were `instanceof` wrappers with no internal callers; narrow with the error matcher (`P.tag("@amqp-contract/RetryableError")`) or `instanceof RetryableError` / `instanceof NonRetryableError`. The factories were one-line `new X(...)` wrappers; construct the classes directly, or use `qualifyRetryable` / `qualifyNonRetryable` at a `fromPromise` boundary.
- `@amqp-contract/contract`: `MatchingRoutingKey` and `formatIssue`. `MatchingRoutingKey` had no caller in the library — `MatchingBindingPattern` is the matcher the builders actually enforce, and it reports a readable error string instead of `never`. `formatIssue` was only ever used by `summarizeIssues`, which stays exported.
- `@amqp-contract/core`: `AmqpClient.addSetup()` (no callers) and `AmqpClient._resetConnectionCacheForTesting()` (superseded by `_internal_resetConnections` from `@amqp-contract/core/internal`).

**Breaking — narrowed overload sets**

- `composeMiddleware` typed overloads now cover 4 middleware instead of 8. Longer chains nest, as documented: `composeMiddleware(composeMiddleware(a, b, c, d), e, f)`.

**Non-breaking**

- `defineExchange` collapses four near-identical overloads into one generic signature; the return type still narrows on `type`.
- `defineEventPublisher`, `defineEventConsumer`, `defineCommandConsumer` and `defineCommandPublisher` merge their byte-identical fanout and headers overloads into a single keyless-exchange signature. `defineCommandPublisher` now returns the caller's exact exchange type rather than the widened union.
- `AmqpClient.waitForConnect` uses `Promise.race` with `AbortSignal.timeout` instead of a hand-rolled `setTimeout` race, so a pending connect timer no longer holds the event loop open.
- The AsyncAPI generator converts each message schema once instead of twice per channel.
- Internal cleanups with no API impact: shared `settleAll` helper in `setupAmqpTopology`, shared publish-instrumentation helper in the client, encoding lookup table in the worker's decompressor, one shared vhost request helper in the testing fixture, and removal of a redundant `try`/`catch` around telemetry calls that already swallow internally.
