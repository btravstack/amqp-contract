---
"@amqp-contract/asyncapi": minor
"@amqp-contract/client": minor
"@amqp-contract/contract": minor
"@amqp-contract/core": minor
"@amqp-contract/testing": minor
"@amqp-contract/worker": minor
---

Remove redundant API surface and collapse duplicated definitions.

**Breaking — removed exports**

Each removal below has an exact, equal-effort replacement already in the public
API. Anything without one was kept, whether or not the library itself used it.

- `@amqp-contract/worker`: `isRetryableError` and `isNonRetryableError` were
  1:1 with `error instanceof RetryableError` / `instanceof NonRetryableError`,
  and both classes are exported. `retryable(message, cause)` and
  `nonRetryable(message, cause)` were 1:1 with `new RetryableError(...)` /
  `new NonRetryableError(...)`. `isHandlerError` is **kept** — `HandlerError` is
  a union type with no runtime counterpart, so no `instanceof` can replace it.
- `@amqp-contract/core`: `AmqpClient._resetConnectionCacheForTesting()`, a
  second path to `_internal_resetConnections` from `@amqp-contract/core/internal`.
  Both are `@internal` with no semver guarantee.

**Non-breaking**

- `defineExchange` collapses four near-identical overloads into one generic
  signature; the return type still narrows on `type`.
- `defineEventPublisher`, `defineEventConsumer`, `defineCommandConsumer` and
  `defineCommandPublisher` merge their byte-identical fanout and headers
  overloads into a single keyless-exchange signature. `defineCommandPublisher`
  now returns the caller's exact exchange type rather than the widened union.
- `AmqpClient.waitForConnect` uses `Promise.race` with `AbortSignal.timeout`
  instead of a hand-rolled `setTimeout` race, so a pending connect timer no
  longer holds the event loop open.
- The AsyncAPI generator converts each message schema once per channel instead
  of twice.
- Internal cleanups with no API impact: shared `settleAll` helper in
  `setupAmqpTopology`, shared publish-instrumentation helper in the client,
  encoding lookup table in the worker's decompressor, one shared vhost request
  helper in the testing fixture, and removal of a redundant `try`/`catch` around
  telemetry calls that already swallow internally.
