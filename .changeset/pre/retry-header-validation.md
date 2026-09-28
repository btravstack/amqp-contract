---
"@amqp-contract/worker": patch
---

Retry headers read from the wire are validated. A malformed `x-retry-count` /
`x-delivery-count` (a string, NaN, a negative, a fraction, a table) counts as 0
instead of bypassing the retry budget (`"abc" >= 3` is never true) or
computing an undeclared ttl-backoff wait-queue name — which silently lost the
retry copy, published via the default exchange without `mandatory`. A retry is
only ever published to a declared wait-queue tier; anything else is
dead-lettered with the reason logged. A malformed `x-first-failure-timestamp`
or `x-original-routing-key` is replaced instead of propagated.
