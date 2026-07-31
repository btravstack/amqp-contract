---
"@amqp-contract/contract": minor
---

Add the `RoutableRoutingKey<Key, Patterns>` type. It resolves to `Key` when the
routing key matches at least one of the declared binding patterns and to a
readable `` `Error: routing key '…' matches none of the declared binding
patterns; …` `` string type otherwise — the same convention as
`MatchingBindingPattern`. Non-literal inputs and an empty pattern union skip the
check and resolve to `Key`, so nothing valid is ever rejected at compile time.

The type is exported for direct use; `defineContract`'s signature is
deliberately not constrained by it (binding patterns are widened to `string` by
the time they reach `defineContract`, so the constraint would be a no-op). The
define-time check added in the same release covers the full binding graph at
runtime.
