---
"@amqp-contract/contract": minor
---

Add the `RoutableRoutingKey<Key, Patterns>` type. It resolves to `Key` when the
routing key matches at least one of the declared binding patterns and to a
readable `` `Error: routing key '…' matches none of the declared binding
patterns; …` `` string type otherwise — the same convention as
`MatchingBindingPattern`. Non-literal inputs and an empty pattern union skip the
check and resolve to `Key`.

Matching is over literal segments only: a pattern built from a template literal
(`` `order.${string}` ``) is not recognised as matching, so a key it would
accept at runtime still resolves to the error string. Use concrete literal
patterns, or leave the key unconstrained and rely on the define-time check.

The type is exported for direct use; `defineContract`'s signature is
deliberately not constrained by it (binding patterns are widened to `string` by
the time they reach `defineContract`, so the constraint would be a no-op). The
define-time check added in the same release covers the full binding graph at
runtime.
