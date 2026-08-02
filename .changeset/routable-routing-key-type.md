---
"@amqp-contract/contract": minor
---

Add the `RoutableRoutingKey<Key, Patterns>` type. It resolves to `Key` when the
routing key matches at least one of the declared binding patterns and to a
readable `` `Error: routing key '…' matches none of the declared binding
patterns; …` `` string type otherwise — the same convention as
`MatchingBindingPattern`. The check runs only when both the key and the
patterns are fully known at compile time. Plain `string`, template-literal
types such as `` `order.${string}` ``, unions containing either, and an empty
pattern union all skip the check and resolve to `Key` — an undecidable case
defers to the define-time check rather than being guessed at.

The type is exported for direct use; `defineContract`'s signature is
deliberately not constrained by it (binding patterns are widened to `string` by
the time they reach `defineContract`, so the constraint would be a no-op). The
define-time check added in the same release covers the full binding graph at
runtime.
