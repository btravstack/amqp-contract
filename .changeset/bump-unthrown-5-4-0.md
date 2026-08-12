---
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Move the `@unthrown/standard-schema` runtime dependency from 5.1.0 to 5.4.0.

The `unthrown` peer range is unchanged (`^5.0.0`), which 5.4.0 already satisfies,
so consumers pinned to any 5.x need no action. 5.4.0 removes four matcher
patterns (`P.any` / `P.string` / `P.number` / `P.union`), none of which this
codebase used; the full type-test suite and unit suite pass unmodified against
it.
