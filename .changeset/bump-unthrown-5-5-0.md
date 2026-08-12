---
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Move the `@unthrown/standard-schema` runtime dependency from 5.1.0 to 5.5.0.

The `unthrown` peer range on `@amqp-contract/worker` is unchanged (`^5.0.0`),
which 5.5.0 already satisfies. 5.4.0 removes four matcher patterns (`P.any` /
`P.string` / `P.number` / `P.union`), none of which this codebase used; the full
type-test suite, unit suite and integration suite pass unmodified against it.
