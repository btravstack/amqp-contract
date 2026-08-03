---
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Move the `@unthrown/standard-schema` runtime dependency from 5.0.0 to 5.1.0.

The `unthrown` peer range is unchanged (`^5.0.0`), which 5.1.0 already satisfies,
so consumers pinned to any 5.x need no action. The bump is a minor on a
first-party package with no API removal: the full type-test suite, unit suite and
integration suite pass unmodified against it.
