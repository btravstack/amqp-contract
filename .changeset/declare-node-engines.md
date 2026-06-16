---
"@amqp-contract/asyncapi": minor
---

Declare `engines.node: ">=22.19"` on all publishable packages. `undici@8.4.1` (transitively required via `@amqp-contract/testing` → `testcontainers`) requires Node `>=22.19.0`; consumers now see this constraint at install time instead of silently inheriting it.
