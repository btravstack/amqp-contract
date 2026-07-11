---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
---

Upgrade `unthrown` to `4.1.0` and require it as the peer version (`^4.1.0`). All internal usage of the operators unthrown 4.1 deprecates has been migrated to the renamed forms — `.orElse` → `.flatMapErr`, `.recover` → `.recoverErr`, `.unwrap` → `.get` (or `.getOrThrow()` on fallible results — the throw-on-failure escape hatch), `.unwrapErr` → `.getErr`, `.unwrapOr` → `.getOr`, `.unwrapOrElse` → `.getOrElse` — and all documentation and examples now use the new names. No amqp-contract API changes; the deprecated unthrown aliases keep working in your own code until the next unthrown major.
