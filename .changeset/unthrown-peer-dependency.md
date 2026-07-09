---
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
"@amqp-contract/core": minor
---

Move `unthrown` from a regular dependency to a **peer dependency** (`^3.0.0`) in `@amqp-contract/core`, `@amqp-contract/client`, and `@amqp-contract/worker`.

`unthrown`'s types are part of the public API (`AsyncResult`, `Result`, `HandlerError`), and unthrown types are nominal — they key on a `unique symbol`, so two copies at different versions (or even the same version, in a non-deduped install) do not unify. As a peer dependency, a single copy is shared between your app and amqp-contract, preventing "two different types with this name exist but are unrelated" errors.

**Action required:** ensure `unthrown` is a direct dependency of your project (the docs already recommend this). Package managers that auto-install peer dependencies (npm 7+, pnpm 8+) handle this for you; stricter setups should add `unthrown` explicitly:

```bash
pnpm add unthrown
```
