---
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
"@amqp-contract/core": minor
---

Adopt [`unthrown`](https://github.com/btravstack/unthrown) 4, and make it a **peer dependency** (`^4.0.0`) of `@amqp-contract/core`, `@amqp-contract/client`, and `@amqp-contract/worker`.

`unthrown`'s types (`AsyncResult`, `Result`, `HandlerError`) are part of the public API and are nominal (keyed on a `unique symbol`), so two copies do not unify — as a peer dependency a single copy is shared between your app and amqp-contract.

**Action required:** ensure `unthrown@^4` is a direct dependency of your project. Package managers that auto-install peers (npm 7+, pnpm 8+) handle this; otherwise:

```bash
pnpm add unthrown@^4
```

unthrown 4 also changes two things you may rely on when consuming results:

- **`.unwrap()` is type-gated** — it compiles only on an infallible result (`E = never`). `(await client.publish(...)).unwrap()` and `(await TypedAmqpClient.create(...)).unwrap()` no longer compile. Use `.match()`, or clear the error channel first with `.recover((e) => { throw e })` before `.unwrap()`.
- **`TaggedError` reserves `message`** — only relevant if you subclass `TaggedError` yourself; move the message out of the payload into `override message = "…"`.

See the [Upgrading guide](https://btravstack.github.io/amqp-contract/guide/upgrading) for details. amqp-contract's own public API shape is unchanged.
