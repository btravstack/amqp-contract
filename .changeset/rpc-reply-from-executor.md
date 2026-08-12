---
"@amqp-contract/client": minor
---

Build the RPC reply future with unthrown's `fromExecutor` instead of a raw
`new Promise` lifted through `fromSafePromise(...).flatMap((result) => result)`.

`fromExecutor`'s settler takes a `Result` directly, so the nested-`Result`
collapse — and the comment explaining why it was safe — are gone. Behavior is
unchanged: the executor runs eagerly and synchronously, exactly like the
`new Promise` it replaces, so the settler is still available to the timeout
timer and the pending-call entry set up on the next lines.

**The `unthrown` peer range on `@amqp-contract/client` rises from `^5.0.0` to
`^5.3.0`**, the release that introduced `fromExecutor`. Consumers already on
5.3.0 or later need no action; those on 5.0–5.2 must bump `unthrown` (a minor,
within the same major). `@amqp-contract/core` and `@amqp-contract/worker` keep
`^5.0.0`.
