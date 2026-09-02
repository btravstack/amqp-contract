---
"@amqp-contract/worker": patch
---

A middleware's `next({ payload: undefined })` is a substitution now, and the
payload schema refuses it — where it used to be indistinguishable from
`next({})` and silently dropped, leaving the handler running on the original
message.

Both the dispatcher and `composeMiddleware` used the VALUE as the sentinel
(`opts?.payload === undefined` meaning "nothing substituted"), so the one
payload a middleware could not substitute was `undefined` — and it failed
quietly, in the direction that keeps processing rather than the direction that
stops. Presence is the sentinel now: `Object.hasOwn(opts, "payload")` in the
dispatcher, and a boxed `{ payload }` threaded through the chain.

**Behaviour change, in the one case that was already broken.** A middleware
calling `next({ payload: undefined })` today gets a no-op; after this it
substitutes `undefined`, the consumer's payload schema rejects it, and the
message is dead-lettered as a `NonRetryableError` — the same route any other
invalid substitution takes. `next({})` and `next()` are unaffected: they
substitute nothing, as before.

Closes #672.
