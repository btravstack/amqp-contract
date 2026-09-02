---
"@amqp-contract/worker": major
---

Handlers take the **helpers record first and the validated message second** —
`({ context, errors, raw }, { payload, headers })` where it was
`({ payload, headers }, rawMessage, { context, errors })`. The raw amqplib
delivery moved from a third parameter into `raw` on the helpers.

oRPC is the reference shape for this family, being the most widely used of the
three transports a `@btravstack/*` application composes: a developer arriving
here has more likely seen `({ errors, context }, input)` than either of the
others. The mint and compose calls already agreed across the three; the leaf a
developer types by hand did not, and it is the one they relearn per transport.
`@temporal-contract`'s activity leaf moves with it.

It is also what makes the AMQP triage site the same SHAPE as the other two, and
that half is not cosmetic: `retryable` and `nonRetryable` ride the helpers
record beside `errors`, so a handler that wants "infrastructure comes back"
reaches for the constructor it was handed instead of importing `RetryableError`
and constructing it by hand.

```ts
processOrder: ({ retryable }, { payload }) =>
  fromPromise(save(payload), (cause) => retryable("database unavailable", cause)),
```

They sit BESIDE `errors` rather than inside it: `errors` is the
contract-declared error map — `errors.ORDER_NOT_FOUND({ orderId })` — which is
what it means on the other two transports, and folding the framework's own two
into that namespace would both break the mirror and collide with a declared code
called `retryable`.

```diff
- processOrder: ({ payload }) => save(payload),
+ processOrder: (_, { payload }) => save(payload),
- handleFailed: ({ payload }, rawMessage) => log(rawMessage.properties.headers),
+ handleFailed: ({ raw }, { payload }) => log(raw.properties.headers),
- getOrder: ({ payload }, _raw, { errors }) => lookup(payload, errors),
+ getOrder: ({ errors }, { payload }) => lookup(payload, errors),
```

A handler that reads its payload fails to compile until it is swapped, since
the first parameter is the helpers record now; one that ignores its message
keeps compiling with a parameter whose name lies — grep the handlers object for
a leaf whose first parameter is neither `_` nor a helpers destructuring. A
handler that needs neither still names the position: `(_, { payload }) => ...`.

Closes #670.
