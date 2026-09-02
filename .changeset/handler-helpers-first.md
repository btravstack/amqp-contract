---
"@amqp-contract/worker": major
---

Handlers take **one record** — `{ input, context, errors, raw, retryable,
nonRetryable }`, where `input` is the validated `{ payload, headers }` — with
that message repeated as a second positional parameter. It was
`({ payload, headers }, rawMessage, { context, errors })`: the raw amqplib
delivery moved onto the record as `raw`, and the message is reachable from
either place.

oRPC is the reference shape for this family, being the most widely used of the
three transports a `@btravstack/*` application composes: a developer arriving
here has more likely seen `({ errors, input })` than either of the others —
down to the word, since oRPC's `ProcedureHandlerOptions` carries `input` and
its handler still takes it positionally, which is exactly the pair of spellings
offered here. The mint and compose calls already agreed across the three; the leaf a
developer types by hand did not, and it is the one they relearn per transport.
`@temporal-contract`'s activity leaf moves with it.

It is also what makes the AMQP triage site the same SHAPE as the other two, and
that half is not cosmetic: `retryable` and `nonRetryable` ride the helpers
record beside `errors`, so a handler that wants "infrastructure comes back"
reaches for the constructor it was handed instead of importing `RetryableError`
and constructing it by hand.

```ts
processOrder: ({ retryable, input: { payload } }) =>
  fromPromise(save(payload), (cause) => retryable("database unavailable", cause)),
```

They sit BESIDE `errors` rather than inside it: `errors` is the
contract-declared error map — `errors.ORDER_NOT_FOUND({ orderId })` — which is
what it means on the other two transports, and folding the framework's own two
into that namespace would both break the mirror and collide with a declared code
called `retryable`.

```diff
- processOrder: ({ payload }) => save(payload),
+ processOrder: ({ input: { payload } }) => save(payload),
- handleFailed: ({ payload }, rawMessage) => log(rawMessage.properties.headers),
+ handleFailed: ({ raw, input: { payload } }) => log(raw.properties.headers),
- getOrder: ({ payload }, _raw, { errors }) => lookup(payload, errors),
+ getOrder: ({ errors, input: { payload } }) => lookup(payload, errors),
```

The message is on the helpers record as well as in the second parameter, which
is oRPC's own shape — `ProcedureHandlerOptions` carries `input` and the handler
still takes it positionally — so both spellings are the same call:

```ts
getOrder: ({ errors, message }) => lookup(message.payload, errors),
getOrder: ({ errors, input: { payload } }) => lookup(payload, errors),
```

A handler that reads its payload fails to compile until it is swapped, since
the first parameter is the helpers record now; one that ignores its message
keeps compiling with a parameter whose name lies — grep the handlers object for
a leaf whose first parameter is neither `_` nor a helpers destructuring. A
handler that wants only its message is `({ input: { payload } }) => ...`, with no placeholder to spell.

Closes #670.
