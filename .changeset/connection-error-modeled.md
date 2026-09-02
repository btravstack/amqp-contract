---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

`TypedAmqpWorker.create` and `TypedAmqpClient.create` report an unreachable
broker as a typed `Err` — the new `ConnectionError` — where it used to arrive as
a `Defect` carrying a `TechnicalError`.

An unreachable broker is the **anticipated** failure of dialing one: a wrong
URL, a rotated credential, a cluster that has not come up yet. Every one of
those is an operator's business rather than a bug in the caller, which is the
definition of this library's `Err` channel. The defect channel keeps its
meaning — the failures nobody anticipated — and a connection LOST later, during
a publish or a delivery, is still one of those.

What it buys a start-up path is the triage it could not have before. Reaching
this failure used to mean recovering EVERY defect, which also swallowed genuine
bugs raised while the graph was being built (`@btravstack/amqp-worker` carried
exactly that blanket `recoverDefect`, and can now drop it):

```ts
const started = await TypedAmqpWorker.create({ contract, handlers, urls }).match({
  ok: (worker) => worker,
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/ConnectionError"), (error) => {
      logger.error({ error }, "broker unreachable");
      process.exitCode = 1;
      return undefined;
    }),
  defect: (cause) => {
    logger.error({ cause }, "bug while starting up");
    process.exitCode = 70;
    return undefined;
  },
});
```

**The compiler catches every call site**: `create(...)` no longer has an empty
error channel, so `.get()` stops compiling on it — `.getOrThrow()` is the
mechanical migration, and a `match` is the point. `close()` is unchanged and
keeps `.get()`.

`ConnectionError` and `isConnectionError` are exported from
`@amqp-contract/core` and re-exported by both the client and the worker.

Closes #645.
