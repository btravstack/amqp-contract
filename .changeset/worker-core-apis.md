---
"@amqp-contract/core": minor
"@amqp-contract/worker": major
---

The worker adopts the core client's options and lifecycle.

- **Role-scoped topology.** The worker declares only what its consumers need —
  the queues it consumes (with their retry wait queues, the bindings into them
  and the exchanges those bind to) plus their dead-letter exchanges and DLQs —
  instead of the whole contract (core's new `workerTopology`, from
  `@amqp-contract/core/internal`). A `topology` option takes the same
  `TopologyMode` as the client: `"assert"` (default), `"passive"` or `"none"`.
- **`connection`** — pass a caller-owned `AmqpConnectionManager` instead of
  `urls` (`ConnectionSource`, like the client). It is borrowed, never closed.
- **`worker.isConnected()`** for readiness probes.
- **`maxMessageBytes`** replaces `maxDecompressedBytes` (it caps plain bodies
  too); the old name still works and is deprecated.
- **Trace context.** Handlers (and `createContext`, middleware) run with the
  consume span active, so their own spans and any message they publish nest
  under it.
- `RetryableError` / `NonRetryableError` expose a static `tag`
  (`P.tag(RetryableError.tag)`) and a stack headed by their name and message.
- The worker re-exports `PublishError`, `PublishFailureReason`,
  `TopologyMode` and `ConnectionSource`.
