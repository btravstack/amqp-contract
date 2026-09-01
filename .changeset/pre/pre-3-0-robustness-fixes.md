---
"@amqp-contract/contract": patch
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Robustness fixes from the pre-3.0 full review.

- **Compression was broken end-to-end**: the channel's JSON mode serialized the compressed Buffer into `'{"type":"Buffer","data":[...]}'` on the wire, so every compressed publish was DLQ'd on arrival. The client now encodes content itself (JSON for plain values, byte-for-byte for Buffers) and JSON mode is off. Retry republishing now passes the original bytes through untouched for all content types.
- **Per-consumer `prefetch` no longer bleeds across consumers**: it now maps to amqp-connection-manager's native per-consumer prefetch, applied immediately before each `basic.consume` and re-applied per-consumer on reconnect (previously, any reconnect applied one consumer's QoS to all).
- **Classic-queue immediate-requeue retries republish to the failing queue via the default exchange** instead of the original exchange — a fanout/topic topology no longer delivers retry duplicates (and foreign retry headers) to sibling queues.
- **Closing a worker under load can no longer crash the process**: the defensive nack in the consume boundary is guarded (a nack racing channel teardown was an unhandled rejection), and `close()` drains in-flight handlers first.
- **Connection pool races fixed**: releases are idempotent per-lease (a double `close()` can no longer close a shared connection under another live client), the last release removes the pool entry _before_ closing (an acquire racing it gets a fresh connection instead of a dead one), and a rejecting close no longer poisons the pool key. `AmqpClient.close()` is memoized (idempotent).
- **Sync-throw escape hatches closed on the client**: a synchronously-throwing publish/call interceptor now resolves to a Defect instead of escaping `publish()`/`call()` as a raw throw, and all telemetry helpers swallow provider/span throws (a buggy TelemetryProvider degrades to "no telemetry" instead of poisoning the data path or converting a successful publish into a Defect). `create()` routes synchronous constructor throws to the defect channel.
- **`handlers: { name: undefined }` now fails fast** at `create()` / `declareHandlers` with a clear error instead of defecting on every delivery.
