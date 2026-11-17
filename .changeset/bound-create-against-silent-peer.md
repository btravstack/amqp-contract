---
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

`TypedAmqpClient.create` and `TypedAmqpWorker.create` now answer `Err(ConnectionError)` within `connectTimeoutMs` against a broker that accepts the TCP connection but never answers the AMQP handshake. They used to hang for as long as the peer kept the socket open.

- With `urls`, each dial gets amqplib's socket timeout (`connectionOptions.connectionOptions.timeout`) of half `connectTimeoutMs`, so the dial fails with `connect ETIMEDOUT` before the deadline and that is the error's cause. A timeout you set yourself is kept; `connectTimeoutMs: null` sets none. Because it is part of the pooled connection's options, clients (or workers) share a pooled connection only when their `connectTimeoutMs` also matches.
- Releasing the last lease on a pooled connection that is not connected no longer waits for its in-flight dial and the reconnect delay after it. `close()` on a client or worker that is still dialing, for example with `connectTimeoutMs: null`, now returns at once.
