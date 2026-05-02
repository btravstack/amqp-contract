---
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Fix connection leak when `TypedAmqpClient.create()` or `TypedAmqpWorker.create()` fails.

Previously, if `waitForConnect` rejected (client/worker) or `consumeAll` errored after some consumers had registered (worker), the underlying connection's reference count remained incremented and any registered consumers stayed running. The caller received a `Result.Error` with no handle to clean up.

Both factories now invoke `close()` before propagating the error, releasing the connection ref-count via the singleton and cancelling any partially-registered consumers.
