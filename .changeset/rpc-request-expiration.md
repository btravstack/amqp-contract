---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
---

`client.call()` publishes its request with `expiration` set to the call's
`timeoutMs`, so a request no worker consumed before the caller gave up is
dropped by the broker instead of being answered for nobody. A per-call
`publishOptions.expiration` still wins.

The RPC round trip is now recorded on its own histogram,
`amqp.client.rpc.duration` (new optional `TelemetryProvider.getRpcCallLatencyHistogram`),
instead of `amqp.client.publish.duration` — a slow handler no longer reads as a
slow broker. RPC calls no longer increment `amqp.client.messages.published`.
