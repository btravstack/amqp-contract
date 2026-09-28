---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
---

`client.call()` publishes its request with `expiration` set to the call's
`timeoutMs`, so a request no worker consumed before the caller gave up is
expired by the broker instead of being answered for nobody. It is
dead-lettered to the RPC queue's dead-letter exchange with an `x-death` reason
of `expired` (discarded only on an `onPoison: "drop"` queue); don't replay
those dead letters — the caller is gone. A per-call
`publishOptions.expiration` still wins. A fractional `timeoutMs` is rounded up,
since AMQP only accepts an integer `expiration`.

`AmqpClient.publish` / `sendToQueue` (and so every client and worker publish)
refuse an `expiration` that is not a non-negative integer (`1500.5`, `-1`,
`"soon"`) as a defect with a `TechnicalError` cause, before it reaches the
broker — which would close the channel with 406, leaving the client unable to
publish until the connection is re-established.

The RPC round trip is now recorded on its own histogram,
`amqp.client.rpc.duration` (new optional `TelemetryProvider.getRpcCallLatencyHistogram`),
instead of `amqp.client.publish.duration` — a slow handler no longer reads as a
slow broker. RPC calls no longer increment `amqp.client.messages.published`.
