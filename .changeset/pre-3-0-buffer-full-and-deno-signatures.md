---
"@amqp-contract/contract": major
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
"@amqp-contract/testing": major
"@amqp-contract/asyncapi": major
---

Pre-3.0 audit: one decision point for a full write buffer, and Deno-style exported signatures.

- **`AmqpClient.publish` / `sendToQueue` return `AsyncResult<void, never>`** (was `AsyncResult<boolean, never>`). The channel wrapper's boolean `false` (write buffer full) used to be re-triaged four different ways downstream; it is now absorbed inside core and surfaces as a Defect with a `TechnicalError` cause ("channel write buffer full"), like every other publish-side infrastructure failure. The worker's RPC reply publish is the one site that still needs a modeled error: it recovers the defect into a `NonRetryableError`, so a failed reply publish keeps routing the request to the DLQ.
- **Exported signatures follow the [Deno style rule](https://docs.deno.com/runtime/contributing/style_guide/)** — max two positional arguments, trailing options object, no positional booleans:
  - `client.ack(msg, allUpTo?, options?)` → `client.ack(msg, { allUpTo?, deliveryEpoch? })`
  - `client.nack(msg, allUpTo?, requeue?, options?)` → `client.nack(msg, { allUpTo?, requeue?, deliveryEpoch? })` (requeue still defaults to true)
  - `client.publish(exchange, routingKey, content, options?)` → `client.publish({ exchange, routingKey }, content, options?)`
  - `@amqp-contract/testing`'s `publishMessage(exchange, routingKey, content, options?)` fixture → `publishMessage({ exchange, routingKey }, content, options?)`

  The typed client's `client.publish(name, message, options?)` is unaffected.

- **`defineExchangeBinding` requires a non-empty routing key for direct/topic source exchanges.** It was the last builder still defaulting a missing key to `""` (silently unroutable); it now throws the same actionable define-time error as `definePublisher` and `defineQueueBinding`. Fanout/headers sources remain exempt.
