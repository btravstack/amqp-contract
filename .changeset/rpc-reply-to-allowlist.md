---
"@amqp-contract/worker": major
---

RPC servers only reply to allowed addresses, and never retry a request.

- **`replyTo` allowlist.** By default the worker publishes a reply only to
  RabbitMQ direct reply-to (`amq.rabbitmq.reply-to`, delivered as
  `amq.rabbitmq.reply-to.<token>`) — what `client.call()` uses. A request with
  any other `replyTo` is dead-lettered with the reason logged instead of being
  answered, so a forged request can no longer make the worker publish into an
  arbitrary queue through the default exchange. Allow other reply queues with
  `TypedAmqpWorker.create({ rpc: { allowReplyTo: (replyTo) => boolean } })`.
- **No retry for RPC requests.** A `RetryableError` from an RPC handler now
  dead-letters the request even when its queue has a `retry` config: the
  caller waits on a `timeoutMs` shorter than most backoffs, so a retry re-ran
  the handler for nobody.
