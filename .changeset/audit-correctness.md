---
"@amqp-contract/asyncapi": minor
"@amqp-contract/client": minor
"@amqp-contract/contract": minor
"@amqp-contract/core": minor
"@amqp-contract/testing": minor
"@amqp-contract/worker": minor
---

Correctness fixes from a project audit. All six packages bump together
(`fixed` group) — most changes land in `core` and `worker`, but the version
move covers the whole release.

**Worker — retry message safety (`packages/worker/src/retry.ts`)**

- `publishForRetry` now publishes the retry copy _first_ and only acks the
  original delivery if the publish is confirmed. Previously the original was
  ack'd before the publish was attempted: a publish failure (channel buffer
  full, channel error, etc.) would lose the message — the broker had already
  discarded the delivery and no retry copy was ever sent. On a publish
  failure the original is now left un-ack'd so amqp-connection-manager (or
  the broker on channel close) can redeliver it.

**Worker — jitter range (`packages/worker/src/retry.ts`)**

- The TTL-backoff retry jitter formula is now `delay * (0.5 + Math.random())`
  giving a symmetric `[0.5x, 1.5x]` range with mean 1.0x. The previous
  formula `0.5 + Math.random() * 0.5` produced `[0.5x, 1.0x]` (mean 0.75x)
  and never overshot — a one-sided bias, not real jitter. The clamp against
  `maxDelayMs` now runs _after_ jitter so the upper jitter bound cannot push
  the calculated delay past the configured maximum. **User-visible change**:
  the average retry delay under jitter increases by ~33% (0.75x → 1.0x of
  the configured base) and individual delays may now exceed the
  pre-clamp base by up to 50%.

**Worker — double-ack guard (`packages/worker/src/worker.ts`)**

- The defensive `nack(requeue=false)` in the consume callback's catch-all is
  now skipped if the message has already been ack'd or nack'd by the
  dispatch path. Previously a throw from anywhere _after_ the success-path
  `ack` (most notably the telemetry tail) would land in the catch-all and
  nack the same delivery tag — RabbitMQ then closed the channel with
  `406 PRECONDITION_FAILED`. Telemetry calls in the dispatch tail are also
  now wrapped in a try/catch so an instrumentation bug cannot crash the
  consume loop.

**Core — `PublishOptions.timeout` removed (`packages/core/src/amqp-client.ts`)**

- **Breaking-shaped change** (shipped as minor under 0.x): the `timeout`
  field on `PublishOptions` has been removed. It was a stale type-level
  declaration that suggested a publish-level timeout this library does not
  meaningfully provide. Code passing `timeout` will now fail to typecheck;
  remove the option (or move to `amqp-connection-manager`'s channel-level
  `publishTimeout` if you actually need it).

**Core — `ConsumerOptions.prefetch` now wired up (`packages/core/src/amqp-client.ts`)**

- `AmqpClient.consume(...)` now applies `options.prefetch` via
  `channel.prefetch(count, false)` registered on the channel wrapper _before_
  the consume call (so the value is in effect when the consumer starts and
  is reapplied on channel reconnect). The value is also stripped from the
  options handed to `channelWrapper.consume(...)` since `prefetch` is not a
  valid `amqplib` `Options.Consume` field. The `prefetch` option advertised
  on the worker's per-handler tuple form is now actually applied.

**Core — connection key URL ordering (`packages/core/src/connection-manager.ts`)**

- Added an inline comment confirming that URL list order is intentionally
  part of the pooled-connection key. `['a','b']` and `['b','a']` continue to
  get different pooled connections because the URL list is a failover list
  with the first entry as the preferred broker — sorting would silently
  merge those into one connection and pin one caller's preference onto the
  other. No behaviour change.
