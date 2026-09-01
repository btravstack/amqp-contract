---
"@amqp-contract/contract": major
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
"@amqp-contract/testing": major
"@amqp-contract/asyncapi": major
---

Uniform `QueueDefinition` + per-delay-tier TTL-backoff wait queues: the `QueueEntry`/`extractQueue` split is gone and the retry schedule no longer degrades to the longest in-flight delay.

- **`defineQueue` always returns a single uniform `QueueDefinition`** — with TTL-backoff retry it no longer returns a branded wrapper carrying wait-queue/exchange infrastructure. Access `queue.name` / `queue.type` directly. Deleted exports: `extractQueue`, `isQueueWithTtlBackoffInfrastructure`, and the `QueueEntry`, `QueueWithTtlBackoffInfrastructure`, `QueueEntryWithDeadLetterExchange` (renamed to `QueueDefinitionWithDeadLetterExchange`), `TtlBackoffRetryInfrastructure` types, plus the `__brand` machinery on queues.
- **TTL-backoff topology is derived, never stored.** The new pure helpers `deriveTtlBackoffInfrastructure(queue)`, `ttlBackoffBaseDelay(retry, retryCount)`, and `ttlBackoffWaitQueueName(queueName, delayMs)` (with the `TtlBackoffInfrastructure` / `TtlBackoffWaitQueueDefinition` types) compute everything from `queue.retry`; `setupAmqpTopology` declares the wait queues at channel-setup time and the worker's retry pipeline publishes to them. `ContractOutput` now matches the runtime `defineContract` object exactly — no more runtime-injected `wait-exchange` / wait-queue entries that failed to typecheck.
- **One wait queue per distinct backoff delay** (`{queue}-wait-{delayMs}ms`) fixes head-of-line blocking: RabbitMQ only dead-letters expired messages at the head of a queue, so the old single shared wait queue let a parked 60s retry block a later 1s retry — the configured schedule silently degraded to the longest in-flight delay. Each tier queue is declared with a queue-level `x-message-ttl` backstop (the jitter ceiling, `ceil(base * 1.5)`) and dead-letters back to the main queue via the default exchange; the per-message `expiration` carries the jittered delay. Within a tier, head-of-line skew is bounded by the jitter spread and is zero with `jitter: false`.
- **The wait/retry headers exchanges are gone.** The retry copy is published straight to the tier queue via the default exchange, so the `x-wait-queue` / `x-retry-queue` routing headers are no longer stamped. Retried deliveries arrive with `fields.routingKey` set to the queue name; the original routing key is preserved in the new `x-original-routing-key` header (also stamped by classic-queue immediate-requeue republishes). The `x-retry-count` / `x-last-error` / `x-first-failure-timestamp` accounting is unchanged.
- **`TtlBackoffRetryOptions` lost `waitQueueName` / `waitExchangeName` / `retryExchangeName`** — tier wait-queue names are derived and no longer configurable.
- **Migration**: broker-side wait-queue names change. The old `{queue}-wait` queues and the `wait-exchange` / `retry-exchange` headers exchanges become unused after upgrading — drain them (any parked retries will still dead-letter back to their main queue when their TTL expires, as long as you leave the old topology in place until empty), then delete them.
