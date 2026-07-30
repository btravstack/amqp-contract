---
title: Retry failed messages - amqp-contract
description: Configure queue-level retry modes, classify handler errors as retryable or not, and inspect retry state on dead-lettered messages.
---

# Retry failed messages

Retry is configured in two places: the queue declares the policy, the handler classifies the failure. [The retry model](/explanation/the-retry-model) explains why. This page is the configuration.

## Decide whether a failure should retry

In the handler, map the underlying cause to one of two errors:

```typescript
import { NonRetryableError, RetryableError } from "@amqp-contract/worker";
import { fromPromise } from "unthrown";

processOrder: ({ payload }) =>
  fromPromise(chargeCard(payload), (cause) => {
    // A declined card will be declined again. Don't spend retries on it.
    if (cause instanceof CardDeclined) {
      return new NonRetryableError("card declined", cause);
    }
    // A 503 might not happen next time.
    return new RetryableError("gateway unavailable", cause);
  }).map(() => undefined),
```

The test: _if the same input hit the same code tomorrow, would it fail the same way?_ If yes, it is non-retryable.

When a whole operation falls on one side, use the prebuilt mappers:

```typescript
import { qualifyNonRetryable, qualifyRetryable } from "@amqp-contract/worker";

fromPromise(callApi(payload), qualifyRetryable("API unavailable"));
fromPromise(validateExternally(payload), qualifyNonRetryable("rejected by provider"));
```

`retryable(message, cause)` and `nonRetryable(message, cause)` are shorthand for the constructors.

## Retry immediately

For failures that resolve in milliseconds:

```typescript
const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 },
});
```

After `maxRetries`, the message is dead-lettered.

On quorum queues the count comes from RabbitMQ's native `x-delivery-count`; on classic queues the worker maintains `x-retry-count` by republishing.

::: warning Quorum delivery limit
Quorum queues enforce their own delivery limit — 20 by default in RabbitMQ 4 — independently of `maxRetries`. If you set `maxRetries` above it, the broker dead-letters the message first. Raise it explicitly when needed:

```typescript
defineQueue("order-processing", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 },
  arguments: { "x-delivery-limit": 20 },
});
```

:::

## Retry with exponential backoff

For failures that take seconds or minutes to clear:

```typescript
const orderQueue = defineQueue("order-processing", {
  deadLetter: { exchange: dlx },
  retry: {
    mode: "ttl-backoff",
    maxRetries: 5,
    initialDelayMs: 1_000,
    maxDelayMs: 60_000,
    backoffMultiplier: 2,
    jitter: true,
  },
});
```

The delay is `initialDelayMs * backoffMultiplier ^ attempt`, capped at `maxDelayMs`. With these values: 1s, 2s, 4s, 8s, 16s.

Keep `jitter: true` unless you have a reason not to. Without it, messages that failed together retry together and hit the recovering dependency simultaneously.

The supporting topology is derived for you at channel-setup time — one wait queue per **distinct** delay in the schedule (`order-processing-wait-1000ms`, `order-processing-wait-2000ms`, …), each dead-lettering back to the main queue when its TTL expires. You do not wire or declare any of it, and none of it appears in the contract.

Per-delay wait queues are what make the schedule hold: RabbitMQ only expires messages at the head of a queue, so a single shared wait queue would let a parked 16s retry block a later 1s retry. With one queue per delay, a message's wait is bounded by its own tier — within a tier the skew is at most the jitter spread (jitter draws the actual delay from `[0.5x, 1.5x]` of the base, and the tier's queue-level TTL backstop is the jitter ceiling), and with `jitter: false` it is zero.

One consequence of the retry hop: retried deliveries re-enter the main queue via the default exchange, so `rawMessage.fields.routingKey` is the queue name from the second attempt on. The routing key of the first delivery is preserved in the `x-original-routing-key` header.

## Turn retries off

`none` is the default, so this is only worth writing down explicitly:

```typescript
retry: { mode: "none" },
```

A `RetryableError` then behaves like a `NonRetryableError`. Use it when retries happen elsewhere — an outbox that replays from the source, or an upstream scheduler — because stacking two retry loops multiplies attempts.

## Pick a mode

| Situation                             | Mode                | `maxRetries` |
| ------------------------------------- | ------------------- | ------------ |
| Network blips, lock contention        | `immediate-requeue` | 3–5          |
| Rate-limited downstream               | `ttl-backoff`       | 5–10         |
| Degraded service, recovers in seconds | `ttl-backoff`       | 5            |
| Idempotent and cheap to retry         | `immediate-requeue` | 5–10         |
| Retries owned upstream (outbox)       | `none`              | —            |

## Use different policies for different work

Retry is a property of the queue, not the handler. To run two policies, use two queues:

```typescript
const fastQueue = defineQueue("orders-fast", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 },
});

const slowQueue = defineQueue("orders-slow", {
  deadLetter: { exchange: dlx },
  retry: { mode: "ttl-backoff", maxRetries: 8, initialDelayMs: 5_000 },
});
```

## Inspect retry state

Diagnostic headers are stamped **only on paths that republish the message** — classic queues under `immediate-requeue`, and any queue under `ttl-backoff`.

| Header                      | Meaning                              | Set on                     |
| --------------------------- | ------------------------------------ | -------------------------- |
| `x-delivery-count`          | Broker-native attempt count          | Quorum queues, by RabbitMQ |
| `x-retry-count`             | Worker-managed attempt count         | Republish paths only       |
| `x-last-error`              | Message from the most recent failure | Republish paths only       |
| `x-first-failure-timestamp` | Epoch ms of the first failure        | Republish paths only       |
| `x-original-routing-key`    | Routing key of the first delivery    | Republish paths only       |

Direct-nack paths — a `NonRetryableError`, a validation failure, a quorum queue exhausting `immediate-requeue` — do **not** republish, so the dead-lettered message arrives byte-identical to what the broker delivered, with no failure context. Error details are in the worker's logs instead.

If you need context on the message itself, use `ttl-backoff` (which always republishes), or set `maxRetries: 1` so a single republish stamps the headers before the message is dead-lettered.

## Avoid the common traps

**No dead-letter exchange.** `nack(requeue=false)` with no `deadLetter` configured discards the message. The worker warns; the body is gone. Configure `deadLetter` if poison messages matter.

**Retrying non-idempotent work.** Retries are delivery attempts, not exactly-once semantics. A handler that charges a card then fails will charge again. Use an idempotency key, an upsert, or a dedupe table keyed on message ID.

**Tuning `maxRetries` by intuition.** The number that matters is how long the dependency actually takes to recover. That lives in your dead-letter queue and your metrics, not in your head.

**Throwing instead of returning.** A thrown error reaches the worker's safety net and is dead-lettered, having lost the classification that would have let it retry.

## Where next

- [The retry model](/explanation/the-retry-model) — why it is split this way.
- [Route dead letters](/how-to/route-dead-letters) — consuming what retrying gave up on.
- [Error model](/reference/error-model) — `RetryableError` and `NonRetryableError` in full.
