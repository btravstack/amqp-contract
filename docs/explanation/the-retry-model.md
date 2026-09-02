---
title: The retry model - amqp-contract
description: Why retry is split between the queue and the handler, what each half decides, and why validation failures bypass retries entirely.
---

# The retry model

Retry in amqp-contract is split across two places, which surprises people. The queue declares _how_ retries happen; the handler decides _whether_ a particular failure should be retried at all. This page explains why the responsibility is divided that way and what follows from it.

For the configuration itself, see [retry failed messages](/how-to/retry-failed-messages).

## Two questions, two owners

When a handler fails, two independent questions need answering.

**"Could this ever succeed?"** Only the handler can answer. It is the only code that knows a 402 from the payment provider is final while a 503 is not. This is a property of the _failure_.

**"How aggressively should we retry, and for how long?"** The handler is the wrong place for this. It is an operational decision — bound up with queue depth, downstream capacity, and how long the business will tolerate a stuck message — and it is properly uniform across every message on a queue. This is a property of the _workload_.

Conflating them is the usual design, and it goes wrong in a familiar way: retry policy gets copy-pasted into handlers, drifts between them, and changing the backoff for a queue means editing every handler that writes to it.

So they are separated. The handler classifies:

```typescript
processOrder: (_, { payload }) =>
  fromPromise(chargeCard(payload), (cause) =>
    cause instanceof CardDeclined
      ? new NonRetryableError("card declined", cause)
      : new RetryableError("gateway unavailable", cause),
  ).map(() => undefined),
```

And the queue sets policy:

```typescript
defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "ttl-backoff", maxRetries: 5, initialDelayMs: 1_000 },
});
```

Change the backoff and no handler is touched. Change a classification and no queue is touched.

## What happens to a message

Three outcomes, and only one of them consults the retry mode:

| Handler returns          | Outcome                                                |
| ------------------------ | ------------------------------------------------------ |
| `Ok`                     | Acknowledged. Gone.                                    |
| `Err(NonRetryableError)` | Dead-lettered immediately. The retry mode is bypassed. |
| `Err(RetryableError)`    | The queue's retry mode decides.                        |

That `NonRetryableError` skips the retry mode entirely is the important asymmetry. It is a statement about the _failure_, and no amount of policy should override it — retrying a permanently declined card five times with exponential backoff is pure latency for a guaranteed dead-letter.

The default mode is `none`, which means a `RetryableError` behaves like a `NonRetryableError` until you opt into a retry policy. This is intentional. Retrying is a choice with real consequences — more duplicate deliveries, and queue growth — so it is not on by default. It does not follow that `none` means no duplicates: a crash, a dropped connection, or a drain timeout redelivers whatever was un-acked, whatever the retry mode. [Delivery guarantees](/explanation/delivery-guarantees) has the full list.

## Why validation failures never retry

A message that fails schema validation goes straight to the dead-letter queue, regardless of the queue's retry mode and regardless of what any handler would say. The handler never runs.

The reasoning is that validation is _deterministic_. The same bytes checked against the same schema produce the same answer every time. Retrying spends the entire retry budget — plus, under `ttl-backoff`, a good deal of wall-clock time — to arrive at a conclusion already known on the first attempt.

Worse, under `immediate-requeue` a malformed message that requeued forever would block the queue behind it. This is the classic poison-message problem, and treating deterministic failures as non-retryable by construction is what prevents it.

The same argument covers a message the worker cannot decompress or JSON-parse. Those are properties of the bytes, not of the world, and the bytes will not change.

## Why the modes differ

The three modes exist because "transient" covers failures with very different time constants.

**`immediate-requeue`** puts the message back at once. This suits failures measured in milliseconds — brief lock contention, a connection that flapped, a leader election that has already resolved. Its virtue is simplicity; its risk is that a failure lasting longer than a few hundred milliseconds burns the whole retry budget in a tight loop while achieving nothing.

**`ttl-backoff`** parks the message in a per-delay wait queue with a TTL, then routes it back, with the delay growing exponentially. Each distinct delay gets its own wait queue because RabbitMQ only expires messages at the head of a queue — a shared queue would let a long retry block a short one. This suits failures measured in seconds or minutes — a degraded downstream, a rate limit window, a database failing over. The growth gives the dependency time to actually recover, which an immediate requeue never does.

`ttl-backoff` also offers jitter, and the reason is worth stating: without it, a hundred messages that failed together retry together, hit the recovering service simultaneously, and knock it over again. Jitter spreads them.

**`none`** does not retry. It is right when retries happen somewhere else — an outbox pattern that replays from the source, or an upstream scheduler that will reissue the work. Two independent retry loops stacked on each other produce a multiplicative number of attempts, which is rarely what anyone intended.

## Counting attempts

How attempts are counted depends on the queue type, and it leaks into what you can observe.

Quorum queues have a broker-native `x-delivery-count` that RabbitMQ increments. Classic queues do not, so under `immediate-requeue` the worker maintains its own `x-retry-count` by republishing the message.

That difference has a consequence for dead-letter tooling. A path that _republishes_ can stamp diagnostic headers — the last error, the first failure timestamp. A path that simply nacks cannot, because the message is never rewritten. So messages arriving in the dead-letter queue via a direct nack (a `NonRetryableError`, a validation failure, an exhausted quorum queue under `immediate-requeue`) look exactly as the broker delivered them, with no failure context attached.

This is not an oversight but it is a real limitation, and it is the thing most likely to surprise you when you go looking in the dead-letter queue for a reason. [Retry failed messages](/how-to/retry-failed-messages#inspect-retry-state) has the full matrix and the workaround.

## Retries are not exactly-once

Nothing here makes a retried operation safe to repeat. If a handler charges a card and then fails while writing the receipt, retrying charges the card again.

The retry model guarantees delivery attempts, not idempotency. Making the _work_ idempotent remains yours, and switching retries on raises how often it matters rather than introducing the problem — delivery is at-least-once either way. [Delivery guarantees](/explanation/delivery-guarantees) covers where duplicates come from, why a failed publish is ambiguous, and the identifier you need before a deduplication table is worth anything.

A related trap: a queue with no dead-letter exchange configured discards messages on `nack(requeue=false)`. The worker warns, but the body is gone. If you care about poison messages, configure `deadLetter` — and bind a queue to the exchange you name. A DLX with nothing bound loses the message just as completely and without the warning, which is why `defineContract` rejects one.

## Where next

- [Delivery guarantees](/explanation/delivery-guarantees) — why at-least-once holds regardless of retry configuration, and where idempotency has to live.
- [Retry failed messages](/how-to/retry-failed-messages) — configuring each mode, and classifying errors in handlers.
- [Route dead letters](/how-to/route-dead-letters) — where messages go when retrying is over.
- [Error model](/reference/error-model) — `RetryableError`, `NonRetryableError` and the rest.
