---
title: Delivery guarantees - amqp-contract
description: Why delivery is at-least-once regardless of retry configuration, why a failed publish is ambiguous, and where idempotency has to live.
---

# Delivery guarantees

amqp-contract delivers **at-least-once**. A message the broker accepts will reach a consumer one or more times. No configuration makes that exactly once, because no such configuration exists to make.

This page is the whole statement in one place: when a message can arrive twice, what a failed publish does and does not tell you, and what those two facts leave you responsible for.

## At-least-once is not a retry setting

The retry mode defaults to `none`, and it is tempting to read that as having opted out of duplicates. It is not: `none` governs what happens to a `RetryableError` raised by your handler, and nothing else.

A message can arrive a second time with no retry configuration at all.

- **The worker crashes mid-handler.** Everything it had not acked returns to the queue. At the default prefetch of 10, that is up to ten messages for each consumer that was running.
- **The connection or channel drops before the ack is written.** Your handler may have finished the work. The broker never heard so, and redelivers.
- **`close()` reaches its drain timeout.** `worker.close()` waits for in-flight handlers, bounded by `drainTimeoutMs` (30 000 ms by default). On timeout the channel closes anyway, and whatever was still un-acked goes back.

Two more appear once you do configure retries:

- **`immediate-requeue`** returns the message to its own queue for another attempt.
- **`ttl-backoff`** republishes it through a wait queue.

The first three are properties of running a consumer against a broker; you did not choose them and you cannot switch them off. The last two are choices. All five produce the same thing at your handler: a message it has seen before.

## A failed publish is ambiguous

`publish()` waits for a publisher confirm, bounded by `publishTimeoutMs` (30 000 ms by default). When that deadline passes, the call settles as a failure.

That failure means the client stopped waiting. It does not mean the broker failed to receive the message.

So a publish error is not proof of non-delivery, and the natural response — send it again — can produce a duplicate. Nothing closes this gap from the publisher's side: an acknowledgement can always be lost after the work it acknowledges is done, and no protocol removes that.

Two honest responses:

- **Republish, and let the consumer cope.** The simplest, and correct as long as the consumer is idempotent.
- **Republish with the same identifier.** If the message carries an id that stays the same across your own retries, the consumer can recognise the repeat. An id regenerated per attempt is worse than none — every retry looks like new work.

## Idempotency lives in your handler

The library does not deduplicate for you, and it attaches no identity you could deduplicate on: it never sets AMQP's `messageId`. If you want one, you set it.

Publish options are amqplib's, so `messageId` is available:

```typescript
await client
  .publish("orderCreated", { orderId: "ORD-123" }, { messageId: idempotencyKey })
  .getOrThrow();
```

A handler reads it from the raw message:

```typescript
processOrder: ({ payload }, rawMessage) => {
  const id = rawMessage.properties.messageId;
  return OkAsync(undefined);
},
```

Or ignore AMQP's field and carry a business key in the payload — which has the advantage of surviving anything that rebuilds the message on the way through.

Whichever you choose, the property that matters is that the id is **stable across the sender's own retries** and unique per logical operation. An id that changes when the sender retries deduplicates nothing.

What you do with it is ordinary application work, and the right answer depends on what the handler touches:

- An **idempotency key** at the downstream provider. Most payment APIs accept one and do the deduplication for you, which is the strongest option when it is available.
- An **upsert** instead of an insert, so a repeat converges instead of conflicting.
- A **deduplication table** keyed on the id, written in the same transaction as the effect. Two separate transactions reintroduce the gap you are trying to close: a crash between them leaves the effect applied and the id unrecorded.

## Where next

- [The retry model](/explanation/the-retry-model) — what retrying does, and why validation failures never do.
- [Consume messages](/how-to/consume-messages) — prefetch, draining, and reaching the raw message.
- [Route dead letters](/how-to/route-dead-letters) — where a message goes when retrying is over.
