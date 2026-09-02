---
title: Delivery guarantees - amqp-contract
description: Why delivery is at-least-once regardless of retry configuration, why a failed publish is ambiguous, and where idempotency has to live.
---

# Delivery guarantees

amqp-contract delivers **at-least-once**: a message the broker accepts will reach a consumer one or more times, provided nothing on the queue evicts it first. That qualifier is not a formality — `x-message-ttl`, `x-expires`, and `x-max-length[-bytes]` are documented, supported queue arguments ([topology options](/reference/topology-options#arguments)), and each gives the broker a reason to discard a message, or the whole queue, before a consumer ever sees it. `x-expires` deletes the queue — and everything still in it — once the queue has gone unused for that long; nothing dead-letters the loss. `x-message-ttl` and `x-max-length[-bytes]` evict individual messages sooner, and land them in the DLQ if the queue has a dead-letter exchange — but only reach a consumer if something consumes that queue too, and a consumed queue can skip a dead-letter exchange entirely via `onPoison: "drop"`, in which case eviction just discards the message. At-least-once is a statement about the acknowledgment protocol, not an unconditional promise that every accepted message arrives. No configuration makes at-least-once exactly-once, because no such configuration exists to make.

This page is the whole statement in one place: when a message can arrive twice, what a publish result does and does not tell you, and what those two facts leave you responsible for.

## At-least-once is not a retry setting

The retry mode defaults to `none`, and it is tempting to read that as having opted out of duplicates. It is not: `none` governs what happens to a `RetryableError` raised by your handler, and nothing else.

A message can arrive a second time with no retry configuration at all.

- **The worker crashes mid-handler.** Everything it had not acked returns to the queue. At the default prefetch of 10, that is up to ten messages for each consumer that was running.
- **The connection or channel drops before the ack is written.** Your handler may have finished the work. The broker never heard so, and redelivers.
- **`close()` reaches its drain timeout.** `worker.close()` waits for in-flight handlers, bounded by `drainTimeoutMs` (30 000 ms by default). On timeout the channel closes anyway, and whatever was still un-acked goes back.

Two more appear once you do configure retries:

- **`immediate-requeue`** returns the message to its own queue for another attempt.
- **`ttl-backoff`** republishes it through a wait queue.

The first three are properties of running a consumer against a broker, and you did not choose them. Two of the three you cannot switch off. The third you can: pass `drainTimeoutMs: null` to `worker.close()` and it waits for every in-flight handler instead of cutting them off at a deadline ([consume messages](/how-to/consume-messages#shut-down-without-dropping-messages)). The last two are choices. All five produce the same thing at your handler: a message it has seen before.

## A failed publish is ambiguous

`publish()` waits for a publisher confirm, bounded by `publishTimeoutMs` (30 000 ms by default). When that deadline passes, the call settles as a failure.

That failure means the client stopped waiting. It does not mean the broker failed to receive the message. And it does not arrive as an `Err` your `errCases` matcher can see: `client.publish` widens the error channel only to `MessageValidationError`, so a publish timeout — like every other transport failure — surfaces as a `Defect` instead. Code written to catch "publish failed" with `.recoverErrCases(...)` or `.flatMapErrCases(...)` will never run for it; the `defect` arm of `.match(...)` is where it lands. [Upgrade guide](/how-to/upgrade#channels-set-a-30s-publish-timeout) walks through the pattern.

So a publish error is not proof of non-delivery, and the natural response — send it again — can produce a duplicate.

A connection drop mid-confirm does not quietly dodge this either: when the channel under a pending publish closes, amqplib rejects every unconfirmed publish on that channel immediately, and the client surfaces that rejection the same way as a timeout — as a `Defect`, not a silent retry. Nothing here republishes the message on your behalf. If a duplicate happens, it is because you saw that failure and sent the message again — which is exactly why the ambiguity above is the one to design around.

Nothing closes this gap within core AMQP 0-9-1: an acknowledgement can always be lost after the work it acknowledges is done, and no message in the protocol lets the broker recognise a repeat on its own. Deduplication does exist outside core AMQP — RabbitMQ's `rabbitmq-message-deduplication` plugin keys off a header you supply, RabbitMQ Streams do it natively, Kafka ships an idempotent producer — but amqp-contract ships none of it.

Two honest responses:

- **Republish, and let the consumer cope.** The simplest, and correct as long as the consumer is idempotent.
- **Republish with the same identifier.** If the message carries an id that stays the same across your own retries, the consumer can recognise the repeat. An id regenerated per attempt is worse than none — every retry looks like new work.

## Idempotency lives in your handler

The library does not deduplicate for you, and it attaches no identity you could deduplicate on: it never sets AMQP's `messageId`. If you want one, you set it.

The client's publish options are amqplib's plus an optional `compression` field, so `messageId` is available:

```typescript
await client
  .publish("orderCreated", { orderId: "ORD-123" }, { messageId: idempotencyKey })
  .getOrThrow();
```

A handler reads it from the raw message:

```typescript
processOrder: ({ raw, input: { payload } }) => {
  const { messageId } = raw.properties;
  const id = typeof messageId === "string" ? messageId : undefined;
  return upsertOrder(payload, id).map(() => undefined);
},
```

`messageId` is typed `any` on the raw amqplib message, so narrow it before use rather than passing it on as-is.

Or carry a business key in the payload instead of relying on `messageId`. Both survive the library's own paths that rebuild the message: retry republishing spreads the original properties — `messageId` included — onto the new message, and a dead-letter hop keeps `messageId` and the body too, though RabbitMQ adds its own `x-death` / `x-first-death-*` headers along the way and drops the per-message `expiration` on a TTL hop.

Whichever you choose, the property that matters is that the id is **stable across the sender's own retries** and unique per logical operation. An id that changes when the sender retries deduplicates nothing.

What you do with it is ordinary application work, and the right answer depends on what the handler touches:

- An **idempotency key** at the downstream provider. Most payment APIs accept one and do the deduplication for you, which is the strongest option when it is available.
- An **upsert** instead of an insert, so a repeat converges instead of conflicting.
- A **deduplication table** keyed on the id, written in the same transaction as the effect. Two separate transactions reintroduce the gap you are trying to close: a crash between them leaves the effect applied and the id unrecorded.

## Where next

- [The retry model](/explanation/the-retry-model) — what retrying does, and why validation failures never do.
- [Consume messages](/how-to/consume-messages) — prefetch, draining, and reaching the raw message.
- [Route dead letters](/how-to/route-dead-letters) — where a message goes when retrying is over.
