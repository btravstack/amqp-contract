---
title: Tune performance - amqp-contract
description: Diagnose and fix throughput, latency and memory problems — prefetch, connections, compression, queue type and publisher confirms.
---

# Tune performance

Start from a symptom, not from a checklist. Each section below begins with what you would observe.

Measure before changing anything. [Instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry) gives you `amqp.worker.process.duration` and the consumed/published counters, which is usually enough to tell a slow handler from a slow broker.

## Prefetch

Prefetch caps unacknowledged messages per consumer. It is the first thing to look at for both throughput and memory.

```typescript
handlers: {
  processOrder: [handler, { prefetch: 10 }],
},
```

or for every consumer:

```typescript
defaultConsumerOptions: { prefetch: 10 },
```

**Throughput is low and the worker looks idle.** Prefetch is too low. At `prefetch: 1` the consumer waits a full network round trip between messages, so a handler taking 5 ms spends most of its time waiting. Raise it — 10 to 100 is a normal range for fast handlers.

**Memory grows, or work distributes unevenly across replicas.** Prefetch is too high. Every prefetched message is held in the worker's memory, and a worker that grabbed 1000 messages leaves its idle peers with nothing. Lower it. For slow handlers — seconds per message — small values are correct, and `prefetch: 1` gives the best distribution.

The rough rule: fast handlers want high prefetch, slow handlers want low. If handler duration varies wildly, favour the low end so one worker cannot hoard a batch.

## Publishing throughput

There is no batch publish API. Publish concurrently rather than sequentially:

```typescript
import { allAsync } from "unthrown";

await allAsync(orders.map((order) => client.publish("orderCreated", order)));
```

Awaiting each publish in turn serializes on the broker's confirm round trip, which is usually the actual bottleneck when bulk publishing feels slow.

Publisher confirms are on by default and are what make that round trip exist. They are also what lets `publish` tell you the broker accepted the message. Turning them off ([configure channels](/how-to/configure-channels)) trades that guarantee for speed — appropriate for high-volume telemetry, wrong for anything you cannot lose.

## Connections and channels

One connection shared across clients and workers is the default and usually right — see [share connections](/how-to/share-connections).

**Publish latency spikes when a consumer is busy.** They are sharing a socket. Split them onto separate connections by giving them different `connectionOptions`.

**The broker reports far more connections than expected.** Something differs in `urls` or `connectionOptions` between call sites, so the cache is not matching. Compare them exactly, including array order.

## Message size

Large messages cost bandwidth and broker memory. Compress selectively:

```typescript
await client.publish("orderCreated", order, {
  compression: JSON.stringify(order).length > 1024 ? "gzip" : undefined,
});
```

Below ~1 KB, compression usually makes the message bigger. See [compress messages](/how-to/compress-messages).

For genuinely large payloads, the better fix is often not to send them: put the blob in object storage and send a reference. A queue is not a file transfer system, and multi-megabyte messages degrade broker memory and redelivery behaviour in ways no compression ratio fixes.

## Queue type

Quorum queues are the default and replicate through Raft, which costs write latency compared to classic queues.

That cost is the durability you are buying, and it is almost always worth it. Switching to `type: "classic"` for speed is only defensible for genuinely disposable data — transient caches, ephemeral notifications — where losing the queue on a broker restart is acceptable.

Under `immediate-requeue`, remember quorum queues also enforce `x-delivery-limit` (20 by default in RabbitMQ 4) independently of your `maxRetries`. See [retry failed messages](/how-to/retry-failed-messages#retry-immediately).

## Validation cost

Every message is validated on publish and again on consume. For typical payloads this is microseconds and not worth thinking about.

If profiling actually shows schema validation on the hot path, the fix is a cheaper schema rather than fewer checks: [Valibot](https://valibot.dev/) is markedly faster than Zod for large objects, and both are Standard Schema, so switching is a contract-level change with no effect on handlers. See [schema libraries](/reference/schema-libraries).

Deeply nested schemas with many refinements are the usual culprit. Flattening them helps more than changing library.

## Retry storms

**Throughput collapses under partial failure.** A `RetryableError` under `immediate-requeue` requeues at once, so a downstream outage becomes a tight loop: the same messages cycle continuously, consuming the whole worker.

Switch to `ttl-backoff` with `jitter: true`. The growing delay gives the dependency room to recover, and jitter stops every retry landing simultaneously.

**The dead-letter queue fills faster than expected.** Check whether failures are being classified as retryable when they are permanent. A `NonRetryableError` reaching the dead-letter queue immediately is correct and cheap; a permanent failure misclassified as retryable burns the whole budget before arriving anyway.

## Heartbeats

**Connections drop under load.** Heartbeats are missed when the event loop is blocked, and the broker closes what it believes is a dead peer.

The usual cause is synchronous work in a handler blocking the loop — not the heartbeat interval. Move CPU-bound work off the main thread before raising `heartbeatIntervalInSeconds`, which only hides the problem.

## What to watch

| Signal                                               | Meaning                                                                         |
| ---------------------------------------------------- | ------------------------------------------------------------------------------- |
| `amqp.worker.process.duration` rising, consumed flat | Handlers slowing — usually a downstream dependency                              |
| consumed < published, sustained                      | Queue growing; add consumers or raise prefetch                                  |
| `success=false` split by `error.type`                | Rising `NonRetryableError` = bad data; rising `RetryableError` = infrastructure |
| Broker queue depth                                   | Not reported by this library — read it from RabbitMQ                            |
| Worker RSS growing with prefetch                     | Prefetch too high for the payload size                                          |

## Where next

- [Instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry) — the measurements above.
- [Troubleshoot](/how-to/troubleshoot) — when it is broken rather than slow.
- [The retry model](/explanation/the-retry-model) — why retry storms happen.
