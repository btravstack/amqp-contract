---
title: Test without a broker - amqp-contract
description: Run the contract pipeline against an in-memory broker — no Docker, in the unit suite.
---

# Test without a broker

`InMemoryAmqpBroker` runs a contract end to end with no container: publish,
routing, both validation passes, middleware and interceptors, RPC correlation,
retry routing and dead-lettering. It is for the tests that are about **your
contract and your handlers**, where a container is a 30-second tax on a
question the broker was never going to answer differently.

It does not replace [testing against RabbitMQ](/how-to/test-with-rabbitmq) —
see [what it does not model](#what-it-does-not-model) below.

## Use it

```ts
import { InMemoryAmqpBroker } from "@amqp-contract/testing";
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";

const broker = new InMemoryAmqpBroker();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  transport: broker.createTransport(contract),
}).getOrThrow();

const client = await TypedAmqpClient.create({
  contract,
  transport: broker.createTransport(contract),
}).getOrThrow();

await client.publish("placeOrder", { orderId: "o-1", total: 42 }).getOrThrow();
```

`transport` replaces `urls`, and exactly one of the two is required — passing
both is refused rather than silently preferring one, so a test that supplies a
transport can never quietly reach a real broker instead.

A transport per facade, as a real deployment has a connection per facade: that
is what gives direct reply-to somewhere to route back to.

## Look inside the broker

Two methods exist for assertions a consumer cannot make:

```ts
broker.queueNames(); // every queue the contract declared, wait queues included
broker.peek("orders-dlq"); // the messages parked on a queue, unconsumed
```

`peek` is the one worth reaching for: a dead-letter assertion otherwise needs a
consumer on the DLQ, which changes what you are testing.

## What it models

- **Routing** — topic (`*` one word, `#` zero or more), direct, fanout, headers
  (`x-match` `all`/`any`), and the default exchange, where the routing key is a
  queue name. That last one carries every RPC request and every retry
  republish.
- **Direct reply-to** — `amq.rabbitmq.reply-to` is rewritten to a per-transport
  pseudo-queue and routed back to the transport that published, which is what
  RabbitMQ does per channel.
- **Settlement** — `ack` drops the delivery; `nack(requeue: false)`
  dead-letters through the queue's `x-dead-letter-exchange` or drops;
  `nack(requeue: true)` redelivers with `redelivered: true` and an incremented
  `x-delivery-count`, which is the header a quorum queue's retry budget counts.
- **TTL** — per-message `expiration` and queue-level `x-message-ttl`, whichever
  is shorter, dead-lettering on expiry. That is what makes TTL-backoff retry
  run for real: the republish carries the `expiration`, the wait queue carries
  the ceiling.
- **Serialization** — a Buffer passes through byte for byte and anything else
  is `JSON.stringify`d, exactly as `AmqpClient` encodes, so a compressed
  payload survives the round trip and the decompression path runs.
- **Asynchronous delivery**, so nothing is delivered re-entrantly inside the
  publish that caused it.

## What it does not model

An unroutable publish is **dropped and confirmed**, as AMQP does without a
mandatory flag — the fake is not kinder than the broker.

Beyond that it is not a RabbitMQ, and these stay the integration suite's job:

- **Topology refusals.** A real broker answers `406 PRECONDITION_FAILED` when a
  queue is redeclared with different arguments; here declaring is idempotent
  and additive.
- **Reconnection.** `currentChannelEpoch` is always `0`, so the stale-delivery
  guard never fires. A reconnect is precisely the behaviour this does not have.
- **Flow control, prefetch limits, persistence and clustering.** `prefetch` is
  accepted and ignored.
- **Exchange-to-exchange bindings** are declarable but carry no traffic.

The rule of thumb: if the assertion is about **your** contract, handlers or
middleware, use this; if it is about what the **broker** does, use
[a real one](/how-to/test-with-rabbitmq).
