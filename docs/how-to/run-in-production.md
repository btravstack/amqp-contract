---
title: Run in production - amqp-contract
description: A checklist for taking an amqp-contract service to production — TLS, credentials, vhosts, prefetch, publisher confirms, quorum queues, dead-letter monitoring, graceful shutdown, health checks and memory bounds.
---

# Run in production

The defaults are chosen to be safe, so most of this list is confirming a default rather than changing it. Work through it once per service.

## Connect over TLS

Use an `amqps://` URL. The TLS options — a private CA, or a client certificate for mutual TLS — go in `connectionOptions.connectionOptions`, which is handed to amqplib's `connect()` and from there to Node's `tls.connect()`:

```typescript
import { readFileSync } from "node:fs";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { contract } from "./contract.js";
import { handlers } from "./handlers.js";

const url = process.env["AMQP_URL"]; // amqps://orders-svc:…@rabbitmq.internal:5671/orders-prod
if (url === undefined) throw new Error("AMQP_URL is not set");

const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  urls: [url],
  connectionOptions: {
    connectionOptions: {
      ca: [readFileSync("/etc/rabbitmq/ca.pem")],
      // For mutual TLS, add `cert` and `key` here too.
    },
  },
}).getOrThrow();
```

`TypedAmqpClient.create` takes the same `urls` and `connectionOptions`. Each entry in `urls` can also be an object — `{ url, connectionOptions }` — when brokers in the list need different TLS settings; several URLs give you failover.

## Keep credentials out of code

Credentials travel in the URL (`amqps://user:password@host/vhost`), so the URL is a secret. Read it from the environment or your secret manager, as above, and never commit it or print it. Give each service its own RabbitMQ user, with permissions limited to the exchanges and queues its contract declares.

## Give each environment its own vhost

The vhost is the URL's path: `amqps://host:5671/orders-prod`. (The default vhost `/` is written `%2F`.) One vhost per environment keeps staging traffic out of production queues even when they share a cluster, and scopes users' permissions per environment.

## Set prefetch deliberately

Every consumer has a prefetch of 10 unless you say otherwise — the number of unacknowledged messages RabbitMQ will push to it at once. Raise it for fast handlers, lower it for slow ones:

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: [processOrder, { prefetch: 50 }],
    generateReport: [generateReport, { prefetch: 1 }],
  },
  defaultConsumerOptions: { prefetch: 20 },
  urls: [url],
}).getOrThrow();
```

`prefetch: "unbounded"` exists but lets the broker push a whole backlog into one process. See [tune performance](/how-to/tune-performance#prefetch) for choosing a value.

## Bound worker memory

A worker can hold, at once, up to:

```
prefetch × consumers in the process × largest message (decompressed)
```

At `prefetch: 10`, three consumers and 1 MiB messages, that is 30 MiB of payload before your handlers allocate anything. Size the container from this number, not from the average message.

Every inbound message is capped at 16 MiB — a plain body as it arrives, a compressed one after decompression (RabbitMQ 4's own default `max_message_size`). A message over the cap is dead-lettered instead of parsed or expanded. Set the cap to your real largest message with `maxMessageBytes` on `TypedAmqpWorker.create`. See [compress messages](/how-to/compress-messages).

## Keep publisher confirms on

The client publishes on a confirm channel: `publish()` resolves only once the broker has taken responsibility for the message. A publish the broker has not confirmed within `publishTimeoutMs` (30 seconds by default) fails instead of hanging. Leave both as they are — turning confirms off trades durability for throughput you rarely need. [Delivery guarantees](/explanation/delivery-guarantees) explains exactly what a confirm promises.

## Use quorum queues

`defineQueue` creates quorum queues unless you pass `type: "classic"`. Quorum queues are replicated across the cluster and survive a node loss. Keep classic queues for what quorum queues cannot do — `exclusive`, `autoDelete` and `maxPriority`. See [topology options](/reference/topology-options).

## Watch the dead-letter queues

`defineContract` guarantees that a message your handlers reject lands in a dead-letter queue rather than disappearing. Someone still has to notice it arrived. For every DLQ:

- **Alert when its depth is above zero** — or above a small baseline, if some rejections are expected. Queue depth comes from RabbitMQ (its management API or Prometheus plugin), not from this library.
- **Alert on the rate of `NonRetryableError`** from the worker's telemetry: a rising rate is bad data arriving, usually from a publisher deploy.
- **Have a replay procedure** before you need one. See [route dead letters](/how-to/route-dead-letters#replay-a-dead-lettered-message).

## Shut down gracefully

On `SIGTERM`, close the worker before the process exits. `worker.close()` stops consuming, waits for in-flight handlers to finish and acknowledge their messages, and only then closes the channel:

```typescript
process.on("SIGTERM", async () => {
  await worker.close().get();
  await client.close().get();
  process.exit(0);
});
```

The drain waits up to 30 seconds by default (`worker.close({ drainTimeoutMs })` changes it). Messages still in flight after that are redelivered to another consumer, so a handler must be safe to run twice. Give the orchestrator longer than the drain: Kubernetes' default `terminationGracePeriodSeconds` is also 30, which kills the process just as the drain gives up — set it to 45 or more. [Share connections](/how-to/share-connections#close-in-the-right-order) covers the close order when a worker and client share a connection.

## Wire health checks

`TypedAmqpWorker.create` and `TypedAmqpClient.create` fail with a `ConnectionError` if the broker cannot be reached within `connectTimeoutMs` (30 seconds by default). Treat that as a failed startup: exit, and let the orchestrator restart the pod. Mark the service ready only after `create()` has succeeded.

After startup, a lost connection is retried automatically. `worker.isConnected()` and `client.isConnected()` report whether the connection is up right now — `false` while reconnecting — so a readiness probe can read them:

```typescript
app.get("/ready", (_req, res) => {
  res.status(worker.isConnected() && client.isConnected() ? 200 : 503).end();
});
```

Keep that on readiness, not liveness: a reconnecting process recovers by itself, and restarting it only adds a cold start. Consumers resume once the connection is back, and publishes issued meanwhile wait up to `publishTimeoutMs` before failing with `PublishError`.

## Where next

- [Tune performance](/how-to/tune-performance) — prefetch, message size and what to watch.
- [Evolve a contract](/how-to/evolve-a-contract) — changing messages without breaking running services.
- [Delivery guarantees](/explanation/delivery-guarantees) — what at-least-once means for your handlers.
