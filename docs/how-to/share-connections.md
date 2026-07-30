---
title: Share connections - amqp-contract
description: Reuse one AMQP connection across clients and workers, configure heartbeats and reconnection, close in the right order, and reset the cache between tests.
---

# Share connections

A client and a worker in the same process share one connection automatically when their URLs and connection options match. RabbitMQ recommends few connections and many channels, and this is how you get that without managing anything.

## Share a connection

Pass the same `urls`:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  urls: ["amqp://localhost"], // same URLs → same connection
}).get();
```

The result is one connection with two channels. There is nothing to opt into.

Connections are cached on URLs **and** connection options together. Pass `connectionOptions` to one and not the other and you get two connections — so keep them identical, or omit them on both.

## Publish from inside a handler

The usual reason to want sharing. The subtlety is not the connection; it is that a publish failure inside a handler has to become a _handler_ error so the worker can route the message:

```typescript
import { RetryableError } from "@amqp-contract/worker";
import { Err, P } from "unthrown";

processOrder: ({ payload }) =>
  client
    .publish("orderProcessed", { orderId: payload.orderId, status: "completed" })
    .map(() => undefined)
    // Modeled validation failure → retryable handler error
    .mapErrCases((matcher) =>
      matcher.with(
        P.tag("@amqp-contract/MessageValidationError"),
        (error) => new RetryableError("failed to publish", error),
      ),
    )
    // Transport failure arrives as a defect → recover it into a handler error
    .recoverDefect((cause) => Err(new RetryableError("failed to publish", cause))),
```

Without the `recoverDefect`, a broker hiccup would surface as a defect and the message would be dead-lettered instead of retried.

Do not wrap `client.publish(...)` in `fromPromise` — it already returns an `AsyncResult`, and wrapping it again nests one inside another.

## Close in the right order

```typescript
await worker.close().get(); // stop consuming first
await client.close().get(); // then stop publishing
```

Each closes its own channel. The shared connection is reference-counted and closes once the last user releases it.

Worker first matters if handlers publish: closing the client first leaves in-flight handlers unable to publish.

## Configure heartbeats and reconnection

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  connectionOptions: {
    heartbeatIntervalInSeconds: 30,
    reconnectTimeInSeconds: 5,
  },
}).get();
```

Heartbeats detect a dead peer that never sent a FIN — a hard-killed broker, a silently dropped NAT mapping. Too long and failures take minutes to notice; too short and a busy event loop can miss one and drop a healthy connection. 30 seconds is a reasonable default; go lower only with evidence.

Remember these options participate in the cache key: whatever you pass here must be passed identically everywhere you want sharing.

## Fail fast when the broker is unreachable

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  connectTimeoutMs: 10_000,
}).get();
```

`create` resolves to a defect if the connection is not ready in time; the default is 30 seconds. Pass `null` to wait indefinitely and let amqp-connection-manager keep retrying — appropriate for a worker that should tolerate the broker starting after it does.

## Use separate connections deliberately

Sharing is usually right, but not always. A high-throughput publisher and a slow consumer on one connection contend for the same socket, and TCP backpressure from one becomes latency for the other.

To separate them, give them different connection options — differing options mean different cache entries and therefore different connections.

## Know the limits

**Same process only.** The cache is a per-process singleton. Every process, worker thread or Lambda instance gets its own connections. Size your broker's connection limits on process count, not service count.

**No manual lifecycle.** You cannot hold, name or pre-warm a connection. It exists while something uses it.

## Reset the cache between tests

The singleton outlives an individual test, so a test that asserts on connection counts — or one that must not inherit a connection from a previous file — should reset it:

```typescript
import { _internal_resetConnections } from "@amqp-contract/core";

afterEach(async () => {
  await _internal_resetConnections();
});
```

`_internal_getConnectionCount()` returns the current count, which is how you verify sharing is actually happening.

Both are test-only helpers.

## Verify sharing is working

Check the broker rather than trusting the code:

```bash
docker exec rabbitmq rabbitmqctl list_connections
docker exec rabbitmq rabbitmqctl list_channels
```

One connection and several channels means it is working. Several connections means your URLs or `connectionOptions` differ somewhere — compare them exactly, including array order.

## Where next

- [Tune performance](/how-to/tune-performance) — when to separate connections.
- [Test with RabbitMQ](/how-to/test-with-rabbitmq) — test isolation.
- [Troubleshoot](/how-to/troubleshoot#connection-problems) — connection failures.
