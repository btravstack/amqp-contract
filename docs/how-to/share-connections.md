---
title: Share connections - amqp-contract
description: How clients and workers pool connections, share one explicitly, configure heartbeats and reconnection, close in the right order, and reset the cache between tests.
---

# Share connections

Connections are pooled per process. Clients with the same URLs and connection options share one connection; workers with the same URLs and options share another. A client and a worker never share a pooled connection: RabbitMQ blocks a _publishing_ connection under a memory or disk alarm, and a consumer on that same connection would stop acknowledging along with it.

## Get the default pooling

Pass the same `urls`:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).getOrThrow();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  urls: ["amqp://localhost"],
}).getOrThrow();
```

The result is two connections — one for publishing, one for consuming — each carrying as many channels as there are clients or workers on it. There is nothing to opt into.

Connections are cached on URLs **and** connection options together. Pass `connectionOptions` to one client and not another and you get two connections — so keep them identical, or omit them on both.

## Share one connection explicitly

To put a client on a connection you own — to share it with a worker anyway, or to manage its lifecycle yourself — create it with amqp-connection-manager and pass `connection` instead of `urls`:

```typescript
import amqp from "amqp-connection-manager";

const connection = amqp.connect(["amqp://localhost"]);

const client = await TypedAmqpClient.create({ contract, connection }).getOrThrow();
const worker = await TypedAmqpWorker.create({ contract, handlers, connection }).getOrThrow();
```

Pass exactly one of `urls` or `connection`. The client or worker opens its channel on the connection and never closes it: closing it is yours, after every client and worker using it has closed. Sharing one connection between a publisher and a consumer gives up the protection above — under a resource alarm, the consumer stalls with the publisher.

## Publish from inside a handler

A handler that publishes needs a client next to its worker. The subtlety is not the connection; it is that a publish failure inside a handler has to become a _handler_ error so the worker can route the message:

```typescript
import { NonRetryableError, RetryableError } from "@amqp-contract/worker";
import { P } from "unthrown";

processOrder: ({ input: { payload } }) =>
  client
    .publish("orderProcessed", { orderId: payload.orderId, status: "completed" })
    .map(() => undefined)
    .mapErrCases((matcher) =>
      matcher
        // The payload we built is wrong — retrying will not fix it
        .with(
          P.tag("@amqp-contract/MessageValidationError"),
          (error) => new NonRetryableError("invalid outgoing message", error),
        )
        // The broker side failed (timeout, nack, closed channel) — try again later
        .with(
          P.tag("@amqp-contract/PublishError"),
          (error) => new RetryableError("failed to publish", error),
        ),
    ),
```

A broker hiccup is a modeled `PublishError`, so mapping it to `RetryableError` sends the message through the retry pipeline instead of dead-lettering it.

Do not wrap `client.publish(...)` in `fromPromise` — it already returns an `AsyncResult`, and wrapping it again nests one inside another.

## Close in the right order

```typescript
await worker.close().get(); // stop consuming first
await client.close().get(); // then stop publishing
```

Each closes its own channel. A pooled connection is reference-counted and closes once the last user releases it; a connection you passed in stays open until you close it.

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
}).getOrThrow();
```

Heartbeats detect a dead peer that never sent a FIN — a hard-killed broker, a silently dropped NAT mapping. Too long and failures take minutes to notice; too short and a busy event loop can miss one and drop a healthy connection. 30 seconds is a reasonable default; go lower only with evidence.

Remember these options participate in the cache key: whatever you pass here must be passed identically everywhere you want sharing.

## Fail fast when the broker is unreachable

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  connectTimeoutMs: 10_000,
}).getOrThrow();
```

`create` answers `Err(ConnectionError)` if the connection is not ready in time; the default is 30 seconds. Pass `null` to wait indefinitely and let amqp-connection-manager keep retrying — appropriate for a worker that should tolerate the broker starting after it does.

## Separate connections further

Publishers and consumers are already on separate connections. To split two clients (or two workers) from each other — say a high-throughput publisher from a latency-sensitive one — give them different connection options (differing options mean different cache entries), or give each its own explicit `connection`.

## Know the limits

**Same process only.** The cache is a per-process singleton. Every process, worker thread or Lambda instance gets its own connections. Size your broker's connection limits on process count, not service count.

**No manual lifecycle for pooled connections.** You cannot hold, name or pre-warm one; it exists while something uses it. For a connection you control, create it yourself and pass it as `connection`.

## Reset the cache between tests

The singleton outlives an individual test, so a test that asserts on connection counts — or one that must not inherit a connection from a previous file — should reset it:

```typescript
import { _internal_resetConnections } from "@amqp-contract/core/internal";

afterEach(async () => {
  await _internal_resetConnections();
});
```

`_internal_getConnectionCount()` returns the current count of pooled connections (a client and a worker on the same URLs count as two), which is how you verify pooling is actually happening.

Both are test-only helpers.

## Verify sharing is working

Check the broker rather than trusting the code:

```bash
docker exec rabbitmq rabbitmqctl list_connections
docker exec rabbitmq rabbitmqctl list_channels
```

One connection per role (publishing, consuming) with several channels each means it is working. More than that means your URLs or `connectionOptions` differ somewhere — compare them exactly, including array order.

## Where next

- [Tune performance](/how-to/tune-performance) — when to separate connections.
- [Test with RabbitMQ](/how-to/test-with-rabbitmq) — test isolation.
- [Troubleshoot](/how-to/troubleshoot#connection-problems) — connection failures.
