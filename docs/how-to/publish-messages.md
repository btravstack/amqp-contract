---
title: Publish messages - amqp-contract
description: Create a client, publish typed messages, set routing keys, headers and AMQP properties, and handle publish failures.
---

# Publish messages

Recipes for the publishing side. For the concepts behind the result type, see [errors as values](/explanation/errors-as-values).

## Create a client

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract.js";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();
```

`create` returns `AsyncResult<TypedAmqpClient, never>`. The `.get()` is required to reach the client — awaiting alone leaves you with a `Result`. The modeled error channel is empty because connection failures are defects, so `.get()` compiles; it rethrows the underlying `TechnicalError` if connecting fails.

To log that failure before it propagates:

```typescript
const client = await TypedAmqpClient.create({ contract, urls: ["amqp://localhost"] })
  .tapDefect((cause) => logger.error({ cause }, "could not connect to the broker"))
  .get();
```

`tapDefect` observes without consuming, so `.get()` still throws afterwards. To recover instead of throwing, use `.recoverDefect(...)`.

Pass several URLs for failover — the client tries them in order:

```typescript
urls: ["amqp://primary:5672", "amqp://secondary:5672"];
```

## Publish a message

```typescript
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  amount: 99.99,
});
```

The first argument must be a publisher name from the contract; the second is checked against that publisher's schema. Both are compile errors when wrong.

Handle the outcome:

```typescript
import { P } from "unthrown";

result.match({
  ok: () => console.log("published"),
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) =>
      console.error("invalid payload:", error.issues),
    ),
  defect: (cause) => {
    throw cause; // transport failure
  },
});
```

`publish` returns `AsyncResult<void, MessageValidationError>`. Validation failure is the only modeled error; anything about the connection arrives as a defect.

## Set default options for every publish

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  defaultPublishOptions: {
    priority: 5,
    headers: { "x-app-version": "1.0.0" },
  },
}).get();
```

Per-call options override these. Messages are `persistent` by default; set `persistent: false` here or per call to opt out.

## Send a message on a different routing key

You cannot. The routing key comes from the publisher definition and there is no per-call override — `publish` uses `publisher.routingKey` and nothing else.

To publish the same payload on several keys, define a publisher per key and let them share a message:

```typescript
const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});
const orderCreatedUrgent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created.urgent",
});
```

```typescript
await client.publish(urgent ? "orderCreatedUrgent" : "orderCreated", order);
```

This is more verbose than a per-call key, deliberately: every routing key a service can emit stays visible in the contract, and therefore in the generated AsyncAPI document.

## Send headers

```typescript
await client.publish(
  "orderCreated",
  { orderId: "ORD-123", amount: 99.99 },
  {
    headers: {
      eventSource: "checkout-service",
      eventVersion: 2,
    },
  },
);
```

Headers are **not** validated on publish — only the payload is. If the message declares a headers schema, it is enforced by the _consumer_, and a mismatch dead-letters the message there rather than failing the publish here. Treat the headers schema as part of the contract and keep the two in step yourself.

## Set AMQP properties

`PublishOptions` is amqplib's `Options.Publish` plus `compression`, so properties are set flat, alongside `headers`:

```typescript
await client.publish(
  "orderCreated",
  { orderId: "ORD-123", amount: 99.99 },
  {
    persistent: false,
    priority: 10,
    expiration: "60000",
    correlationId: "req-123",
    headers: { "x-request-id": "req-123" },
  },
);
```

## Fail the process on a publish error

In a script or a job where a failed publish should stop everything:

```typescript
await client.publish("orderCreated", order).getOrThrow();
```

`getOrThrow` throws the `MessageValidationError` on `Err` and rethrows a defect's cause. Prefer `.match` in long-running services, where you usually want to log and continue rather than take the process down.

## Log failures without handling them

To observe a failure while leaving it in the pipeline:

```typescript
await client
  .publish("orderCreated", order)
  .tapFailure((failure) =>
    logger.error(
      { error: failure.tag === "Err" ? failure.error : failure.cause },
      "publish failed",
    ),
  )
  .tap(() => logger.debug("published"))
  .getOrThrow();
```

`tapFailure` sees both channels — `failure.tag` discriminates `Err` from `Defect`. `tap` only fires on success.

## Publish many messages

There is no batch API; publish in a loop and collect the results.

```typescript
import { allAsync } from "unthrown";

const results = await allAsync(orders.map((order) => client.publish("orderCreated", order)));
```

`allAsync` fails on the first error. To publish everything regardless and report afterwards, await them individually and partition the results yourself. For throughput considerations see [tune performance](/how-to/tune-performance#publishing-throughput).

## Close the client

```typescript
await client.close().get();
```

The `.get()` matters — without it the close result is discarded and a failure passes unnoticed. Closing fails any in-flight RPC calls with `RpcCancelledError`.

In a service, close on shutdown:

```typescript
process.on("SIGTERM", async () => {
  await client.close().get();
  process.exit(0);
});
```

## Where next

- [Use request/reply](/how-to/use-request-reply) — when you need an answer back.
- [Add middleware](/how-to/add-middleware#stamp-headers-on-every-publish) — stamp headers on every publish.
- [Compress messages](/how-to/compress-messages) — for large payloads.
- [Error model](/reference/error-model) — every error `publish` and `call` can produce.
