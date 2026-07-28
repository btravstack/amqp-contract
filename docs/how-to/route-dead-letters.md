---
title: Route dead letters - amqp-contract
description: Configure a dead-letter exchange, consume dead-lettered messages, expire messages by TTL, and replay from the dead-letter queue.
---

# Route dead letters

A dead-letter exchange (DLX) catches messages the system gave up on. Without one they are silently discarded, so configure it on any queue whose failures you care about.

## Configure a dead-letter exchange

```typescript
const ordersDlx = defineExchange("orders-dlx");

const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: {
    exchange: ordersDlx,
    routingKey: "order.failed", // optional
  },
});
```

The DLX is extracted into the contract automatically — you do not list it in `defineContract` yourself.

If `routingKey` is omitted, the message keeps its original routing key. Setting one is usually clearer, because it lets a single DLX distinguish sources.

## Consume dead-lettered messages

The dead-letter queue is an ordinary queue, so consuming it is ordinary too. Define a publisher against the DLX to describe what lands there, then bind a consumer:

```typescript
const ordersDlxQueue = defineQueue("orders-dlx-queue");

const failedOrder = defineEventPublisher(ordersDlx, orderMessage, {
  routingKey: "order.failed",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: {
    processOrder: defineEventConsumer(orderCreated, orderProcessingQueue),
    handleFailedOrders: defineEventConsumer(failedOrder, ordersDlxQueue),
  },
});
```

`failedOrder` exists to give the dead-letter consumer a payload type and a binding. Nothing publishes through it directly — the broker does the routing.

```typescript
handleFailedOrders: ({ payload }, rawMessage) => {
  logger.error(
    { orderId: payload.orderId, death: rawMessage.properties.headers?.["x-death"] },
    "order dead-lettered",
  );
  return OkAsync(undefined);
},
```

The broker's `x-death` header records why the message arrived and how many times.

::: warning
Give the dead-letter queue a `retry: { mode: "none" }` policy, or none at all, and be careful what its handler can fail on. A dead-letter consumer that dead-letters its own messages needs somewhere for _those_ to go.
:::

## Know what triggers dead-lettering

RabbitMQ routes a message to the DLX when:

- it is rejected with `nack` and `requeue: false` — which is what the worker does for a `NonRetryableError`, a validation failure, or an exhausted retry budget;
- its TTL expires;
- the queue hits a length limit.

## Expire old messages automatically

```typescript
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx, routingKey: "order.expired" },
  arguments: {
    "x-message-ttl": 86_400_000, // 24 hours
  },
});
```

Anything unconsumed for 24 hours moves to the DLX rather than sitting in the queue forever. Route expiry to its own key so the dead-letter consumer can tell "too slow" from "failed".

## Separate poison messages from expired ones

Use distinct routing keys and distinct dead-letter queues:

```typescript
const failedOrder = defineEventPublisher(ordersDlx, orderMessage, {
  routingKey: "order.failed",
});
const expiredOrder = defineEventPublisher(ordersDlx, orderMessage, {
  routingKey: "order.expired",
});
```

```typescript
consumers: {
  handleFailedOrders: defineEventConsumer(failedOrder, failedQueue),
  handleExpiredOrders: defineEventConsumer(expiredOrder, expiredQueue),
},
```

They usually want different responses — a failure needs investigating, an expiry usually needs more capacity.

## Replay a dead-lettered message

There is no built-in replay. Consume from the dead-letter queue and publish back through the normal publisher once you have fixed the cause:

```typescript
import { NonRetryableError } from "@amqp-contract/worker";
import { Err, OkAsync, tag } from "unthrown";

handleFailedOrders: ({ payload }) =>
  shouldReplay(payload)
    ? client
        .publish("orderCreated", payload)
        .mapErrCases((matcher) =>
          matcher.with(
            tag("@amqp-contract/MessageValidationError"),
            (error) => new NonRetryableError("replay rejected", error),
          ),
        )
        .recoverDefect((cause) => Err(new NonRetryableError("replay failed", cause)))
    : OkAsync(undefined),
```

Both channels need converting: `publish`'s modeled `MessageValidationError` through `mapErrCases`, and a transport failure — which arrives as a defect — through `recoverDefect`. Without the second, a broker hiccup would dead-letter the replay instead of retrying it.

Do this deliberately, not automatically. A replay loop that re-dead-letters is an infinite loop with extra steps — gate it on a fix having shipped, or on an attempt counter you control.

## Find out why a message died

What you get depends on how it arrived, and this catches people out.

Messages that reached the DLX via a _republish_ path carry `x-last-error` and `x-first-failure-timestamp`. Messages nacked directly — the most common case — carry only the broker's `x-death` and no application-level reason. The reason is in the worker's logs.

[Retry failed messages](/how-to/retry-failed-messages#inspect-retry-state) has the full matrix.

## Where next

- [Retry failed messages](/how-to/retry-failed-messages) — exhausting retries is the main road here.
- [Add logging](/how-to/add-logging) — where the failure reason actually lives.
- [Topology options](/reference/topology-options#definequeue) — `deadLetter` and `arguments` in full.
