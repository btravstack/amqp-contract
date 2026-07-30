---
title: Define a contract - amqp-contract
description: Recipes for contract definitions — events, commands, exchanges, queues, headers, wildcards and compile-time routing-key validation.
---

# Define a contract

Recipes for building contracts. For why contracts are shaped this way, see [core concepts](/explanation/core-concepts); for every available option, see [topology options](/reference/topology-options).

## Structure a contract

Define resources as named constants, then compose them:

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing");
const orderMessage = defineMessage(z.object({ orderId: z.string(), amount: z.number() }));

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderProcessingQueue) },
});
```

`defineContract` takes `publishers`, `consumers` and `rpcs`. The contract it returns also exposes `exchanges`, `queues` and `bindings`, all extracted from what you passed — you rarely list them yourself. The exception is [standalone topology](#declare-standalone-topology): resources with no publisher or consumer attached.

## Broadcast an event to many consumers

Define the publisher first; it owns the fact. Consumers attach to it.

```typescript
const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: {
    processOrder: defineEventConsumer(orderCreated, orderProcessingQueue),
    notifyCustomer: defineEventConsumer(orderCreated, notificationsQueue),
  },
});
```

Each consumer gets its own queue, so both receive every message. Two consumers sharing one queue would compete for messages instead — that is a work queue, not a broadcast.

## Send a command to a single owner

Define the consumer first; it owns the queue and decides what it accepts. The publisher is derived from it.

```typescript
import { defineCommandConsumer, defineCommandPublisher } from "@amqp-contract/contract";

const fulfillmentExchange = defineExchange("fulfillment", { type: "direct" });
const fulfillmentQueue = defineQueue("order-fulfillment");

const fulfillOrder = defineCommandConsumer(
  fulfillmentQueue,
  fulfillmentExchange,
  fulfillmentMessage,
  { routingKey: "order.fulfill" },
);

const requestFulfillment = defineCommandPublisher(fulfillOrder);

export const contract = defineContract({
  publishers: { requestFulfillment },
  consumers: { fulfillOrder },
});
```

Callers cannot drift from the owner's contract because the publisher's payload type and routing key come from the consumer. A direct exchange suits commands: one exact key, one destination.

## Subscribe to several routing keys

Override the consumer's binding pattern:

```typescript
const notifyOrder = defineEventConsumer(orderCreated, notificationsQueue, {
  routingKey: "order.#",
});
```

`*` matches exactly one segment, `#` matches zero or more. So `order.*` matches `order.created` but not `order.created.urgent`, while `order.#` matches both.

The payload type still comes from the publisher. When several publishers feed one wildcard consumer, give it a union message so the type covers everything it can receive:

```typescript
const anyOrderEvent = defineMessage(z.union([orderSchema, orderStatusSchema]));
```

## Choose an exchange type

```typescript
defineExchange("orders"); // topic, durable — the default
defineExchange("tasks", { type: "direct" });
defineExchange("events", { type: "fanout" });
defineExchange("routed", { type: "headers" });
```

Topic is the default because it subsumes direct — a key with no wildcards routes exactly — while leaving room for wildcard consumers later.

Routing keys are required for direct and topic exchanges, and optional (ignored) for fanout and headers. This is enforced at compile time.

## Use a classic queue

Queues are quorum by default. Ask for classic only when you need a feature quorum does not support:

```typescript
const tempQueue = defineQueue("temp-queue", {
  type: "classic",
  durable: false,
  autoDelete: true,
});
```

`durable: false`, `autoDelete`, `exclusive` and priority queues all require `type: "classic"`. TypeScript rejects them on a quorum queue.

## Declare standalone topology

Sometimes a service must assert topology it neither publishes to nor consumes from. The classic cases: a dead-letter queue bound to the auto-extracted DLX so failed messages land somewhere durable, or an audit queue that another process drains. Pass them at the top level of `defineContract`:

```typescript
import { defineQueueBinding } from "@amqp-contract/contract";

const ordersDlxExchange = defineExchange("orders-dlx");
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlxExchange },
});
const orderDlq = defineQueue("order-processing-dlq");

export const contract = defineContract({
  consumers: { processOrder: defineEventConsumer(orderCreated, orderProcessingQueue) },
  queues: { orderDlq },
  bindings: {
    dlqBinding: defineQueueBinding(orderDlq, ordersDlxExchange, { routingKey: "#" }),
  },
});
```

Standalone `exchanges`, `queues` and `bindings` are asserted by client and worker setup exactly like extracted ones. In the contract output, standalone exchanges and queues are re-keyed by their resource name; binding labels are kept verbatim. Dead-letter exchanges and TTL-backoff retry infrastructure are auto-extracted for standalone queues too, just as for consumer queues.

For topology that cannot live in a contract at all, `setupAmqpTopology(channel, contract)` from `@amqp-contract/core` is the low-level escape hatch: it asserts a contract's resources on a raw channel, and you can run your own assertions alongside it.

## Add validated headers to a message

```typescript
const orderMessage = defineMessage(
  z.object({ orderId: z.string().uuid(), amount: z.number().positive() }),
  {
    headers: z.object({
      correlationId: z.string().uuid(),
      tenantId: z.string(),
      priority: z.enum(["low", "medium", "high"]).optional(),
    }),
    summary: "Order created event",
  },
);
```

Handlers then receive typed `headers` alongside `payload`. With no headers schema, `headers` is `undefined`.

Headers are validated on the **consumer** side only — publishing does not check them. See [publish messages](/how-to/publish-messages#send-headers).

`summary` and `description` are documentation, and flow into the generated [AsyncAPI](/how-to/generate-asyncapi) document.

## Validate routing keys at compile time

The exported utility types check routing keys and patterns:

```typescript
import type { BindingPattern, MatchingRoutingKey, RoutingKey } from "@amqp-contract/contract";

type ValidKey = RoutingKey<"order.created">; // 'order.created'
type BadKey = RoutingKey<"order..bad">; // never — empty segment

type ValidPattern = BindingPattern<"order.#">; // 'order.#'
type Matches = MatchingRoutingKey<"order.*", "order.created">; // 'order.created'
type NoMatch = MatchingRoutingKey<"order.*", "user.created">; // never
```

Keys are dot-separated segments of alphanumerics, hyphens and underscores. The `defineEvent*` and `defineCommand*` functions apply these internally; use them directly when writing your own routing helpers.

TypeScript's recursion limit means very long keys fall back to `string`. That affects compile-time checking only, never runtime behaviour.

## Share a contract between services

Put the contract in its own package that both the publishing and consuming services depend on:

```
packages/
  order-contract/     ← defineContract lives here
  order-api/          ← depends on order-contract
  order-worker/       ← depends on order-contract
```

This is what makes a rename break both sides at once. A contract copied into two repositories is just two contracts that happen to agree today.

## Where next

- [Route dead letters](/how-to/route-dead-letters) — where failed messages go.
- [Retry failed messages](/how-to/retry-failed-messages) — queue-level retry configuration.
- [Bridge domains](/how-to/bridge-domains) — routing across service boundaries.
- [Topology options](/reference/topology-options) — the full option tables.
