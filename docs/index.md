---
layout: home
title: amqp-contract - Type-safe AMQP/RabbitMQ messaging for TypeScript
description: End-to-end type safety, runtime validation, and reliable retry patterns for AMQP/RabbitMQ messaging in TypeScript

hero:
  name: "amqp-contract"
  text: "Type-safe contracts for AMQP/RabbitMQ"
  tagline: End-to-end type safety · Runtime validation · Reliable retry patterns
  image:
    light: /logo-light.svg
    dark: /logo-dark.svg
    alt: amqp-contract
  actions:
    - theme: brand
      text: Get Started
      link: /tutorial/getting-started
    - theme: alt
      text: Why amqp-contract?
      link: /explanation/why-amqp-contract
    - theme: alt
      text: GitHub
      link: https://github.com/btravstack/amqp-contract

features:
  - icon: { src: /icons/shield-check.svg }
    title: Type Safety & Validation
    details: End-to-end TypeScript inference with automatic runtime validation using Zod, Valibot, or ArkType.

  - icon: { src: /icons/retry.svg }
    title: Reliable Retry
    details: Built-in immediate or exponential backoff retry mechanisms.

  - icon: { src: /icons/spec.svg }
    title: AsyncAPI Compatible
    details: Generate AsyncAPI 3.1 specs for documentation, visualization, and breaking change detection.
---

## Quick Example

Define your contract once — get type safety everywhere:

::: code-group

```typescript [1. Define Contract]
import {
  defineContract,
  defineExchange,
  defineQueue,
  defineQueueBinding,
  defineEventPublisher,
  defineEventConsumer,
  defineMessage,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
  retry: { mode: "ttl-backoff" }, // Automatic retry with exponential backoff
});
// A dead-letter exchange with no queue bound to it drops what it receives.
// Declare the dead-letter queue and the binding, or `deadLetter` keeps nothing.
const orderDlq = defineQueue("order-processing-dlq");

const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// Event pattern: publisher broadcasts, consumers subscribe
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// Compose contract - exchanges, queues, bindings auto-extracted
export const contract = defineContract({
  publishers: {
    // EventPublisherConfig → auto-extracted to publisher
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    // EventConsumerResult → auto-extracted to consumer + binding
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
  // Standalone topology: the DLQ is declared, never consumed
  queues: { orderDlq },
  bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

```typescript [2. Publish]
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract.js";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();

await client
  .publish("orderCreated", {
    orderId: "ORD-123", // ✅ TypeScript knows!
    amount: 99.99,
  })
  .getOrThrow();
```

```typescript [3. Consume]
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { contract } from "./contract.js";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => {
      console.log(payload.orderId); // ✅ Fully typed!
      return OkAsync();
    },
  },
  urls: ["amqp://localhost"],
}).get();
```

:::
