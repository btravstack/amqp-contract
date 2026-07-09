---
title: Why amqp-contract? - Type-safe AMQP Messaging for TypeScript and Node.js
description: Discover why amqp-contract is the best solution for building type-safe RabbitMQ and AMQP messaging applications with TypeScript and Node.js. Learn about contract-first development and schema validation.
---

# Why amqp-contract?

Working with [RabbitMQ](https://www.rabbitmq.com/) and AMQP messaging is powerful and flexible, but it comes with significant challenges when building TypeScript applications. **amqp-contract** solves these problems by bringing a contract-first, type-safe approach to AMQP messaging.

## The Problem

Traditional AMQP development in TypeScript lacks type safety and validation, leading to several issues:

### 1. No Type Safety

Without types, you're working blind:

```typescript
// ❌ Traditional approach - no type safety
channel.publish(
  "orders",
  "order.created",
  Buffer.from(
    JSON.stringify({
      orderId: "ORD-123", // What fields are required? What types?
    }),
  ),
);

channel.consume("order-processing", (msg) => {
  const data = JSON.parse(msg.content.toString()); // unknown type
  console.log(data.orderId); // No autocomplete, no type checking
  // Is it orderId or order_id? Did the field change?
});
```

### 2. Manual Validation Everywhere

You must validate messages manually at every boundary:

```typescript
// ❌ Validation scattered throughout the codebase
function validateOrder(data: unknown): Order {
  if (!data || typeof data !== "object") {
    throw new Error("Invalid data");
  }
  const order = data as any;
  if (typeof order.orderId !== "string") {
    throw new Error("orderId must be a string");
  }
  // ... dozens more checks ...
  return order as Order;
}
```

### 3. Runtime Errors from Wrong Data

Without validation, invalid messages cause runtime failures:

```typescript
// ❌ No validation - crashes at runtime
channel.consume("orders", (msg) => {
  const order = JSON.parse(msg.content.toString());
  processPayment(order.amount); // TypeError: amount is undefined
});
```

### 4. Scattered Message Definitions

Message schemas are duplicated across services:

```typescript
// ❌ Duplicated schemas in multiple files
// publisher.ts
interface OrderEvent {
  orderId: string;
  amount: number;
}

// consumer.ts
interface OrderEvent {
  // Same schema, different file
  orderId: string;
  amount: number;
}
```

### 5. Difficult Refactoring

Changing a message schema means hunting through multiple files:

```typescript
// ❌ Change orderId to id - must update everywhere manually
// No compile-time checks to catch all usages
```

## The Solution

**amqp-contract** transforms AMQP development with a contract-first approach:

### 1. End-to-End Type Safety

Define your contract once, get types everywhere:

```typescript
import {
  defineContract,
  defineExchange,
  defineQueue,
  defineEventPublisher,
  defineEventConsumer,
  definePublisher,
  defineMessage,
} from "@amqp-contract/contract";
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { z } from "zod";

// 1. Define resources
const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing");

// 2. Define message schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    customerId: z.string(),
    amount: z.number().positive(),
  }),
);

// 3. Event pattern for broadcast messaging
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// 4. Create contract - exchanges, queues, bindings auto-extracted
const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});

// 4. Publisher gets full type safety
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).unwrapOrElse((e) => {
  throw e;
});

await client.publish("orderCreated", {
  orderId: "ORD-123", // ✅ TypeScript knows these fields!
  customerId: "CUST-456", // ✅ Autocomplete works!
  amount: 99.99, // ✅ Type checked at compile time!
});

// 5. Consumer gets fully typed payloads
const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => {
      console.log(payload.orderId); // ✅ Fully typed!
      console.log(payload.customerId); // ✅ Autocomplete!
      console.log(payload.amount); // ✅ Type safe!
      return Ok(undefined).toAsync();
    },
  },
  urls: ["amqp://localhost"],
}).unwrapOrElse((e) => {
  throw e;
});
```

### 2. Automatic Validation

Schema validation happens automatically at network boundaries:

```typescript
// ✅ Validation happens automatically
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  amount: -10, // ❌ Validation error: amount must be positive
});

result.match({
  ok: () => console.log("Published"),
  err: (error) => console.error("Validation failed:", error),
  defect: (cause) => {
    throw cause;
  },
});
```

### 3. Compile-Time Checks

TypeScript catches errors before runtime:

```typescript
// ❌ TypeScript error at compile time
await client.publish("orderCreated", {
  orderId: "ORD-123",
  // Missing customerId and amount - TypeScript error!
});

// ❌ TypeScript error for wrong types
await client.publish("orderCreated", {
  orderId: 123, // Error: orderId must be string
  customerId: "CUST-456",
  amount: 99.99,
});
```

### 4. Single Source of Truth

Your contract is the single source of truth:

```typescript
// ✅ One contract definition using event pattern
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const contract = defineContract({
  // Define once, use across all publishers and consumers
  // Publisher and consumer guaranteed to use same schema
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});
```

### 5. Safe Refactoring

Refactoring is safe and guided by TypeScript:

```typescript
// Change the schema
const orderMessage = defineMessage(
  z.object({
    id: z.string(), // Changed from orderId to id
    customerId: z.string(),
    amount: z.number().positive(),
  }),
);

// TypeScript immediately shows all places that need updates:
// - Publisher calls
// - Consumer handlers
// - Type definitions
```

## Key Benefits

### Better Developer Experience

- **Autocomplete** - Your IDE knows all message fields and types
- **Inline Documentation** - Hover over fields to see schemas
- **Refactoring Support** - Rename fields safely across the codebase
- **Jump to Definition** - Navigate from usage to contract definition

### Compile-Time Safety

- **Catch Errors Early** - TypeScript catches issues before runtime
- **Type Inference** - No manual type annotations needed
- **Exhaustive Checks** - Ensure all consumers are implemented

### Runtime Safety

- **Automatic Validation** - [Zod](https://zod.dev/), [Valibot](https://valibot.dev/), or [ArkType](https://arktype.io/) validate messages
- **Explicit Error Handling** - Result types for predictable error handling
- **No Surprises** - Invalid messages are caught at boundaries

### Maintainability

- **Single Source of Truth** - Contract defines everything
- **Documentation** - AsyncAPI generation for API docs
- **Version Control** - Track contract changes in git
- **Clear Boundaries** - Well-defined publisher/consumer interfaces

## Inspired By

This project adapts the contract-first approach from:

- **[tRPC](https://trpc.io/)** - End-to-end type safety for RPC
- **[oRPC](https://orpc.dev/)** - Contract-first RPC with OpenAPI
- **[ts-rest](https://ts-rest.com/)** - Type-safe REST APIs

We've brought their excellent ideas to the world of [RabbitMQ](https://www.rabbitmq.com/) and AMQP messaging.

## When to Use amqp-contract

**Perfect for:**

- ✅ TypeScript projects using RabbitMQ/AMQP
- ✅ Microservices with message-based communication
- ✅ Projects requiring strong type safety
- ✅ Teams that value developer experience
- ✅ Applications with complex message schemas

**Consider alternatives if:**

- ❌ You're not using TypeScript
- ❌ You need extremely low overhead (though validation overhead is minimal)
- ❌ You prefer dynamic, untyped messaging

## Next Steps

Ready to get started?

- **[Getting Started →](/guide/getting-started)** - Install and create your first contract
- **[Core Concepts →](/guide/core-concepts)** - Understand the fundamentals
- **[Examples →](/examples/)** - See real-world usage patterns
