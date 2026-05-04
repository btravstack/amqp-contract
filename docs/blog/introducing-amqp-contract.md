---
title: "Building Type-Safe AMQP Messaging with amqp-contract"
description: "Discover how amqp-contract brings end-to-end type safety, automatic validation, and AsyncAPI generation to RabbitMQ and AMQP messaging in TypeScript applications"
date: 2025-12-25
author: Benoit TRAVERS
tags: ["TypeScript", "RabbitMQ", "AMQP", "Type Safety", "AsyncAPI", "Microservices"]
---

# Building Type-Safe AMQP Messaging with amqp-contract

If you've worked with [RabbitMQ](https://www.rabbitmq.com/) or AMQP messaging in TypeScript, you've probably experienced the frustration of dealing with untyped messages, scattered validation logic, and the constant fear of runtime errors from mismatched data structures. What if there was a better way?

Today, I'm excited to introduce [**amqp-contract**](https://github.com/btravers/amqp-contract) — a TypeScript library that brings the power of contract-first development, end-to-end type safety, and automatic validation to AMQP messaging.

## The Problem with Traditional AMQP Development

Let's start with a typical scenario. You're building a microservices architecture using RabbitMQ for inter-service communication. Your publisher looks something like this:

```typescript
// ❌ Traditional approach - no type safety
import amqp from "amqplib";

const connection = await amqp.connect("amqp://localhost");
const channel = await connection.createChannel();

await channel.assertExchange("orders", "topic");

// What fields should this have? What types?
channel.publish(
  "orders",
  "order.created",
  Buffer.from(
    JSON.stringify({
      orderId: "ORD-123",
      amount: 99.99,
      // Did I forget any required fields?
    }),
  ),
);
```

And your consumer:

```typescript
// ❌ No type information
channel.consume("order-processing", (msg) => {
  const data = JSON.parse(msg.content.toString()); // unknown type
  console.log(data.orderId); // No autocomplete, no validation
  // Is this the right field name? Who knows!
});
```

This approach has several critical issues:

1. **No Type Safety**: You lose all TypeScript benefits at the messaging boundary
2. **Manual Validation**: You need to manually validate every message, or risk runtime errors
3. **Scattered Definitions**: Message structures are defined implicitly or scattered across your codebase
4. **Refactoring Nightmares**: Change a field name? Good luck finding all the places it's used
5. **Documentation Drift**: Your code and documentation quickly get out of sync

## Enter amqp-contract

**amqp-contract** solves these problems by bringing a contract-first approach to AMQP messaging. Inspired by the excellent [tRPC](https://trpc.io/), [oRPC](https://orpc.dev/), and [ts-rest](https://ts-rest.com/) libraries, it adapts their philosophy of end-to-end type safety to the world of message queues.

Here's what the same code looks like with amqp-contract:

### 1. Define Your Contract

First, define your contract with full type safety using schema validation libraries like [Zod](https://zod.dev/), [Valibot](https://valibot.dev/), or [ArkType](https://arktype.io/):

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
import { z } from "zod";

// Define your AMQP resources
const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing");

// Define your message schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    customerId: z.string(),
    items: z.array(
      z.object({
        productId: z.string(),
        quantity: z.number().int().positive(),
        price: z.number().positive(),
      }),
    ),
    totalAmount: z.number().positive(),
    status: z.enum(["pending", "processing", "completed"]),
  }),
);

// Event pattern ensures consistency
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// Compose your contract - exchanges, queues, bindings auto-extracted
export const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});
```

### 2. Type-Safe Publishing

Now use the contract to create a type-safe client:

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract";

const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
  })
)._unsafeUnwrap();

// ✅ Fully typed! TypeScript knows exactly what fields are required
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  items: [
    {
      productId: "PROD-789",
      quantity: 2,
      price: 49.99,
    },
  ],
  totalAmount: 99.98,
  status: "pending",
  // ✅ TypeScript will error if you forget a required field
  // ✅ TypeScript will error if you use the wrong type
  // ✅ Autocomplete shows you all available fields
});

// Explicit error handling with Result type
result.match(
  () => console.log("✅ Order published successfully"),
  (error) => {
    console.error("❌ Failed to publish order:", error);
    // error is either TechnicalError or MessageValidationError
  },
);
```

### 3. Type-Safe Consuming

And create a type-safe worker for consuming messages:

```typescript
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { contract } from "./contract";

const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      // ✅ payload is fully typed based on your schema
      processOrder: ({ payload }) => {
        console.log(`Processing order: ${payload.orderId}`);
        console.log(`Customer: ${payload.customerId}`);
        console.log(`Total: $${payload.totalAmount}`);

        // ✅ Full autocomplete for all fields
        payload.items.forEach((item) => {
          console.log(`- ${item.quantity}x Product ${item.productId}`);
        });

        // ✅ TypeScript catches typos and wrong field names
        // console.log(payload.ordreId); // ❌ TypeScript error!
        return okAsync(undefined);
      },
    },
    urls: ["amqp://localhost"],
  })
)._unsafeUnwrap();

console.log("✅ Worker ready");
```

## Key Features That Make amqp-contract Special

### 🔒 End-to-End Type Safety

TypeScript types flow automatically from your contract to publishers and consumers. No manual type annotations needed. If you refactor your schema, TypeScript immediately shows you every place that needs updating.

### ✅ Automatic Validation

Messages are automatically validated at network boundaries using [Standard Schema v1](https://github.com/standard-schema/standard-schema). This works with Zod, Valibot, and ArkType, giving you the flexibility to choose your preferred validation library.

```typescript
// If you try to publish invalid data, you get immediate feedback
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  items: [],
  totalAmount: -50, // ❌ Validation error - must be positive
  status: "invalid", // ❌ Validation error - must be pending/processing/completed
});

result.match(
  () => {},
  (error) => {
    if (error instanceof MessageValidationError) {
      console.log("Validation issues:", error.issues);
    }
  },
);
```

### 🛠️ Compile-Time Checks

TypeScript catches errors before runtime:

```typescript
// ❌ TypeScript error - "orderDeleted" doesn't exist in contract
await client.publish("orderDeleted", { orderId: "123" });

// ❌ TypeScript error - missing handler for "processOrder"
await TypedAmqpWorker.create({
  contract,
  handlers: {
    // forgot processOrder!
  },
  urls: ["amqp://localhost"],
});
```

### 📄 AsyncAPI 3.0 Generation

Automatically generate [AsyncAPI](https://www.asyncapi.com/) specifications from your contracts for documentation and tooling:

```typescript
import { AsyncAPIGenerator } from "@amqp-contract/asyncapi";
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
import { contract } from "./contract";

const generator = new AsyncAPIGenerator({
  schemaConverters: [new ZodToJsonSchemaConverter()],
});

const spec = await generator.generate(contract, {
  info: {
    title: "Order Processing API",
    version: "1.0.0",
    description: "AMQP API for order processing",
  },
  servers: {
    production: {
      host: "rabbitmq.example.com:5672",
      protocol: "amqp",
      description: "Production RabbitMQ server",
    },
    development: {
      host: "localhost:5672",
      protocol: "amqp",
      description: "Local development server",
    },
  },
});

// Save to file or use with AsyncAPI tools
console.log(JSON.stringify(spec, null, 2));
```

## Real-World Use Cases

### E-Commerce Order Processing

```typescript
// Define multiple message types for order lifecycle
const orderCreatedMessage = defineMessage(
  z.object({
    orderId: z.string(),
    customerId: z.string(),
    items: z.array(
      z.object({
        productId: z.string(),
        quantity: z.number(),
        price: z.number(),
      }),
    ),
  }),
);

const orderShippedMessage = defineMessage(
  z.object({
    orderId: z.string(),
    trackingNumber: z.string(),
    carrier: z.enum(["fedex", "ups", "usps"]),
  }),
);

const orderCompletedMessage = defineMessage(
  z.object({
    orderId: z.string(),
    completedAt: z.date(),
  }),
);
```

### Notification System

```typescript
// Type-safe notification routing
const emailQueue = defineQueue("notifications.email");
const smsQueue = defineQueue("notifications.sms");
const pushQueue = defineQueue("notifications.push");

const notificationMessage = defineMessage(
  z.object({
    userId: z.string(),
    title: z.string(),
    body: z.string(),
    priority: z.enum(["low", "medium", "high"]),
  }),
);

// Each queue gets its own typed consumer
const contract = defineContract({
  // ... exchanges and queues
  consumers: {
    sendEmail: defineConsumer(emailQueue, notificationMessage),
    sendSms: defineConsumer(smsQueue, notificationMessage),
    sendPush: defineConsumer(pushQueue, notificationMessage),
  },
});
```

### Event-Driven Microservices

```typescript
// Different services can publish and consume
// with full type safety across service boundaries

// User Service publishes
const userCreatedMessage = defineMessage(
  z.object({
    userId: z.string(),
    email: z.string().email(),
    name: z.string(),
    createdAt: z.date(),
  }),
);

// Email Service, Analytics Service, etc. consume
// All with the same type-safe contract
```

## Why Choose amqp-contract?

### Compared to Raw amqplib

- ✅ Type safety vs ❌ No types
- ✅ Automatic validation vs ❌ Manual validation
- ✅ Compile-time checks vs ❌ Runtime errors
- ✅ Refactoring support vs ❌ Find/replace
- ✅ Documentation from code vs ❌ Manual docs

### Compared to Other Solutions

Unlike other AMQP libraries, amqp-contract:

- Focuses on **type safety first** — types are derived from your contract, not the other way around
- Uses **Standard Schema v1** — compatible with multiple validation libraries (Zod, Valibot, ArkType)
- Generates **AsyncAPI specs** — automatic documentation
- Provides **explicit error handling** — uses Result types instead of throwing exceptions
- Is **framework agnostic** — works standalone with any Node.js application

## Getting Started

### Installation

```bash
# Core packages
pnpm add @amqp-contract/contract @amqp-contract/client @amqp-contract/worker

# Choose your schema library
pnpm add zod  # or valibot, or arktype

# AMQP client
pnpm add amqplib @types/amqplib
```

### Quick Start

1. **Define your contract** with schemas
2. **Create a client** to publish messages
3. **Create a worker** to consume messages
4. **Enjoy type safety** end-to-end!

Check out the [full documentation](https://btravers.github.io/amqp-contract) for detailed guides, API reference, and examples.

## What's Next?

The project is under active development with several exciting features planned:

- Enhanced dead letter queue handling
- More schema library integrations
- Performance optimizations
- Community-driven examples and patterns

## Try It Today!

amqp-contract is [open source](https://github.com/btravers/amqp-contract) (MIT license) and available on npm:

- 📦 [npm package](https://www.npmjs.com/package/@amqp-contract/contract)
- 📖 [Documentation](https://btravers.github.io/amqp-contract)
- 💻 [GitHub repository](https://github.com/btravers/amqp-contract)
- 🌟 [Star on GitHub](https://github.com/btravers/amqp-contract)

If you're building microservices with RabbitMQ or AMQP, I'd love to hear your feedback! Give it a try and let me know what you think. Contributions, issues, and feature requests are always welcome.

## Conclusion

Type safety shouldn't stop at your application boundaries. With amqp-contract, you can bring the same level of type safety and developer experience you enjoy with TypeScript to your AMQP messaging layer.

Stop fighting runtime errors. Stop manually validating messages. Stop worrying about refactoring. Start building type-safe, validated, and maintainable messaging systems today.

---

**Links:**

- [amqp-contract Documentation](https://btravers.github.io/amqp-contract)
- [GitHub Repository](https://github.com/btravers/amqp-contract)
- [npm Package](https://www.npmjs.com/package/@amqp-contract/contract)
- [Getting Started Guide](https://btravers.github.io/amqp-contract/guide/getting-started)
- [Examples](https://btravers.github.io/amqp-contract/examples/)

_Have you tried amqp-contract? Share your experience in the comments below!_
