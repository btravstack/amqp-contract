---
title: Core Concepts - Understanding Type-safe AMQP Messaging Contracts
description: Learn the fundamental concepts of amqp-contract including contract-first design, exchanges, queues, publishers, consumers, and schema validation for AMQP/RabbitMQ applications.
---

# Core Concepts

Understanding these core concepts will help you use amqp-contract effectively.

## Contract-First Design

Everything starts with a **contract** that defines:

- **Exchanges** - Where messages are published
- **Queues** - Where messages are stored
- **Bindings** - How queues connect to exchanges
- **Publishers** - What messages can be published
- **Consumers** - What messages can be consumed

Define once, use everywhere with full type safety.

## End-to-End Type Safety

Type safety flows automatically from your contract:

```typescript
import { z } from "zod";

// 1. Define resources and message
const ordersExchange = defineExchange("orders");
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// 2. Event pattern (recommended for broadcasts)
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// 3. Define queue for processing
const orderProcessingQueue = defineQueue("order-processing");

// 4. Compose contract - only publishers and consumers needed
//    Exchanges, queues, and bindings are automatically extracted
const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});

// 5. Client knows exact types
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
  })
).unwrap();

const result = await client.publish("orderCreated", {
  orderId: "ORD-123", // ✅ TypeScript knows!
  amount: 99.99, // ✅ TypeScript knows!
  // invalid: true,     // ❌ TypeScript error!
});

result.match({
  ok: () => console.log("Published"),
  err: (error) => console.error("Failed:", error),
  defect: (cause) => {
    throw cause;
  },
});
```

## Automatic Validation

Messages are validated automatically at network boundaries:

- **On publish**: Client validates before sending
- **On consume**: Worker validates before calling handlers

Invalid messages are caught early with clear error messages.

```typescript
// This returns a validation error (doesn't throw)
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  amount: "not-a-number", // ❌ Validation error!
});

result.match({
  ok: () => console.log("Published"),
  err: (error) => {
    // Handle MessageValidationError or TechnicalError
    console.error("Failed:", error.message);
  },
  defect: (cause) => {
    throw cause;
  },
});
```

## Schema Libraries

amqp-contract uses [Standard Schema](https://github.com/standard-schema/standard-schema), supporting:

- ✅ [Zod](https://zod.dev/) (most popular)
- ✅ [Valibot](https://valibot.dev/)
- ✅ [ArkType](https://arktype.io/)

All examples use [Zod](https://zod.dev/), but you can use any compatible library:

```typescript
import { z } from "zod";
import * as v from "valibot";
import { type } from "arktype";

const ordersExchange = defineExchange("orders");

// All work the same way with defineEventPublisher:
const zodEvent = defineEventPublisher(ordersExchange, defineMessage(z.object({ id: z.string() })), {
  routingKey: "order.created",
});

const valibotEvent = defineEventPublisher(
  ordersExchange,
  defineMessage(v.object({ id: v.string() })),
  { routingKey: "order.created" },
);

const arktypeEvent = defineEventPublisher(ordersExchange, defineMessage(type({ id: "string" })), {
  routingKey: "order.created",
});
```

## AMQP Resources

### Exchanges

Exchanges receive and route messages to queues. By default, exchanges are created as **topic exchanges** and are durable:

```typescript
// Define default topic exchange and durable
const ordersExchange = defineExchange("orders");

// Define exchange with custom options
const tasksExchange = defineExchange("tasks", {
  type: "direct", // one of "topic", "direct", "fanout", "headers" (default: "topic")
  durable: false, // (default: true)
});
```

**Exchange Types:**

- `topic` (default) - Pattern matching with wildcards (`*`, `#`)
- `direct` - Exact routing key match
- `fanout` - Broadcast to all bound queues
- `headers` - Routes based on message headers

### Queues

Queues store messages until consumed. By default, queues are created as **quorum queues** for better durability:

```typescript
// Quorum queue (default, recommended)
const orderProcessingQueue = defineQueue("order-processing");

// Classic queue (for special cases)
const tempQueue = defineQueue("temp-queue", {
  type: "classic",
  durable: false,
});
```

**Queue Types:**

- `quorum` (default) - Better durability and high-availability via Raft consensus (always durable, do not support exclusive, auto-deleting, or priority queues)
- `classic` - Traditional queues for non-durable, exclusive, auto-deleting, or priority queue use cases

### Messages

Messages combine schemas with optional metadata:

```typescript
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
  {
    summary: "Order created event",
    description: "Emitted when a new order is created",
  },
);
```

### Bindings

Bindings connect queues to exchanges:

```typescript
const orderBinding = defineQueueBinding(
  orderProcessingQueue, // queue
  ordersExchange, // exchange
  {
    routingKey: "order.created", // routing pattern
  },
);
```

### Publishers

Publishers define what messages can be published:

```typescript
const orderCreatedPublisher = definePublisher(
  ordersExchange, // exchange
  orderMessage, // message definition
  {
    routingKey: "order.created",
  },
);
```

### Consumers

Consumers define what messages can be consumed:

```typescript
const processOrderConsumer = defineConsumer(
  orderProcessingQueue, // queue
  orderMessage, // message definition
);
```

## Message Flow

Here's how messages flow through the system:

1. **Client publishes** a message
2. Message is **validated** against schema
3. Message sent to **exchange**
4. Exchange routes to **queues** via **bindings**
5. **Worker consumes** from queue
6. Message is **validated** again
7. Handler called with **typed message**
8. Message **acknowledged**

All with automatic type safety and validation!

## Next Steps

- Learn about [Defining Contracts](/guide/defining-contracts)
- Explore [Client Usage](/guide/client-usage)
- Understand [Worker Usage](/guide/worker-usage)
