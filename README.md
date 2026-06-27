<div align="center">

# amqp-contract

**Type-safe contracts for [AMQP](https://www.amqp.org/)/[RabbitMQ](https://www.rabbitmq.com/) messaging with [TypeScript](https://www.typescriptlang.org/)**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/contract.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/contract)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/contract.svg)](https://www.npmjs.com/package/@amqp-contract/contract)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[**Documentation**](https://btravstack.github.io/amqp-contract) · [**Get Started**](https://btravstack.github.io/amqp-contract/guide/getting-started) · [**Examples**](https://btravstack.github.io/amqp-contract/examples/)

</div>

## Why amqp-contract?

Define your AMQP contracts once — get **type safety**, **autocompletion**, and **runtime validation** everywhere.

- 🔒 **End-to-end type safety** — TypeScript knows your message shapes
- 🔄 **Reliable retry** — Built-in exponential backoff with Dead Letter Queue support
- 📄 **AsyncAPI compatible** — Generate documentation from your contracts

## Quick Example

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { Ok } from "unthrown";
import { z } from "zod";

// 1. Define resources with Dead Letter Exchange and retry configuration
const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx, routingKey: "order.failed" },
  retry: { mode: "ttl-backoff", maxRetries: 3, initialDelayMs: 1000 }, // Retry configured at queue level
});

// 2. Define message with schema validation
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// 3. Event pattern: publisher broadcasts, consumers subscribe
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// 4. Define contract - only publishers and consumers needed
//    Exchanges, queues, and bindings are automatically extracted
const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});

// 6. Type-safe publishing with validation
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
  })
).unwrap();

await client.publish("orderCreated", {
  orderId: "ORD-123", // ✅ TypeScript knows!
  amount: 99.99,
});

// 7. Type-safe consuming with automatic retry (configured at queue level)
const worker = (
  await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: ({ payload }) => {
        console.log(payload.orderId); // ✅ TypeScript knows!
        return Ok(undefined).toAsync();
      },
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

## Installation

```bash
pnpm add @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown
```

[`unthrown`](https://github.com/btravstack/unthrown) is exposed in the public types (`AsyncResult<void, HandlerError>`), so consumers need it directly to construct handler results.

## Documentation

📖 **[Full Documentation →](https://btravstack.github.io/amqp-contract)**

- [Get Started](https://btravstack.github.io/amqp-contract/guide/getting-started) — Get running in 5 minutes
- [Core Concepts](https://btravstack.github.io/amqp-contract/guide/core-concepts) — Understand the fundamentals
- [Examples](https://btravstack.github.io/amqp-contract/examples/) — Real-world usage patterns

## Packages

| Package                                        | Description                           |
| ---------------------------------------------- | ------------------------------------- |
| [@amqp-contract/contract](./packages/contract) | Contract builder and type definitions |
| [@amqp-contract/client](./packages/client)     | Type-safe client for publishing       |
| [@amqp-contract/worker](./packages/worker)     | Type-safe worker with retry support   |
| [@amqp-contract/asyncapi](./packages/asyncapi) | AsyncAPI 3.0 generator                |

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
