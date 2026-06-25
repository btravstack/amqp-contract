# @amqp-contract/contract

**Contract builder for amqp-contract - Define type-safe AMQP messaging contracts.**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/contract.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/contract)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/contract.svg)](https://www.npmjs.com/package/@amqp-contract/contract)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/api/contract)**

## Installation

```bash
pnpm add @amqp-contract/contract
```

## Quick Start

### Recommended: Event / Command Patterns

For robust contract definitions with guaranteed consistency, use Event or Command patterns:

| Pattern     | Use Case                                   | Flow                                               |
| ----------- | ------------------------------------------ | -------------------------------------------------- |
| **Event**   | One publisher, many consumers (broadcast)  | `defineEventPublisher` → `defineEventConsumer`     |
| **Command** | Many publishers, one consumer (task queue) | `defineCommandConsumer` → `defineCommandPublisher` |
| **RPC**     | Request / response with typed reply        | `defineRpc` (single bidirectional definition)      |

```typescript
import {
  defineEventPublisher,
  defineEventConsumer,
  defineCommandConsumer,
  defineCommandPublisher,
  defineContract,
  defineExchange,
  defineQueue,
  defineMessage,
} from "@amqp-contract/contract";
import { z } from "zod";

// Event pattern: publisher broadcasts, consumers subscribe
const ordersExchange = defineExchange("orders");
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// Define event publisher
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// Multiple queues can consume the same event
const orderQueue = defineQueue("order-processing");
const analyticsQueue = defineQueue("analytics");

// Compose contract - exchanges, queues, bindings auto-extracted
const contract = defineContract({
  publishers: {
    // EventPublisherConfig → auto-extracted to publisher
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    // EventConsumerResult → auto-extracted to consumer + binding
    processOrder: defineEventConsumer(orderCreatedEvent, orderQueue),
    // For topic exchanges, consumers can override with their own pattern
    trackOrders: defineEventConsumer(orderCreatedEvent, analyticsQueue, {
      routingKey: "order.*", // Subscribe to all order events
    }),
  },
});
```

### RPC Pattern

Use `defineRpc` for typed request/response calls. RPC is bidirectional on both
ends — the worker handler consumes the request and produces a typed response;
the client awaits it via `client.call(name, request, { timeoutMs })`. Both
ends share the same definition, and RPCs live in their own `rpcs` slot of the
contract (not `publishers` or `consumers`). RabbitMQ direct reply-to is used
under the hood, so no reply queue declaration is needed.

```typescript
import { defineContract, defineMessage, defineQueue, defineRpc } from "@amqp-contract/contract";
import { z } from "zod";

const calculate = defineRpc(defineQueue("rpc.calculate"), {
  request: defineMessage(z.object({ a: z.number(), b: z.number() })),
  response: defineMessage(z.object({ sum: z.number() })),
});

const contract = defineContract({
  rpcs: { calculate },
});

// Server handler returns the response value, not void:
//   handlers: { calculate: ({ payload }) => okAsync({ sum: payload.a + payload.b }) }
//
// Client invokes with a required timeout:
//   const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 });
```

**Benefits:**

- ✅ Guaranteed message schema consistency between publishers and consumers
- ✅ Routing key validation and type safety
- ✅ Full type safety with TypeScript inference
- ✅ Event, command, and RPC patterns
- ✅ Flexible routing key patterns for topic exchanges

## Documentation

📖 **[Read the full documentation →](https://btravstack.github.io/amqp-contract)**

- [Getting Started Guide](https://btravstack.github.io/amqp-contract/guide/defining-contracts)
- [Event Pattern](https://btravstack.github.io/amqp-contract/guide/defining-contracts#event-pattern)
- [Command Pattern](https://btravstack.github.io/amqp-contract/guide/defining-contracts#command-pattern)
- [Complete API Reference](https://btravstack.github.io/amqp-contract/api/contract)

## License

MIT
