# @amqp-contract/core

**Core utilities for AMQP setup and management in amqp-contract.**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/core.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/core)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/core.svg)](https://www.npmjs.com/package/@amqp-contract/core)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This package provides centralized functionality for establishing AMQP topology (exchanges, queues, and bindings) from contract definitions, and defines the `Logger` interface used across amqp-contract packages.

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract)**

## Installation

```bash
npm install @amqp-contract/core
# or
pnpm add @amqp-contract/core
# or
yarn add @amqp-contract/core
```

## Usage

### AmqpClient

The core package exports an `AmqpClient` class that handles the creation of all AMQP resources defined in a contract.

```typescript
// contract.ts
import {
  defineContract,
  defineEventPublisher,
  defineEventConsumer,
  defineExchange,
  defineQueue,
  defineQueueBinding,
  defineMessage,
} from "@amqp-contract/contract";
import { z } from "zod";

// Define resources
const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
});
// A dead-letter exchange with nothing bound to it discards every rejected
// message, so declare the DLQ and bind it. `orders-dlx` is topic, so `#`
// catches whatever routing key the message arrived with.
const orderDlq = defineQueue("order-processing-dlq");
const orderMessage = defineMessage(z.object({ orderId: z.string() }));

const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// Define your contract
export const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
  queues: { orderDlq },
  bindings: { orderDlqBinding: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

Hand that contract to an `AmqpClient`, which asserts its topology on connect:

```typescript
import { AmqpClient } from "@amqp-contract/core";

import { contract } from "./contract.js";

// Setup AMQP resources
const amqpClient = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
});

// Clean up — close() returns an AsyncResult with an empty error channel
await amqpClient.close().get();
```

For advanced channel configuration options (custom setup, prefetch, publisher confirms), see the [Channel Configuration Guide](https://btravstack.github.io/amqp-contract/how-to/configure-channels).

### Logger Interface

The core package exports a `Logger` interface that can be used to implement custom logging for AMQP operations:

```typescript
import type { Logger } from "@amqp-contract/core";

const logger: Logger = {
  debug: (message, context) => console.debug(message, context),
  info: (message, context) => console.info(message, context),
  warn: (message, context) => console.warn(message, context),
  error: (message, context) => console.error(message, context),
};

// Pass the logger to client or worker
import { TypedAmqpClient } from "@amqp-contract/client";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  logger, // Optional: logs published messages
}).getOrThrow();
```

## API

For complete API documentation, see the [@amqp-contract/core API Reference](https://btravstack.github.io/amqp-contract/api/core).

## Documentation

📖 **[Read the full documentation →](https://btravstack.github.io/amqp-contract)**

## License

MIT
