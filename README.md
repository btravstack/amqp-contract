<div align="center">

<img src="docs/public/logo.svg" alt="amqp-contract" width="128" height="128" />

# amqp-contract

**Type-safe contracts for [AMQP](https://www.amqp.org/)/[RabbitMQ](https://www.rabbitmq.com/) messaging with [TypeScript](https://www.typescriptlang.org/)**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/contract.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/contract)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/contract.svg)](https://www.npmjs.com/package/@amqp-contract/contract)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[**Documentation**](https://btravstack.github.io/amqp-contract) · [**Get Started**](https://btravstack.github.io/amqp-contract/tutorial/getting-started) · [**Examples**](https://btravstack.github.io/amqp-contract/examples/)

</div>

## Why amqp-contract?

Define your AMQP contracts once — get **type safety**, **autocompletion**, and **runtime validation** everywhere.

- 🔒 **End-to-end type safety** — TypeScript knows your message shapes
- 🔄 **Reliable retry** — Built-in exponential backoff with Dead Letter Queue support
- 📄 **AsyncAPI compatible** — Generate documentation from your contracts

## Quick Example

```typescript
// contract.ts
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
// Every consumed queue needs a dead-letter exchange (or an explicit
// `onPoison: "drop"`), and that exchange must route somewhere — defineContract
// rejects a contract that would silently lose rejected messages.
const orderQueue = defineQueue("order-processing", { deadLetter: { exchange: ordersDlx } });
const orderDlq = defineQueue("order-processing-dlq");

const orderMessage = defineMessage(z.object({ orderId: z.string(), amount: z.number() }));
const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  queues: { orderDlq },
  bindings: { orderDlq: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

Then use that contract — the worker consumes, the client publishes:

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";

import { contract } from "./contract.js";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ input: { payload } }) => {
      console.log(payload.orderId); // typed from the schema
      return OkAsync(undefined);
    },
  },
  urls: ["amqp://localhost"],
}).getOrThrow();

const client = await TypedAmqpClient.create({ contract, urls: ["amqp://localhost"] }).getOrThrow();

// Validated against the schema before it is sent; returns a Result, never throws.
await client.publish("orderCreated", { orderId: "ORD-123", amount: 99.99 }).getOrThrow();

await client.close().get();
await worker.close().get();
```

▶ For the full runnable version (including the RabbitMQ Docker command), follow the [fifteen-minute tutorial](https://btravstack.github.io/amqp-contract/tutorial/getting-started).

## Installation

> [!NOTE]
> This README describes **amqp-contract 3.x**, which is published under the `beta` npm tag until 3.0 is stable — `latest` is still 2.x, and the root of the documentation site documents 2.x (the 3.x docs are at [/beta/](https://btravstack.github.io/amqp-contract/beta/)). Install 3.x with:
>
> ```bash
> pnpm add @amqp-contract/contract@beta @amqp-contract/client@beta @amqp-contract/worker@beta unthrown zod
> ```

Requires **Node.js 22.22+**.

```bash
pnpm add @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown zod
```

[`unthrown`](https://github.com/btravstack/unthrown) is exposed in the public types (`AsyncResult<void, HandlerError>`), so consumers need it directly to construct handler results. `zod` can be swapped for any [Standard Schema](https://standardschema.dev/) library ([Valibot](https://valibot.dev/), [ArkType](https://arktype.io/), …).

Need a local RabbitMQ to try it against?

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## Documentation

📖 **[Full Documentation →](https://btravstack.github.io/amqp-contract)**

- [Get Started](https://btravstack.github.io/amqp-contract/tutorial/getting-started) — Get running in fifteen minutes
- [Core Concepts](https://btravstack.github.io/amqp-contract/explanation/core-concepts) — Understand the fundamentals
- [Examples](https://btravstack.github.io/amqp-contract/examples/) — Real-world usage patterns

## Packages

| Package                                        | Description                                            |
| ---------------------------------------------- | ------------------------------------------------------ |
| [@amqp-contract/contract](./packages/contract) | Contract builder and type definitions                  |
| [@amqp-contract/client](./packages/client)     | Type-safe client for publishing                        |
| [@amqp-contract/worker](./packages/worker)     | Type-safe worker with retry support                    |
| [@amqp-contract/core](./packages/core)         | Shared runtime: topology setup, connections, telemetry |
| [@amqp-contract/asyncapi](./packages/asyncapi) | AsyncAPI 3.1 generator                                 |
| [@amqp-contract/testing](./packages/testing)   | Vitest utilities with a RabbitMQ testcontainer         |

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT
