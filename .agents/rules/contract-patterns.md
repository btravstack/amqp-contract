# Contract Patterns

## Contract Composition

Resources are defined individually then composed into a contract. `defineContract` only accepts `publishers` and `consumers` — exchanges, queues, and bindings are automatically extracted and inferred:

```typescript
const dlx = defineExchange("orders-dlx", { type: "direct" });
const exchange = defineExchange("orders");
const queue = defineQueue("processing", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 5 },
});
const message = defineMessage(z.object({ orderId: z.string() }));

// Define event publisher
const orderCreatedEvent = defineEventPublisher(exchange, message, { routingKey: "order.created" });

// Compose contract — only publishers and consumers are specified
// Exchanges, queues, and bindings are automatically extracted
const contract = defineContract({
  publishers: { orderCreated: orderCreatedEvent },
  consumers: { processOrder: defineEventConsumer(orderCreatedEvent, queue) },
});

// contract.exchanges contains: { orders: exchange, 'orders-dlx': dlx }
// contract.queues contains: { processing: queue }
// contract.bindings contains: { processOrderBinding: ... }
```

## Event and Command Patterns

| Pattern     | Use Case                                   | Flow                                               |
| ----------- | ------------------------------------------ | -------------------------------------------------- |
| **Event**   | One publisher, many consumers (broadcast)  | `defineEventPublisher` → `defineEventConsumer`     |
| **Command** | Many publishers, one consumer (task queue) | `defineCommandConsumer` → `defineCommandPublisher` |

```typescript
// Event Pattern: Publisher broadcasts, multiple consumers subscribe
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

// Consumer can override routing key for topic exchanges
const allOrdersConsumer = defineEventConsumer(orderCreatedEvent, allOrdersQueue, {
  routingKey: "order.*", // Pattern to receive multiple events
});

// Command Pattern: Consumer owns the queue, publishers send to it
const processOrderCommand = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
  routingKey: "order.process",
});

// For topic exchanges, publisher can specify concrete routing key
const createOrderPublisher = defineCommandPublisher(processOrderCommand, {
  routingKey: "order.create",
});

// Compose contract — only publishers and consumers are specified
const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
    createOrder: createOrderPublisher,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, processingQueue),
    allOrders: allOrdersConsumer,
    handleOrder: processOrderCommand,
  },
});
// contract.exchanges, contract.queues, and contract.bindings are auto-populated
```

## Exchange Types

- Use appropriate exchange type: `topic`, `direct`, `fanout`, or `headers`
- **Topic exchanges are the default** and are most flexible for routing patterns
- Direct exchanges for simple point-to-point messaging
- Fanout exchanges for broadcast messaging
- Headers exchanges for complex routing scenarios

## Queue Types

- **Quorum queues are the default** and recommended for most use cases
- Use `type: 'quorum'` (default) for reliable, replicated queues (always durable, do not support exclusive, auto-deleting, or priority queues)
- Use `type: 'classic'` only for special cases (non-durable, exclusive, auto-deleting, or priority queues)

```typescript
// Quorum queue (default, recommended)
const orderQueue = defineQueue("orders", {
  type: "quorum", // default, can be omitted
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 }, // Dead-letter after 3 retry attempts
});

// Classic queue for special cases only
const priorityQueue = defineQueue("priority-tasks", {
  type: "classic",
  maxPriority: 10, // Only supported with classic queues
});
```

## Bindings

- Queue-to-exchange bindings are **auto-generated** by `defineEventConsumer` and `defineCommandConsumer`
- Exchange-to-exchange bindings are **auto-generated** when using `bridgeExchange` (see Bridge Exchange below)
- For other exchange-to-exchange routing, declare them explicitly with `defineExchangeBinding` and add the result to `bindings`
- For fanout exchanges, routing keys are optional

```typescript
// Bindings are auto-generated from event/command consumers:
const consumer = defineEventConsumer(orderCreatedEvent, orderProcessingQueue);
// This auto-generates: orderProcessingQueue → ordersExchange (order.created)

// Bridge exchange auto-generates exchange-to-exchange binding:
const bridgedConsumer = defineEventConsumer(orderCreatedEvent, billingQueue, {
  bridgeExchange: billingExchange,
});
// This auto-generates: billingQueue → billingExchange AND ordersExchange → billingExchange

// Manual exchange-to-exchange binding (via channel setup, for non-bridge cases)
const exchangeBinding = defineExchangeBinding(analyticsExchange, ordersExchange, {
  routingKey: "order.#", // Forward all order events
});
```

## Bridge Exchange (Cross-Domain Communication)

Bridge exchanges enable cross-domain messaging by routing through a local exchange that forwards to or receives from a remote exchange. Both exchanges and the exchange-to-exchange binding are auto-extracted by `defineContract`.

- **Event consumer bridging**: `defineEventConsumer(event, queue, { bridgeExchange })` — queue binds to bridge, e2e binding from source → bridge
- **Command publisher bridging**: `defineCommandPublisher(command, { bridgeExchange })` — publisher publishes to bridge, e2e binding from bridge → target
- Bridge exchange type must be compatible with source: fanout↔fanout, topic/direct↔topic/direct

```typescript
// Consuming events from a remote domain via bridge
const ordersExchange = defineExchange("orders");
const billingExchange = defineExchange("billing");
const billingQueue = defineQueue("billing-orders");

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const contract = defineContract({
  consumers: {
    processOrder: defineEventConsumer(orderCreated, billingQueue, {
      bridgeExchange: billingExchange,
    }),
  },
});
// contract.exchanges: { orders, billing }
// contract.bindings: queue binding + exchange-to-exchange binding (both auto-generated)

// Publishing commands to a remote domain via bridge
const remoteExchange = defineExchange("remote");
const localExchange = defineExchange("local");

const command = defineCommandConsumer(remoteQueue, remoteExchange, message, {
  routingKey: "cmd.run",
});

const contract = defineContract({
  publishers: {
    runCommand: defineCommandPublisher(command, { bridgeExchange: localExchange }),
  },
});
// Publisher publishes to localExchange, e2e binding forwards to remoteExchange
```

## Routing Keys

- Use meaningful, hierarchical routing keys (e.g., `order.created`, `order.updated`)
- Topic patterns: `#` matches zero or more words, `*` matches exactly one word
- Document routing key patterns in comments

## Message Schemas

- Always validate both input and output messages
- Use Standard Schema v1 compliant libraries (Zod, Valibot, ArkType)
- Define schemas as const to enable type inference
- Use `defineMessage` to wrap schemas with optional metadata

```typescript
import { defineMessage } from "@amqp-contract/contract";
import { z } from "zod";

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
  }),
  {
    summary: "Order created event",
    description: "Emitted when a new order is created in the system",
  },
);
```

## Retry Configuration

Retry strategy is configured at the queue level in the contract, not at the handler level.

### Immediate-Requeue Mode (Recommended)

Failed messages are requeued immediately. Simpler, no wait queues needed.

```typescript
const queue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 5 },
});
```

### TTL-Backoff Mode

Uses wait queues with exponential backoff. Infrastructure is **automatically generated** when `defineQueue` is called with TTL-backoff retry.

```typescript
const queue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: {
    mode: "ttl-backoff",
    maxRetries: 5,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    jitter: true,
  },
});
```

### None Mode (Default)

No retry attempts are made. Failed messages are sent directly to DLQ via `nack(requeue=false)` (or dropped if no DLX configured).

```typescript
const queue = defineQueue("orders", {
  deadLetter: { exchange: dlx, routingKey: "failed" },
  retry: { mode: "none" },
});
```

Omitting `retry` defaults to `mode: "none"`.

### Accessing Queue Properties

When retry is configured with TTL-backoff mode, `defineQueue` returns a wrapper object. Use `extractQueue()` to access the underlying queue definition:

```typescript
import { extractQueue } from "@amqp-contract/contract";
const queueName = extractQueue(queue).name;
```

## Type Inference Helpers

The `Infer*` naming pattern indicates type inference helpers that extract types from a contract at compile time. The full set lives in the package `index.ts` files; the most common ones:

- `ClientInferPublisherInput<Contract, "publisherName">` — input shape for `client.publish(...)`
- `ClientInferRpcRequestInput<Contract, "rpcName">` — input shape for `client.call(...)`
- `ClientInferRpcResponseOutput<Contract, "rpcName">` — typed response from `client.call(...)`
- `WorkerInferConsumedMessage<Contract, "consumerName">` — `{ payload, headers }` envelope for a regular consumer
- `WorkerInferRpcConsumedMessage<Contract, "rpcName">` — `{ payload, headers }` envelope for an RPC handler
- `WorkerInferConsumerHandler<Contract, "consumerName">` — handler signature for a regular consumer
- `WorkerInferRpcHandler<Contract, "rpcName">` — handler signature for an RPC
- `WorkerInferHandlers<Contract>` — full handlers object expected by `TypedAmqpWorker.create`
