# Contract Patterns

## Contract Composition

Resources are defined individually then composed into a contract. `defineContract` only accepts `publishers` and `consumers` — exchanges, queues, and bindings are automatically extracted and inferred:

```typescript
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

const dlx = defineExchange("orders-dlx", { type: "direct" });
const exchange = defineExchange("orders");
// A DLX with nothing bound to it is rejected by `defineContract`: RabbitMQ
// discards a message routed to zero queues. Set `deadLetter.routingKey` and
// bind that exact key. Without it a dead letter keeps whatever key it arrived
// under — which a `ttl-backoff` retry or a `classic` queue's immediate requeue
// rewrites to the queue name — and on a DIRECT exchange `#` is a literal key,
// not a wildcard, so it would match nothing.
const queue = defineQueue("processing", {
  deadLetter: { exchange: dlx, routingKey: "processing.dlq" },
  retry: { mode: "immediate-requeue", maxRetries: 5 },
});
const dlq = defineQueue("processing-dlq");
const message = defineMessage(z.object({ orderId: z.string() }));

// Define event publisher
const orderCreatedEvent = defineEventPublisher(exchange, message, { routingKey: "order.created" });

// Compose contract — publishers, consumers, plus the standalone DLQ topology
// Exchanges, queues, and bindings are otherwise automatically extracted
const contract = defineContract({
  publishers: { orderCreated: orderCreatedEvent },
  consumers: { processOrder: defineEventConsumer(orderCreatedEvent, queue) },
  queues: { dlq },
  bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "processing.dlq" }) },
});

// contract.exchanges contains: { orders: exchange, 'orders-dlx': dlx }
// contract.queues contains: { processing: queue, 'processing-dlq': dlq }
// contract.bindings contains: { processOrderBinding: ..., dlqBinding: ... }
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
- Every **consumed** queue needs a `deadLetter` — or an explicit `onPoison: "drop"`. `defineContract` throws otherwise, because a consumed queue with neither discards every rejected message with no record. Declared-but-unconsumed queues (dead-letter queues included) are exempt; a DLQ you _do_ consume needs `onPoison: "drop"`, since it cannot dead-letter to itself.
- Every `deadLetter` exchange needs **something bound to it**. `defineContract` throws for a DLX that routes nowhere, because RabbitMQ discards a message routed to zero queues — the loss is identical to having no DLX, while the worker logs a reassuring `Sending message to DLQ`. Bind a DLQ, or set `externalConsumers: true` on the `deadLetter` config when another service owns that queue. On a **direct** DLX bind the actual key: `#` is a topic wildcard and matches nothing there. A DLX declaring an `alternate-exchange` argument is exempt, as it is for publishers — the broker catches its unmatched messages.

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
const billingDlx = defineExchange("billing-dlx");
const billingQueue = defineQueue("billing-orders", { deadLetter: { exchange: billingDlx } });
// `billing-dlx` is topic and the queue sets no dead-letter routing key, so `#`
// catches whatever key the message arrived with.
const billingDlq = defineQueue("billing-orders-dlq");

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const contract = defineContract({
  consumers: {
    processOrder: defineEventConsumer(orderCreated, billingQueue, {
      bridgeExchange: billingExchange,
    }),
  },
  queues: { billingDlq },
  bindings: { billingDlqBinding: defineQueueBinding(billingDlq, billingDlx, { routingKey: "#" }) },
});
// contract.exchanges: { orders, billing, 'billing-dlx' }
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

Uses per-delay-tier wait queues with exponential backoff. The wait-queue topology is **derived, never stored**: `defineQueue` returns a plain `QueueDefinition`, and `setupAmqpTopology` derives one wait queue per distinct delay (`{queue}-wait-{delayMs}ms`) from the retry config at channel-setup time via `deriveTtlBackoffInfrastructure` (packages/contract/src/builder/ttl-backoff.ts).

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

`defineQueue` always returns a plain `QueueDefinition` — access properties directly (`queue.name`, `queue.type`). The pre-3.0 `extractQueue()` / `QueueEntry` wrapper split is gone.

## Type Inference Helpers

The `Infer*` naming pattern indicates type inference helpers that extract types from a contract at compile time.

**Re-exported from `@amqp-contract/client`:**

- `ClientInferPublisherInput<Contract, "publisherName">` — input shape for `client.publish(...)`
- `ClientInferRpcRequestInput<Contract, "rpcName">` — input shape for `client.call(...)`
- `ClientInferRpcResponseOutput<Contract, "rpcName">` — typed response from `client.call(...)`
- `ClientInferRpcErrors<Contract, "rpcName">` — union of declared `RpcError<code, data>` members in `client.call(...)`'s error channel (`never` when the RPC declares no `errors`)

**Re-exported from `@amqp-contract/worker`:**

- `WorkerInferConsumerHandler<Contract, "consumerName">` — handler signature for a regular consumer
- `WorkerInferConsumedMessage<Contract, "consumerName">` — `{ payload, headers }` envelope for a regular consumer
- `WorkerInferConsumerHeaders<Contract, "consumerName">` — just the headers slice
- `WorkerInferConsumerHandlerEntry<Contract, "consumerName">` — handler-or-`[handler, opts]` tuple shape
- The RPC equivalents (`WorkerInferRpcHandler`, `WorkerInferRpcConsumedMessage`, `WorkerInferRpcRequest`, `WorkerInferRpcResponse`, `WorkerInferRpcHeaders`, `WorkerInferRpcErrors`) and the unified `WorkerInferHandlers<Contract>` — see `packages/worker/src/index.ts`. Inline RPC handlers rarely need them: the `handlers` parameter on `TypedAmqpWorker.create` infers each name's signature automatically.
