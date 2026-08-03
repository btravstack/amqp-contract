---
title: Topology options - amqp-contract
description: Option tables for defineExchange, defineQueue, defineMessage, publishers, consumers and RPCs, plus retry defaults and diagnostic headers.
---

# Topology options

Options for every contract-building function. For recipes see [define a contract](/how-to/define-a-contract); for the generated API surface see the [API reference](/api/).

## `defineContract`

`defineContract({ publishers, consumers, rpcs, exchanges, queues, bindings })`

Every key is optional. A worked example, using all six:

```typescript
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { z } from "zod";

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const auditExchange = defineExchange("audit");
const orderMessage = defineMessage(z.object({ orderId: z.string() }));
// `orders-dlx` is topic and neither queue sets a dead-letter routing key, so
// `#` catches whatever key the message arrived with.
const orderQueue = defineQueue("order-processing", { deadLetter: { exchange: ordersDlx } });
const lookupQueue = defineQueue("order-lookup", { deadLetter: { exchange: ordersDlx } });
const ordersDlq = defineQueue("order-processing-dlq");
const auditQueue = defineQueue("order-audit");

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});
const lookupOrder = defineRpc(lookupQueue, {
  request: orderMessage,
  response: defineMessage(z.object({ status: z.string() })),
});

defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderQueue) },
  rpcs: { lookupOrder },
  exchanges: { auditExchange },
  queues: { ordersDlq, auditQueue },
  bindings: {
    ordersDlq: defineQueueBinding(ordersDlq, ordersDlx, { routingKey: "#" }),
    auditBinding: defineQueueBinding(auditQueue, auditExchange, { routingKey: "order.#" }),
  },
});
```

| Key          | Contains                                                                |
| ------------ | ----------------------------------------------------------------------- |
| `publishers` | Named publishers callable via `client.publish(name, …)`                 |
| `consumers`  | Named consumers requiring a handler on the worker                       |
| `rpcs`       | Named RPCs, callable via `client.call(name, …)` and requiring a handler |
| `exchanges`  | Standalone exchanges with no publisher/consumer attached                |
| `queues`     | Standalone queues with no consumer in this service                      |
| `bindings`   | Standalone bindings (`defineQueueBinding` / `defineExchangeBinding`)    |

The returned contract exposes `exchanges`, `queues` and `bindings` extracted from the publishers, consumers and RPCs — you rarely declare them directly. The standalone keys exist for topology this service asserts without attaching a publisher or consumer to it: the classic cases are a DLQ bound to the auto-extracted dead-letter exchange, or an audit queue another process drains. Dead-letter exchanges are auto-extracted for standalone queues exactly as for consumer queues (TTL-backoff wait queues are derived at setup time and never appear in the contract). In the output, standalone exchanges and queues are re-keyed by their resource name; binding labels are kept verbatim. See [declare standalone topology](/how-to/define-a-contract#declare-standalone-topology).

## `defineExchange`

```typescript
defineExchange(name, options?);
```

| Option       | Type                                           | Default   | Notes                                                              |
| ------------ | ---------------------------------------------- | --------- | ------------------------------------------------------------------ |
| `type`       | `"topic" \| "direct" \| "fanout" \| "headers"` | `"topic"` |                                                                    |
| `durable`    | `boolean`                                      | `true`    |                                                                    |
| `autoDelete` | `boolean`                                      | `false`   | Deleted when all queues have finished using it                     |
| `internal`   | `boolean`                                      | `false`   | Not publishable by clients; only via exchange-to-exchange bindings |
| `arguments`  | `Record<string, unknown>`                      | —         | Raw AMQP exchange arguments (e.g. `alternate-exchange`)            |

| Type      | Routing                                         |
| --------- | ----------------------------------------------- |
| `topic`   | Routing-key patterns with `*` and `#` wildcards |
| `direct`  | Exact routing-key match                         |
| `fanout`  | All bound queues; routing key ignored           |
| `headers` | Message headers; routing key ignored            |

Routing keys are required for `direct` and `topic`, optional and ignored for `fanout` and `headers`. This is enforced at compile time.

## `defineQueue`

```typescript
defineQueue(name, options?);
```

| Option        | Type                        | Default            | Notes                                                        |
| ------------- | --------------------------- | ------------------ | ------------------------------------------------------------ |
| `type`        | `"quorum" \| "classic"`     | `"quorum"`         |                                                              |
| `durable`     | `boolean`                   | `true`             | Quorum queues are always durable; `false` requires `classic` |
| `autoDelete`  | `boolean`                   | `false`            | `classic` only                                               |
| `exclusive`   | `boolean`                   | `false`            | `classic` only                                               |
| `maxPriority` | `number`                    | —                  | `classic` only                                               |
| `deadLetter`  | `{ exchange, routingKey? }` | —                  | See below                                                    |
| `onPoison`    | `"drop"`                    | —                  | Declares deliberate loss; see below                          |
| `retry`       | retry config                | `{ mode: "none" }` | See below                                                    |
| `arguments`   | `Record<string, unknown>`   | —                  | Raw AMQP queue arguments                                     |

Quorum queues replicate through Raft and cannot be exclusive, auto-deleting, or priority queues. TypeScript rejects those options unless `type: "classic"`.

### `deadLetter`

| Field        | Type                | Notes                                                    |
| ------------ | ------------------- | -------------------------------------------------------- |
| `exchange`   | exchange definition | Extracted into the contract automatically                |
| `routingKey` | `string`            | Optional. Omitted, the original routing key is preserved |

### `onPoison`

`defineContract` rejects a **consumed** queue that has neither a `deadLetter` nor
`onPoison: "drop"`, because such a queue discards every message its handler
rejects with no record of the loss. Set `onPoison: "drop"` when losing them is
the intent — a metrics firehose, or a dead-letter queue you consume (which
cannot dead-letter to itself).

Declared-but-unconsumed queues are not checked.

### `arguments`

Common raw arguments:

| Argument           | Effect                                                                      |
| ------------------ | --------------------------------------------------------------------------- |
| `x-message-ttl`    | Per-message TTL in ms; expiry routes to the DLX                             |
| `x-expires`        | Queue idle TTL in ms; deletes the queue and its messages, not dead-lettered |
| `x-max-length`     | Max messages; overflow routes to the DLX                                    |
| `x-delivery-limit` | Quorum-queue redelivery cap (RabbitMQ 4 default: 20)                        |

## Retry configuration

```typescript
retry: { mode, maxRetries, … }
```

| Mode                | Behaviour                                                             |
| ------------------- | --------------------------------------------------------------------- |
| `none` (default)    | No retry. `RetryableError` is dead-lettered like `NonRetryableError`. |
| `immediate-requeue` | Requeued at once, up to `maxRetries`, then dead-lettered.             |
| `ttl-backoff`       | Parked in a per-delay wait queue with growing TTL, then routed back.  |

### `immediate-requeue`

| Option       | Default |
| ------------ | ------- |
| `maxRetries` | 3       |

Attempt counts come from `x-delivery-count` on quorum queues (broker-native) and from a worker-maintained `x-retry-count` on classic queues.

Quorum queues also enforce `x-delivery-limit` independently of `maxRetries`.

### `ttl-backoff`

| Option              | Default | Description                                |
| ------------------- | ------- | ------------------------------------------ |
| `maxRetries`        | 3       | Maximum attempts                           |
| `initialDelayMs`    | 1000    | First delay                                |
| `maxDelayMs`        | 30000   | Delay cap (applied to the base delay)      |
| `backoffMultiplier` | 2       | Exponential factor                         |
| `jitter`            | `true`  | ±50% randomisation, avoids thundering herd |

The base delay is `initialDelayMs * backoffMultiplier ^ attempt`, capped at `maxDelayMs`. Each **distinct** base delay gets its own wait queue, `{queueName}-wait-{delayMs}ms`, declared by `setupAmqpTopology` at channel-setup time (derived from the queue's retry config via `deriveTtlBackoffInfrastructure` — never stored in the contract). The worker publishes the retry copy to the tier queue via the default exchange; the tier dead-letters it back to the main queue when its TTL expires.

RabbitMQ only expires messages at the **head** of a queue, so per-tier queues are what keep a 60s retry from blocking a later 1s retry. Within a tier every message shares the same base delay: the per-message `expiration` carries the jittered value and the tier's queue-level `x-message-ttl` is set to the jitter ceiling (`ceil(base * 1.5)`) as a backstop, so head-of-line skew within a tier is bounded by the jitter spread — and is zero with `jitter: false`.

Retried deliveries arrive via the default exchange, so their `fields.routingKey` is the queue name; the original routing key is preserved in the `x-original-routing-key` header.

## Diagnostic headers

Stamped only on paths that **republish** the message — classic queues under `immediate-requeue`, and any queue under `ttl-backoff`.

| Header                      | Meaning                           | Set on                     |
| --------------------------- | --------------------------------- | -------------------------- |
| `x-delivery-count`          | Broker-native attempt count       | Quorum queues, by RabbitMQ |
| `x-retry-count`             | Worker-managed attempt count      | Republish paths            |
| `x-last-error`              | Most recent failure message       | Republish paths            |
| `x-first-failure-timestamp` | Epoch ms of first failure         | Republish paths            |
| `x-original-routing-key`    | Routing key of the first delivery | Republish paths            |

Direct-nack paths add nothing, so those dead-lettered messages arrive exactly as delivered.

## `defineMessage`

```typescript
defineMessage(payloadSchema, options?);
```

| Option        | Type            | Purpose                                      |
| ------------- | --------------- | -------------------------------------------- |
| `headers`     | Standard Schema | Header shape, validated on the consumer only |
| `summary`     | `string`        | Short description; flows into AsyncAPI       |
| `description` | `string`        | Long description; flows into AsyncAPI        |

With no `headers` schema, a handler's `headers` is `undefined`.

## Publishers

| Function                 | Signature                                |
| ------------------------ | ---------------------------------------- |
| `definePublisher`        | `(exchange, message, { routingKey })`    |
| `defineEventPublisher`   | `(exchange, message, { routingKey })`    |
| `defineCommandPublisher` | `(commandConsumer, { bridgeExchange? })` |

`defineEventPublisher` adds compile-time routing-key validation and is what event consumers attach to. `defineCommandPublisher` derives everything from the command consumer.

All three also accept `externalConsumers?: boolean`. `defineContract` throws when a publisher's routing key reaches no queue in the contract's binding graph — the broker would confirm those messages and discard them. Set it to `true` when another service owns the binding. An exchange declaring `alternate-exchange` is exempt. See [troubleshoot](/how-to/troubleshoot#publisher-is-unroutable-at-define-time).

There is no per-call routing-key override — `publish` always uses the publisher's key.

## Consumers

| Function                | Signature                                              |
| ----------------------- | ------------------------------------------------------ |
| `defineConsumer`        | `(queue, message)`                                     |
| `defineEventConsumer`   | `(publisher, queue, { routingKey?, bridgeExchange? })` |
| `defineCommandConsumer` | `(queue, exchange, message, { routingKey })`           |

`routingKey` on `defineEventConsumer` overrides the binding pattern (for wildcards); the payload type still comes from the publisher.

## `defineRpc`

```typescript
defineRpc(queue, { request, response, errors? });
```

| Key        | Type                                                          |
| ---------- | ------------------------------------------------------------- |
| `request`  | Message definition                                            |
| `response` | Message definition                                            |
| `errors`   | `Record<code, RpcErrorDefinition>` — declared business errors |

Each `errors` entry is `{ data, message? }`: `data` is the Standard Schema validating the error's payload, and the optional `message` is the default human-readable message used when the handler does not supply one.

## Client options

`TypedAmqpClient.create({ … })`:

| Option                  | Type                            | Default                     |
| ----------------------- | ------------------------------- | --------------------------- |
| `contract`              | contract                        | required                    |
| `urls`                  | `ConnectionUrl[]`               | required                    |
| `connectionOptions`     | amqp-connection-manager options | —                           |
| `defaultPublishOptions` | `PublishOptions`                | `{ persistent: true }`      |
| `connectTimeoutMs`      | `number \| null`                | 30000; `null` waits forever |
| `logger`                | `Logger`                        | —                           |
| `telemetry`             | `TelemetryProvider`             | auto-detected               |
| `publishInterceptors`   | `PublishInterceptor[]`          | —                           |
| `callInterceptors`      | `CallInterceptor[]`             | —                           |

`ConnectionUrl` is amqp-connection-manager's URL type — a plain `amqp://` string, an amqplib `Options.Connect` object, or `{ url, connectionOptions }`.

`PublishOptions` is amqplib's `Options.Publish` plus `compression?: "gzip" | "deflate"`. Properties are flat: `persistent`, `priority`, `expiration`, `correlationId`, `headers`, and the rest.

An invalid `connectTimeoutMs` (`NaN`, zero, negative, `Infinity`) surfaces as a **defect** from `create()` rather than silently disabling the timeout. Only `null` disables it.

## Worker options

`TypedAmqpWorker.create({ … })`:

| Option                   | Type                                                            |
| ------------------------ | --------------------------------------------------------------- |
| `contract`               | contract                                                        |
| `handlers`               | one entry per consumer and RPC                                  |
| `urls`                   | `ConnectionUrl[]`                                               |
| `connectionOptions`      | amqp-connection-manager options                                 |
| `connectTimeoutMs`       | `number \| null` (default 30000; `null` waits forever)          |
| `defaultConsumerOptions` | `ConsumerOptions` (see below)                                   |
| `middleware`             | `WorkerMiddleware`, an array of them, or `composeMiddleware(…)` |
| `createContext`          | `(info) => context`                                             |
| `logger`                 | `Logger`                                                        |
| `telemetry`              | `TelemetryProvider`                                             |

A handler entry is either the function or a `[function, ConsumerOptions]` tuple.

`ConsumerOptions` is a curated subset of the AMQP consume options: `prefetch`, `priority`, `arguments`, `consumerTag`, `exclusive`. `noAck` and `noLocal` are deliberately excluded — `noAck: true` would silently break the ack-exactly-once and retry/DLQ invariants, and `noLocal` is not supported by RabbitMQ.

The `middleware` array form composes at runtime like `composeMiddleware(…)` (first entry outermost) but does not thread stepwise context types — see [add middleware](/how-to/add-middleware#chain-several-middleware).

`worker.close(options?)` accepts `{ drainTimeoutMs?: number | null }` — how long to wait for in-flight handlers before tearing the channel down (default `DEFAULT_DRAIN_TIMEOUT_MS`, 30 000 ms; `null` waits forever). See [consume messages](/how-to/consume-messages#shut-down-without-dropping-messages).

`Logger`, `TelemetryProvider` and `TechnicalError` are re-exported by both `@amqp-contract/client` and `@amqp-contract/worker`, so naming an option type or matching a defect cause never forces a direct dependency on `@amqp-contract/core`.

## Routing-key validation types

```typescript
import type { BindingPattern, MatchingRoutingKey, RoutingKey } from "@amqp-contract/contract";
```

| Type                       | Purpose                                               |
| -------------------------- | ----------------------------------------------------- |
| `RoutingKey<K>`            | `K` if it is a valid key, else `never`                |
| `BindingPattern<P>`        | `P` if it is a valid pattern, else `never`            |
| `MatchingRoutingKey<P, K>` | `K` if it matches `P`, `never` if it provably doesn't |

Keys are dot-separated segments of alphanumerics, hyphens and underscores. `*` matches one segment, `#` matches zero or more, and both are valid only in patterns.

`MatchingRoutingKey` always enforces each side's own validity — `RoutingKey<K>`, `BindingPattern<P>` — since that's decidable from one side alone, regardless of the other side's shape. It can only decide the _match_ between a valid pattern and a valid key when both `P` and `K` are fully resolved string literals at compile time; a plain `string`, a template-literal type, a union containing either, or a branded string type, among others, skips the match and resolves to `K` unchecked rather than guessing.

TypeScript's recursion limit means very long keys fall back to `string`. Compile-time checking only; runtime behaviour is unaffected.

## Schema inference types

```typescript
import type { InferSchemaInput, InferSchemaOutput } from "@amqp-contract/contract";

type OrderCreated = InferSchemaInput<typeof contract.publishers.orderCreated.message.payload>;
```

`InferSchemaInput<TSchema>` and `InferSchemaOutput<TSchema>` extract the input and output types of any Standard Schema — what a publisher accepts versus what a handler receives after parsing.

## Where next

- [Define a contract](/how-to/define-a-contract) — how to use these.
- [Error model](/reference/error-model) — the error surface.
- [API reference](/api/) — generated per-package documentation.
