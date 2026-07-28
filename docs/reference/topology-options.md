---
title: Topology options - amqp-contract
description: Option tables for defineExchange, defineQueue, defineMessage, publishers, consumers and RPCs, plus retry defaults and diagnostic headers.
---

# Topology options

Options for every contract-building function. For recipes see [define a contract](/how-to/define-a-contract); for the generated API surface see the [API reference](/api/).

## `defineContract`

```typescript
defineContract({ publishers, consumers, rpcs });
```

| Key          | Contains                                                                |
| ------------ | ----------------------------------------------------------------------- |
| `publishers` | Named publishers callable via `client.publish(name, …)`                 |
| `consumers`  | Named consumers requiring a handler on the worker                       |
| `rpcs`       | Named RPCs, callable via `client.call(name, …)` and requiring a handler |

The returned contract also exposes `exchanges`, `queues` and `bindings`, extracted from the above. You never declare them directly.

## `defineExchange`

```typescript
defineExchange(name, options?);
```

| Option    | Type                                           | Default   |
| --------- | ---------------------------------------------- | --------- |
| `type`    | `"topic" \| "direct" \| "fanout" \| "headers"` | `"topic"` |
| `durable` | `boolean`                                      | `true`    |

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
| `retry`       | retry config                | `{ mode: "none" }` | See below                                                    |
| `arguments`   | `Record<string, unknown>`   | —                  | Raw AMQP queue arguments                                     |

Quorum queues replicate through Raft and cannot be exclusive, auto-deleting, or priority queues. TypeScript rejects those options unless `type: "classic"`.

### `deadLetter`

| Field        | Type                | Notes                                                    |
| ------------ | ------------------- | -------------------------------------------------------- |
| `exchange`   | exchange definition | Extracted into the contract automatically                |
| `routingKey` | `string`            | Optional. Omitted, the original routing key is preserved |

### `arguments`

Common raw arguments:

| Argument           | Effect                                               |
| ------------------ | ---------------------------------------------------- |
| `x-message-ttl`    | Per-message TTL in ms; expiry routes to the DLX      |
| `x-max-length`     | Max messages; overflow routes to the DLX             |
| `x-delivery-limit` | Quorum-queue redelivery cap (RabbitMQ 4 default: 20) |

## Retry configuration

```typescript
retry: { mode, maxRetries, … }
```

| Mode                | Behaviour                                                             |
| ------------------- | --------------------------------------------------------------------- |
| `none` (default)    | No retry. `RetryableError` is dead-lettered like `NonRetryableError`. |
| `immediate-requeue` | Requeued at once, up to `maxRetries`, then dead-lettered.             |
| `ttl-backoff`       | Routed through a wait queue with growing per-message TTL, then back.  |

### `immediate-requeue`

| Option       | Default |
| ------------ | ------- |
| `maxRetries` | 3       |

Attempt counts come from `x-delivery-count` on quorum queues (broker-native) and from a worker-maintained `x-retry-count` on classic queues.

Quorum queues also enforce `x-delivery-limit` independently of `maxRetries`.

### `ttl-backoff`

| Option              | Default            | Description                                |
| ------------------- | ------------------ | ------------------------------------------ |
| `maxRetries`        | 3                  | Maximum attempts                           |
| `initialDelayMs`    | 1000               | First delay                                |
| `maxDelayMs`        | 30000              | Delay cap                                  |
| `backoffMultiplier` | 2                  | Exponential factor                         |
| `jitter`            | `true`             | ±randomisation, to avoid a thundering herd |
| `waitQueueName`     | `{queueName}-wait` | Generated wait queue                       |
| `waitExchangeName`  | `wait-exchange`    | Generated headers exchange                 |
| `retryExchangeName` | `retry-exchange`   | Generated headers exchange                 |

Delay is `initialDelayMs * backoffMultiplier ^ attempt`, capped at `maxDelayMs`. `defineContract` generates the wait queue, both headers exchanges and their bindings.

## Diagnostic headers

Stamped only on paths that **republish** the message — classic queues under `immediate-requeue`, and any queue under `ttl-backoff`.

| Header                           | Meaning                         | Set on                     |
| -------------------------------- | ------------------------------- | -------------------------- |
| `x-delivery-count`               | Broker-native attempt count     | Quorum queues, by RabbitMQ |
| `x-retry-count`                  | Worker-managed attempt count    | Republish paths            |
| `x-last-error`                   | Most recent failure message     | Republish paths            |
| `x-first-failure-timestamp`      | Epoch ms of first failure       | Republish paths            |
| `x-wait-queue` / `x-retry-queue` | Internal `ttl-backoff` pointers | `ttl-backoff` republish    |

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

| Key        | Type                                                         |
| ---------- | ------------------------------------------------------------ |
| `request`  | Message definition                                           |
| `response` | Message definition                                           |
| `errors`   | `Record<code, MessageDefinition>` — declared business errors |

## Client options

`TypedAmqpClient.create({ … })`:

| Option                  | Type                            | Default                     |
| ----------------------- | ------------------------------- | --------------------------- |
| `contract`              | contract                        | required                    |
| `urls`                  | `string[]`                      | required                    |
| `connectionOptions`     | amqp-connection-manager options | —                           |
| `defaultPublishOptions` | `PublishOptions`                | `{ persistent: true }`      |
| `connectTimeoutMs`      | `number \| null`                | 30000; `null` waits forever |
| `logger`                | `Logger`                        | —                           |
| `telemetry`             | `TelemetryProvider`             | auto-detected               |
| `publishInterceptors`   | `PublishInterceptor[]`          | —                           |
| `callInterceptors`      | `CallInterceptor[]`             | —                           |

`PublishOptions` is amqplib's `Options.Publish` plus `compression?: "gzip" | "deflate"`. Properties are flat: `persistent`, `priority`, `expiration`, `correlationId`, `headers`, and the rest.

## Worker options

`TypedAmqpWorker.create({ … })`:

| Option                   | Type                                                |
| ------------------------ | --------------------------------------------------- |
| `contract`               | contract                                            |
| `handlers`               | one entry per consumer and RPC                      |
| `urls`                   | `string[]`                                          |
| `connectionOptions`      | amqp-connection-manager options                     |
| `defaultConsumerOptions` | `{ prefetch?: number }` and amqplib consume options |
| `middleware`             | `WorkerMiddleware`, or `composeMiddleware(…)`       |
| `createContext`          | `(info) => context`                                 |
| `logger`                 | `Logger`                                            |
| `telemetry`              | `TelemetryProvider`                                 |

A handler entry is either the function or a `[function, ConsumerOptions]` tuple.

## Routing-key validation types

```typescript
import type { BindingPattern, MatchingRoutingKey, RoutingKey } from "@amqp-contract/contract";
```

| Type                       | Purpose                                    |
| -------------------------- | ------------------------------------------ |
| `RoutingKey<K>`            | `K` if it is a valid key, else `never`     |
| `BindingPattern<P>`        | `P` if it is a valid pattern, else `never` |
| `MatchingRoutingKey<P, K>` | `K` if it matches `P`, else `never`        |

Keys are dot-separated segments of alphanumerics, hyphens and underscores. `*` matches one segment, `#` matches zero or more, and both are valid only in patterns.

TypeScript's recursion limit means very long keys fall back to `string`. Compile-time checking only; runtime behaviour is unaffected.

## Where next

- [Define a contract](/how-to/define-a-contract) — how to use these.
- [Error model](/reference/error-model) — the error surface.
- [API reference](/api/) — generated per-package documentation.
