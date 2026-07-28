---
title: Glossary - amqp-contract
description: Definitions of amqp-contract's vocabulary, and how it maps onto standard AMQP terminology.
---

# Glossary

Definitions, and where this library's vocabulary differs from AMQP's.

## Mapping to AMQP terms

Two runtime classes are named differently from the AMQP convention. The functionality is identical; only the naming differs.

| amqp-contract                  | AMQP standard          | Role                                |
| ------------------------------ | ---------------------- | ----------------------------------- |
| **Client** (`TypedAmqpClient`) | Publisher / Producer   | Sends messages                      |
| **Worker** (`TypedAmqpWorker`) | Consumer / Subscriber  | Receives and processes messages     |
| **Contract**                   | Schema / specification | Defines exchanges, queues, messages |

Inside a contract, the standard terms are used: a `publisher` is a named publishing endpoint and a `consumer` is a named consuming endpoint. So a single **client** carries many **publishers**, and a single **worker** serves many **consumers**.

Keeping that distinction straight resolves most confusion: _client_ and _worker_ are runtime objects, _publisher_ and _consumer_ are contract entries.

## Terms

### AsyncResult

The asynchronous result type from [unthrown](https://github.com/btravstack/unthrown). `AsyncResult<T, E>` resolves to a `Result<T, E>`. Awaiting one does **not** throw on failure. See [errors as values](/explanation/errors-as-values).

### Binding

A rule connecting an exchange to a queue (or to another exchange) with a routing key or pattern. Bindings are derived from consumers rather than declared — see [core concepts](/explanation/core-concepts#bindings-are-derived-not-declared).

### Bridge exchange

A local exchange that forwards to, or receives from, an exchange in another domain, letting a contract stay free of remote topology. See [bridge domains](/how-to/bridge-domains).

### Client

`TypedAmqpClient` — the runtime object that publishes messages and makes RPC calls. What AMQP calls a publisher or producer.

### Command

A message instructing a single owner to do work. Many callers, one consumer. The consumer is defined first (`defineCommandConsumer`) and the publisher derived from it.

### Consumer

A named entry in a contract's `consumers` map, binding a queue to a message shape. Requires exactly one handler on the worker. Not to be confused with the worker itself.

### Contract

A single definition producing both the TypeScript types and the AMQP topology. Built with `defineContract`.

### Dead letter / DLX / DLQ

A dead-letter exchange receives messages that were rejected, expired, or overflowed a queue limit; the queue bound to it is the dead-letter queue. See [route dead letters](/how-to/route-dead-letters).

### Defect

unthrown's third channel, for failures that were not anticipated. In this library a defect's cause is always a `TechnicalError`. Distinct from a modeled error, which appears in the type signature.

### Event

A message announcing something that happened. One publisher, any number of consumers. The publisher is defined first (`defineEventPublisher`) and consumers attach to it.

### Exchange

The AMQP entity that receives published messages and routes them to queues by type — `topic`, `direct`, `fanout` or `headers`. Defaults to a durable topic exchange.

### Handler

A function processing one message for one consumer or RPC. Returns `AsyncResult<void, HandlerError>` (consumer) or `AsyncResult<TResponse, …>` (RPC). Never `async`, never throws.

### HandlerError

The union `RetryableError | NonRetryableError`. A union type, not a class — discriminate with `isHandlerError`, not `instanceof`.

### Interceptor

A client-side wrapper around `publish()` or `call()`. The publishing counterpart to middleware. See [add middleware](/how-to/add-middleware#stamp-headers-on-every-publish).

### Message

A payload schema plus optional headers schema and documentation, built with `defineMessage`. Shared by the publishers and consumers referencing it.

### Middleware

A worker-side wrapper around handler invocation, able to inject typed context, substitute the payload, or short-circuit. Runs after validation.

### Modeled error

A failure that appears in an operation's `E` — anticipated, branchable, and something the compiler makes you handle. Contrast with a defect.

### Prefetch

The cap on unacknowledged messages a consumer holds at once. Controls concurrency and memory. See [tune performance](/how-to/tune-performance#prefetch).

### Publisher

A named entry in a contract's `publishers` map, pairing an exchange, a message and a routing key. Invoked as `client.publish(name, payload)`.

### Quorum queue

The default queue type, replicating through Raft consensus. Always durable; cannot be exclusive, auto-deleting, or a priority queue.

### Routing key

The string a publisher attaches to a message, matched against binding patterns. `*` matches exactly one dot-separated segment, `#` matches zero or more — wildcards are valid in patterns, not in keys.

### RPC

A request/reply exchange declaring a request schema, a response schema and optionally a map of declared errors. Owns its queue. See [use request/reply](/how-to/use-request-reply).

### Standard Schema

The [common interface](https://standardschema.dev/) this library validates against, letting Zod, Valibot, ArkType and others be used interchangeably.

### TechnicalError

The error class carried as the cause of every defect — transport and framework failures. Never a modeled error. See [error model](/reference/error-model#technicalerror).

### Topology

The set of exchanges, queues and bindings a contract describes, declared against the broker when a worker starts.

### Worker

`TypedAmqpWorker` — the runtime object that consumes messages and dispatches them to handlers. What AMQP calls a consumer or subscriber.

## Coming from another AMQP library

```typescript
// Elsewhere
const publisher = await createPublisher(config);
await publisher.publish(exchange, routingKey, message);
const consumer = await createConsumer(queue, handler);

// Here
const client = await TypedAmqpClient.create({ contract, urls }).get();
await client.publish("orderCreated", message).getOrThrow();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: { processOrder: handler },
  urls,
}).get();
```

The exchange and routing key are absent from the publish call because they live in the contract, keyed by publisher name.

Note the two different extractors: `.get()` on `create` (its modeled error channel is empty) and `.getOrThrow()` on `publish` (which has a modeled `MessageValidationError`). See [error model](/reference/error-model#extracting-values).

## Where next

- [Core concepts](/explanation/core-concepts) — how these fit together.
- [Topology options](/reference/topology-options) — every option on every term above.
