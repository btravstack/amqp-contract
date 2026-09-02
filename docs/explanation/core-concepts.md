---
title: Core concepts - amqp-contract
description: The model behind amqp-contract — contracts as single definitions, the two validation boundaries, and how types flow from schema to handler.
---

# Core concepts

This page explains the model the library is built on. It is not a reference for the `defineX` functions — [topology options](/reference/topology-options) lists those — but an account of how the pieces relate and why they are arranged this way.

## A contract is one definition with two outputs

The central idea is that a single expression produces two things that normally have to be kept in sync by hand:

- the **TypeScript types** your publisher and consumer code is checked against, and
- the **AMQP topology** — exchanges, queues, bindings — declared against the broker at startup.

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

const ordersExchange = defineExchange("orders");
const ordersDlx = defineExchange("orders-dlx");
const orderProcessingQueue = defineQueue("order-processing", {
  deadLetter: { exchange: ordersDlx },
});
// The dead-letter queue is a named resource too. A dead-letter exchange with
// nothing bound to it discards every rejected message, so `defineContract`
// rejects it; the binding below is what makes the DLX actually route.
const orderDlq = defineQueue("order-processing-dlq");
const orderMessage = defineMessage(z.object({ orderId: z.string() }));

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderProcessingQueue) },
  queues: { orderDlq },
  bindings: { orderDlqBinding: defineQueueBinding(orderDlq, ordersDlx, { routingKey: "#" }) },
});
```

There is no separate topology setup step and no separate type declaration. When a worker starts, it walks the contract and asserts every exchange, queue and binding it finds. When you call `client.publish("orderCreated", …)`, the payload type comes from the same object.

This is also why the composition pattern matters: every resource above is a named constant, defined first and then referenced. Inlining a queue inside `defineContract` works, but a named resource can be referenced from several places — a consumer, a dead-letter target, a binding — and referencing the same constant is what guarantees they mean the same queue.

## Bindings are derived, not declared

A conventional AMQP setup declares an exchange, declares a queue, then binds them with a routing key — three statements that must agree. amqp-contract derives the third from the first two.

```typescript
const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const processOrder = defineEventConsumer(orderCreated, orderProcessingQueue);
```

`defineEventConsumer` takes the _publisher_, not a schema and a routing key. From it the library knows which exchange to bind to, which routing key to bind with, and what the payload looks like. There is no opportunity for the consumer's routing key to drift from the publisher's, because there is only one.

You can override the pattern when a consumer wants a broader subscription — `{ routingKey: "order.#" }` — and the payload type still comes from the publisher.

## Events and commands are different shapes

Two patterns cover most messaging, and the library models them differently because their _ownership_ differs.

An **event** is a fact that has happened. One publisher announces it; any number of consumers may care. The publisher is defined first because it owns the fact, and consumers attach themselves to it.

A **command** is an instruction to do work. It has exactly one owner — the service that performs it — and any number of callers. So the definition order inverts: the consumer is defined first, because it owns the queue and decides what it accepts, and `defineCommandPublisher` derives the caller's side from it.

```typescript
const fulfillOrder = defineCommandConsumer(queue, exchange, message, {
  routingKey: "order.fulfill",
});
const requestFulfillment = defineCommandPublisher(fulfillOrder);
```

The rule is the same in both cases — whoever owns the meaning gets defined first, and the other side is derived — which is what stops the two from drifting.

An **RPC** is a command that returns a value. It declares a request schema, a response schema, and optionally a set of named errors, and it owns its queue.

## Validation happens at both boundaries

Schemas are checked twice per message: once by the client before publishing, once by the worker before the handler runs.

This is deliberate redundancy, not an oversight. The two checks defend against different things.

The publish-side check catches a bug in the sending service before a bad message enters the system at all — the failure is reported to the caller, synchronously, as a `MessageValidationError`, and nothing is written to the broker.

The consume-side check catches everything the publish-side check could not possibly see: a producer running an older contract, a message hand-published from the management UI, a replayed dead letter, a producer written in another language. The worker is not entitled to assume the sender validated anything.

A message that fails consume-side validation goes straight to the dead-letter queue without retrying, because a payload that does not match the schema will not start matching it on the third attempt. [The retry model](/explanation/the-retry-model) explains why that path bypasses retries entirely.

## How a type reaches a handler

Nothing in the handler is annotated, so it is worth tracing where its types come from.

```typescript
const orderMessage = defineMessage(z.object({ orderId: z.string() }));
```

`defineMessage` holds a [Standard Schema](https://standardschema.dev/) — Zod here, but Valibot and ArkType work identically — and Standard Schema exposes the validated output type. `defineEventPublisher` carries that type onto the publisher; `defineEventConsumer` carries it from the publisher onto the consumer; `defineContract` collects consumers into an object keyed by name.

When the worker asks for `handlers.processOrder`, it looks up that key and knows the payload is `{ orderId: string }`. Which is why this fails to compile:

```typescript
processOrder: ({ input: { payload } }) => {
  console.log(payload.orderNumber); // Property 'orderNumber' does not exist
  return OkAsync(undefined);
};
```

The same flow supplies header types, RPC response types, and the set of valid publisher names — `client.publish("orderCreatd", …)` is a compile error, not a message that silently goes nowhere.

## Defaults encode an opinion

Two defaults are worth knowing because they differ from what raw AMQP gives you.

**Queues are quorum queues.** Quorum queues replicate through Raft consensus and survive broker failure in ways classic queues do not. They cannot be exclusive, auto-deleting, or priority queues — if you need one of those, ask for `type: "classic"` explicitly. The default is the safe choice; the exception is opt-in.

**Exchanges are durable topic exchanges.** Topic routing subsumes direct routing (a topic key with no wildcards behaves like a direct key) and leaves room to add wildcard consumers later without redeclaring the exchange.

## The path of a message

Putting it together, publishing one message involves:

1. The client validates the payload against the publisher's schema. On failure it returns `Err(MessageValidationError)` and stops here.
2. The payload is serialized and published to the exchange with the publisher's routing key.
3. The broker routes it to every queue whose binding matches.
4. The worker receives it and validates it against the consumer's schema. On failure it dead-letters the message without retrying.
5. Middleware runs, if any, and may substitute the payload — in which case it is validated again.
6. The handler runs with a fully typed payload and returns a result.
7. The worker acknowledges, retries or dead-letters based on that result.

Steps 1 and 4 are the contract being enforced. Step 7 is [the retry model](/explanation/the-retry-model). Step 6 returning a value rather than throwing is [errors as values](/explanation/errors-as-values).

## Where next

- [Define a contract](/how-to/define-a-contract) — the practical recipes.
- [Topology options](/reference/topology-options) — every option on every `defineX`.
- [Glossary](/reference/glossary) — precise definitions, including where this library's vocabulary differs from AMQP's.
