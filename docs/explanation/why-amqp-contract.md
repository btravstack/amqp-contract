---
title: Why amqp-contract? - amqp-contract
description: The problem contract-first messaging solves, and what it costs — an honest discussion of why this library exists.
---

# Why amqp-contract?

This page argues for a position. It is not a tutorial and not a feature list: it is an account of a specific problem with message-driven systems in TypeScript, and why amqp-contract takes the shape it does.

## The gap a message queue opens

A function call has a compiler standing behind it. Change a parameter and every caller breaks loudly, at build time, in your editor.

A message does not. Publishing to RabbitMQ with a raw AMQP client looks roughly like this:

```typescript
channel.publish("orders", "order.created", Buffer.from(JSON.stringify(order)));
```

And consuming it looks like this:

```typescript
channel.consume("order-processing", (msg) => {
  const order = JSON.parse(msg.content.toString());
  processOrder(order.orderId, order.totalAmount);
});
```

Nothing connects those two snippets. They are usually in different files, frequently in different repositories, and often owned by different teams. The publisher believes it is sending `{ orderId, totalAmount }`. The consumer believes it is receiving that. Both beliefs are unverified, and `JSON.parse` returns `any` — so the consumer's `order.totalAmount` type-checks whether or not the field exists.

Three consequences follow, and they compound.

**Mistakes surface late and far away.** Rename `totalAmount` to `total` in the publisher and the code compiles, deploys, and runs. The failure appears minutes later in a different service, as `undefined` flowing into a calculation, or a `TypeError` in a log nobody is watching. The distance between cause and symptom is the expensive part.

**Validation gets written by hand, or not at all.** Every consumer that wants safety writes its own checks. Those checks drift from each other and from the publisher's actual output. The consumer that skipped them is the one that corrupts data.

**The schema lives nowhere.** Ask "what does an `order.created` message contain?" and the honest answer is: read the publisher, then read every consumer, then check whether production agrees. Refactoring under those conditions is guesswork, so it does not happen, so the schema accretes fields nobody dares remove.

## Restoring the compiler

The idea is not new — it is what [tRPC](https://trpc.io/), [oRPC](https://orpc.dev/) and [ts-rest](https://ts-rest.com/) did for HTTP APIs. amqp-contract applies it to AMQP.

You write one contract:

```typescript
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    totalAmount: z.number().positive(),
  }),
);

const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

export const contract = defineContract({
  publishers: { orderCreated },
  consumers: { processOrder: defineEventConsumer(orderCreated, orderProcessingQueue) },
});
```

Both sides import it. The publisher's payload type and the consumer's `payload` type now come from the same expression, so renaming `totalAmount` breaks the build in both places, immediately, in your editor. The schema has one home, and it is executable rather than a wiki page that went stale.

## Why types alone are not enough

A contract shared at compile time only helps within one deployment. Across a broker, two things escape it.

Services deploy independently. The publisher can ship a new schema while the consumer still runs last week's build; they agree at compile time about _different_ contracts. And messages arrive from places the compiler never saw — a legacy producer, a manual republish from the management UI, a replayed dead letter.

So the contract is enforced twice. TypeScript checks what your code _intends_; the schema checks what actually crosses the wire, on publish and again on consume. The two catch different mistakes, and neither substitutes for the other. `"not-an-email"` is a perfectly good `string` — only the runtime schema rejects it.

## Why failures are values, not exceptions

A message broker forces a decision an HTTP handler can avoid. When processing fails, something must happen to the message: acknowledge it, retry it, or dead-letter it. Get that wrong and you either lose data or loop forever.

An exception is a poor way to carry that decision. It has type `unknown`, it can originate anywhere in the call stack, and nothing makes you handle it. A handler that throws leaves the worker guessing at the one question that matters — _is this worth retrying?_

So handlers return their outcome instead:

```typescript
processOrder: ({ payload }) =>
  fromPromise(chargeCard(payload), (cause) =>
    cause instanceof CardDeclined
      ? new NonRetryableError("declined", cause) // → dead letter
      : new RetryableError("gateway down", cause), // → retry
  ).map(() => undefined),
```

Now the routing decision is data the worker can act on deterministically. [Errors as values](/explanation/errors-as-values) develops this further, including the third channel for failures nobody anticipated.

## What it costs

Contract-first is not free, and the trade-offs are real.

**You give up ad-hoc messages.** Publishing something the contract does not describe means editing the contract. That is the point, but it does add friction to spikes and one-off scripts.

**Validation costs time.** Every message is parsed against a schema on the way out and on the way in. It is small — microseconds for typical payloads — but it is not nothing, and at extreme throughput it is measurable. [Tune performance](/how-to/tune-performance) covers where it actually shows up.

**The contract must be shared.** Both services need it, which in practice means a published package or a monorepo. Teams that cannot share code across service boundaries get much less out of this.

**It is TypeScript-only, and AMQP-only.** There is no runtime story for a Python consumer beyond the generated [AsyncAPI](/how-to/generate-asyncapi) document, and no support for Kafka or SQS.

If you are writing a one-off script, or your messaging is genuinely simple and stable, raw [amqplib](https://github.com/amqp-node/amqplib) is a reasonable answer and a smaller dependency. The case for a contract grows with the number of services, the number of message types, and the number of people changing them.

## Where next

- [Core concepts](/explanation/core-concepts) — the model this rests on.
- [Comparison](/explanation/comparison) — how this relates to amqplib, tRPC, BullMQ and Kafka.
- [Getting started](/tutorial/getting-started) — see it work.
