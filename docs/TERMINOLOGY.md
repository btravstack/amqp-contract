# Terminology Guide

This document explains the terminology used in **amqp-contract** and how it maps to standard AMQP/RabbitMQ terms.

## Core Terms

### Client vs Publisher

In **amqp-contract**, we use the term **"client"** where AMQP documentation typically uses **"publisher"**:

- **TypedAmqpClient** = Type-safe message publisher
- **client.publish()** = Publish a message to an exchange

**Why "client"?**

- Conveys the concept of an application that initiates communication
- Clear and intuitive for developers from various backgrounds
- Distinguishes this implementation from generic AMQP publishers

**Standard AMQP terminology:**

- Publisher/Producer = Application that sends messages
- Publishing = Sending messages to an exchange

### Worker vs Consumer

In **amqp-contract**, we use the term **"worker"** where AMQP documentation typically uses **"consumer"**:

- **TypedAmqpWorker** = Type-safe message consumer
- **handlers** = Message processing functions for each consumer

**Why "worker"?**

- Emphasizes the processing/handling aspect
- Familiar to developers from job queue systems (Bull, BullMQ, etc.)
- Conveys background processing behavior
- Implies an active processor rather than passive receiver

**Standard AMQP terminology:**

- Consumer/Subscriber = Application that receives messages
- Consuming = Receiving messages from a queue

## Terminology Mapping

| amqp-contract               | AMQP Standard          | Description                             |
| :-------------------------- | :--------------------- | :-------------------------------------- |
| **Client**                  | Publisher / Producer   | Sends messages                          |
| **Worker**                  | Consumer / Subscriber  | Receives and processes messages         |
| **Contract**                | Schema / Specification | Defines exchanges, queues, and messages |
| **Publisher** (in contract) | Publishing endpoint    | Named message publisher in contract     |
| **Consumer** (in contract)  | Consuming endpoint     | Named message consumer in contract      |

## Important: Contract vs Runtime Terminology

It's important to distinguish between contract-level and runtime-level terminology:

### Contract Level

When defining a contract, we use standard AMQP terms:

```typescript
const contract = defineContract({
  publishers: {
    // ← "publisher" in contract
    orderCreated: definePublisher(exchange, schema, options),
  },
  consumers: {
    // ← "consumer" in contract
    processOrder: defineConsumer(queue, schema),
  },
});
```

These terms (`publishers`, `consumers`) describe the **messaging patterns** in your contract.

### Runtime Level

When implementing the contract, we use our terms:

```typescript
import { OkAsync } from "unthrown";

// Client = runtime publisher
const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();

await client.publish("orderCreated", message).getOrThrow();

// Worker = runtime consumer
const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => {
      // Handle message
      return OkAsync(undefined);
    },
  },
  urls,
}).getOrThrow();
```

These terms (`TypedAmqpClient`, `TypedAmqpWorker`) describe the **runtime components** that implement the contract.

## Why We Made This Choice

### 1. Clarity of Intent

The terms "client" and "worker" make the role of each component immediately clear:

- **Client**: Initiates communication (sends messages)
- **Worker**: Performs work (processes messages)

### 2. Familiarity

Many developers are familiar with these concepts from:

- **Job queues**: Bull, BullMQ, Sidekiq use "worker"
- **HTTP**: "client" is widely understood
- **Distributed systems**: Client/worker is a common pattern

### 3. Avoiding Ambiguity

The term "consumer" can be ambiguous:

- RabbitMQ consumer (technical)
- Business consumer (user of a service)
- Event consumer (event-driven architecture)

"Worker" is more specific to processing/handling behavior.

### 4. TypeScript Type Names

Our class names become:

- `TypedAmqpClient` (clear and concise)
- `TypedAmqpWorker` (clear and concise)

Compare with alternatives:

- `TypedAmqpPublisher` (more technical)
- `TypedAmqpConsumer` (ambiguous)

## For AMQP Veterans

If you're coming from RabbitMQ or other AMQP libraries, here's a quick mental mapping:

```typescript
// Other AMQP libraries might use:
const publisher = await createPublisher(config);
await publisher.publish(exchange, routingKey, message);

const consumer = await createConsumer(queue, handler);

// amqp-contract uses:
const client = await TypedAmqpClient.create({ contract, urls }).getOrThrow();

await client.publish("orderCreated", message).getOrThrow();

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: { processOrder: handler },
  urls,
}).getOrThrow();
```

The functionality is identical; only the naming differs.

## AsyncResult Considerations

We're committed to listening to the community. If there's strong feedback that the standard AMQP terms would be clearer, we may consider:

1. **Type Aliases** (backward compatible)

   ```typescript
   export { TypedAmqpClient as TypedAmqpPublisher };
   export { TypedAmqpWorker as TypedAmqpConsumer };
   ```

2. **Gradual Migration** (deprecation period)
   - Introduce new names with deprecation warnings
   - Provide migration guide
   - Remove old names in v2.0

3. **Documentation Enhancements**
   - More prominent mapping explanations
   - Quick reference guide
   - Migration guide for RabbitMQ users

## Providing Feedback

If you have opinions about our terminology choices, we'd love to hear from you:

- **GitHub Discussions**: Share your thoughts and vote on terminology
- **GitHub Issues**: Report terminology-related confusion
- **Community Survey**: Participate in our annual developer survey

Your feedback helps us improve the developer experience for everyone!

## Summary

**Quick Reference:**

- **Use TypedAmqpClient** when you need to publish messages
- **Use TypedAmqpWorker** when you need to consume messages
- **Remember**: client = publisher, worker = consumer
- **Contract definitions** use standard terms: `publishers` and `consumers`
- **Runtime components** use our terms: `TypedAmqpClient` and `TypedAmqpWorker`

When reading RabbitMQ or AMQP documentation:

- Replace "publisher" with "client" mentally
- Replace "consumer" with "worker" mentally
- The concepts and patterns are identical

---

_This terminology was chosen to provide the clearest possible developer experience while maintaining full compatibility with AMQP semantics._
