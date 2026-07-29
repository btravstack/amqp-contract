---
title: Getting started - amqp-contract
description: Build a working type-safe RabbitMQ publisher and consumer from scratch, in about fifteen minutes.
---

# Getting started

In this tutorial you will build a small email-notification service: one program that publishes a message, and another that consumes it. By the end you will have seen a message travel through RabbitMQ with its shape checked by TypeScript at compile time and by a schema at runtime.

This is a lesson, not a reference. Follow it exactly — every choice here (Zod, npm, a direct exchange) has alternatives, but picking them now would only get in the way. Once it works, the [how-to guides](/how-to/define-a-contract) cover the variations.

You need about fifteen minutes, [Node.js 22.19+](https://nodejs.org/), and Docker.

## Step 1: Start RabbitMQ

```bash
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:4-management
```

Open [http://localhost:15672](http://localhost:15672) and log in with `guest` / `guest`. You should see the RabbitMQ management dashboard. Leave it open — you will come back to it in step 6.

## Step 2: Create the project

```bash
mkdir amqp-demo && cd amqp-demo
npm init -y
npm pkg set type=module
npm install @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown zod
npm install -D typescript tsx
```

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true
  }
}
```

You installed five runtime packages. Three are amqp-contract itself. `zod` describes the shape of your messages. `unthrown` is the library amqp-contract uses to return errors as values instead of throwing — it appears in the types you will write, so you need it directly.

You did not install `amqplib`. It ships as a dependency of the amqp-contract packages.

## Step 3: Define the contract

The contract is the single place that describes both the shape of your messages and the RabbitMQ topology they travel through. Both the publisher and the consumer will import it.

Create `contract.ts`:

```typescript
// contract.ts
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { z } from "zod";

// 1. The AMQP resources.
const notificationsExchange = defineExchange("notifications", { type: "direct" });
const emailQueue = defineQueue("email-notifications");

// 2. The message: a schema, plus documentation.
const emailMessage = defineMessage(
  z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  {
    summary: "Email notification",
    description: "Sent when an email needs to be delivered",
  },
);

// 3. Who publishes it, and where.
const sendEmailEvent = defineEventPublisher(notificationsExchange, emailMessage, {
  routingKey: "email",
});

// 4. The contract. Exchanges, queues and bindings are extracted from the
//    publishers and consumers — you never declare them twice.
export const contract = defineContract({
  publishers: {
    sendEmail: sendEmailEvent,
  },
  consumers: {
    processEmail: defineEventConsumer(sendEmailEvent, emailQueue),
  },
});
```

Notice the order: resources first, then references to them. Defining a queue or an exchange inline inside `defineContract` is possible but works against you — naming them makes them reusable and keeps the contract readable.

Notice too that `defineEventConsumer` takes `sendEmailEvent` — the publisher itself. That is what ties the consumer's payload type to the publisher's schema. You cannot accidentally consume a different shape than you publish.

## Step 4: Publish a message

Create `publisher.ts`:

```typescript
// publisher.ts
import { TypedAmqpClient } from "@amqp-contract/client";
import { P } from "unthrown";
import { contract } from "./contract.js";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();

console.log("Connected.");

const result = await client.publish("sendEmail", {
  to: "user@example.com",
  subject: "Welcome to amqp-contract",
  body: "This message was validated on the way out and on the way in.",
});

result.match({
  ok: () => console.log("Published."),
  errCases: (matcher) =>
    matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) =>
      console.error("The message did not match the schema:", error.message),
    ),
  defect: (cause) => {
    throw cause;
  },
});

await client.close().get();
console.log("Closed.");
```

Three things in this file are worth slowing down for.

`TypedAmqpClient.create(...)` does not return a client. It returns an `AsyncResult`, and `.get()` unwraps it. Awaiting without unwrapping would leave you holding a `Result`, not something you can call `.publish()` on.

`client.publish(...)` does not throw when the message is invalid. It returns a result you inspect. `.match` has three branches, and the compiler makes you handle all of them: `ok`, the modeled errors in `errCases`, and `defect` for the genuinely unexpected. A broken TCP connection is a defect, not a modeled error — you did not ask for it and cannot meaningfully branch on it, so here it is rethrown.

`await client.close().get()` closes the connection. The `.get()` is not decoration: without it the close result is discarded silently.

## Step 5: Consume the message

Create `consumer.ts`:

```typescript
// consumer.ts
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { contract } from "./contract.js";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processEmail: ({ payload }) => {
      console.log("Received an email to send:");
      console.log(`  To:      ${payload.to}`);
      console.log(`  Subject: ${payload.subject}`);
      console.log(`  Body:    ${payload.body}`);

      // Handlers report success by returning a result, never by returning
      // nothing and never by throwing.
      return OkAsync(undefined);
    },
  },
  urls: ["amqp://localhost"],
}).get();

console.log("Waiting for messages. Press Ctrl+C to stop.");

process.on("SIGINT", async () => {
  await worker.close().get();
  process.exit(0);
});
```

`payload` is fully typed — `payload.to` is a `string`, and `payload.recipient` would not compile. You wrote that type once, in the contract, and it flowed here on its own.

Handlers return an `AsyncResult` rather than being `async` functions. That is deliberate: it forces every failure to be a value the worker can route (retry it? dead-letter it?) instead of an exception it has to guess about. Returning `OkAsync(undefined)` says "this message is handled, acknowledge it".

## Step 6: Run it

Open two terminals. In the first:

```bash
npx tsx consumer.ts
```

```
Waiting for messages. Press Ctrl+C to stop.
```

In the second:

```bash
npx tsx publisher.ts
```

```
Connected.
Published.
Closed.
```

And back in the first terminal:

```
Received an email to send:
  To:      user@example.com
  Subject: Welcome to amqp-contract
  Body:    This message was validated on the way out and on the way in.
```

You have sent and received your first type-safe AMQP message. Now look at the RabbitMQ dashboard you left open: under **Exchanges** you will find `notifications`, and under **Queues** the `email-notifications` queue, bound with the routing key `email`. You never declared any of that against the broker yourself — the worker set up the topology from the contract on startup.

## Step 7: Break it on purpose

The point of a contract is what it refuses. Try each of these.

**Send a field that does not exist.** In `publisher.ts`, change `subject` to `title`. Before you run anything, your editor underlines it, and `npx tsc --noEmit` fails. The message never reaches the broker because the program never compiles.

**Send a value the schema rejects.** Change `to` to `"not-an-email"` and keep the field names correct. This compiles — `"not-an-email"` is a perfectly good `string`, and TypeScript has no way to know Zod wanted an email address. Run it:

```
The message did not match the schema: ...
```

The publish returned a `MessageValidationError` and the message was never sent. This is the case the `errCases` branch exists for, and why compile-time types alone are not enough: validation catches at runtime what the type system cannot express.

**Stop the broker.** Run `docker stop rabbitmq` and then the publisher. The program throws, because you told it to — that is your `defect` branch rethrowing. Restart it with `docker start rabbitmq`.

## What you learned

- A **contract** is one definition that produces both the TypeScript types and the AMQP topology.
- Types are checked when you compile; **schemas are checked at runtime**, on publish and again on consume. The two catch different mistakes.
- Nothing in the public API throws. Operations return results with three channels — `ok`, a modeled error, or a **defect** — and the compiler makes you address each one.

## Where next

The natural next lesson is [adding request/reply](/tutorial/adding-request-reply), which extends this same project with an RPC that returns a value to its caller.

If you would rather go sideways than forward:

- [Core concepts](/explanation/core-concepts) explains the model you have just been using.
- [Define a contract](/how-to/define-a-contract) covers commands, wildcards and the rest of the topology.
- [Errors as values](/explanation/errors-as-values) explains why none of this throws.
