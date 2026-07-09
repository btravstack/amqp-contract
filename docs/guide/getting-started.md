---
title: Getting Started with amqp-contract - Type-safe AMQP/RabbitMQ for TypeScript
description: Learn how to build type-safe AMQP messaging applications with amqp-contract. Step-by-step guide for Node.js developers using TypeScript and RabbitMQ.
---

# Getting Started

Get **amqp-contract** running in 5 minutes with a complete working example.

## What is amqp-contract?

amqp-contract brings end-to-end type safety to [AMQP](https://www.amqp.org/)/[RabbitMQ](https://www.rabbitmq.com/) messaging. Define your contract once, and get automatic validation, type inference, and compile-time checks throughout your application.

## Prerequisites

- **Node.js 22.19+** - [Download Node.js](https://nodejs.org/)
- **RabbitMQ running locally** - We'll use Docker (see below)
- **Basic TypeScript knowledge** - Understanding of TypeScript syntax

## Step 1: Start RabbitMQ

Use Docker to run RabbitMQ with the management plugin:

```bash
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:4-management
```

**Verify it's running:**

- Open [http://localhost:15672](http://localhost:15672)
- Login with `guest` / `guest`

::: tip
If you already have RabbitMQ running locally, you can skip this step!
:::

### Manual Installation

Follow the [official RabbitMQ installation guide](https://www.rabbitmq.com/docs/download).

## Step 2: Install Packages

Create a new project and install dependencies:

::: code-group

```bash [pnpm]
# Create project
mkdir amqp-demo && cd amqp-demo
npm init -y

# Install dependencies
pnpm add @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown zod
pnpm add -D typescript tsx

# Initialize TypeScript
npx tsc --init --target ES2022 --module NodeNext --moduleResolution NodeNext
```

```bash [npm]
# Create project
mkdir amqp-demo && cd amqp-demo
npm init -y

# Install dependencies
npm install @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown zod
npm install -D typescript tsx

# Initialize TypeScript
npx tsc --init --target ES2022 --module NodeNext --moduleResolution NodeNext
```

```bash [yarn]
# Create project
mkdir amqp-demo && cd amqp-demo
npm init -y

# Install dependencies
yarn add @amqp-contract/contract @amqp-contract/client @amqp-contract/worker unthrown zod
yarn add -D typescript tsx

# Initialize TypeScript
npx tsc --init --target ES2022 --module NodeNext --moduleResolution NodeNext
```

:::

::: info Why these packages?
[`unthrown`](https://github.com/btravstack/unthrown) appears in the public types (handlers return `AsyncResult<void, HandlerError>`), so you need it directly to construct handler results. `zod` can be any [Standard Schema](https://standardschema.dev/) library — see [Alternative Schema Libraries](#alternative-schema-libraries) below. You do **not** need to install `amqplib` yourself; it ships as a dependency of the amqp-contract packages.
:::

### Optional Packages

#### Testing

For integration testing with RabbitMQ testcontainers:

```bash
pnpm add -D @amqp-contract/testing
```

See the [Testing Guide](/guide/testing) for more details.

#### AsyncAPI Generation

For generating AsyncAPI 3.0 specifications:

```bash
pnpm add @amqp-contract/asyncapi
```

#### Alternative Schema Libraries

Instead of [Zod](https://zod.dev/), use [Valibot](https://valibot.dev/) or [ArkType](https://arktype.io/):

```bash
# Valibot
pnpm add valibot

# ArkType
pnpm add arktype
```

## Step 3: Create Contract

Create `contract.ts` - this defines your message schema and AMQP topology:

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

// 1. Define resources
const notificationsExchange = defineExchange("notifications", { type: "direct" });

const emailQueue = defineQueue("email-notifications");

// 2. Define message schema with Zod
const emailMessage = defineMessage(
  z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  {
    summary: "Email notification message",
    description: "Sent when an email needs to be delivered",
  },
);

// 3. Define event publisher
const sendEmailEvent = defineEventPublisher(notificationsExchange, emailMessage, {
  routingKey: "email",
});

// 4. Compose contract - exchanges, queues, and bindings are auto-extracted
export const contract = defineContract({
  publishers: {
    sendEmail: sendEmailEvent,
  },
  consumers: {
    processEmail: defineEventConsumer(sendEmailEvent, emailQueue),
  },
});
```

## Step 4: Publisher

Create `publisher.ts` - publishes a message:

```typescript
// publisher.ts
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract.js";

async function main() {
  console.log("🚀 Starting publisher...");

  // Create client
  const client = (
    await TypedAmqpClient.create({
      contract,
      urls: ["amqp://localhost"],
    })
  ).unwrap();

  console.log("✅ Connected to RabbitMQ");

  // Publish message - fully typed!
  const result = await client.publish("sendEmail", {
    to: "user@example.com",
    subject: "Welcome to amqp-contract!",
    body: "This is a type-safe message from amqp-contract.",
  });

  result.match({
    ok: () => console.log("📧 Email message published!"),
    err: (error) => console.error("❌ Failed:", error.message),
    defect: (cause) => {
      throw cause;
    },
  });

  // Clean up
  await client.close();
  console.log("👋 Publisher closed");
}

main().catch(console.error);
```

## Step 5: Consumer

Create `consumer.ts` - processes messages:

```typescript
// consumer.ts
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { Ok } from "unthrown";
import { contract } from "./contract.js";

async function main() {
  console.log("⚙️ Starting worker...");

  // Create worker with handlers
  const worker = (
    await TypedAmqpWorker.create({
      contract,
      handlers: {
        processEmail: ({ payload }) => {
          // Payload is fully typed!
          console.log("\n📬 Received email:");
          console.log(`  To: ${payload.to}`);
          console.log(`  Subject: ${payload.subject}`);
          console.log(`  Body: ${payload.body}`);

          // Report success — handlers return a Result, they don't throw
          return Ok(undefined).toAsync();
        },
      },
      urls: ["amqp://localhost"],
    })
  ).unwrap();

  console.log("✅ Worker ready, waiting for messages...\n");
  console.log("Press Ctrl+C to stop\n");

  // Handle graceful shutdown
  process.on("SIGINT", async () => {
    console.log("\n👋 Shutting down...");
    await worker.close();
    process.exit(0);
  });
}

main().catch(console.error);
```

::: tip Doing real async work in a handler?
Wrap the promise with `fromPromise(promise, qualify)` (or `fromSafePromise` if it can never reject) instead of `async`/`await` — handlers return an `AsyncResult`, not a bare promise. See the [Error Model guide](/guide/error-model) and [Worker Usage](/guide/worker-usage).
:::

## Step 6: Run It

Open **two terminal windows**:

**Terminal 1 - Start the consumer:**

```bash
npx tsx consumer.ts
```

You should see:

```
⚙️ Starting worker...
✅ Worker ready, waiting for messages...

Press Ctrl+C to stop
```

**Terminal 2 - Run the publisher:**

```bash
npx tsx publisher.ts
```

## Expected Output

**Publisher terminal:**

```
🚀 Starting publisher...
✅ Connected to RabbitMQ
📧 Email message published!
👋 Publisher closed
```

**Consumer terminal:**

```
📬 Received email:
  To: user@example.com
  Subject: Welcome to amqp-contract!
  Body: This is a type-safe message from amqp-contract.
```

**🎉 Success!** You've just sent and received your first type-safe AMQP message!

## What Just Happened?

1. **Contract Definition** - You defined the message schema with Zod and AMQP topology
2. **Type Safety** - TypeScript enforced the message structure at compile time
3. **Automatic Validation** - Zod validated the message at runtime
4. **Publisher** - The client published a message to RabbitMQ
5. **Consumer** - The worker received and processed the message

## Try This Next

**Experiment with type safety:**

In `publisher.ts`, try to publish an invalid message:

```typescript
// ❌ This will cause a TypeScript error!
await client.publish("sendEmail", {
  to: "not-an-email", // Invalid email format
  subject: "Test",
  // Missing 'body' field
});
```

**Notice:**

- TypeScript shows errors immediately
- Your IDE provides autocomplete for message fields
- You can't send invalid messages!

## Key Benefits

- ✅ **Type Safety** - Full TypeScript inference from contract to handlers
- ✅ **Auto Validation** - [Zod](https://zod.dev/) validates messages at publish and consume time
- ✅ **Compile Checks** - TypeScript catches errors before runtime
- ✅ **Better DX** - Autocomplete, refactoring, inline docs
- ✅ **Explicit Errors** - Result types for predictable error handling

## Common Issues

### "Connection refused" or "ECONNREFUSED"

**Cause:** RabbitMQ is not running or not accessible

**Solution:**

```bash
# Check if RabbitMQ container is running
docker ps | grep rabbitmq

# If not running, start it:
docker start rabbitmq

# Or create a new one:
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

### "Cannot find module" errors

**Cause:** Missing dependencies or incorrect import extensions

**Solution:**

```bash
# Reinstall dependencies
rm -rf node_modules package-lock.json
npm install

# Ensure you're using .js extensions in imports
# import { contract } from "./contract.js";  ✅
# import { contract } from "./contract";     ❌
```

### TypeScript errors about module resolution

**Cause:** Incorrect TypeScript configuration

**Solution:** Update `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "esModuleInterop": true,
    "strict": true
  }
}
```

### Messages not being consumed

**Cause:** Consumer not running or binding mismatch

**Solution:**

1. Ensure consumer is running in a separate terminal
2. Check that routing keys match between publisher and binding
3. Verify RabbitMQ management UI shows the queue has bindings

## Next Steps

Now that you have amqp-contract working, explore more:

- **[Core Concepts](/guide/core-concepts)** - Understand the architecture and patterns
- **[Defining Contracts](/guide/defining-contracts)** - Learn advanced contract features
- **[Basic Order Processing Example](/examples/basic-order-processing)** - See a complete real-world example
- **[Testing](/guide/testing)** - Write tests for your AMQP code

::: tip Need Help?

- Check the [Troubleshooting Guide](/guide/troubleshooting)
- Browse [GitHub Issues](https://github.com/btravstack/amqp-contract/issues)
- Read more [Examples](/examples/)
  :::
