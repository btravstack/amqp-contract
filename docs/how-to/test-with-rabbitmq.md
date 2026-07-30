---
title: Test with RabbitMQ - amqp-contract
description: Run integration tests against a real broker with the Vitest extension — fixtures, isolation, timeouts and CI.
---

# Test with RabbitMQ

`@amqp-contract/testing` runs your tests against a real RabbitMQ in a container, one isolated virtual host per test. Testing messaging against a mock mostly tests the mock; this tests routing, bindings, acknowledgement and dead-lettering as the broker actually implements them.

Requires Docker and Vitest 4+.

## Set it up

```bash
pnpm add -D @amqp-contract/testing
```

```typescript
// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["@amqp-contract/testing/global-setup"],
    testTimeout: 10_000,
    hookTimeout: 10_000,
  },
});
```

The global setup starts one container before the suite and stops it after. The default timeouts are too tight for broker round trips — raise them as above.

No TypeScript configuration is needed for the fixtures: the `it` exported by `@amqp-contract/testing/extension` is fully typed. Only if you call Vitest's `inject()` yourself to read the raw container context (IP, ports, credentials) do you need the `ProvidedContext` augmentation — load it with a side-effect import in that test file:

```typescript
import "@amqp-contract/testing/global-setup";
import { inject } from "vitest";

const host = inject("__TESTCONTAINERS_RABBITMQ_IP__");
```

## Test a worker end to end

Import `it` from the extension — it is Vitest's `it` plus AMQP fixtures.

```typescript
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker, declareHandlers } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { contract } from "./contract.js";

describe("order worker", () => {
  it("processes a created order", async ({ amqpConnectionUrl, publishMessage }) => {
    const processed: unknown[] = [];

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: declareHandlers(contract, {
        processOrder: ({ payload }) => {
          processed.push(payload);
          return OkAsync();
        },
      }),
      urls: [amqpConnectionUrl],
    }).get();

    try {
      const order = { orderId: "TEST-001", amount: 59.98 };

      publishMessage(
        contract.publishers.orderCreated.exchange.name,
        contract.publishers.orderCreated.routingKey,
        order,
      );

      await vi.waitFor(() => {
        if (processed.length < 1) throw new Error("not processed yet");
      });
      expect(processed).toEqual([order]);
    } finally {
      await worker.close().get();
    }
  });
});
```

Two things make this reliable. Take the exchange and routing key **from the contract** rather than retyping them, so a contract change breaks the test instead of silently bypassing it. And close the worker in `finally`, so a failing assertion does not leak a consumer into the next test.

`vi.waitFor` is the right tool for consumption: delivery is asynchronous and a fixed `setTimeout` is either flaky or slow.

## Test a client

```typescript
it("publishes a valid order", async ({ amqpConnectionUrl }) => {
  const client = await TypedAmqpClient.create({
    contract,
    urls: [amqpConnectionUrl],
  }).get();

  const result = await client.publish("orderCreated", { orderId: "T-1", amount: 10 });

  expect(result.isOk()).toBe(true);
  await client.close().get();
});
```

And that validation actually rejects:

```typescript
const result = await client.publish("orderCreated", { orderId: "T-1", amount: -1 });
expect(result.isErr()).toBe(true);
```

## Use the fixtures

| Fixture             | What it gives you                                                       |
| ------------------- | ----------------------------------------------------------------------- |
| `vhost`             | A unique virtual host, created before the test and deleted after        |
| `amqpConnectionUrl` | Connection URL scoped to that vhost — what you pass to `urls`           |
| `amqpConnection`    | An open amqplib `ChannelModel`, closed automatically                    |
| `amqpChannel`       | An open amqplib `Channel`, closed automatically                         |
| `publishMessage`    | `(exchange, routingKey, content) => void`, JSON-serializing             |
| `initConsumer`      | `(exchange, routingKey) => Promise<waitFn>` for collecting raw messages |

The per-test vhost is what makes tests independent: queues, exchanges and messages are destroyed with it, so tests can run in any order without cleanup code.

## Assert on raw messages

When you care what actually went on the wire, consume directly:

```typescript
it("routes urgent updates to the urgent queue", async ({ initConsumer, publishMessage }) => {
  const waitForMessages = await initConsumer("orders", "order.*.urgent");

  publishMessage("orders", "order.updated.urgent", { orderId: "1" });

  const messages = await waitForMessages({ count: 1, timeoutMs: 10_000 });
  expect(JSON.parse(messages[0].content.toString())).toEqual({ orderId: "1" });
});
```

Note `initConsumer` must be awaited _before_ publishing — the queue has to exist and be bound, or the message is routed nowhere and dropped.

## Test failure paths

Dead-lettering is worth testing precisely because it only shows up when things go wrong:

```typescript
it("dead-letters a non-retryable failure", async ({
  amqpConnectionUrl,
  publishMessage,
  initConsumer,
}) => {
  const waitForDead = await initConsumer("orders-dlx", "order.failed");

  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: declareHandlers(contract, {
      processOrder: () => ErrAsync(new NonRetryableError("permanent")),
    }),
    urls: [amqpConnectionUrl],
  }).get();

  try {
    publishMessage("orders", "order.created", { orderId: "1", amount: 1 });
    expect(await waitForDead({ count: 1, timeoutMs: 10_000 })).toHaveLength(1);
  } finally {
    await worker.close().get();
  }
});
```

Be careful testing `ttl-backoff`: the real delays apply, so a test with `initialDelayMs: 1000` and five retries takes half a minute. Use short delays in test contracts.

## Pin the broker version

```bash
RABBITMQ_IMAGE=rabbitmq:4.2.1-management-alpine pnpm test
```

Worth doing in CI so a new upstream image cannot change behaviour under you.

## Reset shared connections

Connections are cached per process, so a suite asserting on connection counts should reset between tests — see [share connections](/how-to/share-connections#reset-the-cache-between-tests).

## Run in CI

The container needs a Docker daemon. On GitHub Actions' `ubuntu-latest` that is already present, so no service definition is required — testcontainers manages the lifecycle.

Allow for the image pull on a cold runner: the first test run is much slower than later ones, which is a `hookTimeout` problem rather than a broker problem.

## Where next

- [Consume messages](/how-to/consume-messages) — the handler shapes under test.
- [Route dead letters](/how-to/route-dead-letters) — what the failure test above verifies.
