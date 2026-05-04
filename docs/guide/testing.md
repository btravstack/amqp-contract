# Testing

The `@amqp-contract/testing` package provides testing utilities for Node.js projects that use RabbitMQ with Vitest. It automatically manages RabbitMQ containers for your integration tests using testcontainers, ensuring each test runs in an isolated environment.

## Features

- 🐳 **Automatic Container Management**: Starts and stops RabbitMQ containers for your tests
- 🔒 **Test Isolation**: Each test gets its own virtual host (vhost) for complete isolation
- ✅ **Vitest Integration**: Works seamlessly with Vitest's globalSetup and fixtures
- 🚀 **Fast and Reliable**: Built on testcontainers for consistent test environments
- 📊 **Management Console**: Includes RabbitMQ management plugin for debugging
- 🛠️ **Test Helpers**: Pre-configured connections, channels, and helper functions

## Installation

```bash
pnpm add -D @amqp-contract/testing
```

::: info Prerequisites

- Docker must be installed and running on your system
- Vitest 4.0 or higher
  :::

## Configuration

### 1. Configure Vitest Global Setup

Add the global setup to your `vitest.config.ts`:

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["@amqp-contract/testing/global-setup"],
  },
});
```

This will start a RabbitMQ container before all tests run and stop it after tests complete.

### 2. TypeScript Support (Optional)

For TypeScript projects, you can add type definitions for the test context variables:

**Option A**: Add to `tsconfig.json`:

```json
{
  "compilerOptions": {
    "types": ["@amqp-contract/testing/types/vitest"]
  }
}
```

**Option B**: Add a triple-slash reference in your test files:

```typescript
/// <reference types="@amqp-contract/testing/types/vitest" />
```

## Usage

### Basic Example

The package provides a custom `it` function that extends Vitest's test runner with AMQP fixtures:

```typescript
import { describe, expect } from "vitest";
import { it } from "@amqp-contract/testing/extension";

describe("Message Processing", () => {
  it("should publish and consume messages", async ({
    amqpChannel,
    publishMessage,
    initConsumer,
  }) => {
    // Declare exchange
    await amqpChannel.assertExchange("test-exchange", "topic", { durable: false });

    // Set up consumer
    const waitForMessages = await initConsumer("test-exchange", "test.routing.key");

    // Publish message
    publishMessage("test-exchange", "test.routing.key", {
      orderId: "123",
      amount: 100,
    });

    // Wait for and verify message
    const messages = await waitForMessages();
    expect(messages).toHaveLength(1);

    const content = JSON.parse(messages[0].content.toString());
    expect(content).toEqual({
      orderId: "123",
      amount: 100,
    });
  });
});
```

### Testing with Contracts

You can use the testing utilities with your AMQP contracts:

```typescript
import { describe, expect } from "vitest";
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { okAsync, ResultAsync, Result } from "neverthrow";
import { contract } from "./contract.js";

describe("Order Processing Contract", () => {
  it("should process orders through the contract", async ({
    amqpConnection,
    amqpConnectionUrl,
  }) => {
    // Create client
    const client = (
      await TypedAmqpClient.create({
        contract,
        urls: [amqpConnectionUrl],
      })
    )._unsafeUnwrap();

    // Create worker with handler
    const receivedPayloads: unknown[] = [];
    const worker = (
      await TypedAmqpWorker.create({
        contract,
        handlers: {
          processOrder: ({ payload }) => {
            receivedPayloads.push(payload);
            return okAsync(undefined);
          },
        },
        urls: [amqpConnectionUrl],
      })
    )._unsafeUnwrap();

    // Publish message
    const result = await client.publish("orderCreated", {
      orderId: "123",
      customerId: "456",
      amount: 99.99,
    });
    expect(result.isOk()).toBe(true);

    // Wait for message to be processed
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(receivedPayloads).toHaveLength(1);
    expect(receivedPayloads[0]).toMatchObject({
      orderId: "123",
      customerId: "456",
      amount: 99.99,
    });

    // Cleanup
    await client.close();
    await worker.close();
  });
});
```

## Available Fixtures

The Vitest extension provides the following fixtures:

### `vhost`

- **Type**: `string`
- **Description**: A unique virtual host (vhost) created for each test
- **Lifecycle**: Automatically created before test and deleted after

```typescript
it("example", async ({ vhost }) => {
  console.log(`Test running in vhost: ${vhost}`);
});
```

### `amqpConnectionUrl`

- **Type**: `string`
- **Description**: Pre-configured AMQP connection URL with the test vhost
- **Format**: `amqp://guest:guest@host:port/vhost`

```typescript
it("example", async ({ amqpConnectionUrl }) => {
  // Use with custom connections
  const connection = await amqp.connect(amqpConnectionUrl);
});
```

### `amqpConnection`

- **Type**: `ChannelModel` (from amqplib)
- **Description**: Active AMQP connection to RabbitMQ
- **Lifecycle**: Automatically closed after test

```typescript
it("example", async ({ amqpConnection }) => {
  const channel = await amqpConnection.createChannel();
  // ... use channel
});
```

### `amqpChannel`

- **Type**: `Channel` (from amqplib)
- **Description**: AMQP channel for operations
- **Lifecycle**: Automatically closed after test

```typescript
it("example", async ({ amqpChannel }) => {
  await amqpChannel.assertExchange("test-exchange", "topic");
  await amqpChannel.assertQueue("test-queue");
});
```

### `publishMessage`

- **Type**: `(exchange: string, routingKey: string, content: unknown) => void`
- **Description**: Helper function to publish messages
- **Note**: Content is automatically JSON serialized

```typescript
it("example", async ({ publishMessage }) => {
  publishMessage("my-exchange", "routing.key", { data: "test" });
});
```

### `initConsumer`

- **Type**: `(exchange: string, routingKey: string) => Promise<(options?: { nbEvents?: number; timeout?: number }) => Promise<ConsumeMessage[]>>`
- **Description**: Initialize a message consumer on a temporary queue
- **Returns**: Function to wait for and collect messages

```typescript
it("example", async ({ initConsumer, publishMessage }) => {
  // Initialize consumer
  const waitForMessages = await initConsumer("my-exchange", "routing.key");

  // Publish messages
  publishMessage("my-exchange", "routing.key", { data: "test1" });
  publishMessage("my-exchange", "routing.key", { data: "test2" });

  // Wait for 2 messages with 10 second timeout
  const messages = await waitForMessages({ nbEvents: 2, timeout: 10000 });
  expect(messages).toHaveLength(2);
});
```

## Container Details

The RabbitMQ container is configured with:

- **Image**: `rabbitmq:4.2.1-management-alpine` (default)
  - Can be configured via `RABBITMQ_IMAGE` environment variable
- **Ports**:
  - 5672 (AMQP)
  - 15672 (Management console)
- **Credentials**:
  - Username: `guest`
  - Password: `guest`
- **Health Check**: Waits for RabbitMQ to be fully ready

### Custom RabbitMQ Image

You can use a custom RabbitMQ image by setting the `RABBITMQ_IMAGE` environment variable:

```bash
# Use a specific version
RABBITMQ_IMAGE=rabbitmq:3.13-management pnpm test:integration

# Use a custom image
RABBITMQ_IMAGE=my-registry.com/rabbitmq:custom pnpm test:integration
```

## Environment Variables

### Configuration Variables

These environment variables can be set to configure the test environment:

- `RABBITMQ_IMAGE`: Docker image to use for the RabbitMQ container
  - Default: `rabbitmq:4.2.1-management-alpine`
  - Can be set to any compatible RabbitMQ image with management plugin

### Test Context Variables

The following variables are provided to tests via Vitest's context:

- `__TESTCONTAINERS_RABBITMQ_IP__`: Container host IP address
- `__TESTCONTAINERS_RABBITMQ_PORT_5672__`: Mapped AMQP port
- `__TESTCONTAINERS_RABBITMQ_PORT_15672__`: Mapped management console port
- `__TESTCONTAINERS_RABBITMQ_USERNAME__`: RabbitMQ username (default: "guest")
- `__TESTCONTAINERS_RABBITMQ_PASSWORD__`: RabbitMQ password (default: "guest")

These are automatically used by the fixtures, but you can access them directly if needed:

```typescript
import { inject } from "vitest";

const rabbitMQIP = inject("__TESTCONTAINERS_RABBITMQ_IP__");
const amqpPort = inject("__TESTCONTAINERS_RABBITMQ_PORT_5672__");
```

## Advanced Usage

### Multiple Consumers

Test scenarios with multiple consumers:

```typescript
it("should route messages to multiple consumers", async ({
  amqpChannel,
  publishMessage,
  initConsumer,
}) => {
  await amqpChannel.assertExchange("orders", "topic", { durable: false });

  // Initialize multiple consumers
  const waitForCreated = await initConsumer("orders", "order.created");
  const waitForUpdated = await initConsumer("orders", "order.updated");
  const waitForAll = await initConsumer("orders", "order.#");

  // Publish different events
  publishMessage("orders", "order.created", { id: "1" });
  publishMessage("orders", "order.updated", { id: "2" });

  // Verify routing
  const createdMessages = await waitForCreated({ nbEvents: 1 });
  const updatedMessages = await waitForUpdated({ nbEvents: 1 });
  const allMessages = await waitForAll({ nbEvents: 2 });

  expect(createdMessages).toHaveLength(1);
  expect(updatedMessages).toHaveLength(1);
  expect(allMessages).toHaveLength(2);
});
```

### Custom Timeouts

Adjust timeout for slow operations:

```typescript
it("should handle slow message processing", async ({
  amqpChannel,
  initConsumer,
  publishMessage,
}) => {
  await amqpChannel.assertExchange("exchange", "topic", { durable: false });

  const waitForMessages = await initConsumer("exchange", "key");

  publishMessage("exchange", "key", { task: "slow-operation" });

  // Wait up to 30 seconds
  const messages = await waitForMessages({
    nbEvents: 1,
    timeout: 30000,
  });

  expect(messages).toHaveLength(1);
});
```

### Testing Error Scenarios

Test error handling and dead letter exchanges:

```typescript
it("should handle message failures", async ({ amqpChannel }) => {
  // Set up dead letter exchange
  await amqpChannel.assertExchange("dlx", "direct", { durable: false });
  await amqpChannel.assertQueue("dlq", { durable: false });
  await amqpChannel.bindQueue("dlq", "dlx", "");

  // Set up main queue with DLX
  await amqpChannel.assertQueue("main-queue", {
    durable: false,
    deadLetterExchange: "dlx",
  });

  // Test message rejection flow
  // ... your error handling tests
});
```

## Best Practices

1. **Use Test Isolation**: Each test automatically gets its own vhost - take advantage of this for independent tests

2. **Clean Up Resources**: Fixtures automatically clean up connections and vhosts, but close any additional resources you create

3. **Use Appropriate Timeouts**: Default timeout is 5 seconds; adjust based on your test needs

4. **Test Realistic Scenarios**: Use the helpers to test actual message flows through your contracts

5. **Debug with Management Console**: Access the management console during test development:
   ```typescript
   it("debug test", async () => {
     const ip = inject("__TESTCONTAINERS_RABBITMQ_IP__");
     const port = inject("__TESTCONTAINERS_RABBITMQ_PORT_15672__");
     console.log(`Management: http://${ip}:${port}`);
     // Add a long timeout to inspect the state
     await new Promise((resolve) => setTimeout(resolve, 60000));
   });
   ```

## Troubleshooting

### Container Won't Start

**Problem**: Docker container fails to start

**Solutions**:

- Ensure Docker is running
- Check Docker has enough resources (memory, disk space)
- Verify no port conflicts (5672, 15672)
- Check Docker logs for errors

### Tests Timeout

**Problem**: Tests timeout waiting for messages

**Solutions**:

- Increase timeout: `waitForMessages({ timeout: 10000 })`
- Verify exchanges and queues are properly declared
- Check routing keys match between publisher and consumer
- Use management console to inspect message flow

### Type Errors

**Problem**: TypeScript errors about test context

**Solutions**:

- Add type reference: `/// <reference types="@amqp-contract/testing/types/vitest" />`
- Or update `tsconfig.json` with the types
- Ensure `vitest` peer dependency is satisfied

## API Reference

For complete API documentation, see [@amqp-contract/testing API Reference](/api/testing/).

## Examples

Check out the [examples directory](https://github.com/btravers/amqp-contract/tree/main/examples) in the repository for more testing examples.
