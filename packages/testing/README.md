# @amqp-contract/testing

**Testing utilities for AMQP contracts using testcontainers.**

[![CI](https://github.com/btravers/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravers/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/testing.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/testing)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/testing.svg)](https://www.npmjs.com/package/@amqp-contract/testing)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📖 **[Full documentation →](https://btravers.github.io/amqp-contract/guide/getting-started)**

## Features

- 🐳 Automatically starts RabbitMQ container for tests
- ✅ Works with Vitest globalSetup
- 🚀 Fast and reliable integration testing
- 📊 Includes RabbitMQ management console

## Installation

```bash
pnpm add -D @amqp-contract/testing
```

## Usage

### 1. Configure Vitest

Add to your `vitest.config.ts`:

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["@amqp-contract/testing/global-setup"],
  },
});
```

### 2. TypeScript Support

For TypeScript projects, reference the type definitions in your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "types": ["@amqp-contract/testing/types/vitest"]
  }
}
```

Or add a triple-slash reference in your test files:

```typescript
/// <reference types="@amqp-contract/testing/types/vitest" />
```

This provides type-safe access to the test container context variables.

### 3. Use Vitest Extension in Tests

The package provides a Vitest extension that automatically manages RabbitMQ connections:

```typescript
import { describe, expect } from "vitest";
import { it } from "@amqp-contract/testing/extension";

describe("Order Processing", () => {
  it("should publish and consume messages", async ({ amqpConnection }) => {
    // amqpConnection is automatically provided and cleaned up
    // Your test code here using amqpConnection
  });
});
```

The extension provides:

- `vhost`: A unique virtual host created for test isolation (automatically cleaned up after the test)
- `amqpConnectionUrl`: A connection URL pre-configured with the test vhost
- `amqpConnection`: An established connection to the RabbitMQ testcontainer
- `amqpChannel`: A channel for AMQP operations
- `publishMessage`: Helper function for publishing test messages
- `initConsumer`: Helper function for setting up test consumers
- Automatic connection and vhost cleanup after each test

## What It Does

**Global Setup:**

1. Starts a RabbitMQ container with management plugin
2. Waits for RabbitMQ to be healthy
3. Provides connection details to your tests
4. Cleans up the container after tests complete

**Test Extension:**

1. Creates a unique virtual host (vhost) for each test to ensure complete isolation
2. Provides pre-configured connections and helpers for interacting with RabbitMQ
3. Automatically cleans up the vhost and connections after each test completes

## Container Details

- **Image**: `rabbitmq:4.2.1-management-alpine` (default)
  - Can be configured via `RABBITMQ_IMAGE` environment variable
- **Ports**:
  - 5672 (AMQP)
  - 15672 (Management console)
- **Credentials**:
  - User: `guest`
  - Password: `guest`

## Environment Variables

### Configuration

- `RABBITMQ_IMAGE` - Docker image to use for the RabbitMQ container
  - Default: `rabbitmq:4.2.1-management-alpine`
  - Can be set to any compatible RabbitMQ image with management plugin

### Test Context

The following variables are provided to tests:

- `__TESTCONTAINERS_RABBITMQ_IP__` - Container host IP
- `__TESTCONTAINERS_RABBITMQ_PORT_5672__` - Mapped AMQP port
- `__TESTCONTAINERS_RABBITMQ_PORT_15672__` - Mapped management port

## Documentation

📖 **[Read the full documentation →](https://btravers.github.io/amqp-contract)**

## License

MIT
