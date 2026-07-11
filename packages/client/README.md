# @amqp-contract/client

**Type-safe AMQP client for publishing messages using amqp-contract with explicit error handling via `Result` types.**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/client.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/client)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/client.svg)](https://www.npmjs.com/package/@amqp-contract/client)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/api/client)**

## Installation

```bash
pnpm add @amqp-contract/client
```

## Usage

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract";

// Create client from contract (automatically connects and waits for connection)
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
  })
).getOrThrow();

// Publish message with explicit error handling
const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  amount: 99.99,
});
result.match({
  ok: () => console.log("Published successfully"),
  err: (error) => console.error("Publish failed:", error),
  defect: (cause) => {
    throw cause;
  },
});

// Clean up
await client.close();
```

## Error Handling

The client uses `Result` types from [unthrown](https://github.com/btravstack/unthrown) for explicit error handling. Runtime errors are part of the type signature:

```typescript
publish(): Result<boolean, TechnicalError | MessageValidationError>
```

**Error Types:**

- `TechnicalError` - Runtime failures (channel buffer full, network issues, etc.)
- `MessageValidationError` - Message fails schema validation

**Programming Errors** (client not initialized, invalid publisher name) throw exceptions since they indicate bugs caught by TypeScript at compile-time.

## API

For complete API documentation, see the [Client API Reference](https://btravstack.github.io/amqp-contract/api/client).

## Documentation

📖 **[Read the full documentation →](https://btravstack.github.io/amqp-contract)**

## License

MIT
