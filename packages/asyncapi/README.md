# @amqp-contract/asyncapi

**AsyncAPI 3.0.0 specification generator for amqp-contract.**

[![CI](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/btravstack/amqp-contract/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@amqp-contract/asyncapi.svg?logo=npm)](https://www.npmjs.com/package/@amqp-contract/asyncapi)
[![npm downloads](https://img.shields.io/npm/dm/@amqp-contract/asyncapi.svg)](https://www.npmjs.com/package/@amqp-contract/asyncapi)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-blue?logo=typescript)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/api/asyncapi)**

## Installation

```bash
pnpm add @amqp-contract/asyncapi
```

## Usage

```typescript
import { AsyncAPIGenerator } from "@amqp-contract/asyncapi";
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
import { writeFileSync } from "fs";

import { contract } from "./contract";

// Create generator with schema converters
const generator = new AsyncAPIGenerator({
  schemaConverters: [new ZodToJsonSchemaConverter()],
});

// Generate AsyncAPI specification
const asyncAPISpec = await generator.generate(contract, {
  info: {
    title: "My AMQP API",
    version: "1.0.0",
    description: "Type-safe AMQP messaging API",
  },
  servers: {
    development: {
      host: "localhost:5672",
      protocol: "amqp",
      description: "Development RabbitMQ server",
    },
    production: {
      host: "rabbitmq.example.com:5672",
      protocol: "amqp",
      description: "Production RabbitMQ server",
    },
  },
});

// Output as JSON
console.log(JSON.stringify(asyncAPISpec, null, 2));

// Or write to file
writeFileSync("asyncapi.json", JSON.stringify(asyncAPISpec, null, 2));
```

## Features

- ✅ **AsyncAPI 3.0 compliant** with proper AMQP bindings (v0.3.0)
- ✅ **Schema validation** - Converts Zod, Valibot, and ArkType schemas to JSON Schema
- ✅ **Queue-exchange binding documentation** in channel descriptions
- ✅ **Type-safe** with full TypeScript support

For examples and detailed guides, see the [documentation](https://btravstack.github.io/amqp-contract/api/asyncapi).

## API

For complete API documentation, see the [AsyncAPI API Reference](https://btravstack.github.io/amqp-contract/api/asyncapi).

## Documentation

📖 **[Read the full documentation →](https://btravstack.github.io/amqp-contract)**

## License

MIT
