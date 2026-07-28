---
title: Generate AsyncAPI - amqp-contract
description: Produce an AsyncAPI 3.0 document from a contract, export it as JSON or YAML, and check it into CI.
---

# Generate AsyncAPI

`@amqp-contract/asyncapi` turns a contract into an [AsyncAPI 3.0](https://www.asyncapi.com/) document — the messaging counterpart to OpenAPI. Because it is generated from the contract your code already uses, it cannot drift from what the services actually do.

## Generate a document

```bash
pnpm add @amqp-contract/asyncapi
pnpm add -D @orpc/zod
```

```typescript
import { AsyncAPIGenerator } from "@amqp-contract/asyncapi";
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
import { contract } from "./contract.js";

const generator = new AsyncAPIGenerator({
  schemaConverters: [new ZodToJsonSchemaConverter()],
});

export const spec = await generator.generate(contract, {
  info: {
    title: "Order Processing API",
    version: "1.0.0",
    description: "Type-safe AMQP messaging for order processing",
  },
  servers: {
    production: {
      host: "rabbitmq.example.com:5672",
      protocol: "amqp",
      description: "Production",
    },
    development: {
      host: "localhost:5672",
      protocol: "amqp",
      description: "Local development",
    },
  },
});
```

The converter is what turns your schemas into JSON Schema. Without one, payloads degrade to a generic `{ type: "object" }` placeholder — the document still generates, but its message shapes are useless.

## Write it to a file

```typescript
// scripts/generate-asyncapi-json.ts
import { writeFileSync } from "node:fs";
import { spec } from "./generate-spec.js";

writeFileSync("asyncapi.json", JSON.stringify(spec, null, 2));
```

For YAML:

```typescript
// scripts/generate-asyncapi-yaml.ts
import { writeFileSync } from "node:fs";
import YAML from "yaml";
import { spec } from "./generate-spec.js";

writeFileSync("asyncapi.yaml", YAML.stringify(spec));
```

```json
{
  "scripts": {
    "generate:asyncapi:json": "tsx scripts/generate-asyncapi-json.ts",
    "generate:asyncapi:yaml": "tsx scripts/generate-asyncapi-yaml.ts"
  }
}
```

## Fail the build on an unconvertible schema

By default an unconvertible schema silently becomes a placeholder. In CI — especially if anyone generates code from the document — you want that to be an error:

```typescript
const generator = new AsyncAPIGenerator({
  schemaConverters: [new ZodToJsonSchemaConverter()],
  failOnMissingConverter: true,
});
```

## Improve the generated document

The generator can only publish what the contract carries, so descriptions come from `defineMessage`:

```typescript
const orderMessage = defineMessage(orderSchema, {
  summary: "Order created event",
  description: "Emitted when a new order enters the system",
});
```

Field-level descriptions come from the schema itself:

```typescript
const orderSchema = z.object({
  orderId: z.string().describe("Unique order identifier"),
  amount: z.number().positive().describe("Total in the order's currency"),
});
```

Both flow into the document, so documentation quality is a property of the contract rather than a separate artefact to maintain.

## Keep it current in CI

Regenerate and diff, so a contract change that was not committed fails the build:

```yaml
- run: pnpm generate:asyncapi:json
- run: git diff --exit-code asyncapi.json
```

Validate it too:

```bash
npx @asyncapi/cli validate asyncapi.json
```

## Use the document

```bash
# Human-readable HTML docs
npx @asyncapi/cli generate fromTemplate asyncapi.json @asyncapi/html-template -o docs/

# Client code in another language
npx @asyncapi/cli generate fromTemplate asyncapi.json @asyncapi/python-paho-template -o clients/python
```

[AsyncAPI Studio](https://studio.asyncapi.com/) renders a pasted document interactively, which is the fastest way to share a contract with someone who does not read TypeScript.

Codegen is also the practical answer to "how do non-TypeScript services join in?" — they get generated types from the same source of truth, though not the runtime validation.

## Read cross-domain routing

Bridge exchanges appear on both channels, with a readable summary in the description and structure under `x-amqp-exchange-bindings`. See [bridge domains](/how-to/bridge-domains#see-bridges-in-the-asyncapi-document).

## Where next

- [Define a contract](/how-to/define-a-contract#add-validated-headers-to-a-message) — where `summary` and `description` live.
- [Schema libraries](/reference/schema-libraries) — converter support per library.
