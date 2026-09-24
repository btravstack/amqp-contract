---
title: Schema libraries - amqp-contract
description: Which validation libraries work, how they compare, and how to use each one with defineMessage.
---

# Schema libraries

amqp-contract validates through [Standard Schema v1](https://standardschema.dev/), so any conforming library works. This page compares the three common choices; for how to _use_ schemas, see [define a contract](/how-to/define-a-contract).

## Comparison

|                  | [Zod](https://zod.dev/) | [Valibot](https://valibot.dev/) | [ArkType](https://arktype.io/) |
| ---------------- | ----------------------- | ------------------------------- | ------------------------------ |
| API style        | Chainable               | Functional, modular             | Type-syntax strings            |
| Bundle size      | Largest                 | Smallest (tree-shakeable)       | Middle                         |
| Validation speed | Good                    | Fastest                         | Good                           |
| Ecosystem        | Largest                 | Growing                         | Growing                        |
| Learning curve   | Low                     | Low                             | Medium                         |
| AsyncAPI schemas | Native                  | Needs a converter               | Native                         |

Bundle size rarely matters here — contracts run on a server, not in a browser. Validation speed matters only if profiling puts schema validation on your hot path, which for typical message sizes it will not.

The practical advice: **use Zod unless you have a specific reason not to.** It has the largest ecosystem, the most examples, and converts itself to JSON Schema for [AsyncAPI](#asyncapi-conversion) with no extra dependency. Reach for Valibot when you have measured validation cost and it matters, or when a shared contract package genuinely ships to a browser. Reach for ArkType if you prefer its syntax.

## Usage

All three work identically with `defineMessage`:

```typescript
import { defineMessage } from "@amqp-contract/contract";
import { z } from "zod";

defineMessage(z.object({ orderId: z.string(), amount: z.number().positive() }));
```

```typescript
import { defineMessage } from "@amqp-contract/contract";
import * as v from "valibot";

defineMessage(v.object({ orderId: v.string(), amount: v.pipe(v.number(), v.minValue(0)) }));
```

```typescript
import { defineMessage } from "@amqp-contract/contract";
import { type } from "arktype";

defineMessage(type({ orderId: "string", amount: "number>0" }));
```

Payload types are inferred from whichever you pick; handlers are unaffected by the choice.

## Mixing libraries

Nothing stops you using different libraries for different messages in one contract — validation is per message. It is legal but rarely a good idea, since readers then need to know all of them.

## AsyncAPI conversion

Generating an AsyncAPI document turns each schema into JSON Schema. How depends on the library:

| Library | Conversion                                                                      |
| ------- | ------------------------------------------------------------------------------- |
| Zod 4   | Native — implements Standard JSON Schema (`~standard.jsonSchema`); no converter |
| ArkType | Native — implements Standard JSON Schema; no converter                          |
| Valibot | Needs a converter, e.g. `@orpc/valibot`                                         |

A native schema converts itself to draft-07 and needs no configuration. For the rest, pass a converter:

```typescript
import { AsyncAPIGenerator } from "@amqp-contract/asyncapi";
import { experimental_ValibotToJsonSchemaConverter } from "@orpc/valibot";

const generator = new AsyncAPIGenerator({
  schemaConverters: [new experimental_ValibotToJsonSchemaConverter()],
});
```

`schemaConverters` takes the package's own `SchemaConverter` shape, which the oRPC converters satisfy; it is only consulted for schemas that do not convert themselves. Without either path, generation fails — `failOnMissingConverter` defaults to `true`. Set it to `false` to generate anyway, degrading payload schemas to a generic `{ type: "object" }` placeholder whose message shapes carry no information. See [generate AsyncAPI](/how-to/generate-asyncapi#convert-valibot-and-other-schemas).

## Validation is stricter than types

Worth stating plainly, whichever library you choose. TypeScript checks the _shape_; the schema checks the _values_. `z.string().email()` is a `string` to the compiler, so an invalid address compiles and then fails validation at runtime.

That gap is deliberate — it is why validation exists in addition to types. See [core concepts](/explanation/core-concepts#validation-happens-at-both-boundaries).

## Switching libraries

Because validation is confined to `defineMessage`, switching is a contract-level change. Handlers, publishers and consumers are untouched as long as the inferred type is the same.

Migrate one message at a time and let the compiler find the drift: if the new schema infers a different type, every affected handler stops compiling.

## Where next

- [Define a contract](/how-to/define-a-contract) — using schemas in practice.
- [Generate AsyncAPI](/how-to/generate-asyncapi) — generation, and converter setup for Valibot.
- [Tune performance](/how-to/tune-performance#validation-cost) — when validation cost is real.
