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

Bundle size rarely matters here — contracts run on a server, not in a browser. Validation speed matters only if profiling puts schema validation on your hot path, which for typical message sizes it will not.

The practical advice: **use Zod unless you have a specific reason not to.** It has the largest ecosystem, the most examples, and the best-supported AsyncAPI converter. Reach for Valibot when you have measured validation cost and it matters, or when a shared contract package genuinely ships to a browser. Reach for ArkType if you prefer its syntax.

## Usage

All three work identically with `defineMessage`:

```typescript
import { z } from "zod";

defineMessage(z.object({ orderId: z.string(), amount: z.number().positive() }));
```

```typescript
import * as v from "valibot";

defineMessage(v.object({ orderId: v.string(), amount: v.pipe(v.number(), v.minValue(0)) }));
```

```typescript
import { type } from "arktype";

defineMessage(type({ orderId: "string", amount: "number>0" }));
```

Payload types are inferred from whichever you pick; handlers are unaffected by the choice.

## Mixing libraries

Nothing stops you using different libraries for different messages in one contract — validation is per message. It is legal but rarely a good idea, since readers then need to know all of them.

## AsyncAPI conversion

Generating an AsyncAPI document needs a converter that turns your schema into JSON Schema:

```typescript
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";

const generator = new AsyncAPIGenerator({
  schemaConverters: [new ZodToJsonSchemaConverter()],
});
```

Zod's converter is the best supported. Without a converter matching your library, payload schemas degrade to a generic `{ type: "object" }` placeholder — the document generates, but its message shapes carry no information. Set `failOnMissingConverter: true` in CI to make that an error rather than a silent downgrade. See [generate AsyncAPI](/how-to/generate-asyncapi).

This is the strongest practical argument for Zod: if you want a useful AsyncAPI document, its conversion path is the most complete.

## Validation is stricter than types

Worth stating plainly, whichever library you choose. TypeScript checks the _shape_; the schema checks the _values_. `z.string().email()` is a `string` to the compiler, so an invalid address compiles and then fails validation at runtime.

That gap is deliberate — it is why validation exists in addition to types. See [core concepts](/explanation/core-concepts#validation-happens-at-both-boundaries).

## Switching libraries

Because validation is confined to `defineMessage`, switching is a contract-level change. Handlers, publishers and consumers are untouched as long as the inferred type is the same.

Migrate one message at a time and let the compiler find the drift: if the new schema infers a different type, every affected handler stops compiling.

## Where next

- [Define a contract](/how-to/define-a-contract) — using schemas in practice.
- [Generate AsyncAPI](/how-to/generate-asyncapi) — converter setup.
- [Tune performance](/how-to/tune-performance#validation-cost) — when validation cost is real.
