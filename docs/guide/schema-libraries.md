# Schema Library Comparison

amqp-contract supports any validation library that implements [Standard Schema v1](https://github.com/standard-schema/standard-schema). This guide compares the three most popular options: Zod, Valibot, and ArkType.

## Quick Comparison

| Feature        | Zod       | Valibot    | ArkType     |
| -------------- | --------- | ---------- | ----------- |
| Bundle Size    | ~57KB     | ~6KB       | ~32KB       |
| API Style      | Chainable | Functional | Type-syntax |
| Type Inference | Excellent | Excellent  | Excellent   |
| Learning Curve | Low       | Low        | Medium      |
| Ecosystem      | Largest   | Growing    | Growing     |
| Performance    | Good      | Best       | Good        |

## Installation

```bash
# Choose one:
pnpm add zod
pnpm add valibot
pnpm add arktype
```

## Syntax Comparison

### Defining a Simple Schema

**Zod:**

```typescript
import { z } from "zod";

const orderSchema = z.object({
  orderId: z.string().uuid(),
  amount: z.number().positive(),
  status: z.enum(["pending", "completed", "cancelled"]),
});
```

**Valibot:**

```typescript
import * as v from "valibot";

const orderSchema = v.object({
  orderId: v.pipe(v.string(), v.uuid()),
  amount: v.pipe(v.number(), v.minValue(0)),
  status: v.picklist(["pending", "completed", "cancelled"]),
});
```

**ArkType:**

```typescript
import { type } from "arktype";

const orderSchema = type({
  orderId: "string.uuid",
  amount: "number > 0",
  status: "'pending' | 'completed' | 'cancelled'",
});
```

### Using with amqp-contract

All three work identically with `defineMessage`:

```typescript
import { defineMessage } from "@amqp-contract/contract";

const orderMessage = defineMessage(orderSchema, {
  summary: "Order event",
});
```

## Feature Comparison

### Object Schemas

**Zod:**

```typescript
z.object({
  name: z.string(),
  email: z.string().email(),
  age: z.number().int().min(0).max(120),
});
```

**Valibot:**

```typescript
v.object({
  name: v.string(),
  email: v.pipe(v.string(), v.email()),
  age: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(120)),
});
```

**ArkType:**

```typescript
type({
  name: "string",
  email: "string.email",
  age: "integer >= 0 <= 120",
});
```

### Optional Fields

**Zod:**

```typescript
z.object({
  required: z.string(),
  optional: z.string().optional(),
  nullable: z.string().nullable(),
  defaulted: z.string().default("default value"),
});
```

**Valibot:**

```typescript
v.object({
  required: v.string(),
  optional: v.optional(v.string()),
  nullable: v.nullable(v.string()),
  defaulted: v.optional(v.string(), "default value"),
});
```

**ArkType:**

```typescript
type({
  required: "string",
  "optional?": "string",
  nullable: "string | null",
  // Defaults handled differently in ArkType
});
```

### Arrays

**Zod:**

```typescript
z.array(z.string());
z.array(z.string()).min(1).max(10);
z.string().array(); // Alternative syntax
```

**Valibot:**

```typescript
v.array(v.string());
v.pipe(v.array(v.string()), v.minLength(1), v.maxLength(10));
```

**ArkType:**

```typescript
type("string[]");
type({ "items[]": "string" }); // Named tuple
```

### Union Types

**Zod:**

```typescript
z.union([z.string(), z.number()]);
z.string().or(z.number()); // Alternative
```

**Valibot:**

```typescript
v.union([v.string(), v.number()]);
```

**ArkType:**

```typescript
type("string | number");
```

### Discriminated Unions

**Zod:**

```typescript
z.discriminatedUnion("type", [
  z.object({ type: z.literal("a"), a: z.string() }),
  z.object({ type: z.literal("b"), b: z.number() }),
]);
```

**Valibot:**

```typescript
v.variant("type", [
  v.object({ type: v.literal("a"), a: v.string() }),
  v.object({ type: v.literal("b"), b: v.number() }),
]);
```

**ArkType:**

```typescript
type({
  type: "'a'",
  a: "string",
}).or({
  type: "'b'",
  b: "number",
});
```

## Performance

Based on community benchmarks (results vary by use case):

| Operation     | Zod     | Valibot | ArkType |
| ------------- | ------- | ------- | ------- |
| Parse (small) | ~500K/s | ~1.5M/s | ~800K/s |
| Parse (large) | ~50K/s  | ~150K/s | ~100K/s |
| Bundle (gzip) | ~12KB   | ~2KB    | ~8KB    |

**Note:** Performance matters less for message validation (typically not the bottleneck) than for API request validation in hot paths.

## When to Choose Each

### Choose Zod When:

- You want the largest ecosystem and community
- You need extensive documentation and examples
- Bundle size is not critical
- You prefer chainable APIs

```typescript
// Zod excels at complex transformations
const userSchema = z
  .object({
    email: z.string().email(),
    birthday: z.coerce.date(),
  })
  .transform((data) => ({
    ...data,
    age: calculateAge(data.birthday),
  }));
```

### Choose Valibot When:

- Bundle size is critical (frontend, edge functions)
- You need maximum validation performance
- You prefer functional composition
- You're comfortable with a smaller ecosystem

```typescript
// Valibot's tree-shakeable design
import { object, string, pipe, email } from "valibot";

const schema = object({
  email: pipe(string(), email()),
});
```

### Choose ArkType When:

- You want TypeScript-like syntax for schemas
- You need complex type expressions
- You prefer concise definitions
- You're comfortable with a newer library

```typescript
// ArkType's expressive syntax
const schema = type({
  age: "integer >= 0 <= 120",
  email: "string.email",
  tags: "(string & /^[a-z]+$/)[]",
});
```

## Migrating Between Libraries

### Zod to Valibot

```typescript
// Zod
const zodSchema = z.object({
  name: z.string().min(1),
  email: z.string().email(),
});

// Valibot equivalent
const valibotSchema = v.object({
  name: v.pipe(v.string(), v.minLength(1)),
  email: v.pipe(v.string(), v.email()),
});
```

### Zod to ArkType

```typescript
// Zod
const zodSchema = z.object({
  id: z.string().uuid(),
  count: z.number().int().positive(),
});

// ArkType equivalent
const arktypeSchema = type({
  id: "string.uuid",
  count: "integer > 0",
});
```

## amqp-contract Integration

All libraries work identically once the schema is defined:

```typescript
import { defineMessage, definePublisher, defineConsumer } from "@amqp-contract/contract";

// Works with any Standard Schema v1 compatible library
const message = defineMessage(schema);
const publisher = definePublisher(exchange, message, { routingKey: "event" });
const consumer = defineConsumer(queue, message);
```

The type inference works automatically:

```typescript
// TypeScript infers the correct payload type regardless of library
const handler: WorkerInferConsumerHandler<typeof contract, "processOrder"> = ({ payload }) => {
  // payload is fully typed based on your schema
  console.log(payload.orderId); // string
  console.log(payload.amount); // number
  return Ok(undefined).toAsync();
};
```

## Recommendations

1. **For most projects:** Start with Zod - best documentation, largest community
2. **For frontend-heavy projects:** Consider Valibot for smaller bundles
3. **For TypeScript purists:** Try ArkType's type-syntax approach

Remember: The schema library choice doesn't lock you in. Since amqp-contract uses Standard Schema v1, you can migrate between libraries with minimal code changes.
