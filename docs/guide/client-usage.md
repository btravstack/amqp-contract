# Client Usage

Learn how to use the type-safe AMQP client to publish messages.

## Creating a Client

Create a type-safe client from your contract. `TypedAmqpClient.create(...)` returns `AsyncResult<TypedAmqpClient, never>` — its modeled error channel is empty because infrastructure failures surface as [defects](./error-model.md#framework-defects), not modeled errors. `await` yields a `Result` you pattern-match with `.match()`, or extract with [`.get()`](./error-model.md#getting-the-value-out) (unthrown gates `.get()` to infallible results — a failed `create()` still panics, rethrowing the underlying `TechnicalError`):

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();
```

### Default Publish Options

Configure default publish options that apply to all messages published by the client:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  defaultPublishOptions: {
    priority: 5,
    headers: { "x-app-version": "1.0.0" },
  },
}).get();
```

Default publish options can be overridden by options passed to individual `publish` calls.

By default, messages are `persistent` for message durability, but this can be overridden by explicitly setting `persistent: false` in `defaultPublishOptions` when creating the client, or in the options passed to the `publish` method when publishing messages.

## Publishing Messages

Publish messages with full type safety and explicit error handling:

```typescript
import { tag } from "unthrown";

const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  amount: 99.99,
  items: [{ productId: "PROD-A", quantity: 2 }],
});

result.match({
  ok: () => console.log("✅ Published"),
  errCases: (matcher) =>
    matcher.with(tag("@amqp-contract/MessageValidationError"), (error) =>
      console.error("❌ Invalid payload:", error.message),
    ),
  defect: (cause) => {
    // transport failures (TechnicalError) surface here as defects
    throw cause;
  },
});
```

### Type Safety

The client enforces:

- ✅ **Valid publisher names** - Only publishers from contract
- ✅ **Message schema** - Messages must match schema
- ✅ **Autocomplete** - Full IDE support
- ✅ **Explicit errors** - Returned via `Result` type

```typescript
import { tag } from 'unthrown';

// ❌ TypeScript error: 'unknownPublisher' not in contract
const result = await client.publish('unknownPublisher', { ... });

// ❌ TypeScript error: missing required field
const result = await client.publish('orderCreated', {
  customerId: 'CUST-456',
});

// ❌ Runtime validation error returned in Result
const result = await client.publish('orderCreated', {
  orderId: 123, // should be string
  customerId: 'CUST-456',
  amount: 99.99,
});

result.match({
  ok: () => console.log('Published'),
  errCases: (matcher) =>
    matcher.with(tag('@amqp-contract/MessageValidationError'), (error) =>
      console.error('Validation failed:', error),
    ),
  defect: (cause) => {
    // transport failures (TechnicalError) surface here as defects
    throw cause;
  },
});
```

## Publishing Options

### Custom Routing Key

Override the routing key for specific messages:

```typescript
const result = await client.publish(
  "orderCreated",
  { orderId: "ORD-123", amount: 99.99 },
  { routingKey: "order.created.urgent" },
);
```

### Message Properties

Set AMQP message properties:

```typescript
const result = await client.publish(
  "orderCreated",
  { orderId: "ORD-123", amount: 99.99 },
  {
    options: {
      persistent: false,
      priority: 10,
      headers: { "x-request-id": "req-123" },
    },
  },
);
```

### Publishing with Headers

When your message schema defines a [headers schema](/guide/defining-contracts#message-headers), pass headers via the `options.headers` property:

```typescript
const result = await client.publish(
  "orderCreated",
  { orderId: "ORD-123", amount: 99.99 },
  {
    headers: {
      correlationId: "550e8400-e29b-41d4-a716-446655440000",
      priority: "high",
      tenantId: "tenant-42",
    },
  },
);
```

Headers are validated by the consumer at runtime using the headers schema defined in `defineMessage`. On the publish side, headers are passed as raw AMQP message properties — make sure to match the expected schema to avoid consumer-side validation errors.

## Connection Management

### Closing the Connection

```typescript
await client.close();
```

### Error Handling

Errors are returned via `Result` types, not thrown:

```typescript
import { tag } from "unthrown";

const result = await client.publish("orderCreated", {
  orderId: "ORD-123",
  amount: 99.99,
});

result.match({
  ok: () => console.log("✅ Published"),
  errCases: (matcher) =>
    matcher.with(tag("@amqp-contract/MessageValidationError"), (err) =>
      console.error("Validation failed:", err.issues),
    ),
  defect: (cause) => {
    // network / runtime failures (TechnicalError) surface here as defects
    console.error("Technical error:", cause);
    throw cause;
  },
});
```

**Error Types:**

- `MessageValidationError` - Schema validation failed (a modeled `Err` in `E`)
- `TechnicalError` - Network or runtime failures, surfaced as a **defect** (handled in the `defect` arm), not a modeled error

**Note:** Programming errors (like invalid publisher name) still throw exceptions, since TypeScript should catch those at compile-time.

## Complete Example

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { tag } from "unthrown";
import { contract } from "./contract";

async function main() {
  let client;

  try {
    client = await TypedAmqpClient.create({
      contract,
      urls: ["amqp://localhost"],
    }).get();

    const result = await client.publish("orderCreated", {
      orderId: "ORD-123",
      customerId: "CUST-456",
      amount: 99.99,
      items: [{ productId: "PROD-A", quantity: 2 }],
    });
    result.match({
      ok: () => console.log("✅ Message published"),
      errCases: (matcher) =>
        matcher.with(tag("@amqp-contract/MessageValidationError"), (err) =>
          console.error("❌ Validation failed:", err.issues),
        ),
      defect: (cause) => {
        // transport failures (TechnicalError) surface here as defects
        console.error("❌ Technical error:", cause);
        throw cause;
      },
    });
  } catch (error) {
    console.error("Unexpected error:", error);
  } finally {
    await client?.close();
  }
}

main();
```

## Next Steps

- Learn about [Worker Usage](/guide/worker-usage)
- Explore [Defining Contracts](/guide/defining-contracts)
- Check out [Examples](/examples/)
