# Testing

## Strategy

- **Integration tests preferred** — real RabbitMQ via testcontainers, not mocking
- **Test file naming**: `*.spec.ts` for unit tests, `src/__tests__/*.spec.ts` for integration
- **Test isolation** — each integration test uses a separate RabbitMQ vhost

## Test Framework

- Use `vitest` for all tests
- Place tests alongside source: `feature.spec.ts`
- Use integration tests in `__tests__` directories

## Integration Test Setup

Use `@amqp-contract/testing` for RabbitMQ integration tests. Configure `globalSetup` in vitest.config.ts for container lifecycle:

```typescript
// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["@amqp-contract/testing/global-setup"],
  },
});
```

## Test Fixtures

Import `it` from `@amqp-contract/testing/extension` for fixtures. Available fixtures:

- `vhost` — isolated RabbitMQ vhost (one per test)
- `amqpConnectionUrl` — connection URL pointing at that vhost
- `amqpConnection` / `amqpChannel` — opened connection + channel for direct broker calls
- `publishMessage(exchange, routingKey, content, options?)` — publish a JSON-serialised message
- `initConsumer(exchange, routingKey)` — returns a `(opts?) => Promise<ConsumeMessage[]>` waiter that you call to await N messages

```typescript
import { describe, expect } from "vitest";
import { it } from "@amqp-contract/testing/extension";

describe("Order Processing", () => {
  it("should consume order messages", async ({ initConsumer, publishMessage, vhost }) => {
    // vhost is automatically created and isolated for this test
    const waitForMessages = await initConsumer("orders-exchange", "order.created");

    publishMessage("orders-exchange", "order.created", { orderId: "123" });

    const messages = await waitForMessages({ nbEvents: 1, timeout: 5000 });
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ orderId: "123" });
  });
});
```

## Handler Testing

Handlers return `Ok(undefined).toAsync()` in mocks:

```typescript
// Test handler mock
const mockHandler = vi.fn().mockReturnValue(Ok(undefined).toAsync());

// Assertion pattern
expect(mockHandler).toHaveBeenCalledWith(
  expect.objectContaining({
    payload: expect.objectContaining({ orderId: "123" }),
  }),
  expect.anything(), // rawMessage
);
```

## Test Structure

```typescript
import { describe, expect, it } from "vitest";

describe("Feature Name", () => {
  describe("specific function/method", () => {
    it("should do something specific", () => {
      // GIVEN
      const input = {
        /* ... */
      };

      // WHEN
      const result = functionUnderTest(input);

      // THEN
      expect(result).toEqual(expectedValue);
    });
  });
});
```

## Test Naming

- Use descriptive test names: "should [expected behavior] when [condition]"
- Group related tests in `describe` blocks
- Keep tests focused on one thing

## Assertion Best Practices

- Merge multiple assertions into one whenever possible for clarity
- Use `expect.objectContaining()` or `toMatchObject()` for complex object validation
- When testing multiple calls to mocked functions, use `toHaveBeenNthCalledWith(n, ...)` to verify specific call order and arguments

```typescript
// Bad — multiple fragmented assertions
it("should create exchange definition", () => {
  const exchange = defineExchange("test");
  expect(exchange.name).toBe("test");
  expect(exchange.type).toBe("topic");
  expect(exchange.durable).toBe(true);
});

// Good — merged into comprehensive assertion
it("should create exchange definition", () => {
  const exchange = defineExchange("test");
  expect(exchange).toEqual({
    name: "test",
    type: "topic",
    durable: true,
  });
});

// Bad — using toHaveBeenCalledWith without specifying order
it("should call function multiple times", () => {
  mockFn("first");
  mockFn("second");
  expect(mockFn).toHaveBeenCalledWith("first");
  expect(mockFn).toHaveBeenCalledWith("second");
});

// Good — using toHaveBeenNthCalledWith for ordered calls
it("should call function multiple times", () => {
  mockFn("first");
  mockFn("second");
  expect(mockFn).toHaveBeenCalledTimes(2);
  expect(mockFn).toHaveBeenNthCalledWith(1, "first");
  expect(mockFn).toHaveBeenNthCalledWith(2, "second");
});
```

## Coverage

- Write tests for all exported functions
- Test happy path and error cases
- Test edge cases and boundary conditions
- Every new feature needs tests
- Every bug fix needs a regression test
