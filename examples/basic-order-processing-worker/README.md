# Basic Order Processing - Worker

Consumer application demonstrating type-safe AMQP message consumption with multiple handlers.

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/examples/basic-order-processing)**

## Quick Start

```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:4-management

# Run the worker
pnpm --filter @amqp-contract-examples/basic-order-processing-worker dev
```

## Code Organization

This sample demonstrates two approaches to organizing worker handlers:

### Inline Handlers (src/index.ts - Main Example)

Handlers are defined directly in the worker creation. This approach is suitable for:

- Simple applications with few handlers
- Quick prototypes
- When handlers don't need to be reused

```typescript
const worker = (
  await TypedAmqpWorker.create({
    contract: orderContract,
    handlers: {
      processOrder: ({ payload }) => {
        // Handler logic here
        return Ok(undefined).toAsync();
      },
      notifyOrder: ({ payload }) => {
        // Handler logic here
        return Ok(undefined).toAsync();
      },
    },
    urls: [env.AMQP_URL],
  })
).getOrThrow();
```

### External Handlers (src/handlers.ts)

Handlers can be organized in separate files using `defineHandler` or `defineHandlers`. The `src/handlers.ts` file demonstrates this pattern, which is recommended for:

- Production applications
- Better code organization and testability
- Reusable handlers across multiple workers
- Clearer separation of concerns

```typescript
// handlers.ts
export const processOrderHandler = defineHandler(orderContract, "processOrder", ({ payload }) => {
  // Handler logic here
  return Ok(undefined).toAsync();
});

// index.ts - to use external handlers, import them:
import { processOrderHandler /* other handlers */ } from "./handlers.js";

const worker = (
  await TypedAmqpWorker.create({
    contract: orderContract,
    handlers: {
      processOrder: processOrderHandler,
      // ... other handlers
    },
    urls: [env.AMQP_URL],
  })
).getOrThrow();
```

The main `src/index.ts` file uses inline handlers for simplicity, while `src/handlers.ts` provides an example of how to organize handlers externally for better maintainability.

## Environment Variables

| Variable    | Default                 | Description                   |
| ----------- | ----------------------- | ----------------------------- |
| `AMQP_URL`  | `amqp://localhost:5672` | RabbitMQ connection URL       |
| `LOG_LEVEL` | `info`                  | Log level (info, debug, etc.) |

For detailed documentation, visit the **[website](https://btravstack.github.io/amqp-contract/examples/basic-order-processing)**.
