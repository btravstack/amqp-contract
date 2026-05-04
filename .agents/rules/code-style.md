# Code Style

The cross-cutting language and tooling rules ("no `any`", "ResultAsync handlers", catalog dependencies, etc.) live in [`AGENTS.md` → Key Constraints](../../AGENTS.md). This file covers the patterns that aren't enforced by the linter or commit hooks.

## Composition Pattern

Define resources first, then reference them. Never define resources inline:

```typescript
// Bad — defining resources inline
const contract = defineContract({
  publishers: {
    orderCreated: definePublisher(
      defineExchange("orders"),
      defineMessage(z.object({ orderId: z.string() })),
      { routingKey: "order.created" },
    ),
  },
});

// Good — define resources first, then reference
const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing");
const orderMessage = defineMessage(z.object({ orderId: z.string() }));

const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const contract = defineContract({
  publishers: { orderCreated: orderCreatedEvent },
  consumers: { processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue) },
});
```

## Anti-Patterns

```typescript
// Bad — using async handlers
processOrder: async ({ payload }) => {
  await process(payload);
};

// Good — use the ResultAsync pattern from neverthrow.
// fromPromise REQUIRES the error mapper as the second argument; chaining
// .mapErr afterwards is a type error since fromPromise has no `unknown` overload.
processOrder: ({ payload }) =>
  ResultAsync.fromPromise(
    process(payload),
    (e) => new RetryableError("Failed", e),
  ).map(() => undefined);

// Bad — accessing message directly
processOrder: (message) => {
  console.log(message.orderId);
};

// Good — destructure payload
processOrder: ({ payload }) => {
  console.log(payload.orderId);
};

// Bad — using classic queues without retry config
defineQueue("orders", { type: "classic" });

// Good — use quorum queues with retry config
defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "immediate-requeue", maxRetries: 3 },
});

// Bad — accessing .name directly on a TTL-backoff queue
const queue = defineQueue("orders", {
  deadLetter: { exchange: dlx },
  retry: { mode: "ttl-backoff" },
});
console.log(queue.name); // Error: queue may be a wrapper object

// Good — use extractQueue() to access queue properties
import { extractQueue } from "@amqp-contract/contract";
console.log(extractQueue(queue).name);

// Bad — hardcoded version in package.json
"devDependencies": {
  "vitest": "^4.0.0"
}

// Good — using catalog
"devDependencies": {
  "vitest": "catalog:"
}

// Bad — missing .js extension
import { helper } from "./utils";

// Good
import { helper } from "./utils.js";

// Bad — using any
function process(data: any): any {}

// Good
function process(data: unknown): string {
  if (typeof data === "string") {
    return data.toUpperCase();
  }
  throw new Error("Invalid data");
}

// Bad — using interface
export interface PublishOptions extends Options.Publish {
  compression?: string;
}

// Good — using type alias
export type PublishOptions = Options.Publish & {
  compression?: CompressionAlgorithm;
};

// Bad — using || for optional objects
function process(options) {
  const { field, ...rest } = options || {};
}

// Good — using ?? for optional objects
function process(options) {
  const { field, ...rest } = options ?? {};
}
```

## Additional Guidance

- Avoid `@ts-ignore` and `@ts-expect-error`. Fix the root cause when you can; if you genuinely can't, leave a comment explaining why and link the upstream issue.
- Public APIs need JSDoc.
- Comments explain _why_, not _what_ — well-named identifiers already say what.
- Use Standard Schema v1 for validation; don't roll your own.
- Pick the narrowest exchange type that fits — don't reach for `topic` when `direct` would do.
- `quorum` queues are the default. Reach for `classic` only when you need a feature quorum doesn't support (`exclusive`, `autoDelete`, `maxPriority`).
- Prefer `readonly` arrays and properties where it doesn't hurt ergonomics.
- Prefer `const` over `let`.
