---
title: Command pattern - amqp-contract
description: Illustrative walkthrough of a task queue — many publishers, one consumer — built with defineCommandConsumer and defineCommandPublisher.
---

# Command Pattern

Where the [event pattern](/how-to/define-a-contract#broadcast-an-event-to-many-consumers) is one publisher and many consumers (broadcast), the **command pattern** is the inverse: many publishers, one consumer (a task queue). Use it when work has a single owner and you want producers anywhere in the system to enqueue jobs for it.

This example shows a `payment-service` that owns a `payments` queue and exposes a single command, `chargeCustomer`. Multiple services (orders, subscriptions, refunds) publish to it.

## The contract

```ts
// payment-contract.ts — owned by the payment-service team, imported by callers
import {
  defineCommandConsumer,
  defineCommandPublisher,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { z } from "zod";

const paymentsExchange = defineExchange("payments", { type: "direct" });
const paymentsDlx = defineExchange("payments-dlx", { type: "direct" });

const paymentsQueue = defineQueue("payments", {
  deadLetter: { exchange: paymentsDlx, routingKey: "payments.dead" },
  retry: { mode: "ttl-backoff", maxRetries: 5, initialDelayMs: 1000 },
});

// A dead-letter exchange with nothing bound to it discards every rejected
// message, so `defineContract` rejects it. `payments-dlx` is DIRECT, so the
// binding must name `payments.dead` exactly — `#` is a topic wildcard and on a
// direct exchange it is a literal key that matches nothing.
const paymentsDlq = defineQueue("payments-dlq");

const chargeCommandMessage = defineMessage(
  z.object({
    customerId: z.string(),
    amountCents: z.number().int().positive(),
    currency: z.enum(["USD", "EUR", "GBP"]),
    idempotencyKey: z.string().min(1),
  }),
  { summary: "Charge a customer's saved payment method" },
);

// The consumer side: declares the queue, exchange, and routing key the
// command listens on.
const chargeCustomerCommand = defineCommandConsumer(
  paymentsQueue,
  paymentsExchange,
  chargeCommandMessage,
  { routingKey: "payments.charge" },
);

// The publisher side: derived from the consumer. Every caller imports this.
const chargeCustomerPublisher = defineCommandPublisher(chargeCustomerCommand);

export const contract = defineContract({
  publishers: {
    chargeCustomer: chargeCustomerPublisher,
  },
  consumers: {
    chargeCustomer: chargeCustomerCommand,
  },
  queues: { paymentsDlq },
  bindings: {
    paymentsDlqBinding: defineQueueBinding(paymentsDlq, paymentsDlx, {
      routingKey: "payments.dead",
    }),
  },
});
```

## The publisher (caller side)

Any service that needs to charge a customer publishes via the contract. The shape of the payload is enforced at compile time.

```ts
// orders-service/src/charge.ts
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "@org/payment-contract";
import { randomUUID } from "node:crypto";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();

await client
  .publish("chargeCustomer", {
    customerId: "cust_123",
    amountCents: 4_999,
    currency: "USD",
    idempotencyKey: randomUUID(),
  })
  .getOrThrow();
```

A `subscriptions-service`, `refunds-service`, or any other publisher does the same — they all use `chargeCustomer`. Routing-key dispatch is handled by the contract; callers never name `payments.charge` themselves.

## The worker (consumer side)

Only one place implements the command. The worker runs as part of the `payment-service`.

```ts
// payment-service/src/worker.ts
import {
  TypedAmqpWorker,
  declareHandler,
  RetryableError,
  NonRetryableError,
} from "@amqp-contract/worker";
import { fromPromise } from "unthrown";
import { contract } from "@org/payment-contract";

const chargeHandler = declareHandler(contract, "chargeCustomer", ({ input: { payload } }) =>
  fromPromise(
    chargeProvider({
      customerId: payload.customerId,
      amount: payload.amountCents,
      currency: payload.currency,
      idempotencyKey: payload.idempotencyKey,
    }),
    (error) => {
      // Card declined / fraud / closed account — won't change with retry.
      if (error instanceof PermanentDeclineError) {
        return new NonRetryableError(`Charge declined: ${error.code}`, error);
      }
      // 5xx, network, timeout — let the queue retry with backoff.
      return new RetryableError("Payment provider unavailable", error);
    },
  ).map(() => undefined),
);

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    chargeCustomer: [chargeHandler, { prefetch: 5 }],
  },
  urls: ["amqp://localhost"],
}).get();

process.on("SIGINT", async () => {
  await worker.close().get();
  process.exit(0);
});
```

Notes worth calling out:

- **`prefetch: 5`** caps the worker at five in-flight charges at a time. Tune to your provider's rate limits.
- **`idempotencyKey`** in the payload makes retries safe: the second attempt sees the same key and the provider returns the original result instead of double-charging.
- **DLQ** receives anything that fails permanently or exhausts retries. A separate process can replay or alert on `payments-dlx`.

## Why this works well

- **Single source of truth**: only the payment-service team controls the queue, retry policy, and handler implementation. Consumers see only the publisher type.
- **Idempotent retries**: combined with `ttl-backoff`, transient failures recover automatically without operator involvement.
- **Compile-time safety**: a caller passing the wrong field name gets a TypeScript error before deploy. A handler returning the wrong shape — same.

## Compare with the event pattern

| Aspect          | Event pattern                                  | Command pattern                                    |
| --------------- | ---------------------------------------------- | -------------------------------------------------- |
| Direction       | One publisher → many consumers (broadcast)     | Many publishers → one consumer (task queue)        |
| Who owns queue  | Each consumer owns its queue                   | The single consumer owns the queue                 |
| Builder         | `defineEventPublisher` / `defineEventConsumer` | `defineCommandConsumer` / `defineCommandPublisher` |
| Typical use     | Domain events, audit, notifications            | Background jobs, RPC, fanout-to-one                |
| Adding consumer | Add a new queue + binding                      | Not applicable — there is only one                 |

## See also

- [Define a contract](/how-to/define-a-contract) — full reference for both patterns.
- [Retry failed messages](/how-to/retry-failed-messages) — picking a retry mode.
- [Bridge domains](/how-to/bridge-domains) — using the command pattern across domain boundaries.
