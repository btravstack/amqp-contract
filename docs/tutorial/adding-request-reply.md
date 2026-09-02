---
title: Adding request/reply - amqp-contract
description: Extend the tutorial project with an RPC that returns a typed value to its caller, including a declared error case.
---

# Adding request/reply

[Getting started](/tutorial/getting-started) built a one-way flow: the publisher sent an email notification and never heard back. Plenty of messaging works that way. But sometimes the caller needs an answer — "is this address deliverable?" — and needs it before it can continue.

In this lesson you will add an RPC to the same project. You will define a request and a response, handle it in the worker, call it from the client, and then give it a declared failure that the caller can branch on without parsing strings.

Start from the finished code of the previous lesson. About fifteen minutes.

## Step 1: Declare the RPC in the contract

An RPC is different from a publisher/consumer pair in one important way: it owns a queue, and it declares two schemas instead of one.

Open `contract.ts` and add to the imports:

```typescript
import { defineRpc } from "@amqp-contract/contract";
```

`defineQueueBinding` is already imported from step 1 — the RPC's dead-letter queue needs it too.

Then, above `defineContract`, define the RPC:

```typescript
// The queue the RPC server listens on. Callers never name it — they name
// the RPC, and the library routes to it.
const addressCheckDlx = defineExchange("address-check-dlx");
const addressCheckQueue = defineQueue("address-check", {
  deadLetter: { exchange: addressCheckDlx },
});
// Same rule as the email queue in step 1: a dead-letter exchange with nothing
// bound to it discards what it receives, so the DLQ is declared here and bound
// in the contract below.
const addressCheckDlq = defineQueue("address-check-dlq");

const checkAddress = defineRpc(addressCheckQueue, {
  request: defineMessage(z.object({ address: z.string() })),
  response: defineMessage(
    z.object({
      deliverable: z.boolean(),
      reason: z.string(),
    }),
  ),
});
```

And register it in the contract, alongside what is already there. `contract.ts` now reads in full:

```typescript
// contract.ts
import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { z } from "zod";

// From the previous lesson.
const notificationsExchange = defineExchange("notifications", { type: "direct" });
const notificationsDlx = defineExchange("notifications-dlx");
const emailQueue = defineQueue("email-notifications", {
  deadLetter: { exchange: notificationsDlx },
});
const emailDlq = defineQueue("email-notifications-dlq");
const emailMessage = defineMessage(
  z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  {
    summary: "Email notification",
    description: "Sent when an email needs to be delivered",
  },
);
const sendEmailEvent = defineEventPublisher(notificationsExchange, emailMessage, {
  routingKey: "email",
});

// New in this lesson.
const addressCheckDlx = defineExchange("address-check-dlx");
const addressCheckQueue = defineQueue("address-check", {
  deadLetter: { exchange: addressCheckDlx },
});
const addressCheckDlq = defineQueue("address-check-dlq");

const checkAddress = defineRpc(addressCheckQueue, {
  request: defineMessage(z.object({ address: z.string() })),
  response: defineMessage(
    z.object({
      deliverable: z.boolean(),
      reason: z.string(),
    }),
  ),
});

export const contract = defineContract({
  publishers: {
    sendEmail: sendEmailEvent,
  },
  consumers: {
    processEmail: defineEventConsumer(sendEmailEvent, emailQueue),
  },
  rpcs: {
    checkAddress,
  },
  queues: {
    emailDlq,
    addressCheckDlq,
  },
  bindings: {
    emailDlq: defineQueueBinding(emailDlq, notificationsDlx, { routingKey: "#" }),
    addressCheckDlq: defineQueueBinding(addressCheckDlq, addressCheckDlx, { routingKey: "#" }),
  },
});
```

`rpcs` is a third section beside `publishers` and `consumers`. An RPC is both — a caller sends to it, a server consumes from it and replies — so it gets its own place rather than being split across the two.

## Step 2: Answer the call in the worker

The worker gains one handler. Add it to the `handlers` object in `consumer.ts`:

```typescript
    checkAddress: (_, { payload }) => {
      const deliverable = payload.address.endsWith("@example.com");
      return OkAsync({
        deliverable,
        reason: deliverable ? "known domain" : "unknown domain",
      });
    },
```

The shape is the same as the `processEmail` handler, with one difference: where the consumer returned `OkAsync(undefined)` — "done, acknowledge it" — this one returns a value. That value is the reply.

It is typed. Return `{ deliverable: true }` without `reason` and it will not compile; the response schema in the contract says both fields are required.

## Step 3: Call it from the client

Replace the body of `publisher.ts` with:

```typescript
// publisher.ts
import { TypedAmqpClient } from "@amqp-contract/client";
import { P } from "unthrown";
import { contract } from "./contract.js";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).get();

const result = await client.call(
  "checkAddress",
  { address: "user@example.com" },
  { timeoutMs: 5_000 },
);

result.match({
  ok: (reply) => console.log(`deliverable=${reply.deliverable} (${reply.reason})`),
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/RpcTimeoutError"), () =>
        console.error("No reply within 5s — is the worker running?"),
      )
      .with(P.tag("@amqp-contract/RpcCancelledError"), () =>
        console.error("The client closed while the call was in flight."),
      )
      .with(P.tag("@amqp-contract/MessageValidationError"), (error) =>
        console.error("The reply arrived but failed the response schema.", error.issues),
      ),
  defect: (cause) => {
    throw cause;
  },
});

await client.close().get();
```

`client.call(...)` is the RPC counterpart to `client.publish(...)`. It takes the RPC name, the request, and options — `timeoutMs` matters here, because unlike a publish, a call waits.

`reply` is typed as the response schema: `reply.deliverable` is a `boolean`, and `reply.reasons` would not compile.

Three failures are possible on any call and the compiler lists them: the reply never arrived (`RpcTimeoutError`), the client shut down while waiting (`RpcCancelledError`), or the reply arrived but failed the response schema (`MessageValidationError`). None is an exception you have to remember to catch.

## Step 4: Run it

The contract changed, so restart the worker:

```bash
npx tsx consumer.ts
```

Then:

```bash
npx tsx publisher.ts
```

```
deliverable=true (known domain)
```

Change the address to `user@elsewhere.org` and run the publisher again:

```
deliverable=false (unknown domain)
```

Now stop the worker (Ctrl+C) and run the publisher once more. After five seconds:

```
No reply within 5s — is the worker running?
```

That is `RpcTimeoutError` — a value returned from `call`, not a thrown exception.

## Step 5: Declare a real failure

"Not deliverable" is an answer, so returning it as a successful response was reasonable. But some failures are not answers. Suppose the address is syntactically malformed — there is nothing to check, and the caller should treat it differently from a clean "no".

Squeezing that into the response schema (adding a `malformed: boolean` nobody remembers to read) is how error handling rots. Declare it instead.

In `contract.ts`, add an `errors` map to the RPC:

```typescript
const checkAddress = defineRpc(addressCheckQueue, {
  request: defineMessage(z.object({ address: z.string() })),
  response: defineMessage(
    z.object({
      deliverable: z.boolean(),
      reason: z.string(),
    }),
  ),
  errors: {
    MALFORMED_ADDRESS: { data: z.object({ address: z.string() }) },
  },
});
```

Each entry is a code mapped to a schema for its payload — the structured data the caller gets alongside the code.

In `consumer.ts`, import `rpcError` and `ErrAsync`, then return one:

```typescript
import { rpcError } from "@amqp-contract/worker";
import { ErrAsync, OkAsync } from "unthrown";
```

```typescript
    checkAddress: (_, { payload }) => {
      if (!payload.address.includes("@")) {
        return ErrAsync(
          rpcError("MALFORMED_ADDRESS", { address: payload.address }, "missing @"),
        );
      }

      const deliverable = payload.address.endsWith("@example.com");
      return OkAsync({
        deliverable,
        reason: deliverable ? "known domain" : "unknown domain",
      });
    },
```

Only codes declared in the contract are accepted. Invent one the contract does not list and the handler will not compile.

Finally, handle it in `publisher.ts` by adding an arm to the matcher:

```typescript
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/RpcError"), (error) =>
        console.error(`${error.code}: ${error.message}`, error.data),
      )
      .with(P.tag("@amqp-contract/RpcTimeoutError"), () =>
        console.error("No reply within 5s — is the worker running?"),
      )
      .with(P.tag("@amqp-contract/RpcCancelledError"), () =>
        console.error("The client closed while the call was in flight."),
      )
      .with(P.tag("@amqp-contract/MessageValidationError"), (error) =>
        console.error("The reply arrived but failed the response schema.", error.issues),
      ),
```

Restart the worker, set the address to `"not-an-address"`, and run the publisher:

```
MALFORMED_ADDRESS: missing @ { address: 'not-an-address' }
```

The code, the message, and the structured data crossed the broker and arrived typed. `error.data` is `{ address: string }` because that is what the contract declared for `MALFORMED_ADDRESS`.

## What you learned

- An **RPC** declares a request schema, a response schema, and optionally a map of **declared errors**, and owns the queue it listens on.
- The handler returns the reply as a value. `OkAsync(reply)` for success, `ErrAsync(rpcError(code, data, message))` for a declared failure.
- The caller gets a result whose error channel lists everything that can go wrong: declared errors plus `RpcTimeoutError`, `RpcCancelledError`, and `MessageValidationError`. The compiler enumerates them so you cannot forget one.
- Declared errors beat encoding failure into the response schema, because the caller branches on a code instead of on a convention.

## Where next

You have now used every major piece of the library. From here the [how-to guides](/how-to/use-request-reply) answer specific questions:

- [Use request/reply](/how-to/use-request-reply) — timeouts, concurrency, and what happens to a reply nobody is waiting for.
- [Retry failed messages](/how-to/retry-failed-messages) — what to do when a handler fails and the failure might be temporary.
- [Test with RabbitMQ](/how-to/test-with-rabbitmq) — run this against a real broker in your test suite.

For the reasoning behind the three-channel result you have been matching on, read [errors as values](/explanation/errors-as-values).
