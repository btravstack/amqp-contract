---
title: Use request/reply - amqp-contract
description: Define an RPC, handle it, call it with a timeout, declare typed error codes, and understand what happens to late or malformed replies.
---

# Use request/reply

An RPC is a message that returns a value. For a guided introduction see [adding request/reply](/tutorial/adding-request-reply); this page is the recipes.

## Define an RPC

```typescript
import { defineContract, defineMessage, defineQueue, defineRpc } from "@amqp-contract/contract";
import { z } from "zod";

const rpcDlx = defineExchange("rpc-dlx");

const calculate = defineRpc(defineQueue("rpc.calculate", { deadLetter: { exchange: rpcDlx } }), {
  request: defineMessage(z.object({ a: z.number(), b: z.number() })),
  response: defineMessage(z.object({ sum: z.number() })),
});

export const contract = defineContract({ rpcs: { calculate } });
```

An RPC owns its queue and goes in `rpcs`, not in `publishers` or `consumers` — it is both.

## Handle it

```typescript
handlers: {
  calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
},
```

The returned value is the reply, typed by the response schema. Async work looks the same as in a consumer:

```typescript
calculate: ({ payload }) =>
  fromPromise(lookupRate(payload), qualifyRetryable("rate service down")).map((rate) => ({
    sum: payload.a * rate,
  })),
```

## Call it

```typescript
import { P } from "unthrown";

const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 });

result.match({
  ok: (reply) => console.log(reply.sum),
  errCases: (matcher) =>
    matcher
      .with(P.tag("@amqp-contract/RpcTimeoutError"), () => console.error("no reply in time"))
      .with(P.tag("@amqp-contract/RpcCancelledError"), () => console.error("client closing"))
      .with(P.tag("@amqp-contract/MessageValidationError"), (e) =>
        console.error("reply failed its schema", e.issues),
      ),
  defect: (cause) => {
    throw cause;
  },
});
```

Always set `timeoutMs`. A call with no reply is otherwise bounded only by the server-side default, and a caller holding a request open is holding memory.

## Declare typed errors

Business failures belong in the contract, not squeezed into the response schema:

```typescript
const getOrder = defineRpc(defineQueue("rpc.get-order", { deadLetter: { exchange: rpcDlx } }), {
  request: defineMessage(z.object({ orderId: z.string() })),
  response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
  errors: {
    ORDER_NOT_FOUND: { data: z.object({ orderId: z.string() }) },
  },
});
```

Return one from the handler:

```typescript
import { rpcError } from "@amqp-contract/worker";

getOrder: ({ payload }) => {
  const order = orders.get(payload.orderId);
  return order
    ? OkAsync({ orderId: order.id, status: order.status })
    : ErrAsync(rpcError("ORDER_NOT_FOUND", { orderId: payload.orderId }));
},
```

Or, with autocomplete over the declared codes, via `helpers.errors`:

```typescript
getOrder: ({ payload }, _raw, { errors }) =>
  ErrAsync(errors.ORDER_NOT_FOUND({ orderId: payload.orderId })),
```

Handle it on the caller:

```typescript
import { isRpcError } from "@amqp-contract/client";

if (result.isErr() && isRpcError(result.error)) {
  // result.error.code is "ORDER_NOT_FOUND"
  // result.error.data is { orderId: string }
}
```

A declared error is a _business outcome_, not a processing failure: the worker validates the data, publishes the error reply, and **acknowledges the request**. Declared errors are never retried. Only `RetryableError` and `NonRetryableError` enter the retry pipeline.

## Set a per-call timeout

```typescript
await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 30_000 });
```

Size it to the work, not to a house default. When it expires the pending call is cleared and you get `RpcTimeoutError` — but note the request may still be processed by the server. A timeout tells you no reply arrived, not that nothing happened.

## Retry a timed-out call

Use a call interceptor so the policy lives in one place rather than at every call site — see [add middleware](/how-to/add-middleware#retry-a-timed-out-rpc-call).

Be careful: retrying a call whose handler is not idempotent runs the work twice.

## Understand what makes a call fail

| Failure                  | Cause                                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| `RpcTimeoutError`        | No reply within `timeoutMs`. Also what you get when the reply was dropped for being malformed. |
| `RpcCancelledError`      | The client closed while the call was in flight.                                                |
| `RpcError<code, data>`   | A declared business error from the handler.                                                    |
| `MessageValidationError` | The reply arrived but failed the response schema.                                              |
| Defect                   | Transport failure, or an error reply whose code the local contract does not declare.           |

That first row is worth dwelling on. If the handler returns a value that fails the response schema, the worker refuses to publish a malformed reply — so the caller sees a timeout rather than a wrong answer. A call timing out while the server looks healthy usually means a response-schema mismatch between the two sides' contracts.

## Know the runtime guarantees

The type system stops undeclared error codes, but a cast or a version skew can get past it. The runtime holds the line:

- **Worker** — an undeclared code, or error data failing its schema, is a contract violation. No reply is published and the request is dead-lettered as a `NonRetryableError`. The caller times out.
- **Client** — an error reply whose code is not in the local contract resolves to a **defect**. Error data failing its schema resolves to `Err(MessageValidationError)`.

An RPC also requires `replyTo` and `correlationId` on the request. A request missing either is dead-lettered rather than answered, since there is nowhere to reply to.

Error data is validated twice — on the worker before publishing, on the client on arrival — the same as responses.

## Where next

- [Adding request/reply](/tutorial/adding-request-reply) — the guided version.
- [Error model](/reference/error-model#typed-rpc-errors) — the RPC wire format and `RPC_ERROR_CODE_HEADER`.
- [Add middleware](/how-to/add-middleware) — caching an RPC by short-circuiting.
