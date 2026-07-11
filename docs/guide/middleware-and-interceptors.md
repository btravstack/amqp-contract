# Middleware & Interceptors

Cross-cutting concerns — trace propagation, auth, idempotency, logging — shouldn't be copy-pasted into every handler or wrapped around every `publish` call. amqp-contract provides two composable primitives:

- **Worker middleware** wraps every handler invocation (consumers and RPCs) after message validation, and can inject **typed context** that handlers receive as their third argument.
- **Client interceptors** wrap `publish(...)` and `call(...)`, and can patch the outgoing message/options, observe outcomes, retry, or short-circuit.

Both follow the same shape: a function receiving `(args, next)` that calls `next()` to continue — or doesn't, to short-circuit. Everything stays inside unthrown's `AsyncResult`; nothing throws.

## Worker middleware

### Injecting typed context (guard-and-narrow)

A middleware validates something once and passes the proven result downstream. Handlers declare the context as their third parameter:

```ts
import {
  composeMiddleware,
  defineMiddleware,
  nonRetryable,
  TypedAmqpWorker,
  type EmptyContext,
} from "@amqp-contract/worker";
import { Err, Ok } from "unthrown";

const auth = defineMiddleware<EmptyContext, { tenantId: string }>((args, next) => {
  const tenantId = args.rawMessage.properties.headers?.["x-tenant-id"];
  if (typeof tenantId !== "string") {
    // Short-circuit: routed to DLQ like any handler error — the handler never runs.
    return Err(nonRetryable("Missing x-tenant-id header")).toAsync();
  }
  return next({ context: { tenantId } });
});

const timing = defineMiddleware<{ tenantId: string }, { tenantId: string }>((args, next) => {
  const start = Date.now();
  return next().tap(() => {
    console.log(`${args.handlerName} (${args.context.tenantId}): ${Date.now() - start}ms`);
  });
});

const worker = (
  await TypedAmqpWorker.create({
    contract,
    middleware: composeMiddleware(auth, timing),
    handlers: {
      // context is typed as { tenantId: string } — proven by the middleware
      processOrder: ({ payload }, _raw, context) =>
        processFor(context.tenantId, payload).mapErr((e) => nonRetryable("failed", e)),
    },
    urls: ["amqp://localhost"],
  })
).unwrap();
```

`composeMiddleware(outermost, ..., innermost)` runs left-to-right; context types accumulate across the chain, and the final context type is what handlers receive. Without a `middleware` option, handlers get an empty object (`EmptyContext`).

### Semantics

- Middleware runs **after** payload/header validation — `args.message` is already schema-checked. Parse/validation failures go to the DLQ before any middleware runs.
- The chain wraps consumers **and** RPC handlers; `args.isRpc` discriminates, and `args.handlerName` carries the contract key.
- Short-circuit results route exactly like handler results:
  - `Err(retryable(...))` → queue retry mode; `Err(nonRetryable(...))` → DLQ.
  - `Err(rpcError(code, data))` on an RPC with a declared `errors` map → typed error reply to the caller (see [Error Model](./error-model.md#typed-rpc-errors-rpcerror)).
  - `Ok(value)` skips the handler; for an RPC, `value` is validated against the response schema and published as the reply (cache pattern).
- Calling `next()` returns the handler's `AsyncResult` — middleware can `.tap` / `.mapErr` / `.orElse` it for post-processing.

## Client interceptors

### Publish interceptors

Run outside validation and publishing — a patched message goes through schema validation exactly like the original. The canonical use is stamping headers:

```ts
import { TypedAmqpClient, type PublishInterceptor } from "@amqp-contract/client";

const stampTrace: PublishInterceptor = (args, next) =>
  next({
    options: {
      ...args.options,
      headers: { ...args.options.headers, traceparent: currentTraceparent() },
    },
  });

const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
    publishInterceptors: [stampTrace],
  })
).unwrap();
```

### Call interceptors

Wrap the full RPC round trip (request validation, publish, reply await). They can adjust `timeoutMs`, patch the request, observe typed errors, or retry by calling `next` again:

```ts
import { RpcTimeoutError, type CallInterceptor } from "@amqp-contract/client";
import { Err } from "unthrown";

const retryTimeoutsOnce: CallInterceptor = (args, next) =>
  next().orElse((error) => (error instanceof RpcTimeoutError ? next() : Err(error).toAsync()));
```

The first interceptor in either array is the **outermost**. Telemetry spans stay outside the chain, so interceptor work is covered by the existing OpenTelemetry instrumentation.

## Trace propagation end to end

The two sides compose into W3C trace-context propagation without touching a single handler: a publish interceptor stamps `traceparent` from the active span, and a worker middleware reads `args.rawMessage.properties.headers.traceparent`, resumes the remote context, and injects the span into the handler context. See [OpenTelemetry](./opentelemetry-observability.md) for the instrumentation this hooks into.

## See also

- [Error Model](./error-model.md) — how short-circuit errors route.
- [Worker Usage](./worker-usage.md) — handler signatures.
- [Client Usage](./client-usage.md) — publish/call return types.
