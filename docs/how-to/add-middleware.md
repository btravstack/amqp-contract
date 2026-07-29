---
title: Add middleware - amqp-contract
description: Wrap handlers with worker middleware and typed context, and wrap publish/call with client interceptors.
---

# Add middleware

Cross-cutting concerns — authentication, tracing, timing, dependency injection — belong in one place rather than at the top of every handler. There are two mechanisms:

- **Worker middleware** wraps handler invocation and can inject typed context.
- **Client interceptors** wrap `publish()` and `call()` and can patch the outgoing message.

Both take `(args, next)` and call `next()` to continue. Everything stays inside `AsyncResult`; nothing throws.

## Inject typed context into handlers

A middleware proves something once and passes the result downstream. Handlers receive it as `helpers.context`, their third argument.

```typescript
import {
  composeMiddleware,
  defineMiddleware,
  nonRetryable,
  TypedAmqpWorker,
  type EmptyContext,
} from "@amqp-contract/worker";
import { ErrAsync } from "unthrown";

const auth = defineMiddleware<EmptyContext, { tenantId: string }>((args, next) => {
  const tenantId = args.rawMessage.properties.headers?.["x-tenant-id"];
  if (typeof tenantId !== "string") {
    // Short-circuit: routes like any handler error. The handler never runs.
    return ErrAsync(nonRetryable("Missing x-tenant-id header"));
  }
  return next({ context: { tenantId } });
});

const worker = await TypedAmqpWorker.create({
  contract,
  middleware: auth,
  handlers: {
    // context is typed { tenantId: string } — proven by the middleware
    processOrder: ({ payload }, _raw, { context }) => processFor(context.tenantId, payload),
  },
  urls: ["amqp://localhost"],
}).get();
```

The two type parameters on `defineMiddleware` are the context going in and the context coming out. Without any middleware or `createContext`, handlers get `EmptyContext`.

## Chain several middleware

```typescript
const timing = defineMiddleware<{ tenantId: string }, { tenantId: string }>((args, next) => {
  const start = Date.now();
  return next().tap(() => {
    console.log(`${args.handlerName} (${args.context.tenantId}): ${Date.now() - start}ms`);
  });
});

middleware: composeMiddleware(auth, timing),
```

`composeMiddleware(outermost, …, innermost)` runs left to right. Context types accumulate along the chain, so `timing` sees the `tenantId` that `auth` added, and handlers see the final accumulated type.

Because `next()` returns the handler's `AsyncResult`, a middleware can post-process it with `.tap`, `.mapErr` or `.flatMapErr`.

## Inject dependencies per message

`createContext` seeds the chain. It runs once per message after validation, so it can build request-scoped values; close over singletons for anything per-worker.

```typescript
const worker = await TypedAmqpWorker.create({
  contract,
  createContext: (info) => ({
    log: baseLogger.child({
      handler: info.handlerName,
      correlationId: info.rawMessage.properties.correlationId,
    }),
    orderRepo,
  }),
  middleware: auth, // seeded with { log, orderRepo }
  handlers: {
    // context: { log, orderRepo } & { tenantId: string }
    processOrder: ({ payload }, _raw, { context }) => context.orderRepo.process(payload),
  },
  urls: ["amqp://localhost"],
}).get();
```

If `createContext` throws or rejects, the message is dead-lettered as a `NonRetryableError` and the handler never runs.

For a dependency-injection graph, [demesne](https://btravstack.github.io/demesne/)'s `Layer.forkScope` is the recommended implementation: build the graph once at startup, fork a scope per message.

## Substitute the payload

`next({ payload })` replaces the payload for everything downstream.

```typescript
const normalize = defineMiddleware<EmptyContext, EmptyContext>((args, next) =>
  next({ payload: { ...args.message.payload, email: args.message.payload.email.toLowerCase() } }),
);
```

The substituted payload is **re-validated against the consumer's schema** before the handler runs. An invalid substitution is a `NonRetryableError`, so middleware cannot smuggle unvalidated data past the contract boundary.

## Short-circuit without running the handler

Return a result instead of calling `next()`. It routes exactly like a handler result:

| Returned                    | Effect                                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------------------- |
| `Err(retryable(…))`         | Queue retry mode applies                                                                                |
| `Err(nonRetryable(…))`      | Dead-lettered                                                                                           |
| `Err(rpcError(code, data))` | Typed error reply to the caller (RPC with a declared `errors` map)                                      |
| `Ok(value)`                 | Handler skipped. On an RPC, `value` is validated against the response schema and published as the reply |

That last row is how you build a cache: check the cache in middleware, return `Ok(hit)` to reply without invoking the handler.

## Use typed error constructors in RPC handlers

RPC handlers with a declared `errors` map get typed constructors in `helpers.errors`, with per-code data inference:

```typescript
handlers: {
  getOrder: ({ payload }, _raw, { errors }) =>
    orders.has(payload.orderId)
      ? OkAsync(orders.get(payload.orderId))
      : ErrAsync(errors.ORDER_NOT_FOUND({ orderId: payload.orderId })),
},
```

Equivalent to `rpcError("ORDER_NOT_FOUND", { orderId })`, but autocompleted and checked against the contract.

## Know what middleware can and cannot see

- Middleware runs **after** payload and header validation, so `args.message` is already schema-checked. Parse and validation failures dead-letter the message before any middleware runs.
- The chain wraps consumers **and** RPC handlers. `args.isRpc` discriminates; `args.handlerName` carries the contract key.
- `args.rawMessage` is the underlying AMQP message, which is where raw headers live.

## Stamp headers on every publish

Client interceptors run outside validation, so a patched message is validated exactly like an unpatched one.

```typescript
import { TypedAmqpClient, type PublishInterceptor } from "@amqp-contract/client";

const stampTrace: PublishInterceptor = (args, next) =>
  next({
    options: {
      ...args.options,
      headers: { ...args.options.headers, traceparent: currentTraceparent() },
    },
  });

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  publishInterceptors: [stampTrace],
}).get();
```

The first interceptor in the array is the outermost.

## Retry a timed-out RPC call

Call interceptors wrap the whole round trip — request validation, publish, and awaiting the reply — so calling `next()` again reissues the call.

```typescript
import { RpcTimeoutError, type CallInterceptor } from "@amqp-contract/client";
import { ErrAsync, P } from "unthrown";

const retryTimeoutsOnce: CallInterceptor = (args, next) =>
  next().flatMapErrCases((matcher) =>
    matcher.with(
      P.tag("@amqp-contract/MessageValidationError"),
      P.tag("@amqp-contract/RpcTimeoutError"),
      P.tag("@amqp-contract/RpcCancelledError"),
      P.tag("@amqp-contract/RpcError"),
      (error) => (error instanceof RpcTimeoutError ? next() : ErrAsync(error)),
    ),
  );
```

They can also adjust `timeoutMs` or patch the request before it goes out.

A transport failure is a defect, not a modeled error, so it flows through `flatMapErrCases` untouched. Use `.recoverDefect(…)` if you need to act on one.

## Propagate a trace across services

The two mechanisms compose into W3C trace-context propagation without touching a handler: a publish interceptor stamps `traceparent` from the active span, and a worker middleware reads `args.rawMessage.properties.headers.traceparent`, resumes the remote context, and puts the span in the handler context.

Telemetry spans sit outside the interceptor chain, so interceptor work is already covered by the built-in instrumentation. See [instrument with OpenTelemetry](/how-to/instrument-with-opentelemetry).

## Where next

- [Consume messages](/how-to/consume-messages) — handler signatures.
- [Error model](/reference/error-model) — how short-circuit errors route.
- [Add logging](/how-to/add-logging) — the built-in logger, which may be all you need.
