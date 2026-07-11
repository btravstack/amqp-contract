---
"@amqp-contract/worker": minor
"@amqp-contract/client": minor
---

Add worker middleware and client interceptors for cross-cutting concerns (trace propagation, auth, idempotency, logging) without handler wrapping.

**Worker middleware** — wraps every handler invocation (consumers and RPCs) after message validation, with oRPC-style typed context injection:

```typescript
const auth = defineMiddleware<EmptyContext, { tenantId: string }>((args, next) => {
  const tenantId = args.rawMessage.properties.headers?.["x-tenant-id"];
  if (typeof tenantId !== "string") return Err(nonRetryable("unauthenticated")).toAsync();
  return next({ context: { tenantId } });
});

TypedAmqpWorker.create({
  contract,
  middleware: composeMiddleware(auth, timing),
  handlers: {
    // third argument is typed as { tenantId: string }
    processOrder: ({ payload }, _raw, context) => ...,
  },
  urls,
});
```

Short-circuit results route exactly like handler results: `retryable`/`nonRetryable` → retry/DLQ, `rpcError(code, data)` → typed RPC error reply, `Ok(value)` → skips the handler (RPC reply validated as usual). New exports: `defineMiddleware`, `composeMiddleware`, `WorkerMiddleware`, `WorkerMiddlewareArgs`, `WorkerMiddlewareNext`, `EmptyContext`.

**Client interceptors** — `publishInterceptors` wrap validation + publish (patched messages are re-validated), `callInterceptors` wrap the full RPC round trip; both can patch args, observe outcomes, retry by calling `next` again, or short-circuit. First array entry is outermost. New exports: `PublishInterceptor`, `CallInterceptor` (+ args/next types).

**Note:** handlers now always receive a third `context` argument (an empty object when no middleware is configured). Existing two-parameter handlers are unaffected; only tests asserting exact handler call arity need a third `expect.anything()`.
