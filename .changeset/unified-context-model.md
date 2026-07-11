---
"@amqp-contract/worker": minor
---

Unified context model (org DNA alignment, #549 — counterpart of temporal-contract#302):

- **`createContext` factory** on `TypedAmqpWorker.create`: builds the per-message dependency context that _seeds_ the middleware chain (and reaches handlers directly when no middleware is configured). Runs once per message after validation; a throw/rejection routes to the DLQ as `NonRetryableError`. demesne's `Layer.forkScope` is the recommended implementation for DI graphs.
- **Handler helpers**: the third handler argument is now `{ context, errors }` — `context` from `createContext` + middleware accumulation, `errors` a bag of typed constructors for the RPC's declared errors (`ErrAsync(errors.ORDER_NOT_FOUND({ orderId }))`, per-code data inference; the free `rpcError(code, data)` form remains). This reshapes the (unreleased) plain-context third argument introduced with the middleware feature.
- **Payload substitution**: middleware `next({ payload })` substitutes the message payload downstream; the dispatcher re-validates it against the consumer's schema before the handler runs — invalid substitutions fail terminally (DLQ).
- `composeMiddleware` overloads generalized to arbitrary chain-input context (so chains compose over the `createContext` seed type). New exports: `WorkerCreateContextInfo`, `WorkerHandlerHelpers`, `WorkerInferRpcErrorConstructors`.
