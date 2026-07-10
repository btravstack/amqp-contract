---
"@amqp-contract/contract": minor
"@amqp-contract/core": minor
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
---

Add typed RPC error maps: declare per-RPC business errors in the contract and get them typed end-to-end.

`defineRpc` now accepts an optional `errors` map (error code → `defineMessage(...)` for the error's `data` payload):

```typescript
const getOrder = defineRpc(queue, {
  request: defineMessage(z.object({ orderId: z.string() })),
  response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
  errors: {
    ORDER_NOT_FOUND: defineMessage(z.object({ orderId: z.string() })),
  },
});
```

- **Worker**: RPC handlers can return `Err(rpcError(code, data))` for declared codes — the handler's error channel widens to `HandlerError | RpcError<code, data>`. The worker validates `data` against the declared schema, publishes an error reply, and acks the request (business errors are never retried). Undeclared codes or invalid data route to the DLQ.
- **Client**: `client.call(...)` error union gains the declared `RpcError<code, data>` members; error data is re-validated on arrival. Discriminate with `isRpcError(error)` and narrow on `error.code`.
- New exports: `RpcError`, `isRpcError`, `rpcError` (worker), `RpcErrorMap` (contract), `ClientInferRpcErrors` / `WorkerInferRpcErrors` inference helpers, `RPC_ERROR_CODE_HEADER` (core).

The wire format is backward compatible: success replies are unchanged; error replies are marked by the `x-amqp-contract-error-code` AMQP header with a `{ message, data }` JSON body. RPCs that declare no errors behave exactly as before.
