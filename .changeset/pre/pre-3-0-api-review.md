---
"@amqp-contract/contract": major
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
"@amqp-contract/testing": major
"@amqp-contract/asyncapi": major
---

Pre-3.0 API review: breaking changes taken while the major window is open.

- **`defineHandler` / `defineHandlers` / `defineMiddleware` → `declareHandler` / `declareHandlers` / `declareMiddleware`.** The family convention (shared with temporal-contract) is `define*` for contract authoring and `declare*` for implementation-side APIs, making the contract boundary visible in the name.
- **`defineRpc` error map entries are now `{ data: schema, message? }`** instead of `defineMessage(schema)`. `data` is the raw Standard Schema for the error payload; the optional `message` is a default human-readable message used when the handler constructs the error without one (and as the client-side fallback when a reply carries none).
- **`defineContract` accepts standalone topology**: top-level `exchanges`, `queues`, and `bindings` entries declare resources with no publisher/consumer attached (a DLQ bound to the auto-extracted DLX, audit queues). Dead-letter exchanges are auto-extracted for standalone queues exactly as for consumer queues.
- **Worker `middleware` accepts an array** (first = outermost), composed like `composeMiddleware(...)`. Pre-compose with `composeMiddleware` when you want stepwise context types inferred.
- **`worker.close()` now drains in-flight handlers** before closing the channel (their acks land; completed work is not redelivered), bounded by a new `drainTimeoutMs` option (default `DEFAULT_DRAIN_TIMEOUT_MS` = 30 s; `null` waits forever).
- **Worker `ConsumerOptions` is now a curated subset** (`prefetch`, `priority`, `arguments`, `consumerTag`, `exclusive`). `noAck` is gone — it silently broke the ack-exactly-once and retry/DLQ invariants.
- **RPC handler response types use the schema's _input_** (defaults optional, transforms not yet applied) — the worker validates before replying, matching the existing RPC error-data convention.
- **Invalid `connectTimeoutMs`** (NaN, 0, negative, Infinity) now surfaces as a Defect from `create()` instead of silently disabling the timeout. Pass `null` to disable.
- **Testing fixture wait options renamed**: `{ nbEvents, timeout }` → `{ count, timeoutMs }`, aligning with the library-wide `timeoutMs` convention. The fixture record is exported as `AmqpTestFixtures`, the wait options as `WaitForMessagesOptions`; the package now exposes `./package.json`.
- **`@amqp-contract/asyncapi`: `failOnMissingConverter` defaults to `true`** — a spec that silently degrades schemas to `{ type: "object" }` placeholders lies to its consumers. It generates AsyncAPI 3.1 (docs previously claimed 3.0).
- **Deprecated `_*ForTesting` aliases removed** from `@amqp-contract/core` (`_resetConnectionsForTesting`, `_getConnectionCountForTesting`, `_resetTelemetryCacheForTesting`) — use the `_internal_*` forms.
- **Export hygiene**: `RetryOptions`, `NoneRetryOptions`, `DefineQueueOptionsWithDeadLetterExchange`, `QueueDefinitionWithDeadLetterExchange`, `RpcErrorDefinition`, `InferSchemaInput`, `InferSchemaOutput` (contract); `PublishError`, `CallError`, `ClientInferCallError` (client); `AnyWorkerMiddleware`, `DEFAULT_DRAIN_TIMEOUT_MS` (worker); `isTechnicalError`, `isMessageValidationError` guards (core). Client and worker re-export `Logger`, `LoggerContext`, `TelemetryProvider`, and `TechnicalError` so naming an option type never forces a direct dependency on core. `defineCommandPublisher` now preserves literal routing-key types. All packages declare `sideEffects: false`.
