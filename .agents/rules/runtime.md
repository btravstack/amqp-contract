# Runtime

Cross-cutting concerns for code that touches the live AMQP layer: telemetry, connection pooling, and compression. Most of this lives in `@amqp-contract/core`.

## Connection management

`@amqp-contract/core` keeps a process-wide `ConnectionManagerSingleton` keyed on the URL set + connection options. `TypedAmqpClient` and `TypedAmqpWorker` share connections automatically when they're constructed with the same URLs — you don't need to (and shouldn't) wire connections manually.

Two invariants matter when touching this layer:

- **Ref-counted lifecycle.** `getConnection(...)` increments the count; `releaseConnection(...)` decrements and closes the underlying connection when the count hits zero. Every `getConnection` must be paired with a `releaseConnection` — otherwise the connection lives forever.
- **Failure-path cleanup.** If `waitForConnect()` (or any setup step before the worker/client returns to the user) errors, you must call `close()` to release the ref-count _before_ returning the error. `TypedAmqpClient.create` and `TypedAmqpWorker.create` already do this; if you write a new factory, mirror the pattern.

`AmqpClient.waitForConnect()` accepts a `connectTimeoutMs` (default 30s). `null` disables it; `Infinity`/`NaN`/`<= 0` are coerced to `null` because Node's `setTimeout` clamps and silently mis-fires on those. See `DEFAULT_CONNECT_TIMEOUT_MS` in [`packages/core/src/amqp-client.ts`](../../packages/core/src/amqp-client.ts).

## Telemetry (OpenTelemetry, optional)

`@opentelemetry/api` is an **optional peer dependency** of `@amqp-contract/core`. If a consumer installs it, telemetry flows automatically; if not, the default provider is a no-op.

Public surface from `@amqp-contract/core`:

| Export                                        | Use                                                                                                                  |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `TelemetryProvider`                           | Type. Pass a custom one via `CreateClientOptions.telemetry` etc.                                                     |
| `defaultTelemetryProvider`                    | Auto-detects `@opentelemetry/api`; no-op if absent.                                                                  |
| `startPublishSpan` / `startConsumeSpan`       | Open a span around a publish or consume operation.                                                                   |
| `endSpanSuccess` / `endSpanError`             | Close it; `endSpanError(span, error)` records the exception too.                                                     |
| `recordPublishMetric` / `recordConsumeMetric` | Counter for success/failure + duration histogram.                                                                    |
| `recordLateRpcReply`                          | Counter for replies that arrived after the caller gave up.                                                           |
| `MessagingSemanticConventions`                | Pre-defined attribute keys (`messaging.rabbitmq.message.delivery_tag`, etc.) — use these rather than ad-hoc strings. |

When adding a new public method to `TypedAmqpClient` / `TypedAmqpWorker`, wrap it in spans and metrics consistent with the surrounding code. See `client.ts:publish` for the canonical pattern (start span → run → `andTee` records success metric, `orTee` records failure metric).

## Compression

Both ends support gzip and deflate, controlled by the publisher:

- **Client** opts in per-publish via `options.compression: 'gzip' | 'deflate'`. The body is compressed and `contentEncoding` is set automatically — don't set `contentEncoding` yourself when using `compression`.
- **Worker** decompresses transparently before validation, based on `properties.contentEncoding`. Unknown encodings produce a `TechnicalError` (parse failure → DLQ via single `nack`, never enters retry).
- **RPC requests do not support compression.** The reply path doesn't decompress, so a compressed request would round-trip incorrectly. The client drops `compression` from RPC publish options on purpose (see `client.ts:call`).

The `CompressionAlgorithm` type is exported from `@amqp-contract/contract`.

## Logging

`Logger` is a structured-logging interface (compatible with `pino`'s shape). It's optional everywhere — `TypedAmqpClient` and `TypedAmqpWorker` accept a `logger?: Logger` option. When writing a new code path:

- Log at `info` for routine successes (one per published message is fine).
- Log at `warn` for recoverable issues (consumer cancelled by server, retry attempts).
- Log at `error` for handler failures and DLQ routing — include `consumerName`, `queueName`, and the error.
- Never log payload contents at info/warn — they may contain PII. Log identifiers (orderId, etc.) instead, and only at error if needed for triage.
