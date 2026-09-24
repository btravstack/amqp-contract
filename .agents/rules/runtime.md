# Runtime

Cross-cutting concerns for code that touches the live AMQP layer: telemetry, connection pooling, and compression. Most of this lives in `@amqp-contract/core`.

## Connection management

`@amqp-contract/core` keeps a process-wide `ConnectionManagerSingleton` keyed on **pool partition** + URL set + connection options. `TypedAmqpClient` leases from the `"client"` pool and `TypedAmqpWorker` from the `"worker"` pool (`AmqpClientOptions.connectionPool`, default `"default"`), so a publisher and a consumer never share a TCP connection by default — RabbitMQ blocks publishing connections under resource alarms. Clients share among themselves, workers among themselves. A caller-owned `AmqpConnectionManager` can be passed as `connection` instead of `urls` (exactly one of the two; see `ConnectionSource` in [`packages/core/src/amqp-client.ts`](../../packages/core/src/amqp-client.ts)) — it is borrowed, never closed.

Two invariants matter when touching this layer:

- **Ref-counted lifecycle.** `getConnection(...)` increments the count; `releaseConnection(...)` decrements and closes the underlying connection when the count hits zero. Every `getConnection` must be paired with a `releaseConnection` — otherwise the connection lives forever.
- **Failure-path cleanup.** If `waitForConnect()` (or any setup step before the worker/client returns to the user) errors, you must call `close()` to release the ref-count _before_ returning the error. `TypedAmqpClient.create` and `TypedAmqpWorker.create` both go through `startOrClose` ([`packages/core/src/lifecycle.ts`](../../packages/core/src/lifecycle.ts), exported from `@amqp-contract/core/internal`); a new factory should too.

`AmqpClient.waitForConnect()` answers `Err(ConnectionError)` on timeout, with the last `connectFailed` error from amqp-connection-manager as `cause`; the first failed dial is logged at `warn`. `isConnected()` (core `AmqpClient`, delegated by `TypedAmqpClient`) is the readiness accessor. `connectTimeoutMs` defaults to 30s. `null` disables it; `Infinity`/`NaN`/`<= 0` are coerced to `null` because Node's `setTimeout` clamps and silently mis-fires on those. See `DEFAULT_CONNECT_TIMEOUT_MS` in [`packages/core/src/amqp-client.ts`](../../packages/core/src/amqp-client.ts).

## Topology

`setupAmqpTopology(channel, contract, { mode })` runs on every (re)connect. `TopologyMode` is `"assert"` (default: declare), `"passive"` (`checkExchange` / `checkQueue` only, bindings skipped) or `"none"`. Scope is the contract you hand `AmqpClient`: `TypedAmqpClient` passes `publisherTopology(contract)`: publisher exchanges, everything they route to (e2e bindings transitively, the queues bound to any of them and those bindings) and the RPC request queues — so a message published before a worker declared its queue is retained, not confirmed-and-dropped (the runtime twin of invariant 19). Those queues are declared with the worker's exact arguments but with the DLX inlined as a raw argument (not declared) and no retry config (no wait queues); exclusive queues are skipped. `TypedAmqpWorker` passes `workerTopology(contract)`: its consumed queues (consumers + RPCs) intact — retry wait queues, the bindings into them and their source exchanges — plus their DLXs and everything those route to (the DLQs), never publisher-only exchanges or unrelated queues. Both take `topology?: TopologyMode`. See [`packages/core/src/setup.ts`](../../packages/core/src/setup.ts).

## Publish failures

Core's `AmqpClient.publish` / `sendToQueue` classify the channel wrapper's outcome **once**: its `timeout` / `message nacked` / `Channel closed` rejections → `PublishError("timeout" | "nacked" | "channel-closed")`, all on the `E` channel; anything else (unencodable payload, unknown rejection) → defect with a `TechnicalError` cause. Callers never see the boolean: on the confirm channel amqp-connection-manager resolves `false` only _after_ the broker confirmed the message (it signals a full write buffer — backpressure), so `false` is success, logged at `debug`. Treating it as a failure would make callers republish a delivered message. The worker routes `PublishError` at its two publish sites: a failed retry republish requeues the original (`nack(requeue: true)`, retry headers unchanged — never dead-lettered for a broker hiccup); a failed RPC reply becomes a `NonRetryableError` (→ DLQ). See `publishForRetry` in [`packages/worker/src/retry.ts`](../../packages/worker/src/retry.ts) and `publishReply` in [`packages/worker/src/rpc-reply.ts`](../../packages/worker/src/rpc-reply.ts).

## Worker dispatch

Every delivery ends in one modeled `Outcome` — `acked` | `retried` | `requeued` | `dead-lettered` ([`packages/worker/src/outcome.ts`](../../packages/worker/src/outcome.ts)). `processMessage` computes it without touching the channel; `dispatchMessage` then settles the delivery **once** (`settle`) and records telemetry from the same outcome (`recordOutcome`: only `acked` is a success; failures record the causing error class). Retry routing is split into the pure `decideRetry(error, queue, headers, rand)` and the thin executor `handleError` ([`packages/worker/src/retry.ts`](../../packages/worker/src/retry.ts)). Retry headers read from the wire go through `readCount` (anything but a non-negative safe integer counts as 0) and a retry is only published to a declared wait-queue tier. Defects are for genuine bugs (a throwing handler); they are still dead-lettered. Don't route control flow through the defect channel.

## Telemetry (OpenTelemetry, optional)

`@opentelemetry/api` is an **optional peer dependency** of `@amqp-contract/core`. If a consumer installs it, telemetry flows automatically; if not, the default provider is a no-op.

Public surface from `@amqp-contract/core`: `TelemetryProvider` (type — pass a custom one via `CreateClientOptions.telemetry` etc.), `defaultTelemetryProvider` (auto-detects `@opentelemetry/api`; no-op if absent) and `MessagingSemanticConventions` (attribute keys — use these rather than ad-hoc strings).

Implementation helpers, from `@amqp-contract/core/internal` (no semver guarantee):

| Export                                                                | Use                                                                                                                                                                                                                                                                                                                       |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `startPublishSpan` / `startConsumeSpan`                               | Open a span around a publish or consume operation.                                                                                                                                                                                                                                                                        |
| `endSpanSuccess` / `endSpanError`                                     | Close it; `endSpanError(span, error)` records the exception too.                                                                                                                                                                                                                                                          |
| `recordPublishMetric` / `recordConsumeMetric` / `recordRpcCallMetric` | Success/failure counter + duration histogram (`recordRpcCallMetric` → `amqp.client.rpc.duration`, histogram only).                                                                                                                                                                                                        |
| `recordLateRpcReply`                                                  | Counter for replies that arrived after the caller gave up.                                                                                                                                                                                                                                                                |
| `injectTraceContext` / `runWithTraceContext(headers, span, fn)`       | Trace propagation. Core's `publish` injects the active context into headers; core's `consume` runs each delivery inside the extracted context. `runWithTraceContext` runs `fn` under extracted-or-active context with `span` active — the client wraps its core publish in it so the producer span is what gets injected. |

When adding a new public method to `TypedAmqpClient` / `TypedAmqpWorker`, wrap it in spans and metrics consistent with the surrounding code. See `client.ts:instrument` for the canonical pattern (start span → run → `.tap` records success, `.tapFailure` records failure on both `Err` and `Defect`). Every helper swallows its own throws — telemetry never throws into the data path (invariant 16).

## Compression and the message codec

All encoding lives in one module, [`packages/core/src/codec.ts`](../../packages/core/src/codec.ts) (`encodeBody` / `encodeMessage` / `decompressBuffer` / `decodeMessage`, from `@amqp-contract/core/internal`). Don't hand-roll JSON or zlib calls elsewhere.

- **Client** opts in per-publish via `options.compression: 'gzip' | 'deflate'`; `encodeMessage` compresses and the client sets `contentEncoding` from what it produced — don't set `contentEncoding` yourself when using `compression`.
- **Worker** runs `decodeMessage` before validation, based on `properties.contentEncoding`. Unknown encodings, corrupt streams and bodies over the size cap surface from the codec as a `Defect` (its cause a `TechnicalError`); the worker's parse boundary (`parseOrPoison`) triages them, like a schema failure (`MessageValidationError`, modeled), into a `dead-lettered` outcome — DLQ via single `nack`, never enters retry.
- **Size cap**: `DEFAULT_MAX_MESSAGE_BYTES` = 16 MiB, applied to decompressed output (enforced by zlib's `maxOutputLength` while inflating) **and** to plain bodies. The worker's `maxMessageBytes` overrides it (`maxDecompressedBytes` is its deprecated alias). The client decodes RPC replies through the same codec.
- **RPC requests don't carry compression.** The worker's parse/validate path _does_ decompress an RPC request fine if it sees `contentEncoding`, and replies are always uncompressed — so a compressed RPC request would round-trip mechanically. Even so, `client.call()` deliberately strips any inherited `compression` from `defaultPublishOptions` before publishing, so the on-wire convention stays consistent (no compression in either direction of an RPC). If you're wiring a new code path involving RPCs, mirror that — don't compress RPC requests.

The `CompressionAlgorithm` type is exported from `@amqp-contract/contract`.

## Logging

`Logger` is a structured-logging interface (compatible with `pino`'s shape). It's optional everywhere — `TypedAmqpClient` and `TypedAmqpWorker` accept a `logger?: Logger` option. When writing a new code path:

- Log at `info` for routine successes (one per published message is fine).
- Log at `warn` for recoverable issues (consumer cancelled by server, retry attempts).
- Log at `error` for handler failures and DLQ routing — include `consumerName`, `queueName`, and the error.
- Never log payload contents at info/warn — they may contain PII. Log identifiers (orderId, etc.) instead, and only at error if needed for triage.
