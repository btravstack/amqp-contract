# @amqp-contract/contract

## 3.0.0-beta.5

### Major Changes

- 22ea72b: `defineContract` now throws when a consumed queue has no dead-letter exchange,
  because such a queue silently discards every message its handler rejects.
  Declared-but-unconsumed queues (including dead-letter queues themselves) are not
  checked.

  Three forms satisfy the check: `deadLetter: { exchange: … }`, an explicit
  `onPoison: "drop"`, and — least discoverably — an `x-dead-letter-exchange` set
  through the raw `arguments` passthrough, which `setupAmqpTopology` forwards to
  the broker verbatim. This check verifies only that a DLX is _declared_; whether
  the exchange it names actually routes anywhere is enforced by a separate check
  in this same release — see the entry on a dead-letter exchange with nothing
  bound to it.

  **Read this before adding `deadLetter` to a queue that already exists in
  production.** A queue's dead-letter configuration is part of its identity:
  `deadLetter` becomes the `x-dead-letter-exchange` argument, and RabbitMQ refuses
  to redeclare an existing queue with different arguments —
  `PRECONDITION_FAILED - inequivalent arg`, a channel-level 406 at worker startup.
  Adding `deadLetter` to a live queue therefore fails at deploy time, not at
  define time. Your routes out:

  - **Declare a new queue with the DLX and migrate consumers to it**, draining the
    old one first. The only option that changes nothing about the running queue.
  - **Apply the dead-lettering as a broker policy** (`rabbitmqctl set_policy` with
    `dead-letter-exchange`) instead of a queue argument. Policies are not part of
    queue identity, so they apply to existing queues — but the contract still
    needs `onPoison: "drop"` to pass this check, since it cannot see the policy.
  - **`onPoison: "drop"`** if you accept the loss. Correct for a metrics firehose
    or any queue whose rejected messages genuinely have no value; a lie anywhere
    else.

  On a queue that does not exist yet, `deadLetter: { exchange: … }` is the right
  answer and costs nothing.

  The predicate behind the check is shared with `@amqp-contract/worker`'s
  terminal-nack logging through a new `@amqp-contract/contract/internal` entry
  point, so the two can never disagree about whether a queue dead-letters. That
  subpath carries **no semver guarantee** and is not part of the
  contract-authoring API — application code has no reason to import it.

  See "`defineContract` says my queue has no dead-letter exchange" and
  "`PRECONDITION_FAILED - inequivalent arg`" in the troubleshooting guide.

- 8b50784: Pre-3.0 audit: one decision point for a full write buffer, and Deno-style exported signatures.

  - **`AmqpClient.publish` / `sendToQueue` return `AsyncResult<void, never>`** (was `AsyncResult<boolean, never>`). The channel wrapper's boolean `false` (write buffer full) used to be re-triaged four different ways downstream; it is now absorbed inside core and surfaces as a Defect with a `TechnicalError` cause ("channel write buffer full"), like every other publish-side infrastructure failure. The worker's RPC reply publish is the one site that still needs a modeled error: it recovers the defect into a `NonRetryableError`, so a failed reply publish keeps routing the request to the DLQ.
  - **Exported signatures follow the [Deno style rule](https://docs.deno.com/runtime/contributing/style_guide/)** — max two positional arguments, trailing options object, no positional booleans:

    - `client.ack(msg, allUpTo?, options?)` → `client.ack(msg, { allUpTo?, deliveryEpoch? })`
    - `client.nack(msg, allUpTo?, requeue?, options?)` → `client.nack(msg, { allUpTo?, requeue?, deliveryEpoch? })` (requeue still defaults to true)
    - `client.publish(exchange, routingKey, content, options?)` → `client.publish({ exchange, routingKey }, content, options?)`
    - `@amqp-contract/testing`'s `publishMessage(exchange, routingKey, content, options?)` fixture → `publishMessage({ exchange, routingKey }, content, options?)`

    The typed client's `client.publish(name, message, options?)` is unaffected.

  - **`defineExchangeBinding` requires a non-empty routing key for direct/topic source exchanges.** It was the last builder still defaulting a missing key to `""` (silently unroutable); it now throws the same actionable define-time error as `definePublisher` and `defineQueueBinding`. Fanout/headers sources remain exempt.

- 6d8593b: Uniform `QueueDefinition` + per-delay-tier TTL-backoff wait queues: the `QueueEntry`/`extractQueue` split is gone and the retry schedule no longer degrades to the longest in-flight delay.

  - **`defineQueue` always returns a single uniform `QueueDefinition`** — with TTL-backoff retry it no longer returns a branded wrapper carrying wait-queue/exchange infrastructure. Access `queue.name` / `queue.type` directly. Deleted exports: `extractQueue`, `isQueueWithTtlBackoffInfrastructure`, and the `QueueEntry`, `QueueWithTtlBackoffInfrastructure`, `QueueEntryWithDeadLetterExchange` (renamed to `QueueDefinitionWithDeadLetterExchange`), `TtlBackoffRetryInfrastructure` types, plus the `__brand` machinery on queues.
  - **TTL-backoff topology is derived, never stored.** The new pure helpers `deriveTtlBackoffInfrastructure(queue)`, `ttlBackoffBaseDelay(retry, retryCount)`, and `ttlBackoffWaitQueueName(queueName, delayMs)` (with the `TtlBackoffInfrastructure` / `TtlBackoffWaitQueueDefinition` types) compute everything from `queue.retry`; `setupAmqpTopology` declares the wait queues at channel-setup time and the worker's retry pipeline publishes to them. `ContractOutput` now matches the runtime `defineContract` object exactly — no more runtime-injected `wait-exchange` / wait-queue entries that failed to typecheck.
  - **One wait queue per distinct backoff delay** (`{queue}-wait-{delayMs}ms`) fixes head-of-line blocking: RabbitMQ only dead-letters expired messages at the head of a queue, so the old single shared wait queue let a parked 60s retry block a later 1s retry — the configured schedule silently degraded to the longest in-flight delay. Each tier queue is declared with a queue-level `x-message-ttl` backstop (the jitter ceiling, `ceil(base * 1.5)`) and dead-letters back to the main queue via the default exchange; the per-message `expiration` carries the jittered delay. Within a tier, head-of-line skew is bounded by the jitter spread and is zero with `jitter: false`.
  - **The wait/retry headers exchanges are gone.** The retry copy is published straight to the tier queue via the default exchange, so the `x-wait-queue` / `x-retry-queue` routing headers are no longer stamped. Retried deliveries arrive with `fields.routingKey` set to the queue name; the original routing key is preserved in the new `x-original-routing-key` header (also stamped by classic-queue immediate-requeue republishes). The `x-retry-count` / `x-last-error` / `x-first-failure-timestamp` accounting is unchanged.
  - **`TtlBackoffRetryOptions` lost `waitQueueName` / `waitExchangeName` / `retryExchangeName`** — tier wait-queue names are derived and no longer configurable.
  - **Migration**: broker-side wait-queue names change. The old `{queue}-wait` queues and the `wait-exchange` / `retry-exchange` headers exchanges become unused after upgrading — drain them (any parked retries will still dead-letter back to their main queue when their TTL expires, as long as you leave the old topology in place until empty), then delete them.

- e479a35: Pre-3.0 audit: routing-key safety and naming/export hygiene, taken while the major window is open.

  - **`defineEventConsumer` topic routing-key overrides are now checked against the publisher's routing key at compile time.** A pattern that can never match — `defineEventConsumer(orderCreatedEvent, queue, { routingKey: "user.*" })` against a publisher on `order.created` — used to compile and silently receive nothing at runtime; it is now a type error whose message names both sides (`"Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'"`). The new `MatchingBindingPattern<Pattern, PublisherKey>` type is exported; pattern matching also learned that a trailing `#` matches zero words (`order.created.#` now accepts `order.created`).
  - **Direct/topic publishers and queue bindings require a non-empty routing key at define time.** `definePublisher` silently defaulted a missing routing key to `""` and `defineQueueBinding` left it `undefined` — both silently misroute. `definePublisher`, `defineQueueBinding`, `defineEventPublisher`, and `defineCommandConsumer` (via its binding) now throw an actionable error at define time for JavaScript callers and casts. Fanout/headers exchanges are exempt (they ignore routing keys).
  - **`@amqp-contract/core`'s option types renamed**: `ConsumerOptions` → `AmqpConsumeOptions`, `PublishOptions` → `AmqpPublishOptions`. They collided with the user-facing `ConsumerOptions` (worker) and `PublishOptions` (client), which keep their names and shapes.
  - **`_internal_*` helpers moved off `@amqp-contract/core`'s root** to a new `@amqp-contract/core/internal` subpath: `_internal_getConnectionCount`, `_internal_resetConnections`, `_internal_resetTelemetryCache`. They are test-lifecycle helpers with no semver guarantee and no longer clutter the public root.
  - **`defineEventPublisher`'s `arguments` option renamed to `bindingArguments`.** It never was a publish argument — it is forwarded as the default AMQP binding arguments for that event's consumers' queue bindings (a consumer's own `arguments` option overrides it). The name now says so; `EventPublisherConfig`/`EventPublisherConfigBase` carry `bindingArguments` accordingly.
  - **Builder-result brands are now a non-exported `unique symbol`** instead of `__brand: "EventConsumerResult"`-style string fields on `EventPublisherConfig`, `EventConsumerResult`, `CommandConsumerConfig`, and `BridgedPublisherConfig` (and their `*Base` types). The brand no longer shows up in IDE hovers and cannot be forged by user code; nominal separation and the `is*` type guards behave as before. Code that constructed these config objects by hand (rather than via `defineEvent*` / `defineCommand*`) no longer compiles — use the builders.

- 783f6f9: `defineContract` now throws when a queue's dead-letter exchange has nothing bound
  to it. RabbitMQ discards a message routed to zero queues, so such a queue lost
  every rejected message exactly as silently as one with no dead-letter exchange at
  all — while the worker logged a reassuring "Sending message to DLQ". Bind a queue
  to the exchange, or set `externalConsumers: true` on the deadLetter config if
  another service owns it. A dead-letter exchange supplied through the raw
  `arguments` passthrough names an exchange this contract cannot inspect and is not
  checked, and a dead-letter exchange declaring an `alternate-exchange` argument is
  always accepted — the broker hands its unmatched messages there rather than
  discarding them, exactly as for publishers.

  `DeadLetterConfig.externalConsumers?: boolean` is the new opt-out, accepted by
  `defineQueue` and mirroring `PublisherDefinition.externalConsumers`.

  Bind the key that will actually arrive. On a `direct` dead-letter exchange `#` is
  a literal that matches nothing — the error message says so, because when the
  queue sets no `deadLetter.routingKey` the check accepts any binding and cannot
  catch it. On a queue with `retry: { mode: "ttl-backoff" }` a retried message
  re-enters through the wait queue carrying the queue name as its routing key, so
  the publisher's key is not what reaches the dead-letter exchange either. Setting
  an explicit `deadLetter.routingKey` sidesteps both.

- 9aae6a2: `defineContract` now throws when a publisher's routing key reaches no queue.
  RabbitMQ confirms an unroutable message and then discards it, so a mistyped
  binding pattern silently dropped every message while `publish()` returned
  `Ok`. Publishers whose consumers live in another service opt out with
  `externalConsumers: true`, accepted by `definePublisher`,
  `defineEventPublisher`, and `defineCommandPublisher` alike. An exchange
  declaring an `alternate-exchange` argument is always routable — the broker
  catches its unmatched keys.

  The check runs on the bindings passed to `defineContract`: mutating
  `contract.bindings` afterwards no longer makes a publisher routable, since the
  verdict was already reached. Declare every binding in the `defineContract` call.

### Minor Changes

- 9729fa6: Add the `RoutableRoutingKey<Key, Patterns>` type. It resolves to `Key` when the
  routing key matches at least one of the declared binding patterns and to a
  readable `` `Error: routing key '…' matches none of the declared binding
patterns; …` `` string type otherwise — the same convention as
  `MatchingBindingPattern`. The check runs only when both the key and the
  patterns are fully known at compile time. Plain `string`, template-literal
  types such as `` `order.${string}` ``, unions containing either, and an empty
  pattern union all skip the check and resolve to `Key` — an undecidable case
  defers to the define-time check rather than being guessed at.

  The type is exported for direct use; `defineContract`'s signature is
  deliberately not constrained by it (binding patterns are widened to `string` by
  the time they reach `defineContract`, so the constraint would be a no-op). The
  define-time check added in the same release covers the full binding graph at
  runtime.

### Patch Changes

- a80a3d7: Fixed `defineEventConsumer` rejecting a routing-key override typed as a template
  literal. A pattern such as `` `${string}.created` `` matches `order.created` at
  runtime, but `MatchingBindingPattern` treated any type that was not plain
  `string` as decidable, could not decide it, and failed the build with
  "binding pattern '${string}.created' can never match the publisher routing key
  'order.created'". Tenant- and environment-prefixed routing keys are the common
  way to hit this.

  The three matcher types — `MatchingBindingPattern`, `MatchingRoutingKey`, and
  `RoutableRoutingKey` — now share one test for whether a string is fully known at
  compile time, and skip the match when it is not. `MatchingRoutingKey` also loses
  an asymmetry where a plain-`string` pattern collapsed to `never` while a
  plain-`string` key did not.

  Rejection is unchanged only when both the pattern and the publisher routing
  key are fully known at compile time. When either side is not — a hole
  anywhere in the type, a union containing one, or a type carrying extra
  structure such as a brand — the check is skipped, even when the known side
  alone already proves no match is possible. ``MatchingBindingPattern<"user.x",
`${string}.created`>`` is one such case: `"user.x"` can never equal any
  string ending in `.created`, and this used to fail the build; it is accepted
  now, unchecked, because the key side is not fully known. The same happens
  when the pattern side is the undecidable one — a pattern with a hole and a
  literal tail, such as `` `${string}.updated` ``, or a pattern narrowed by an
  intersection such as `"user.*" & {__b: "x"}`, is accepted against a fully
  known key for the same reason. That trade is deliberate — rejecting a valid
  contract is the costlier error.

  The define-time routability check in `defineContract` covers the publisher
  side — it fails a contract whose publisher reaches no queue. It does not cover
  `MatchingBindingPattern` or `MatchingRoutingKey`'s undecidable cases: a
  consumer binding that receives nothing while a sibling binding keeps the
  publisher routable gets no compile-time and no define-time signal.

- d30cbf3: Second robustness pass from the pre-3.0 audit — correctness fixes, resource-safety guards, and internal idiom alignment. All additive or bug-fix; no further breaking changes beyond those already listed in the other pre-3.0 changesets.

  **Correctness fixes**

  - **A channel `error` event no longer crashes the process.** amqp-connection-manager's `ChannelWrapper` emits plain `'error'` events for conditions it recovers from by reconnecting (topology setup failure on connect/reconnect, publish-worker faults, consumer re-establishment). With no listener attached, Node escalated the emit to `ERR_UNHANDLED_ERROR` and the process died. `AmqpClient` now always attaches a listener that degrades the event to `logger.error`; the typed client/worker thread their logger down, and user `on('error', …)` listeners still fire. (`AmqpClientOptions` gains an optional `logger`.)
  - **A single un-composed middleware now merges its context over the `createContext` seed** instead of replacing it, so `middleware: mw` and `middleware: [mw]` behave identically (previously the bare form silently dropped every seed field).
  - **`client.publish(...)` / `client.call(...)` with a name the contract does not declare** now resolve to a `Defect` (a `TechnicalError` naming the culprit and the declared names) instead of throwing a raw `TypeError`, honoring the client's "nothing in the public API throws" contract.
  - **Reconnect-safe settles.** Delivery tags are per-channel, but a buffered retry publish or RPC reply can confirm on a _new_ channel; the follow-up ack/nack then targeted a foreign tag (channel-closing 406, or settling an unrelated delivery whose own DLQ nack was lost). `AmqpClient` now tracks a channel epoch (`currentChannelEpoch`) and skips a settle stamped with a stale epoch — the broker's redelivery preserves at-least-once.
  - **The RPC timeout stays armed through async reply validation**, so a slow or never-settling response validator can no longer leave the caller hanging past `timeoutMs`.

  **Resource-safety guards**

  - **`publishTimeoutMs`** (client and worker `create` options): bounds how long a publish may sit buffered during a broker outage before its promise settles as a `Defect`, instead of buffering unboundedly forever.
  - **`maxDecompressedBytes`** (worker `create` option, default 64 MiB): caps inbound decompression so a decompression bomb follows the poison-message DLQ path instead of exhausting memory.
  - **Connection-pool keys distinguish function-valued options** (`findServers`, amqplib `credentials`), which `JSON.stringify` dropped — two clients differing only in a callback no longer collapse onto one shared connection.
  - **`TypedAmqpWorker.create` fails fast on a handler key that names no contract entry** (a stale key from a spread, or a missed rename), before any connection is acquired, instead of silently leaving that message class unprocessed.
  - **A poison message nacked on a queue with no DLX is now logged on the validation path too**, matching the retry path's existing diagnostic. (Superseded within this same release: see the `@amqp-contract/worker` entry for the final wording and level — a _declared_ `onPoison: "drop"` is recorded at `info`, and only an undeclared loss warns.)

  **Internal idiom alignment (no observable behavior change)**

  - Adopt `@unthrown/standard-schema`'s `fromSchemaAsync` at the six hand-rolled Standard Schema validation boundaries in the client and worker.
  - Hoist the `technicalDefect` defect-mint seam into `@amqp-contract/core` (deleting the three copies), and give `safeJsonParse` the full `(cause, defect)` qualify signature so callers no longer model-then-defect.

## 3.0.0-beta.4

### Major Changes

- 7f406c6: Pre-3.0 API review: breaking changes taken while the major window is open.

  - **`defineHandler` / `defineHandlers` / `defineMiddleware` → `declareHandler` / `declareHandlers` / `declareMiddleware`.** The family convention (shared with temporal-contract) is `define*` for contract authoring and `declare*` for implementation-side APIs, making the contract boundary visible in the name.
  - **`defineRpc` error map entries are now `{ data: schema, message? }`** instead of `defineMessage(schema)`. `data` is the raw Standard Schema for the error payload; the optional `message` is a default human-readable message used when the handler constructs the error without one (and as the client-side fallback when a reply carries none).
  - **`defineContract` accepts standalone topology**: top-level `exchanges`, `queues`, and `bindings` entries declare resources with no publisher/consumer attached (a DLQ bound to the auto-extracted DLX, audit queues). Dead-letter exchanges and TTL-backoff infrastructure are auto-extracted for standalone queues exactly as for consumer queues.
  - **Worker `middleware` accepts an array** (first = outermost), composed like `composeMiddleware(...)`. Pre-compose with `composeMiddleware` when you want stepwise context types inferred.
  - **`worker.close()` now drains in-flight handlers** before closing the channel (their acks land; completed work is not redelivered), bounded by a new `drainTimeoutMs` option (default `DEFAULT_DRAIN_TIMEOUT_MS` = 30 s; `null` waits forever).
  - **Worker `ConsumerOptions` is now a curated subset** (`prefetch`, `priority`, `arguments`, `consumerTag`, `exclusive`). `noAck` is gone — it silently broke the ack-exactly-once and retry/DLQ invariants.
  - **RPC handler response types use the schema's _input_** (defaults optional, transforms not yet applied) — the worker validates before replying, matching the existing RPC error-data convention.
  - **Invalid `connectTimeoutMs`** (NaN, 0, negative, Infinity) now surfaces as a Defect from `create()` instead of silently disabling the timeout. Pass `null` to disable.
  - **Testing fixture wait options renamed**: `{ nbEvents, timeout }` → `{ count, timeoutMs }`, aligning with the library-wide `timeoutMs` convention. The fixture record is exported as `AmqpTestFixtures`, the wait options as `WaitForMessagesOptions`; the package now exposes `./package.json`.
  - **`@amqp-contract/asyncapi`: `failOnMissingConverter` defaults to `true`** — a spec that silently degrades schemas to `{ type: "object" }` placeholders lies to its consumers. It generates AsyncAPI 3.1 (docs previously claimed 3.0).
  - **Deprecated `_*ForTesting` aliases removed** from `@amqp-contract/core` (`_resetConnectionsForTesting`, `_getConnectionCountForTesting`, `_resetTelemetryCacheForTesting`) — use the `_internal_*` forms.
  - **Export hygiene**: `RetryOptions`, `NoneRetryOptions`, `DefineQueueOptionsWithDeadLetterExchange`, `QueueEntryWithDeadLetterExchange`, `RpcErrorDefinition`, `InferSchemaInput`, `InferSchemaOutput` (contract); `PublishError`, `CallError`, `ClientInferCallError` (client); `AnyWorkerMiddleware`, `DEFAULT_DRAIN_TIMEOUT_MS` (worker); `isTechnicalError`, `isMessageValidationError` guards (core). Client and worker re-export `Logger`, `LoggerContext`, `TelemetryProvider`, and `TechnicalError` so naming an option type never forces a direct dependency on core. `defineCommandPublisher` now preserves literal routing-key types. All packages declare `sideEffects: false`.

### Patch Changes

- 519c670: Internal cleanup from enabling all `@unthrown/oxlint` rules (`no-throw` and `prefer-ensure` on top of the recommended set): redundant `Promise<Result<...>>` return annotations dropped in favor of inference, and every deliberate `throw` site now carries a targeted lint disable naming its reason. No runtime behavior change.
- 7f406c6: Robustness fixes from the pre-3.0 full review.

  - **Compression was broken end-to-end**: the channel's JSON mode serialized the compressed Buffer into `'{"type":"Buffer","data":[...]}'` on the wire, so every compressed publish was DLQ'd on arrival. The client now encodes content itself (JSON for plain values, byte-for-byte for Buffers) and JSON mode is off. Retry republishing now passes the original bytes through untouched for all content types.
  - **Per-consumer `prefetch` no longer bleeds across consumers**: it now maps to amqp-connection-manager's native per-consumer prefetch, applied immediately before each `basic.consume` and re-applied per-consumer on reconnect (previously, any reconnect applied one consumer's QoS to all).
  - **Classic-queue immediate-requeue retries republish to the failing queue via the default exchange** instead of the original exchange — a fanout/topic topology no longer delivers retry duplicates (and foreign retry headers) to sibling queues.
  - **Closing a worker under load can no longer crash the process**: the defensive nack in the consume boundary is guarded (a nack racing channel teardown was an unhandled rejection), and `close()` drains in-flight handlers first.
  - **Connection pool races fixed**: releases are idempotent per-lease (a double `close()` can no longer close a shared connection under another live client), the last release removes the pool entry _before_ closing (an acquire racing it gets a fresh connection instead of a dead one), and a rejecting close no longer poisons the pool key. `AmqpClient.close()` is memoized (idempotent).
  - **Sync-throw escape hatches closed on the client**: a synchronously-throwing publish/call interceptor now resolves to a Defect instead of escaping `publish()`/`call()` as a raw throw, and all telemetry helpers swallow provider/span throws (a buggy TelemetryProvider degrades to "no telemetry" instead of poisoning the data path or converting a successful publish into a Defect). `create()` routes synchronous constructor throws to the defect channel.
  - **`handlers: { name: undefined }` now fails fast** at `create()` / `declareHandlers` with a clear error instead of defecting on every delivery.

## 3.0.0-beta.3

## 3.0.0-beta.2

## 3.0.0-beta.1

## 3.0.0-beta.0

### Major Changes

- 9222c06: Adopt unthrown v5 (beta): error combinators and `match`'s `err` handler now take a ts-pattern matcher callback; peer bumped to `^5.0.0-beta.3`.

## 2.4.0

### Minor Changes

- 7dda7f4: Define-time structural validation, shared issue formatters, and the `_internal_` prefix convention (org DNA alignment, #552):

  - **Builders now fail at definition time on structural mistakes** the type system can't catch for JavaScript callers: unknown option keys (`durabel: false` used to be silently ignored) on `defineExchange`/`defineQueue` (incl. `retry`/`deadLetter` bags)/`defineMessage`/`defineRpc`, empty names, unknown exchange types, and non-Standard-Schema payload/headers/error-data schemas (duck-checked via `~standard.validate`).
  - **`formatIssue` / `summarizeIssues`** exported from `@amqp-contract/contract` — the single source of truth for rendering Standard Schema issues. `MessageValidationError.message` now includes the summarized issues (`Message validation failed for "x": field: message (+2 more)`).
  - **`_internal_` prefix** for cross-package internals with no semver guarantee: `_internal_getConnectionCount`, `_internal_resetConnections`, `_internal_resetTelemetryCache` (the `*ForTesting` names remain as deprecated aliases).

- 8b69031: Add typed RPC error maps: declare per-RPC business errors in the contract and get them typed end-to-end.

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

## 2.3.0

## 2.2.0

### Minor Changes

- bfc138c: Upgrade [`unthrown`](https://github.com/btravstack/unthrown) (and `@unthrown/vitest`) to `3.0.0`.

  amqp-contract's own public surface is unchanged — the `Result` / `AsyncResult` you receive from the client and worker keep the same shape and methods, and no source changes were needed.

  unthrown 3.0 does change how a **defect** is produced inside a `qualify` mapper, which matters only if you author handlers that intentionally route an unexpected failure to the `Defect` channel:

  - The standalone `Defect` constructor is no longer exported.
  - `qualify` now receives a second argument — a `defect` callback — so its signature is `(cause, defect) => E | defect(cause)`.

  ```diff
  - import { fromPromise, Defect } from "unthrown";
  - fromPromise(work(), (cause) => isExpected(cause) ? new MyError(cause) : Defect(cause));
  + import { fromPromise } from "unthrown";
  + fromPromise(work(), (cause, defect) => isExpected(cause) ? new MyError(cause) : defect(cause));
  ```

  Mappers that only return a modeled error (the common case — e.g. `(cause) => new RetryableError("…", cause)`) are unaffected.

## 2.1.0

### Minor Changes

- a8628d5: Upgrade [`unthrown`](https://github.com/btravstack/unthrown) (and `@unthrown/vitest`) to `2.0.0`.

  unthrown 2.0 is **additive** relative to 1.x — it adds an `AsyncResult` value namespace (`AsyncResult.fromPromise` / `.all` / …), a `flatTapErr` combinator, and an `isResult` guard. None of amqp-contract's public API changed: the `Result` / `AsyncResult` types you receive from the client and worker keep the same shape (2.0's types are a superset of 1.x's), and `Ok` / `Err` / `fromPromise` / `matchTags` / `TaggedError` / `.isOk()` / `.match()` are unchanged.

  No code changes are required to adopt this. If you import `unthrown` directly alongside amqp-contract, bump your own dependency to `^2.0.0` so a single copy is shared.

## 2.0.0

### Major Changes

- 8707df1: **BREAKING:** Upgrade [`unthrown`](https://github.com/btravstack/unthrown) to `1.0.0`.

  unthrown 1.0 renames the value constructors — **`ok` → `Ok`, `err` → `Err`, `defect` → `Defect`** (the lowercase forms are removed). All packages now depend on `unthrown@1.0.0`, so consumers that build `Result` / `AsyncResult` values directly must update their call sites:

  ```diff
  - import { ok, err } from "unthrown";
  - return ok(undefined).toAsync();
  - return err(new RetryableError("...")).toAsync();
  + import { Ok, Err } from "unthrown";
  + return Ok(undefined).toAsync();
  + return Err(new RetryableError("...")).toAsync();
  ```

  Everything else is unchanged: the `.match({ ok, err, defect })` handler keys stay lowercase (they're case branches, not constructors), and `fromPromise` / `fromSafePromise` / `fromThrowable` / `all` / `allAsync` / `TaggedError(tag, { name })` / `.isOk()` / `.toAsync()` / `.unwrap()` keep the same signatures.

## 1.0.0

### Major Changes

- d5fec7e: **BREAKING:** Replace the `neverthrow` dependency with [`unthrown`](https://github.com/btravstack/unthrown) for value-based error handling across all packages.

  `unthrown` keeps the errors-as-values model but adds a third **`Defect`** channel for unexpected failures, and renames/​reshapes several APIs. If you consume the `Result` / `AsyncResult` values returned by the client, worker, and core, you will need to update your call sites.

  ### What changed
  - **`ResultAsync` → `AsyncResult`.** All async-returning methods (`publish`, `call`, `create`, `close`, handler return types, …) now return `unthrown`'s `AsyncResult<T, E>`.
  - **`.match()` is now boxed and has three channels.** `result.match(okFn, errFn)` → `result.match({ ok, err, defect })`. The extra `defect` branch handles unexpected throws.
  - **`.andThen` → `.flatMap`**, **`.andTee` → `.tap`**, **`.orTee` → `.tapErr`** (`.map`, `.mapErr`, `.orElse` are unchanged).
  - **No `okAsync` / `errAsync`.** Build async results with `ok(value).toAsync()` / `err(error).toAsync()`.
  - **No static `ResultAsync.fromPromise` / `Result.fromThrowable`.** Use the free functions `fromPromise(promise, qualify)`, `fromSafePromise(promise)`, and `fromThrowable(fn, qualify)`. The `qualify` mapper returns `E | Defect`.
  - **`._unsafeUnwrap()` → `.unwrap()`**, **`._unsafeUnwrapErr()` → `.unwrapErr()`** (these now throw `UnwrapError` on the wrong variant, and re-throw the original cause on a `Defect`).
  - **`.isOk()` / `.isErr()` / `.isDefect()` narrow** like neverthrow's did (they guard `this`); standalone `isOk(result)` / `isErr(result)` / `isDefect(result)` functions are also available.
  - **Error classes are now `TaggedError`s.** `TechnicalError`, `MessageValidationError`, `RetryableError`, `NonRetryableError`, `RpcTimeoutError`, and `RpcCancelledError` each carry a `_tag` for exhaustive dispatch via `matchTags`. The tags are **namespaced** — `"@amqp-contract/TechnicalError"`, `"@amqp-contract/RetryableError"`, etc. — so they don't collide with other libraries' tags in a shared `matchTags`. The human-facing `Error.name` is kept bare (`"TechnicalError"`, `"RetryableError"`, …), so stack traces and `.name` checks are unaffected. Their positional constructors are unchanged.
  - **`HandlerError` is now a tagged-union type, not an abstract class.** It is `RetryableError | NonRetryableError`. Replace `error instanceof HandlerError` with `isHandlerError(error)`.

### Patch Changes

- 4ed4abe: Update repository, homepage, and bug-tracker URLs after the project moved from the `btravers` GitHub profile to the `btravstack` organization. Documentation now lives at https://btravstack.github.io/amqp-contract/.

## 0.25.0

### Minor Changes

- abc6de3: Correctness fixes from a project audit. All six packages bump together
  (`fixed` group) — most changes land in `core` and `worker`, but the version
  move covers the whole release.

  **Worker — retry message safety (`packages/worker/src/retry.ts`)**

  - `publishForRetry` now publishes the retry copy _first_ and only acks the
    original delivery if the publish is confirmed. Previously the original was
    ack'd before the publish was attempted: a publish failure (channel buffer
    full, channel error, etc.) would lose the message — the broker had already
    discarded the delivery and no retry copy was ever sent. On a publish
    failure the original is now left un-ack'd so amqp-connection-manager (or
    the broker on channel close) can redeliver it.

  **Worker — jitter range (`packages/worker/src/retry.ts`)**

  - The TTL-backoff retry jitter formula is now `delay * (0.5 + Math.random())`
    giving a symmetric `[0.5x, 1.5x]` range with mean 1.0x. The previous
    formula `0.5 + Math.random() * 0.5` produced `[0.5x, 1.0x]` (mean 0.75x)
    and never overshot — a one-sided bias, not real jitter. The clamp against
    `maxDelayMs` now runs _after_ jitter so the upper jitter bound cannot push
    the calculated delay past the configured maximum. **User-visible change**:
    the average retry delay under jitter increases by ~33% (0.75x → 1.0x of
    the configured base) and individual delays may now exceed the
    pre-clamp base by up to 50%.

  **Worker — double-ack guard (`packages/worker/src/worker.ts`)**

  - The defensive `nack(requeue=false)` in the consume callback's catch-all is
    now skipped if the message has already been ack'd or nack'd by the
    dispatch path. Previously a throw from anywhere _after_ the success-path
    `ack` (most notably the telemetry tail) would land in the catch-all and
    nack the same delivery tag — RabbitMQ then closed the channel with
    `406 PRECONDITION_FAILED`. Telemetry calls in the dispatch tail are also
    now wrapped in a try/catch so an instrumentation bug cannot crash the
    consume loop.

  **Core — `PublishOptions.timeout` removed (`packages/core/src/amqp-client.ts`)**

  - **Breaking-shaped change** (shipped as minor under 0.x): the `timeout`
    field on `PublishOptions` has been removed. It was a stale type-level
    declaration that suggested a publish-level timeout this library does not
    meaningfully provide. Code passing `timeout` will now fail to typecheck;
    remove the option (or move to `amqp-connection-manager`'s channel-level
    `publishTimeout` if you actually need it).

  **Core — `ConsumerOptions.prefetch` now wired up (`packages/core/src/amqp-client.ts`)**

  - `AmqpClient.consume(...)` now applies `options.prefetch` via
    `channel.prefetch(count, false)` registered on the channel wrapper _before_
    the consume call (so the value is in effect when the consumer starts and
    is reapplied on channel reconnect). The value is also stripped from the
    options handed to `channelWrapper.consume(...)` since `prefetch` is not a
    valid `amqplib` `Options.Consume` field. The `prefetch` option advertised
    on the worker's per-handler tuple form is now actually applied.

  **Core — connection key URL ordering (`packages/core/src/connection-manager.ts`)**

  - Added an inline comment confirming that URL list order is intentionally
    part of the pooled-connection key. `['a','b']` and `['b','a']` continue to
    get different pooled connections because the URL list is a failover list
    with the first entry as the preferred broker — sorting would silently
    merge those into one connection and pin one caller's preference onto the
    other. No behaviour change.

- bf08a27: Close public-API gaps surfaced by the audit:
  - `defineHandler` / `defineHandlers` now accept RPC names in addition to consumer names. The handler type for `defineHandler` is inferred from the contract — consumer names yield `WorkerInferConsumerHandler`, RPC names yield `WorkerInferRpcHandler`. `defineHandlers` is typed against `WorkerInferHandlers<TContract>`, which already spans `consumers ∪ rpcs`. Runtime validation walks both sets and the error message lists both.
  - The RPC-side `Infer*` helpers and the unified handlers type are now re-exported from `@amqp-contract/worker`: `WorkerInferHandlers`, `WorkerInferRpcHandler`, `WorkerInferRpcHandlerEntry`, `WorkerInferRpcConsumedMessage`, `WorkerInferRpcRequest`, `WorkerInferRpcResponse`, `WorkerInferRpcHeaders`. This makes the worker package symmetrical with the client package's RPC-side exports.
  - `HandlerError` is now an abstract base class (`error instanceof HandlerError` works). `RetryableError` and `NonRetryableError` extend it, and the `name` property still discriminates so exhaustive narrowing in user code keeps working. Public type signature is unchanged (a class can be used as a type).
  - **Removed** `WorkerInferConsumerHandlers` (was `@deprecated` for one cycle). Use `WorkerInferHandlers` instead — same shape, accurate name.

## 0.24.0

### Minor Changes

- 91a9d47: Replace `@swan-io/boxed` with `neverthrow` for the public Result/async API.

  **Breaking.** Public method signatures change from `Future<Result<T, E>>` to `ResultAsync<T, E>`. Handlers must now return `ResultAsync<void, HandlerError>` (regular consumers) or `ResultAsync<TResponse, HandlerError>` (RPCs).

  Migration cheat-sheet:

  | Before (`@swan-io/boxed`)             | After (`neverthrow`)                  |
  | ------------------------------------- | ------------------------------------- |
  | `Future.value(Result.Ok(x))`          | `okAsync(x)`                          |
  | `Future.value(Result.Error(e))`       | `errAsync(e)`                         |
  | `Result.Ok(x)` / `Result.Error(e)`    | `ok(x)` / `err(e)`                    |
  | `Future.fromPromise(p).mapError(fn)`  | `ResultAsync.fromPromise(p, fn)`      |
  | `f.mapOk(fn)` / `f.mapError(fn)`      | `f.map(fn)` / `f.mapErr(fn)`          |
  | `f.flatMapOk(fn)` / `f.flatMapError`  | `f.andThen(fn)` / `f.orElse(fn)`      |
  | `f.tapOk(fn)` / `f.tapError(fn)`      | `f.andTee(fn)` / `f.orTee(fn)`        |
  | `r.match({ Ok, Error })`              | `r.match(okFn, errFn)` (positional)   |
  | `Future.all([...])` of Result-Futures | `ResultAsync.combine([...])`          |
  | `await x.resultToPromise()` (unwrap)  | `(await x)._unsafeUnwrap()`           |
  | `await x.toPromise()` (Result wrap)   | `await x` (`ResultAsync` is thenable) |
  | `result.isError()`                    | `result.isErr()`                      |

  `Future` semantics that are not preserved: laziness and cancellation. `ResultAsync` is eager and Promise-backed. None of the library internals depended on Future-side cancel.

## 0.23.1

### Patch Changes

- 1cf3b2d: Pin `amqplib` back to the `0.10.x` line (was `1.0.3`). The `1.x` series ships breaking API changes that the worker and client paths haven't been validated against; staying on `0.10.9` keeps runtime behaviour aligned with what's covered by the integration tests and what `amqp-connection-manager@5` expects.

  Workspace housekeeping with no user-visible impact: top-level `pnpm` settings in `pnpm-workspace.yaml` are now under the correct keys (the previous `settings:` nested block was silently ignored by pnpm 9+), and a `peerDependencyRules.ignoreMissing` entry is added for `search-insights` — VitePress bundles `@docsearch/react` even when the docs site uses `provider: "local"`, and the missing peer was tripping `strictPeerDependencies` once the settings actually took effect.

## 0.23.0

### Minor Changes

- 91959fb: Harden the worker dispatch loop, surface AMQP topology details in the AsyncAPI generator, and tighten a few public defaults.

  **Worker**

  - The consume callback is now wrapped in a defensive try/catch — a handler that throws synchronously (or an unexpected fault inside the dispatch chain) no longer leaves messages neither acked nor nacked. The message is logged and nacked with `requeue=false` so a configured DLX still receives it.
  - Schema validation and parse errors take an explicit DLQ path and never enter the queue's retry pipeline. Retrying a malformed payload cannot succeed, so the previous behaviour wasted retry budget on guaranteed failures.
  - RPC reply-side failures (missing `replyTo`, missing `correlationId`, response schema failure, reply publish failure) now return `NonRetryableError` instead of being swallowed or surfacing as `RetryableError`. The original message lands in the DLQ for inspection rather than being silently retried against a caller that has already gone away.
  - The retry re-publish path respects `properties.contentType`: only round-trip JSON payloads, pass binary content through unchanged.

  **Contract**

  - `defineContract` now throws when two publishers/consumers reference the same exchange or queue _name_ with conflicting definitions (e.g. different `type`, `durable`, or `retry` settings). Identical re-declarations continue to deduplicate silently — the common pattern of one exchange flowing into the contract through both a publisher and a consumer is unaffected.

  **Core**

  - `AmqpClient.connectTimeoutMs` defaults to 30 s (`DEFAULT_CONNECT_TIMEOUT_MS`). Pass `null` to opt back into the legacy "wait forever" behaviour. Avoids hangs on misconfigured URLs or down brokers.
  - `ConnectionManagerSingleton` is no longer part of the public API. Use the underscore-prefixed `_resetConnectionsForTesting` and `_getConnectionCountForTesting` helpers instead.
  - New `recordLateRpcReply` telemetry helper and `amqp.client.rpc.late_reply` counter. The client uses it whenever a reply arrives without a matching pending call (caller already timed out / cancelled / unknown correlationId), and elevates the corresponding log from `debug` to `warn`.
  - The OpenTelemetry instrumentation scope version is now sourced from `package.json` instead of a hardcoded constant.

  **AsyncAPI**

  - Queue channels surface dead-lettering via `x-dead-letter-exchange` / `x-dead-letter-routing-key` in the AMQP binding's `arguments`, the queue description summarises DLX + retry mode, and an `x-amqp-retry` extension carries the structured retry config.
  - Exchange channels surface bridge / e2e bindings via the description (`forwards to '…'`, `receives from '…'`) and an `x-amqp-exchange-bindings` extension so cross-domain topology is visible in the generated spec.
  - New `failOnMissingConverter` generator option throws when a payload schema cannot be converted instead of falling back to a generic `{ type: "object" }` placeholder. Recommended for CI pipelines.

  **Docs**

  - New guide pages: `error-model`, `retry-strategies`, `bridge-exchanges`. New example: `command-pattern`. `worker-usage` now leads with `defineHandler`. `CONTRIBUTING` documents the changesets-driven release workflow.

## 0.22.0

### Minor Changes

- 203ad3a: Add RPC pattern: typed request/response over RabbitMQ via a single
  `defineRpc` builder and a dedicated `rpcs` slot on the contract.

  ```typescript
  import { defineContract, defineMessage, defineQueue, defineRpc } from "@amqp-contract/contract";
  import { z } from "zod";

  const calculate = defineRpc(defineQueue("rpc.calculate"), {
    request: defineMessage(z.object({ a: z.number(), b: z.number() })),
    response: defineMessage(z.object({ sum: z.number() })),
  });

  const contract = defineContract({
    rpcs: { calculate },
  });
  ```

  The worker handler returns the response payload (validated against the
  response schema before being published back to the caller's `replyTo`):

  ```typescript
  TypedAmqpWorker.create({
    contract,
    handlers: {
      calculate: ({ payload }) => Future.value(Result.Ok({ sum: payload.a + payload.b })),
    },
    urls: ["amqp://localhost"],
  });
  ```

  The client calls with a required timeout and receives a typed `Result`:

  ```typescript
  const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 }).toPromise();
  // Result<{ sum: number }, TechnicalError | MessageValidationError | RpcTimeoutError | RpcCancelledError>
  ```

  **Design notes:**

  - RPC is bidirectional on both ends (server consumes requests + publishes
    responses; client publishes requests + consumes responses), so it has
    its own `rpcs` slot rather than being shoehorned into `publishers` or
    `consumers`.
  - A single `defineRpc(queue, { request, response })` produces one
    definition shared by both ends — no client/server split, no risk of
    schema drift.
  - Worker handler keys live in the same object as `consumers` handlers;
    RPC handlers return the typed response payload, regular consumers
    return `void`.
  - Uses RabbitMQ direct reply-to (`amq.rabbitmq.reply-to`) — no reply
    queue declaration needed.
  - A single reply consumer demultiplexes responses by `correlationId`;
    the client manages an in-memory pending-call map.
  - Closing the client rejects every in-flight call with `RpcCancelledError`.
  - Response-schema validation failures on the server map to
    `NonRetryableError` (handler bug → DLQ).
  - AsyncAPI generation does not yet emit dedicated requestReply pairs for
    RPCs — tracked as a follow-up.

## 0.21.0

### Minor Changes

- Retry system and configuration normalization

  ### **Changes Overview**

  #### 🔄 **Retry System Overhaul**
  1. **None retry**
  - Introduced new `none` retry option to represent the "no retry" mode
  - Changed queue builder default from `ttl-backoff` retry to `none` retry
  - Removed implicit TTL-backoff infrastructure creation when no retry config specified
  - Worker error handler now detects `none` retry mode and rejects failed messages without retry
  - Aligns with "explicit over implicit" configuration philosophy
  2. **Immediate-requeue retry**
  - Migrated from `quorum-native` (quorum-only) to `immediate-requeue` (universal)
  - Now works with both quorum and classic queues
  - Improved handling: quorum uses native `x-delivery-count`, classic uses custom headers
  - Simplified API: `maxRetries` parameter replaces `deliveryLimit`
  3. **TTL-backoff via headers exchanges**
  - Replaced DLX routing with headers exchange infrastructure
  - Preserves original routing keys through retry flow
  - Eliminates dangerous infinite retry loop behavior
  - Configurable infrastructure names (`waitQueueName`, `waitExchangeName`, `retryExchangeName`)

  #### ⚙️ **Configuration Normalization**
  4. **Exchange configuration normalization**
  - Exchange `type` defaults to `topic` (most used)
  - `durable` defaults to `true` (production-friendly)
  - Added support for headers exchange types
  - Reduced verbosity while supporting all exchange types
  5. **Queue configuration normalization**
  - Queue `type` defaults to `quorum` (modern choice)
  - `durable` defaults to `true` (production-friendly)
  - `autoDelete` mode restricted to classic queues only (like `exclusive` and `maxPriority`)
  - Better type safety and runtime validation of queue options
  - Removed over-specific queue definition helpers: `defineQuorumQueue()`, `defineTtlBackoffQueue()`
  - Removed `deliveryLimit` in favor of `maxRetries`
  - Retry config consolidated at queue level

  #### 🎯 **New Features**
  6. **Default publish/consumer options**
  - Added `defaultPublishOptions` to `TypedAmqpClient`
    - Set once, applies to all publishes (can be overridden per-call)
    - `persistent` defaults to `true` (production-friendly)
  - Added `defaultConsumerOptions` to `TypedAmqpWorker`
    - Set once, applies to all consumers (can be overridden per-consumer handler)
  - Removed custom prefetch implementation in favor of built-in configuration in `amqp-connection-manager`
  - Eliminates configuration repetition across codebase

  #### 🐛 **Type Safety Improvements**
  7. **Handler type safety fix**
  - Consumer handler payloads and headers now properly typed from schema output types
  - Removed unnecessary type extraction utilities

  ***

  ### **Breaking Changes**

  ⚠️ **Users upgrading will need to:**

  1. Configure TTL-backoff explicitly, since queues now default to no retry
  2. Migrate TTL-backoff queue names if using custom infrastructure naming
  3. Change `mode: "quorum-native"` to `mode: "immediate-requeue"`
  4. Replace `deliveryLimit` with `maxRetries` in retry config
  5. Replace `type` parameter from `defineExchange()` calls with `type` options property (defaults to `topic`)
  6. Replace `defineQuorumQueue()` and `defineTtlBackoffQueue()` helpers with generic `defineQueue()`

  ***

  ### **Before/After Examples**

  **Exchange Definition**

  ```typescript
  // Before
  defineExchange("orders", "topic", { durable: true });

  // After
  defineExchange("orders"); // topic + durable by default
  ```

  **Queue Definition**

  ```typescript
  // Before
  defineQueue("orders", { durable: true });

  // After
  defineQueue("orders"); // quorum + durable by default
  ```

  **Retry Configuration**

  ```typescript
  // Before: TTL-backoff created automatically
  defineQueue("orders"); // Had retry: ttl-backoff by default

  // After: No retry by default
  defineQueue("orders"); // Now has no retry by default

  // To enable TTL-backoff retry, explicitly opt-in:
  defineQueue("orders", {
    retry: { mode: "ttl-backoff", maxRetries: 3 },
  });

  // Before: "quorum-native" with deliveryLimit (for quorum queues only)
  defineQueue("orders", {
    type: "quorum",
    deliveryLimit: 3,
    retry: { mode: "quorum-native" },
  });

  // After: "immediate-requeue" with maxRetries (for any queue)
  defineQueue("orders", {
    retry: { mode: "immediate-requeue", maxRetries: 3 },
  });
  ```

  **Default Publish/Consumer Options**

  ```typescript
  // Default publish options in client
  const client = await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
    defaultPublishOptions: { priority: 5 },
  });

  // Default consumer options in worker
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers,
    urls: ["amqp://localhost"],
    defaultConsumerOptions: { prefetch: 10 },
  });
  ```

  ***

## 0.20.0

### Minor Changes

- Remove `@amqp-contract/client-nestjs` and `@amqp-contract/worker-nestjs` packages. The NestJS wrappers have been removed to simplify the repository — use the standalone `@amqp-contract/client` and `@amqp-contract/worker` packages directly instead.

## 0.19.0

## 0.18.0

### Minor Changes

- Add `bridgeExchange` support for cross-domain communication

## 0.17.0

### Minor Changes

- 22242a4: Unify dead letter configuration to use nested `deadLetter` shape

  `defineQuorumQueue()` and `defineTtlBackoffQueue()` now accept `deadLetter: { exchange, routingKey? }` (the `DeadLetterConfig` type) instead of flat `deadLetterExchange` and `deadLetterRoutingKey` properties. This aligns them with `defineQueue()`, giving all queue builders a single consistent pattern for declaring dead lettering.

  **Migration:**

  ```typescript
  // Before
  defineQuorumQueue("orders", {
    deadLetterExchange: dlx,
    deadLetterRoutingKey: "failed",
    deliveryLimit: 3,
  });

  // After
  defineQuorumQueue("orders", {
    deadLetter: { exchange: dlx, routingKey: "failed" },
    deliveryLimit: 3,
  });
  ```

## 0.16.0

### Minor Changes

- Update JSDoc examples and tests to use the simplified `defineContract()` API where only publishers and consumers are specified, with exchanges, queues, and bindings automatically extracted.

## 0.15.0

### Minor Changes

- Simplify contract definition API and preserve literal types in ContractOutput

## 0.14.0

### Minor Changes

- feat: add Event/Command Pattern API for intuitive messaging patterns

  ### New Features

  **Event Pattern** - For broadcasting events to multiple consumers:

  - `defineEventPublisher(exchange, message, options)` - Define an event publisher
  - `defineEventConsumer(eventPublisher, queue, options)` - Subscribe to an event (auto-generates binding)

  **Command Pattern** - For task queues with single consumer:

  - `defineCommandConsumer(queue, exchange, message, options)` - Define a command consumer (auto-generates binding)
  - `defineCommandPublisher(commandConsumer, options)` - Create a publisher for a command

  **Helper Functions**:

  - `extractConsumer(entry)` - Extract ConsumerDefinition from any ConsumerEntry type

  ### Breaking Changes
  - Removed `definePublisherFirst` and `defineConsumerFirst` (replaced by Event/Command patterns)

  ### Example

  ```typescript
  // Event pattern: one publisher, many consumers
  const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
    routingKey: "order.created",
  });

  const processOrder = defineEventConsumer(orderCreated, orderQueue);
  const notifyOrder = defineEventConsumer(orderCreated, notificationQueue);

  // Command pattern: many publishers, one consumer
  const shipOrder = defineCommandConsumer(shippingQueue, ordersExchange, shipMessage, {
    routingKey: "order.ship",
  });

  const sendShipOrder = defineCommandPublisher(shipOrder);

  // Use in contract - bindings are auto-generated
  const contract = defineContract({
    exchanges: { orders: ordersExchange },
    queues: { orderQueue, notificationQueue, shippingQueue },
    publishers: { orderCreated, sendShipOrder },
    consumers: { processOrder, notifyOrder, shipOrder },
  });
  ```

## 0.13.0

### Minor Changes

- ## Breaking Change: Simplified Contract API

  Removed separate `events` and `commands` sections from `defineContract`. Event and command configs now go directly in `publishers` and `consumers` sections.

  ### Before (removed)

  ```typescript
  const contract = defineContract({
    events: { orderCreated: eventConfig },
    commands: { processOrder: commandConfig },
    publishers: { ... },
    consumers: { ... },
  });
  ```

  ### After (new simplified API)

  ```typescript
  const contract = defineContract({
    publishers: {
      // EventPublisherConfig → auto-extracted to publisher
      orderCreated: defineEventPublisher(exchange, message, {
        routingKey: "order.created",
      }),
    },
    consumers: {
      // EventConsumerResult → auto-extracted to consumer + binding
      processOrder: defineEventConsumer(orderCreatedEvent, queue),
      // CommandConsumerConfig → auto-extracted to consumer + binding
      handleCommand: defineCommandConsumer(queue, exchange, message, {
        routingKey: "cmd",
      }),
    },
  });
  ```

  ### Migration
  1. Move `EventPublisherConfig` from `events` section to `publishers` section
  2. Move `CommandConsumerConfig` from `commands` section to `consumers` section
  3. Pass `defineEventConsumer()` results directly to `consumers` (no more destructuring needed)
  4. Remove manual `bindings` entries for event consumers and commands - they are now auto-generated

  ### New Features
  - `PublisherEntry` type: accepts `PublisherDefinition | EventPublisherConfig`
  - `ConsumerEntry` type: accepts `ConsumerDefinition | EventConsumerResult | CommandConsumerConfig`
  - Auto-generated bindings with `{consumerName}Binding` naming convention
  - Added `isEventConsumerResult` type guard

## 0.12.0

### Minor Changes

- ## New Features

  ### Testing Package Exports
  - Added main export entry point for `@amqp-contract/testing` - users can now `import { it, globalSetup } from '@amqp-contract/testing'`

  ### OpenTelemetry Documentation
  - Added comprehensive OpenTelemetry observability guide covering traces, metrics, and configuration

  ### CI Improvements
  - Added security audit job to CI pipeline
  - Added bundle size monitoring with GitHub Step Summary reporting

  ## Improvements

  ### Contract Package Refactoring
  - Split `builder.ts` (1,911 lines) into modular files for better maintainability:
    - `builder/exchange.ts` - defineExchange
    - `builder/queue.ts` - defineQueue, extractQueue
    - `builder/message.ts` - defineMessage
    - `builder/binding.ts` - defineQueueBinding, defineExchangeBinding
    - `builder/publisher.ts` - definePublisher
    - `builder/consumer.ts` - defineConsumer
    - `builder/contract.ts` - defineContract
    - `builder/publisher-first.ts` - definePublisherFirst
    - `builder/consumer-first.ts` - defineConsumerFirst
    - `builder/ttl-backoff.ts` - defineTtlBackoffRetryInfrastructure
    - `builder/routing-types.ts` - RoutingKey, BindingPattern types
  - All existing imports continue to work (backward compatible)

  ### Worker Package
  - Improved compression validation error messages with helpful context:
    - Shows received encoding
    - Lists supported encodings (gzip, deflate)
    - Suggests checking publisher configuration

  ## Security
  - Fixed high severity vulnerability in `preact` dependency (CVE in vitepress transitive dependency)

## 0.11.0

### Minor Changes

- feat: move retry configuration from worker to contract level

  **Breaking Change:** Retry configuration has moved from handler-level to queue-level.

  ### Before (0.10.x)

  ```typescript
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: [handler, { retry: { maxRetries: 3, initialDelayMs: 1000 } }],
    },
    urls: ["amqp://localhost"],
  });
  ```

  ### After (0.11.0)

  ```typescript
  // Configure retry at queue level in the contract
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx },
    retry: {
      mode: "ttl-backoff",
      maxRetries: 3,
      initialDelayMs: 1000,
    },
  });

  // Worker no longer specifies retry options
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: handler,
    },
    urls: ["amqp://localhost"],
  });
  ```

  ### Key Changes
  - **Retry types moved to contract package**: `RetryOptions`, `TtlBackoffRetryOptions`, `QuorumNativeRetryOptions` are now exported from `@amqp-contract/contract`
  - **Queue-level retry configuration**: Use `retry` option in `defineQueue()` instead of handler tuples
  - **Automatic TTL-backoff infrastructure**: `defineContract()` automatically generates wait queues and bindings for TTL-backoff mode
  - **`extractQueue()` helper**: Use this to access queue properties from `QueueWithTtlBackoffInfrastructure` wrapper
  - **Removed `setupWaitQueues`**: Wait queues are now created by `setupAmqpTopology` like any other queue

  ### Migration Guide
  1. Move `retry` configuration from handler options to queue definition
  2. Add `mode: "ttl-backoff"` or `mode: "quorum-native"` to your retry config
  3. Remove handler tuple syntax `[handler, { retry: ... }]` - just use `handler` directly
  4. Use `extractQueue()` when accessing queue properties if using TTL-backoff mode

## 0.10.0

### Patch Changes

- Automatically bind main queue to DLX for retry flow

  The worker now automatically creates a binding from the Dead Letter Exchange (DLX) to the main queue using the queue name as the routing key. This completes the retry flow: DLX → wait queue → DLX → main queue.

  Users no longer need to manually create a `waitBinding` in their contracts when implementing retry logic. The binding is now handled automatically by the worker setup process.

## 0.9.0

### Minor Changes

- Add OpenTelemetry instrumentation for spans and metrics

  This release adds comprehensive OpenTelemetry instrumentation support for AMQP operations:

  - **Automatic tracing**: Distributed tracing spans for publish and consume operations with semantic conventions following OpenTelemetry standards
  - **Metrics collection**: Counters and histograms for message throughput and latency monitoring
  - **Optional dependency**: OpenTelemetry is an optional peer dependency that is gracefully loaded when available
  - **Zero configuration**: Instrumentation automatically integrates with your existing OpenTelemetry setup
  - **Semantic conventions**: Follows OpenTelemetry messaging semantic conventions for AMQP/RabbitMQ

  Key features:

  - Producer and consumer spans with proper span kinds
  - Message metadata tracking (message ID, routing key, delivery tag, payload size)
  - Error tracking with error types and attributes
  - Performance metrics for publish and consume operations
  - Compatible with any OpenTelemetry-compliant APM solution

  See the documentation for configuration details and usage examples.

## 0.8.0

## 0.7.0

### Minor Changes

- Release version 0.7.0 with runtime message compression support for AMQP payloads.

  This release adds the ability to compress messages at runtime using gzip or deflate algorithms. Key features include:

  - Added `CompressionAlgorithm` type supporting 'gzip' and 'deflate'
  - Added optional `compression` parameter to the `publish()` method for runtime compression
  - Automatic decompression in workers based on content-encoding header
  - Backward compatible - no compression by default
  - New sample demonstrating compression usage

  See PR #225 for complete details.

## 0.6.0

### Minor Changes

- Restructure repository to follow vitest pattern with docs as workspace package

  This release includes a major refactoring of the repository structure:

  - Move documentation to workspace package for better integration
  - Simplify docs build workflow
  - Remove orchestration scripts in favor of turbo
  - Improve overall project organization following vitest pattern

## 0.5.0

### Minor Changes

- Add routing key parameters with type validation for all exchange types

  This release introduces comprehensive routing key parameter support with compile-time type validation:

  **New Features:**

  - Added routing key parameter support for topic and direct exchanges
  - Implemented type-level validation for routing keys and binding patterns
    - `RoutingKey<T>` type validates routing key format and character set
    - `BindingPattern<T>` type validates AMQP pattern syntax (\*, #)
    - `MatchingRoutingKey<Pattern, Key>` validates key matches pattern
  - Enhanced `definePublisherFirst` and `defineConsumerFirst` functions:
    - `createPublisher()` accepts routing key parameter for topic exchanges
    - `createConsumer()` accepts optional routing key pattern
  - Routing key validation ensures AMQP compliance at compile-time:
    - Validates allowed characters (a-z, A-Z, 0-9, -, \_)
    - Validates proper segment formatting with dot separators
    - Implements AMQP topic exchange pattern matching logic

  **Type Safety Improvements:**

  - When consumer uses pattern with wildcards (e.g., "order.\*"), publishers can use any matching string
  - When consumer uses concrete key, publishers must use exact same key
  - When publisher uses concrete key, consumers can use any pattern
  - Pattern matching logic:
    - `*` matches exactly one word
    - `#` matches zero or more words

  **Usage Example:**

  ```typescript
  // Topic exchange with routing key parameters
  const consumer = defineConsumerFirst(
    topicExchange,
    "order.*", // Pattern with wildcard
    orderSchema,
  );

  // Publishers can specify concrete keys matching the pattern
  const publisher = consumer.createPublisher("order.created");

  // Or define publisher first with concrete key
  const publisher2 = definePublisherFirst(
    topicExchange,
    "order.updated", // Concrete routing key
    orderSchema,
  );

  // Consumers can subscribe with any pattern
  const consumer2 = publisher2.createConsumer("order.*");
  ```

  This feature provides end-to-end type safety for routing keys and binding patterns, catching configuration errors at compile time rather than runtime.

## 0.4.0

### Minor Changes

- Release version 0.4.0

  This release includes stability improvements and prepares the packages for wider adoption.

## 0.3.5

## 0.3.4

### Patch Changes

- Add generic type parameters to NestJS module forRoot/forRootAsync methods

  This change replaces ConfigurableModuleBuilder with manual forRoot/forRootAsync implementations that support generic type parameters. This enables full type safety for worker handlers and client publishers based on the specific contract type.

  **BREAKING CHANGE**: MODULE_OPTIONS_TOKEN is now a Symbol instead of string|symbol union

## 0.3.3

## 0.3.2

### Patch Changes

- Add optional Logger interface for message publishing and consumption

  This release introduces an optional Logger interface that allows users to integrate their preferred logging framework with amqp-contract:

  **New Features:**

  - Added `Logger` interface in `@amqp-contract/core` with debug, info, warn, and error methods
  - Added `LoggerContext` type for structured logging context
  - Client and Worker now accept an optional `logger` option to enable message logging
  - NestJS modules support logger injection

  **Usage:**

  ```typescript
  // Simple console logger implementation
  const logger: Logger = {
    debug: (message, context) => console.debug(message, context),
    info: (message, context) => console.info(message, context),
    warn: (message, context) => console.warn(message, context),
    error: (message, context) => console.error(message, context),
  };

  // Use with client
  const client = await TypedAmqpClient.create({
    contract,
    urls,
    logger,
  });

  // Use with worker
  const worker = await TypedAmqpWorker.create({
    contract,
    urls,
    logger,
  });
  ```

## 0.3.1

## 0.3.0

### Minor Changes

- Add waitForConnectionReady feature

  This release introduces connection readiness handling with the following changes:

  **Breaking Changes:**

  - `TypedAmqpClient.create()` now returns `Future<Result<TypedAmqpClient, TechnicalError>>` instead of directly returning the client instance
  - `TypedAmqpWorker.create()` now returns `Future<Result<TypedAmqpWorker, TechnicalError>>` instead of directly returning the worker instance

  **New Features:**

  - Added `waitForConnectionReady()` method to ensure AMQP connection is established before operations
  - Improved error handling with explicit Result types for connection failures

  **Migration Guide:**
  Update your client/worker creation code to handle the new async Result type:

  Before:

  ```typescript
  const client = TypedAmqpClient.create({ contract, urls });
  ```

  After:

  ```typescript
  const result = await TypedAmqpClient.create({ contract, urls });
  if (result.isError()) {
    // Handle connection error
    console.error("Failed to create client:", result.getError());
    return;
  }
  const client = result.get();
  ```

## 0.2.1

### Patch Changes

- Documentation improvements including TypeDoc-generated API documentation and standardized package READMEs with badges and documentation links.

## 0.2.0

### Minor Changes

- Extract AMQP setup logic into core package

  This release introduces a new `@amqp-contract/core` package that centralizes AMQP infrastructure setup logic. The core package provides a `setupInfra` function that handles the creation of exchanges, queues, and bindings, eliminating code duplication across client and worker packages.

  **New Features:**

  - New `@amqp-contract/core` package with centralized AMQP setup logic
  - `setupInfra` function for creating exchanges, queues, and bindings from contract definitions

  **Changes:**

  - Updated `@amqp-contract/client` to use core setup function
  - Updated `@amqp-contract/worker` to use core setup function
  - All packages are now versioned together as a fixed group

  **Migration:**
  No breaking changes. Existing code will continue to work as before. The core package is used internally by client and worker packages.

## 0.1.4

## 0.1.3

### Patch Changes

- Add exchange-to-exchange binding support

## 0.1.2

### Patch Changes

- Fix: configurable module type

## 0.1.1

### Patch Changes

- 498358d: Patch version bump for all packages

## 0.1.0

## 0.0.6

### Patch Changes

- Release version 0.0.6 for all packages

## 0.0.5

### Patch Changes

- Refactor to use factory pattern with static create() methods. Remove unnecessary type casts and improve internal implementation.

## 0.0.4

### Patch Changes

- Release version 0.0.4

## 0.0.3

### Patch Changes

- Documentation updates and API improvements for 0.0.4 release

## 0.0.2

### Patch Changes

- Release version 0.0.2
