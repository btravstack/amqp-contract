# Recipes

End-to-end how-tos for the changes that come up most. Each recipe lists the exact files to touch.

## Add a new event consumer

1. **Schema** — define (or reuse) a `defineMessage(...)` for the payload, in the contract that owns the publisher.
2. **Queue** — `defineQueue(...)` with a `deadLetter` and a `retry` mode (immediate-requeue or ttl-backoff). Quorum by default; classic only if you need priority/exclusive/auto-delete.
3. **Consumer entry** — `defineEventConsumer(eventPublisher, queue, { routingKey: ... })`. The queue↔exchange binding is auto-generated.
4. **Add to `defineContract`** under `consumers: { ... }`. Don't add the queue or binding yourself — they're auto-extracted.
5. **Handler** — implement with `defineHandler(contract, "yourConsumerName", ({ payload, headers }) => …)` returning `AsyncResult<void, HandlerError>`. See [handlers.md](./handlers.md).
6. **Tests** — integration test in `src/__tests__/<consumer>.spec.ts` using `it` from `@amqp-contract/testing/extension`. Mock the handler with `vi.fn().mockReturnValue(Ok(undefined).toAsync())`.
7. **Changeset** — `pnpm changeset` with a minor bump. Public API surface grew.

## Add a new RPC

1. **Schemas** — request and response, both via `defineMessage`.
2. **Queue** — `defineQueue(...)` for the RPC. Quorum by default. **Configure a `deadLetter`** even though replies, not the queue, drive most failure modes: missing `replyTo` / `correlationId` and response-schema mismatches are surfaced as `NonRetryableError` and the worker `nack`s them without requeue, so without a DLX they're dropped silently.
3. **RPC entry** — `defineRpc(queue, { request, response })`.
4. **Add to `defineContract`** under `rpcs: { ... }`.
5. **Server-side handler** — define it with `defineHandler(contract, "yourRpcName", ({ payload }) => Ok({ /* response */ }).toAsync())`, via `defineHandlers`, or inline in the `handlers` object passed to `TypedAmqpWorker.create({ handlers: { … } })`. All three are RPC-aware: `defineHandler` / `defineHandlers` are overloaded against `InferRpcNames` and validate the name against both `contract.consumers` and `contract.rpcs`. The worker validates the response against the response schema and publishes back automatically.
6. **Client call** — `client.call("yourRpcName", request, { timeoutMs: 5_000 })`. `timeoutMs` is required.
7. **Tests** — round-trip integration test (worker + client both wired up). For "no server" scenarios, just create the client without a worker; for "request validation fails", pass a deliberately wrong payload through `as unknown as ...`.
8. **Changeset** — minor bump.

## Add a new publisher (event or command)

1. **Event publisher**: `defineEventPublisher(exchange, message, { routingKey })`. One publisher, many consumers.
2. **Command publisher**: derived from `defineCommandConsumer(...)` via `defineCommandPublisher(consumer)`. Many publishers, one consumer.
3. **Add to `defineContract`** under `publishers: { ... }`.
4. **Use** `client.publish("yourPublisherName", payload, options?)`. Returns `AsyncResult<void, TechnicalError | MessageValidationError>`.
5. **Changeset** — minor bump if it's part of the public contract surface.

## Add a new publishable package

If you're spinning up a new `@amqp-contract/*` package:

1. Create `packages/<name>/` with at minimum: `package.json`, `tsconfig.json` (extends `@amqp-contract/tsconfig`), and `src/index.ts`. Add `vitest.config.ts` only if the package has tests; add `tsdown.config.ts` only if you need config beyond what the CLI flags can express (see existing packages — `contract` and `testing` skip the config file, the others use one).
2. Mirror the metadata fields from `packages/contract/package.json`: `homepage`, `bugs`, `license`, `author`, `repository` (with the correct `directory`), `files`, `type: "module"`, plus the appropriate `exports` map (single entry like `contract` or multi-entry like `testing`). **All of these are required** — npm Trusted Publishing rejects on missing or empty `repository.url` (we hit that during the migration).
3. Pick the build shape that matches your package's exports:
   - **Single-entry, dual ESM+CJS** (most packages): `tsdown src/index.ts --format cjs,esm --dts --clean`, with a `tsdown.config.ts` if you need to mark deps external (see [Build & Release](./build-and-release.md)).
   - **Multi-entry, ESM-only** (like `testing`): pass each entry as a positional and use `--format esm`.
     If `AsyncResult` (or any other dep) appears in your public types, mark that dep `external` to prevent its types being inlined.
4. Add the package to the `fixed` group in [`.changeset/config.json`](../../.changeset/config.json) so it versions with the rest.
5. Configure the npmjs Trusted Publisher for the new package (npm UI → package settings → trusted publishing → point at `.github/workflows/release.yml` in `btravstack/amqp-contract`). Until this is done, the publish will fail with `ENEEDAUTH`.
6. `pnpm install` to update the lockfile and turbo's package graph.
7. Initial release — add a changeset documenting the package, merge through the normal flow.

## Migrate a handler from `async` to `AsyncResult`

Old shape (now banned):

```typescript
processOrder: async ({ payload }) => {
  await processPayment(payload);
};
```

New shape:

```typescript
processOrder: ({ payload }) =>
  fromPromise(processPayment(payload), (error) => new RetryableError("Payment failed", error)).map(
    () => undefined,
  );
```

Three things to remember:

- `fromPromise` requires the error mapper as the second arg — chaining `.mapErr` afterwards is a type error.
- For permanent failures, return `Err(new NonRetryableError(...)).toAsync()`.
- For success with no value, `Ok(undefined).toAsync()`.

See [handlers.md](./handlers.md) for the full unthrown API and the common-mistakes list in [`AGENTS.md`](../../AGENTS.md#common-mistakes).
