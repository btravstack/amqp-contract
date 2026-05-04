# Recipes

End-to-end how-tos for the changes that come up most. Each recipe lists the exact files to touch.

## Add a new event consumer

1. **Schema** — define (or reuse) a `defineMessage(...)` for the payload, in the contract that owns the publisher.
2. **Queue** — `defineQueue(...)` with a `deadLetter` and a `retry` mode (immediate-requeue or ttl-backoff). Quorum by default; classic only if you need priority/exclusive/auto-delete.
3. **Consumer entry** — `defineEventConsumer(eventPublisher, queue, { routingKey: ... })`. The queue↔exchange binding is auto-generated.
4. **Add to `defineContract`** under `consumers: { ... }`. Don't add the queue or binding yourself — they're auto-extracted.
5. **Handler** — implement with `defineHandler(contract, "yourConsumerName", ({ payload, headers }) => …)` returning `ResultAsync<void, HandlerError>`. See [handlers.md](./handlers.md).
6. **Tests** — integration test in `src/__tests__/<consumer>.spec.ts` using `it` from `@amqp-contract/testing/extension`. Mock the handler with `vi.fn().mockReturnValue(okAsync(undefined))`.
7. **Changeset** — `pnpm changeset` with a minor bump. Public API surface grew.

## Add a new RPC

1. **Schemas** — request and response, both via `defineMessage`.
2. **Queue** — `defineQueue(...)` for the RPC. Almost always quorum, no DLQ for the RPC queue itself (the reply path handles the failure modes; see [handlers.md → RPC error semantics](./handlers.md#rpc-handler)).
3. **RPC entry** — `defineRpc(queue, { request, response })`.
4. **Add to `defineContract`** under `rpcs: { ... }`.
5. **Server-side handler** — `defineHandler(contract, "yourRpcName", ({ payload }) => okAsync({ /* response */ }))`. The worker validates the response and replies automatically.
6. **Client call** — `client.call("yourRpcName", request, { timeoutMs: 5_000 })`. `timeoutMs` is required.
7. **Tests** — round-trip integration test (worker + client both wired up). For "no server" scenarios, just create the client without a worker; for "request validation fails", pass a deliberately wrong payload through `as unknown as ...`.
8. **Changeset** — minor bump.

## Add a new publisher (event or command)

1. **Event publisher**: `defineEventPublisher(exchange, message, { routingKey })`. One publisher, many consumers.
2. **Command publisher**: derived from `defineCommandConsumer(...)` via `defineCommandPublisher(consumer)`. Many publishers, one consumer.
3. **Add to `defineContract`** under `publishers: { ... }`.
4. **Use** `client.publish("yourPublisherName", payload, options?)`. Returns `ResultAsync<void, TechnicalError | MessageValidationError>`.
5. **Changeset** — minor bump if it's part of the public contract surface.

## Add a new publishable package

If you're spinning up a new `@amqp-contract/*` package:

1. Create `packages/<name>/` with `package.json`, `tsconfig.json`, `tsdown.config.ts`, `vitest.config.ts`, `src/index.ts`.
2. Mirror the metadata fields from `packages/contract/package.json`: `homepage`, `bugs`, `license`, `author`, `repository` (with the correct `directory`), `files`, `type: "module"`, `exports`, `main`/`module`/`types`. **All six fields are required** — npm Trusted Publishing rejects on missing/empty `repository.url` (we hit this in the migration).
3. In `tsdown.config.ts`, mark `neverthrow` external if the package re-exports `ResultAsync` types. Add any other dep whose types surface publicly.
4. Add the package to the `fixed` group in [`.changeset/config.json`](../../.changeset/config.json) so it versions with the rest.
5. Configure the npmjs Trusted Publisher for the new package (npm UI → package settings → trusted publishing → point at `.github/workflows/release.yml` in `btravers/amqp-contract`). Until this is done, the publish will fail with `ENEEDAUTH`.
6. `pnpm install` to update the lockfile and turbo's package graph.
7. Initial release — add a changeset documenting the package, merge through the normal flow.

## Migrate a handler from `async` to `ResultAsync`

Old shape (now banned):

```typescript
processOrder: async ({ payload }) => {
  await processPayment(payload);
};
```

New shape:

```typescript
processOrder: ({ payload }) =>
  ResultAsync.fromPromise(
    processPayment(payload),
    (error) => new RetryableError("Payment failed", error),
  ).map(() => undefined);
```

Three things to remember:

- `fromPromise` requires the error mapper as the second arg — chaining `.mapErr` afterwards is a type error.
- For permanent failures, return `errAsync(new NonRetryableError(...))`.
- For success with no value, `okAsync(undefined)`.

See [handlers.md](./handlers.md) for the full neverthrow API and the common-mistakes list in [`AGENTS.md`](../../AGENTS.md#common-mistakes).
