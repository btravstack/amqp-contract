# amqp-contract

Type-safe contracts for AMQP/RabbitMQ messaging with automatic runtime validation. TypeScript ESM monorepo using pnpm catalogs and Turbo.

## Rules

| Rule                                                    | Read this when…                                                 |
| ------------------------------------------------------- | --------------------------------------------------------------- |
| [Project Overview](.agents/rules/project-overview.md)   | Orienting yourself in the repo, looking up which package owns X |
| [Commands](.agents/rules/commands.md)                   | Running anything (`pnpm`, turbo filters, integration tests)     |
| [Contract Patterns](.agents/rules/contract-patterns.md) | Defining or modifying a contract (publishers/consumers/RPCs)    |
| [Handlers](.agents/rules/handlers.md)                   | Writing worker handlers, dealing with `AsyncResult` / unthrown  |
| [Runtime](.agents/rules/runtime.md)                     | Touching connections, telemetry, compression                    |
| [Code Style](.agents/rules/code-style.md)               | Reviewing or writing TS — composition, anti-patterns            |
| [Testing](.agents/rules/testing.md)                     | Adding unit or integration tests, using the testing fixtures    |
| [Build & Release](.agents/rules/build-and-release.md)   | Adding a package, modifying tsdown config, shipping a release   |
| [Dependencies](.agents/rules/dependencies.md)           | Adding a dep, picking a schema lib, catalog questions           |
| [Recipes](.agents/rules/recipes.md)                     | "How do I add a new consumer / RPC / publishable package?"      |

## Key Constraints

This is the canonical list — sub-files reference these rather than restating them.

### Language and types

- No `any` — use `unknown` and narrow (enforced by oxlint).
- Type aliases over interfaces — `type Foo = {}`, not `interface Foo {}`.
- `.js` extensions required in all imports (ESM).
- Standard Schema v1 for validation (Zod, Valibot, ArkType — anything that implements the spec).

### Handlers and error handling

- Error handling uses [`unthrown`](https://github.com/btravstack/unthrown) (not neverthrow). It adds a third **`Defect`** channel for unexpected throws alongside `Ok` / `Err`.
- Handlers return `AsyncResult<void, HandlerError>` (regular consumer) or `AsyncResult<TResponse, HandlerError>` (RPC). No `async`/`await` in handler signatures.
- `await asyncResult` resolves to a `Result<T, E>` — it does **not** throw on `Err`.
- `result.match({ ok, err, defect })` is **boxed and has three branches**. Positional `match(okFn, errFn)` is the old neverthrow shape and is not supported.
- Build async results with `ok(value).toAsync()` / `err(error).toAsync()` — there is no `okAsync` / `errAsync`.
- Wrap promises with the free function `fromPromise(promise, qualify)` (not a static `ResultAsync.fromPromise`); `qualify` maps the rejection reason to `E | defect(cause)` and is required.
- `.isOk()` / `.isErr()` / `.isDefect()` are type guards that **narrow `this`** (since unthrown 0.2.0), so `if (result.isErr()) result.error` works. **Prefer the method form** — the standalone `isOk(result)` / `isErr(result)` / `isDefect(result)` functions narrow identically but are not used in this codebase.

### Topology and contract authoring

- Quorum queues by default. Classic queues only for features quorum doesn't support (`exclusive`, `autoDelete`, `maxPriority`).
- Composition pattern — define resources first, then reference; never inline inside `defineContract`.

### Tooling and process

- Catalog dependencies via `pnpm-workspace.yaml` — never hardcode versions in `package.json`.
- Conventional commits required (feat, fix, docs, chore, test, refactor, ci, build, perf, revert, style). Enforced by commitlint on `commit-msg`.
- Git hooks: lefthook runs `oxfmt` and `oxlint` on `pre-commit`; commitlint on `commit-msg`. `pnpm typecheck` is **not** in the hook, so run it before pushing if you changed types.

## Safety / blast radius

Before doing any of the following, confirm with the user:

- Pushing to `main`, force-pushing, or rewriting history (main is protected, but local mistakes happen — see [Build & Release](.agents/rules/build-and-release.md) for the release flow).
- Running `git reset --hard`, `git clean -fdx`, or any destructive `git` operation when the working tree contains uncommitted state.
- Closing or merging a PR.
- Anything that publishes to npm or pushes to GitHub Releases (the changeset/release pipeline owns publishing — never run `pnpm release` or `npm publish` manually).
- Skipping commit hooks (`--no-verify`) or signing flags. If a hook fails, fix the underlying issue.
- Editing `.github/workflows/*.yml` — release uses Trusted Publishing OIDC; small changes can break npm auth. Read [Build & Release](.agents/rules/build-and-release.md) first.

These do **not** need confirmation:

- Local commits, branches, file edits, running `pnpm build`/`test`/`lint`.
- Pushing a feature branch to origin.
- Creating a PR (always go via PR — never push directly to `main`).

## Common mistakes

These have been re-introduced more than once across recent migrations / reviews — flag them in self-review:

- **Treating `await TypedAmqp(Client|Worker).create(...)` as a client/worker.** It returns `AsyncResult<Client, TechnicalError>`; `await` gives you a `Result`. Unwrap with `.unwrap()` (or pattern-match) before calling instance methods.
- **Wrapping `client.publish(...)` in `fromPromise(...)`.** `publish` already returns an `AsyncResult` — wrap it again and you get `AsyncResult<AsyncResult<...>>`. Chain `.map` / `.mapErr` / `.flatMap` directly.
- **Calling `fromPromise(p)` without the `qualify` mapper.** The mapper is a required second argument and must return `E | defect(cause)`. The fix to the opaque "expected 2 arguments, got 1" error is always: pass the mapper.
- **Using positional `result.match(okFn, errFn)`.** That's the old neverthrow shape. unthrown's `match` is boxed with three branches: `result.match({ ok, err, defect })`.
- **Reaching for `okAsync` / `errAsync` or `ResultAsync` / `_unsafeUnwrap`.** Those are neverthrow. Use `ok(v).toAsync()` / `err(e).toAsync()`, the `AsyncResult` type, and `.unwrap()`.
- **Adding a publishable package without `repository`, `homepage`, `bugs`, `author`, `license`** — npm will reject with a 422 on provenance validation under Trusted Publishing.
- **Hardcoding a dep version in a `package.json`.** Use `"catalog:"` and add the actual version once in `pnpm-workspace.yaml`.
- **Forgetting to add a changeset** when changing public API. The release will silently skip your change.

## Workflow rules for agents

- **Before claiming a refactor is done**, run `pnpm typecheck` (it isn't in the pre-commit hook). If you changed a public type in package A, also rebuild it (`pnpm --filter @amqp-contract/<a> build`) before typechecking package B that depends on it — workspace packages are typed against their `dist/` output.
- **Public API changes need a changeset** (`pnpm changeset`). The six publishable packages are in a `fixed` group — they all bump together; you only add one entry.
- **Don't claim integration tests "pass" if they didn't run.** They require Docker; if you can't run them locally, say so explicitly rather than implying coverage.
- **When deferring to a doc**, link to a specific `path/to/file.ts` line rather than restating it. Stale duplication is the dominant failure mode of these rule files.
