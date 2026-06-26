# Project Overview

**amqp-contract** is a TypeScript monorepo providing type-safe contracts for AMQP/RabbitMQ messaging with automatic runtime validation.

## Key Technologies

- **TypeScript** — strict type safety
- **Standard Schema v1** — universal schema validation interface (Zod, Valibot, ArkType)
- **amqplib** — AMQP 0.9.1 client for Node.js
- **unthrown** — `AsyncResult` / `Result` functional error handling
- **Vitest** — test framework
- **Turbo** — monorepo build orchestrator
- **pnpm** — package manager (catalog-based dependency management)
- **oxlint / oxfmt** — linter and formatter

Pinned versions live in [`pnpm-workspace.yaml`](../../pnpm-workspace.yaml) — that file is the source of truth, do not duplicate versions in docs.

## Packages

| Package                   | Purpose                                                      |
| ------------------------- | ------------------------------------------------------------ |
| `@amqp-contract/contract` | Contract definition builder and types (foundation)           |
| `@amqp-contract/core`     | AMQP connection management, topology setup, telemetry        |
| `@amqp-contract/client`   | Type-safe publishing and RPC via `TypedAmqpClient`           |
| `@amqp-contract/worker`   | Type-safe consumption via `TypedAmqpWorker` with retry logic |
| `@amqp-contract/asyncapi` | AsyncAPI 3.0 specification generator                         |
| `@amqp-contract/testing`  | Testcontainers setup and Vitest fixtures                     |

`@amqp-contract/asyncapi` is purely a code-generation aid: feed it a contract and it emits an AsyncAPI 3.0 document for catalogues, doc sites, or other tooling. Entry point is the `AsyncAPIGenerator` class exported from the package — instantiate, call `.generate(contract, { info, ... })`, get a JSON spec back. It has no runtime dependency on the broker.

## Monorepo Structure

```
amqp-contract/
├── docs/                  # VitePress documentation site
├── packages/
│   ├── contract/          # Contract builder (foundation)
│   ├── core/              # AMQP connection / topology / telemetry
│   ├── client/            # TypedAmqpClient (publish + RPC)
│   ├── worker/            # TypedAmqpWorker (consume + retry)
│   ├── asyncapi/          # AsyncAPI 3.0 generator
│   └── testing/           # Vitest fixtures + testcontainers setup
├── examples/              # Runnable example apps
├── tests/                 # Cross-package integration tests
└── tools/                 # Shared tsconfig / typedoc presets
```

## Package Source Layout

The shape varies by package — there's no single template. The pieces that _do_ recur:

- `src/index.ts` — public API surface (always).
- `src/<feature>.ts` — implementation modules, `.js` extensions in imports.
- `package.json` — entry points, `exports` map, build script, deps; metadata fields (`repository`, `homepage`, `bugs`, `author`, `license`) required on every publishable package, see [Build & Release](./build-and-release.md).
- `tsconfig.json` — extends from `tools/tsconfig`.

Where the packages **diverge**:

- **Build config.** `core`, `client`, `worker`, `asyncapi` use a `tsdown.config.ts` (mostly to mark `unthrown` external). `contract` and `testing` configure tsdown via CLI flags in `package.json`. `testing` is multi-entry (`index`, `global-setup`, `extension`) and ESM-only; the others are dual ESM + CJS.
- **Test layout.** `core`, `client`, `worker` have a unit / integration split: unit specs sit next to source (`feature.spec.ts`), integration specs under `src/__tests__/`, and `vitest.config.ts` runs them as two named projects. `contract` and `asyncapi` only have unit specs and keep them next to source. `testing` has no tests (it _is_ the testing package — see [Testing](./testing.md)).
- **Build/test scripts.** Confirm via `package.json` rather than assuming.

Look at `packages/contract` for the simplest case, `packages/worker` for a package with an integration suite, `packages/testing` for the multi-entry/ESM-only shape.
