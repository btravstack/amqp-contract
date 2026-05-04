# Project Overview

**amqp-contract** is a TypeScript monorepo providing type-safe contracts for AMQP/RabbitMQ messaging with automatic runtime validation.

## Key Technologies

- **TypeScript** — strict type safety
- **Standard Schema v1** — universal schema validation interface (Zod, Valibot, ArkType)
- **amqplib** — AMQP 0.9.1 client for Node.js
- **neverthrow** — `ResultAsync` / `Result` functional error handling
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

```
packages/<name>/
├── src/
│   ├── index.ts             # Public API surface
│   ├── <feature>.ts         # Implementation
│   ├── <feature>.spec.ts    # Unit tests (no broker required)
│   └── __tests__/
│       └── <feature>.spec.ts  # Integration tests (require RabbitMQ)
├── package.json
├── tsconfig.json
├── tsdown.config.ts         # Build config
└── vitest.config.ts         # Splits unit / integration projects
```

Unit specs live next to the source they cover. Integration specs go under `src/__tests__/` so the `vitest.config.ts` `unit` and `integration` projects can target them by glob.
