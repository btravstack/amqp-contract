# amqp-contract

Type-safe contracts for AMQP/RabbitMQ messaging with automatic runtime validation.

## Rules

| Rule                                                    | Description                                                                  |
| ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| [Project Overview](.agents/rules/project-overview.md)   | Architecture, packages, monorepo structure                                   |
| [Commands](.agents/rules/commands.md)                   | Dev, quality, test, versioning commands                                      |
| [Contract Patterns](.agents/rules/contract-patterns.md) | Contract composition, event/command, retry, type inference                   |
| [Handlers](.agents/rules/handlers.md)                   | Handler signatures, ResultAsync, neverthrow API, error types, worker exports |
| [Code Style](.agents/rules/code-style.md)               | Composition pattern, anti-patterns, advisory style guidance                  |
| [Testing](.agents/rules/testing.md)                     | Testing strategy, integration tests, fixtures, assertions                    |
| [Dependencies](.agents/rules/dependencies.md)           | Key deps, catalog management, monorepo tooling                               |

## Key Constraints

This is the canonical list — sub-files refer back to it rather than restating these.

- No `any` types — use `unknown` and narrow (enforced by oxlint)
- Type aliases over interfaces — `type Foo = {}` not `interface Foo {}`
- `.js` extensions required in all imports (ESM)
- Handlers return `ResultAsync<void, HandlerError>` (neverthrow) — not `async`/`await`
- Standard Schema v1 for validation (Zod, Valibot, ArkType)
- Catalog dependencies via `pnpm-workspace.yaml` — never hardcode versions in `package.json`
- Conventional commits required (feat, fix, docs, chore, test, refactor, ci, build, perf, refactor, revert, style — Conventional Commits spec, enforced by commitlint on `commit-msg`)
- Quorum queues by default — classic queues only when you need a feature quorum doesn't support (`exclusive`, `autoDelete`, `maxPriority`)
- Composition pattern — define resources first, then reference; never inline
- Git hooks: lefthook runs `oxfmt` and `oxlint` on `pre-commit`, commitlint on `commit-msg`
