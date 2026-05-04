# Dependencies

## Key Dependencies

| Package                   | Version | Purpose                                |
| ------------------------- | ------- | -------------------------------------- |
| `neverthrow`              | 8.2.0   | ResultAsync/Result functional types    |
| `amqplib`                 | 0.10.9  | AMQP 0.9.1 client                      |
| `amqp-connection-manager` | 5.0.0   | Connection management                  |
| `zod`                     | 4.3.6   | Schema validation (Standard Schema v1) |
| `valibot`                 | 1.2.0   | Schema validation alternative          |
| `arktype`                 | 2.1.29  | Schema validation alternative          |
| `@standard-schema/spec`   | 1.1.0   | Universal schema interface             |
| `vitest`                  | 4.0.18  | Test framework                         |
| `testcontainers`          | 11.11.0 | Docker containers for tests            |

## Monorepo Tooling

- **Package manager**: pnpm
- **Build**: turbo + tsdown (generates CJS/ESM with TypeScript definitions)
- **Linting**: oxlint (Rust-based, enforces strict type rules)
- **Formatting**: oxfmt (Rust-based)
- **Pre-commit**: lefthook (runs format, lint). Commitlint runs via the `commit-msg` hook.
- **Commits**: conventional commits required (e.g. feat, fix, docs, chore, test, refactor; see `@commitlint/config-conventional` for the full list of types)

## Catalog-Based Dependencies

This project uses pnpm's catalog feature for dependency management. All shared dependencies are defined in `pnpm-workspace.yaml` under the `catalog` key. Reference catalog dependencies in package.json as `"package-name": "catalog:"`.

```json
// Good — in package.json
"devDependencies": {
  "vitest": "catalog:",
  "typescript": "catalog:",
  "zod": "catalog:"
}
```

```json
// Bad — hardcoded version in package.json
"devDependencies": {
  "vitest": "^4.0.0",
  "typescript": "^6.0.0"
}
```

## Workspace Protocol

Use `workspace:*` for internal package dependencies:

```json
"dependencies": {
  "@amqp-contract/contract": "workspace:*"
}
```

## Adding New Dependencies

1. Add to the catalog in `pnpm-workspace.yaml` first
2. Then reference as `catalog:` in package.json
3. Run `pnpm install` to update lock file

## General Guidelines

- Minimize external dependencies
- Use peer dependencies for amqplib and schema libraries
- Keep bundle size small
