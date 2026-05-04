# Commands

## Development

```bash
pnpm install              # Install dependencies
pnpm build                # Build all packages
pnpm dev                  # Watch mode for development
```

## Code Quality

```bash
pnpm typecheck            # Type check without emitting
pnpm lint                 # Run oxlint (no any types, type aliases)
pnpm lint --fix           # Auto-fix linting issues
pnpm format               # Format with oxfmt (import sorting)
pnpm format --check       # Check formatting only
```

## Testing

```bash
pnpm test                 # Run unit tests (no Docker required)
pnpm test:integration     # Run integration tests (requires Docker)

# Run a single package's tests via pnpm's filter
pnpm --filter @amqp-contract/core test:integration
pnpm --filter @amqp-contract/client test:integration
pnpm --filter @amqp-contract/worker test:integration
```

## Versioning

```bash
pnpm changeset            # Create changeset entry for version bumps
pnpm version              # Version packages
pnpm release              # Publish packages
```

## Pre-Commit Checklist

Lefthook runs `oxfmt` and `oxlint` on every `git commit`, and commitlint on `commit-msg`, so format / lint / message-format breakage is caught automatically. Before pushing, also run:

- `pnpm typecheck` — type errors are not in the pre-commit hook
- `pnpm test` — unit tests (integration tests run in CI)
