# Contributing to amqp-contract

Thank you for your interest in contributing to amqp-contract!

## Development Setup

### Prerequisites

- **Node.js >= 22.22** (enforced via `engines`; `.node-version` pins the version used in CI)
- **pnpm 12.4.1** — the repo pins it via the `packageManager` field, so `corepack enable` gives you the right version automatically
- **Docker running** (not just installed) — required only for `pnpm test:integration`, which spins up RabbitMQ via testcontainers

### Setup

1. Install dependencies:

```bash
pnpm install
```

2. Build all packages:

```bash
pnpm build
```

3. Run tests:

```bash
# Run unit tests
pnpm test

# Run integration tests (requires Docker)
pnpm test:integration
```

### Development Loop

```bash
# Rebuild packages on change (tsdown --watch)
pnpm dev

# Run one package's unit tests
pnpm --filter @amqp-contract/worker test

# Run a single test file (from the package directory)
cd packages/worker && pnpm vitest run --project unit src/retry.spec.ts

# Type check everything
pnpm typecheck
```

> [!IMPORTANT]
> Two gotchas worth knowing up front:
>
> - **`pnpm typecheck` is not in the pre-commit hook.** Lefthook only runs `oxfmt` and `oxlint` on commit, so run `pnpm typecheck` (and `pnpm test`) yourself before pushing.
> - **Packages typecheck against each other's `dist/` output**, not `src/`. If you change a public type in package A, rebuild it (`pnpm --filter @amqp-contract/<a> build`) before typechecking a package that depends on it — otherwise you'll see stale, confusing type errors.

## Testing

Every workspace builds its Vitest config from [`vitest.shared.ts`](./vitest.shared.ts), which fixes the layout:

- **Unit tests** — `src/**/*.spec.ts`, next to the source they test. Run by `pnpm test`; no Docker, no broker.
- **Integration tests** — `src/__tests__/*.spec.ts`. Run by `pnpm test:integration` against a real RabbitMQ started through testcontainers, one isolated vhost per test.
- **Type tests** — `src/**/*.test-d.ts`, typechecked rather than executed, in the packages that declare them.

Integration tests are preferred for anything that touches the broker. Fixtures, conventions and examples are in [`.agents/rules/testing.md`](./.agents/rules/testing.md).

## Project Structure

- `packages/contract` — contract builder and types (the foundation)
- `packages/core` — connection management, topology setup, telemetry
- `packages/client` — `TypedAmqpClient` (publish and request/reply)
- `packages/worker` — `TypedAmqpWorker` (consume, retry, dead-lettering)
- `packages/asyncapi` — AsyncAPI 3.1 generator
- `packages/testing` — Vitest fixtures and the RabbitMQ testcontainer
- `tests/` — cross-package integration tests and documentation checks
- `examples/` — runnable example apps
- `docs/` — the VitePress documentation site

## Coding Guidelines

[`AGENTS.md`](./AGENTS.md) is the canonical list of constraints — language and type rules, the unthrown error-handling conventions, contract authoring rules and the load-bearing invariants — with topic-specific detail under [`.agents/rules/`](./.agents/rules/). It is written for humans and AI agents alike.

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `chore:` - Maintenance tasks
- `test:` - Test changes
- `refactor:` - Code refactoring

`ci`, `build`, `perf`, `revert` and `style` are accepted too. commitlint enforces the format on every commit.

## Pull Request Process

1. Create a feature branch
2. Make your changes
3. Add tests for new functionality
4. Ensure all tests pass: `pnpm test`
5. Ensure code is formatted: `pnpm format`
6. Ensure code passes linting: `pnpm lint`
7. **Add a changeset** describing your change (see below)
8. Submit a pull request

## Releasing

This project uses [Changesets](https://github.com/changesets/changesets) for version management and publishing. Every change that affects a published package needs a changeset; the release workflow turns those into version bumps, changelogs, and npm releases.

### Adding a changeset (every PR)

```bash
pnpm changeset
```

The CLI is interactive:

1. Pick the affected packages.
2. Choose a bump level (`patch` / `minor` / `major`) per package, following [Semantic Versioning](https://semver.org/).
3. Write a short, user-facing summary — this becomes the changelog entry.

The result is a new file under `.changeset/`. Commit it alongside your code changes. PRs without a changeset (when one is needed) won't be released even if merged.

If your PR doesn't change anything published — e.g. tests, docs, repo tooling — you don't need one.

### Release workflow

Releases run in CI only — never run `pnpm release` or `npm publish` yourself.

1. A PR with a changeset is merged to `main`.
2. Once CI passes on `main`, the [release workflow](.github/workflows/release.yml) calls the shared btravstack release workflow, which runs [`changesets/action`](https://github.com/changesets/action). It either opens (or updates) a `chore: release packages` PR that consumes the pending changesets — bumping versions and writing each package's `CHANGELOG.md` — or, when the versions on `main` are not yet on npm, runs the root `release` script (`pnpm build && changeset publish`).
3. Publishing uses npm Trusted Publishing (OIDC); there is no npm token to manage.

So releasing is: merge the `chore: release packages` PR.

The repository is currently in changesets **pre mode** (`.changeset/pre.json`, tag `beta`), so releases publish as `3.0.0-beta.N` under the `beta` dist-tag. See [Build & Release](./.agents/rules/build-and-release.md#prerelease-mode) for how pre mode is exited.

### Versioning policy

- Public APIs follow SemVer.
- Breaking changes to the contract type system count as `major`. Be conservative.
- Bug fixes that change behavior in a way users could rely on (even unintentionally) deserve at least a `minor` and a changelog note explaining the change.
- Internal refactors with no surface change can ship as `patch`.

## Conventions shared with sibling libraries

amqp-contract shares its foundations with [unthrown](https://btravstack.github.io/unthrown/) (errors as values) and [temporal-contract](https://btravstack.github.io/temporal-contract/) (typed contracts for Temporal). The shared conventions are deliberate and stable: Standard Schema v1 validation, `define*` for contract authoring vs `declare*` for implementations, static `Typed*.create(...)` factories returning an `AsyncResult` (an unreachable broker is a modeled `ConnectionError`; a bug during start-up is a defect), namespaced `TaggedError` tags (`@amqp-contract/X`, `@temporal-contract/X`), and [Deno-style exported signatures](https://docs.deno.com/runtime/contributing/style_guide/) (at most two positional arguments, a trailing options object, no positional booleans).

The divergences are equally deliberate — do not expect a future release to "align" them:

- **Vocabulary.** amqp-contract speaks choreography (events, commands — see the [glossary](https://btravstack.github.io/amqp-contract/reference/glossary#choreography)); temporal-contract speaks orchestration (workflows, activities). Different coordination models earn different words.
- **Retry configuration.** amqp-contract uses unit-suffixed retry-count semantics; temporal-contract exposes Temporal's native `RetryPolicy`. The mapping: `maxRetries` ≈ `maximumAttempts − 1`, `initialDelayMs` ≈ `initialInterval`, `maxDelayMs` ≈ `maximumInterval`, `backoffMultiplier` ≈ `backoffCoefficient`.
- **Validation errors.** amqp-contract has a single `MessageValidationError` (one wire, one boundary); temporal-contract has per-surface errors because Temporal has five distinct invocation surfaces.
- **Module format.** amqp-contract ships dual CJS + ESM; this is a compatibility stance, not an accident.

## Questions?

Feel free to open an issue for any questions or concerns.
