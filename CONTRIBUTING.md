# Contributing to amqp-contract

Thank you for your interest in contributing to amqp-contract!

## Development Setup

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

## Testing Strategy

This project uses a **integration-first testing approach** that prioritizes testing against real RabbitMQ instances over mocked unit tests.

### Test Types

#### Integration Tests (`*.integration.spec.ts`)

- Test against **real RabbitMQ** instances using testcontainers
- Each test runs in an isolated vhost for complete test isolation
- Located alongside source files: `src/*.integration.spec.ts`
- Run with: `pnpm test:integration`
- **Preferred** for testing AMQP behavior, message flow, and contract setup

**Example packages with integration tests:**

- `packages/core` - 16 integration tests (AmqpClient, connection sharing)
- `packages/client` - 10 integration tests (publishing, validation, topology)
- `packages/worker` - 9 integration tests (consuming, error handling, bindings)

#### Unit Tests (`*.unit.spec.ts`)

- Test pure logic without external dependencies
- No mocking of AMQP libraries
- Located alongside source files: `src/*.unit.spec.ts`
- Run with: `pnpm test`
- **Only used** for testing pure functions, utilities, and simple logic

**Example packages with unit tests:**

- `packages/core` - 4 unit tests (logger utility)

### Why Integration Tests?

✅ **More Robust**: Tests validate actual AMQP behavior, not mocked assumptions
✅ **Catch Real Issues**: Detects problems with RabbitMQ integration that unit tests miss
✅ **Less Brittle**: No complex mock setup that breaks with implementation changes
✅ **Better Confidence**: Higher assurance that code works in production

### Running Tests

```bash
# Run all unit tests (fast, no Docker needed)
pnpm test

# Run integration tests for a specific package (requires Docker)
pnpm test:integration --filter @amqp-contract/core
pnpm test:integration --filter @amqp-contract/client
pnpm test:integration --filter @amqp-contract/worker

# Run all integration tests (requires Docker)
pnpm test:integration
```

### Writing New Tests

**For new AMQP features:**

1. Write integration tests using `@amqp-contract/testing/extension`
2. Use test fixtures: `amqpConnectionUrl`, `amqpChannel`, `publishMessage`, `initConsumer`
3. Place tests next to source: `feature.integration.spec.ts`

**For pure utility functions:**

1. Write unit tests without external dependencies
2. Place tests next to source: `utility.unit.spec.ts`

**Example integration test:**

```typescript
import { it } from "@amqp-contract/testing/extension";
import { defineContract, defineExchange } from "@amqp-contract/contract";
import { AmqpClient } from "@amqp-contract/core";

describe("Feature Integration", () => {
  it("should setup exchange", async ({ amqpConnectionUrl, amqpChannel }) => {
    // GIVEN
    const contract = defineContract({
      exchanges: {
        test: defineExchange("test", { durable: false }),
      },
    });

    // WHEN
    const client = new AmqpClient(contract, { urls: [amqpConnectionUrl] });
    (await client.waitForConnect()).unwrap();

    // THEN
    await expect(amqpChannel.checkExchange("test")).resolves.toBeDefined();

    // CLEANUP
    await client.close();
  });
});
```

## Project Structure

- `packages/contract` - Contract definition builder
- `packages/client` - Type-safe AMQP client
- `packages/worker` - Type-safe AMQP worker
- `packages/asyncapi` - AsyncAPI specification generator
- `examples/` - Example implementations

## Coding Guidelines

📋 **[Read the complete coding guidelines](.github/copilot-instructions.md)**

This project uses AI-assisted code review with GitHub Copilot. Our guidelines document:

- TypeScript & type safety requirements
- AMQP/RabbitMQ patterns & best practices
- Code style & formatting rules
- Testing conventions
- Error handling patterns

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `chore:` - Maintenance tasks
- `test:` - Test changes
- `refactor:` - Code refactoring

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

Releases are driven by GitHub Actions:

- **On every push to `main`**: the [release workflow](.github/workflows/release.yml) runs `changeset version` against the accumulated changesets. It opens (or updates) a "Version Packages" PR that bumps `package.json` versions and updates each package's `CHANGELOG.md`.
- **When the Version Packages PR is merged**: the same workflow runs `changeset publish`, tagging the release and pushing the bumped packages to npm.

Manual steps to release locally (rarely needed):

```bash
pnpm changeset version  # consume changesets, bump versions, update changelogs
pnpm release            # builds and publishes via `changeset publish`
```

Both require write access to the npm org and the appropriate `RELEASE_PAT` for tagging.

### Versioning policy

- Public APIs follow SemVer.
- Breaking changes to the contract type system count as `major`. Be conservative.
- Bug fixes that change behavior in a way users could rely on (even unintentionally) deserve at least a `minor` and a changelog note explaining the change.
- Internal refactors with no surface change can ship as `patch`.

## Questions?

Feel free to open an issue for any questions or concerns.
