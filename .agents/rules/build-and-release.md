# Build and Release

## Build (per package)

Builds use `tsdown`, but the wiring varies — always confirm with the package's `package.json` and (if present) `tsdown.config.ts`:

- **Entry points.** Entries are positional arguments of the `build` script. `client`, `worker` and `asyncapi` bundle `src/index.ts` only; `contract` and `core` also bundle `src/internal.ts` (the `./internal` sub-path the sibling packages import); `@amqp-contract/testing` bundles three (`index`, `global-setup`, `extension`) because it advertises sub-path exports.
- **Output formats.** `core`, `client`, `worker`, `contract`, `asyncapi` emit dual ESM + CJS with matching `.d.mts` / `.d.cts`. `@amqp-contract/testing` is ESM-only.
- **Config style.** Every package sets entries, formats, `--dts` and `--clean` as CLI flags in `package.json`'s `build` script. A `tsdown.config.ts` only adds what flags don't cover: `core`, `client`, `worker` set `external: ["unthrown"]` and `inlineOnly: false`; `asyncapi` sets only `inlineOnly: false`; `contract` and `testing` have no config file.
- **Externals.** Anything whose types appear in the public declaration files must be marked `external` so tsdown does not inline them. The packages that do this today (`core`, `client`, `worker`) externalise `unthrown` because `AsyncResult<T, E>` shows up in their public signatures — without that, unthrown's types get duplicated into our `.d.ts` files and break cross-package type compatibility. When adding a new dep that surfaces in types, add it to the `external` array (or the equivalent CLI flag).
- **`inlineOnly: false`** is set in every `tsdown.config.ts` so tsdown doesn't warn about deps bundled into the declaration files.
- Build via `pnpm build` (root; `turbo run build` over every workspace except the docs site) or `pnpm --filter <pkg> build`. The docs site (TypeDoc API pages + VitePress) builds separately with `pnpm build:docs`.

When typechecking package B that depends on workspace package A, package A must be **built** first — `tsc` resolves workspace deps against their `dist/` output, not source. After editing a public type in A, run `pnpm --filter @amqp-contract/<a> build` before `pnpm --filter @amqp-contract/<b> typecheck` or you'll see stale errors.

## Versioning and changesets

Versioning runs through [changesets](https://github.com/changesets/changesets). Configuration lives in [`.changeset/config.json`](../../.changeset/config.json).

- All six publishable packages (`asyncapi`, `client`, `contract`, `core`, `testing`, `worker`) are in a `fixed` group — they always bump together. A single changeset entry covering one of them bumps all six to the same version.
- Bumps follow SemVer: a breaking change is `major`.
- Run `pnpm changeset` to interactively add an entry; commit the resulting `.changeset/<slug>.md` alongside the code change.
- Internal-only changes (test infrastructure, examples, docs) don't need a changeset. CI does not enforce — use judgement.

The root `version` script is wired to `changeset version` (the script that consumes pending entries and updates `package.json` files). Do not call `pnpm version` expecting npm's built-in — pnpm intercepts and runs the script. We've previously had bugs where this collision left package.jsons untouched; it's now wired correctly via `pnpm run version`.

## Release flow (CI-driven)

Releases are not run from a developer's machine — never run `pnpm release` or `npm publish` locally. The flow:

1. PR with code change + changeset → reviewed → merged to `main`.
2. When CI succeeds on `main`, [`.github/workflows/release.yml`](../../.github/workflows/release.yml) (a `workflow_run` trigger, pinned to the exact commit CI measured) calls the reusable `release-reusable.yml` workflow from `btravstack/tools`, which runs `changesets/action`. It either:
   - Opens / updates a `chore: release packages` PR that runs `pnpm run version` (`changeset version`): bumps versions and writes changelogs from pending entries; or
   - If versions in `package.json` aren't yet on npm (i.e. that PR was just merged), runs `pnpm run release` — the root script `pnpm build && changeset publish` — publishing all six packages and tagging them.
3. Publishing uses **npm Trusted Publishing via OIDC** — there is no `NPM_TOKEN` secret. The release workflow has `id-token: write` and the npmjs Trusted Publisher config points at `.github/workflows/release.yml`.

### Prerelease mode

The repo is currently in changesets **pre mode**: [`.changeset/pre.json`](../../.changeset/pre.json) sets `"mode": "pre"` and `"tag": "beta"`. While it is there, every release PR produces `x.y.z-beta.N` versions published under the `beta` dist-tag (npm `latest` does not move), consumed changesets are moved into `.changeset/pre/` rather than deleted, and the docs deploy builds main's docs under `/beta/` next to the latest stable tag's docs at the root.

To ship the stable release, open a PR that runs `pnpm changeset pre exit` and commit the result. Review the entries in `.changeset/pre/` first — they become the changelog of the stable version, so drop any that only mattered between betas. The next release PR then versions to the stable number, and merging it publishes under `latest`. (`pnpm changeset pre enter <tag>` starts a new prerelease line.)

Implications when changing CI:

- The reusable CI and release workflows (in `btravstack/tools`) install Node from [`.node-version`](../../.node-version); the repo's own jobs (`package-check` in `ci.yml`, `deploy-docs.yml`) go through the `./.github/actions/setup` composite, which reads the same file. **Node 24** is required for Trusted Publishing (older npm doesn't recognise OIDC env vars), so don't pin `.node-version` or a workflow lower.
- Every publishable package must have these `package.json` fields filled with the canonical GitHub URL — `repository.url`, `homepage`, `bugs`, `author`, `license`. Provenance attestations include the GitHub repo URL, and npm rejects mismatches with a 422. `packages/testing/package.json` was the package that bit us last time.
- When adding a new publishable package: add it to the `fixed` group in `.changeset/config.json`, mirror the `package.json` metadata fields from `packages/contract/package.json`, and decide on a build shape — `tsdown.config.ts` (most packages, with `external: ["unthrown", …]` for any deps surfaced in public types) or CLI-flag tsdown like `contract` / `testing`. Multi-entry / ESM-only is fine if it matches the package's exports map (see `testing` for the canonical example).

Workflows to be careful around — see [Safety in `AGENTS.md`](../../AGENTS.md#safety--blast-radius) before editing:

- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-docs.yml`
- `.github/actions/setup/action.yml`

## What to do for common changes

| Change you're making                  | Add a changeset? | Build before typecheck of consumers? |
| ------------------------------------- | ---------------- | ------------------------------------ |
| New public export from a package      | Yes              | Yes                                  |
| Bug fix in public method              | Yes              | If types changed                     |
| Internal refactor, types unchanged    | No               | No                                   |
| New private function / file           | No               | No                                   |
| Rename or remove a public symbol      | Yes (breaking)   | Yes                                  |
| Doc / README / `.agents/rules/*` only | No               | No                                   |
| Test-only change                      | No               | No                                   |
