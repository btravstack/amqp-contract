---
"@amqp-contract/asyncapi": patch
"@amqp-contract/client": patch
"@amqp-contract/contract": patch
"@amqp-contract/core": patch
"@amqp-contract/testing": patch
"@amqp-contract/worker": patch
---

Pin `amqplib` back to the `0.10.x` line (was `1.0.3`). The `1.x` series ships breaking API changes that the worker and client paths haven't been validated against; staying on `0.10.9` keeps runtime behaviour aligned with what's covered by the integration tests and what `amqp-connection-manager@5` expects.

Workspace housekeeping with no user-visible impact: top-level `pnpm` settings in `pnpm-workspace.yaml` are now under the correct keys (the previous `settings:` nested block was silently ignored by pnpm 9+), and a `peerDependencyRules.ignoreMissing` entry is added for `search-insights` — VitePress bundles `@docsearch/react` even when the docs site uses `provider: "local"`, and the missing peer was tripping `strictPeerDependencies` once the settings actually took effect.
