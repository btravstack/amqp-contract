---
"@amqp-contract/contract": minor
"@amqp-contract/core": minor
---

Define-time structural validation, shared issue formatters, and the `_internal_` prefix convention (org DNA alignment, #552):

- **Builders now fail at definition time on structural mistakes** the type system can't catch for JavaScript callers: unknown option keys (`durabel: false` used to be silently ignored) on `defineExchange`/`defineQueue` (incl. `retry`/`deadLetter` bags)/`defineMessage`/`defineRpc`, empty names, unknown exchange types, and non-Standard-Schema payload/headers/error-data schemas (duck-checked via `~standard.validate`).
- **`formatIssue` / `summarizeIssues`** exported from `@amqp-contract/contract` — the single source of truth for rendering Standard Schema issues. `MessageValidationError.message` now includes the summarized issues (`Message validation failed for "x": field: message (+2 more)`).
- **`_internal_` prefix** for cross-package internals with no semver guarantee: `_internal_getConnectionCount`, `_internal_resetConnections`, `_internal_resetTelemetryCache` (the `*ForTesting` names remain as deprecated aliases).
