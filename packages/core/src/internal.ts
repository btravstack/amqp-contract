/**
 * `@amqp-contract/core/internal` — cross-package internals with **no semver
 * guarantee**, kept off the package root so the public API surface stays
 * honest. Today: test-lifecycle helpers for the module-level connection pool
 * and telemetry cache.
 *
 * Reach for these in test teardown only (see `docs/how-to/share-connections.md`);
 * production code never needs them.
 */
export { _internal_getConnectionCount, _internal_resetConnections } from "./connection-manager.js";
export { _internal_resetTelemetryCache } from "./telemetry.js";
