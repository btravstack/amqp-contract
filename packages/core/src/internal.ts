/**
 * `@amqp-contract/core/internal` — cross-package internals with **no semver
 * guarantee**, kept off the package root so the public API surface stays
 * honest: the implementation helpers client and worker share (codec,
 * lifecycle, topology setup, span/metric recording, defect minting), plus
 * test-lifecycle helpers for the module-level connection pool and telemetry
 * cache (see `docs/how-to/share-connections.md`).
 *
 * Application code never needs these; anything here may change in a patch.
 */
export {
  decodeMessage,
  type DecodeOptions,
  decompressBuffer,
  encodeBody,
  encodeMessage,
} from "./codec.js";
export {
  _internal_getConnectionCount,
  _internal_resetConnections,
  type ConnectionLease,
} from "./connection-manager.js";
export { technicalDefect } from "./defect.js";
export { recaptureStack } from "./errors.js";
export { startOrClose } from "./lifecycle.js";
export { safeJsonParse } from "./parsing.js";
export { publisherTopology, setupAmqpTopology } from "./setup.js";
export {
  _internal_resetTelemetryCache,
  endSpanError,
  endSpanSuccess,
  injectTraceContext,
  recordConsumeMetric,
  recordLateRpcReply,
  recordPublishMetric,
  recordRpcCallMetric,
  runWithTraceContext,
  startConsumeSpan,
  startPublishSpan,
} from "./telemetry.js";
