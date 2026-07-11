export {
  AmqpClient,
  type AmqpClientOptions,
  type ConsumeCallback,
  type ConsumerOptions,
  DEFAULT_CONNECT_TIMEOUT_MS,
  type PublishOptions,
} from "./amqp-client.js";
export {
  _getConnectionCountForTesting,
  _internal_getConnectionCount,
  _internal_resetConnections,
  _resetConnectionsForTesting,
} from "./connection-manager.js";
export {
  isRpcError,
  MessageValidationError,
  RPC_ERROR_CODE_HEADER,
  RpcError,
  rpcError,
  TechnicalError,
} from "./errors.js";
export type { Logger, LoggerContext } from "./logger.js";
export { safeJsonParse } from "./parsing.js";
export { setupAmqpTopology } from "./setup.js";
export {
  _internal_resetTelemetryCache,
  _resetTelemetryCacheForTesting,
  defaultTelemetryProvider,
  endSpanError,
  endSpanSuccess,
  MessagingSemanticConventions,
  recordConsumeMetric,
  recordLateRpcReply,
  recordPublishMetric,
  startConsumeSpan,
  startPublishSpan,
  type TelemetryProvider,
} from "./telemetry.js";
