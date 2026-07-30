export {
  AmqpClient,
  type AmqpClientOptions,
  type ConsumeCallback,
  type ConsumerOptions,
  DEFAULT_CONNECT_TIMEOUT_MS,
  type PublishOptions,
} from "./amqp-client.js";
export {
  _internal_getConnectionCount,
  _internal_resetConnections,
  type ConnectionLease,
} from "./connection-manager.js";
export {
  isMessageValidationError,
  isRpcError,
  isTechnicalError,
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
