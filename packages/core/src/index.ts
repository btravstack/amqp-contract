export {
  AmqpClient,
  type AmqpClientOptions,
  type AmqpConsumeOptions,
  type AmqpPublishOptions,
  type ConsumeCallback,
  DEFAULT_CONNECT_TIMEOUT_MS,
} from "./amqp-client.js";
export { type ConnectionLease } from "./connection-manager.js";
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
