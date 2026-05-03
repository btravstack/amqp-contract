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
  _resetConnectionsForTesting,
} from "./connection-manager.js";
export { MessageValidationError, TechnicalError } from "./errors.js";
export type { Logger, LoggerContext } from "./logger.js";
export { setupAmqpTopology } from "./setup.js";
export {
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
