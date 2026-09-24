export {
  AmqpClient,
  type AmqpClientOptions,
  type AmqpConsumeOptions,
  type AmqpPublishOptions,
  type ConnectionSource,
  type ConsumeCallback,
  DEFAULT_CONNECT_TIMEOUT_MS,
  DEFAULT_PREFETCH,
  DEFAULT_PUBLISH_TIMEOUT_MS,
} from "./amqp-client.js";
export { DEFAULT_MAX_MESSAGE_BYTES } from "./codec.js";
export {
  ConnectionError,
  isConnectionError,
  isMessageValidationError,
  isRpcError,
  isTechnicalError,
  MessageValidationError,
  PublishError,
  type PublishFailureReason,
  RPC_ERROR_CODE_HEADER,
  RpcError,
  rpcError,
  TechnicalError,
} from "./errors.js";
export type { Logger, LoggerContext } from "./logger.js";
export type { TopologyMode } from "./setup.js";
export {
  defaultTelemetryProvider,
  MessagingSemanticConventions,
  type TelemetryProvider,
} from "./telemetry.js";
