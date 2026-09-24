export { TypedAmqpClient } from "./client.js";
export type { CallOptions, CreateClientOptions, PublishOptions } from "./client.js";
export {
  isRpcError,
  MessageValidationError,
  RpcCancelledError,
  RpcError,
  RpcTimeoutError,
} from "./errors.js";
// Re-exported from core so naming an option type or matching a defect cause
// (`TechnicalError`) never forces a direct dependency on @amqp-contract/core.
export {
  isMessageValidationError,
  ConnectionError,
  isConnectionError,
  isTechnicalError,
  PublishError,
  type PublishFailureReason,
  TechnicalError,
  type Logger,
  type LoggerContext,
  type ConnectionSource,
  type TelemetryProvider,
  type TopologyMode,
} from "@amqp-contract/core";
export type {
  CallError,
  CallInterceptor,
  CallInterceptorArgs,
  CallInterceptorNext,
  ClientPublishError,
  PublishInterceptor,
  PublishInterceptorArgs,
  PublishInterceptorNext,
} from "./interceptors.js";
export type {
  ClientInferCallError,
  ClientInferPublisherInput,
  ClientInferRpcErrors,
  ClientInferRpcRequestInput,
  ClientInferRpcResponseOutput,
} from "./types.js";
