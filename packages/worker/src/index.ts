export { DEFAULT_DRAIN_TIMEOUT_MS, TypedAmqpWorker } from "./worker.js";
export type { ConsumerOptions, CreateWorkerOptions, WorkerCreateContextInfo } from "./worker.js";
export {
  // Error classes
  MessageValidationError,
  NonRetryableError,
  RetryableError,
  RpcError,
  // Type guards
  isRpcError,
  // Factory functions
  qualifyNonRetryable,
  qualifyRetryable,
  rpcError,
} from "./errors.js";
// HandlerError is now a tagged union type (RetryableError | NonRetryableError),
// not a class — re-export it as a type.
export type { HandlerError } from "./errors.js";
// Re-exported from core so naming an option type or matching a defect cause
// (`TechnicalError`) never forces a direct dependency on @amqp-contract/core.
export {
  isMessageValidationError,
  isTechnicalError,
  TechnicalError,
  type Logger,
  type LoggerContext,
  type TelemetryProvider,
} from "@amqp-contract/core";
export { declareHandler, declareHandlers } from "./handlers.js";
export { composeMiddleware, declareMiddleware } from "./middleware.js";
export type {
  AnyWorkerMiddleware,
  EmptyContext,
  WorkerMiddleware,
  WorkerMiddlewareArgs,
  WorkerMiddlewareNext,
} from "./middleware.js";
export type {
  WorkerConsumedMessage,
  WorkerInferConsumedMessage,
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlerEntry,
  WorkerInferConsumerHeaders,
  WorkerInferHandlers,
  WorkerHandlerHelpers,
  WorkerInferRpcConsumedMessage,
  WorkerInferRpcErrorConstructors,
  WorkerInferRpcErrors,
  WorkerInferRpcHandler,
  WorkerInferRpcHandlerEntry,
  WorkerInferRpcHeaders,
  WorkerInferRpcRequest,
  WorkerInferRpcResponse,
} from "./types.js";
