export { TypedAmqpWorker } from "./worker.js";
export type { CreateWorkerOptions, ConsumerOptions } from "./worker.js";
export {
  // Error classes
  MessageValidationError,
  NonRetryableError,
  RetryableError,
  RpcError,
  // Type guards
  isHandlerError,
  isNonRetryableError,
  isRetryableError,
  isRpcError,
  // Factory functions
  nonRetryable,
  qualifyNonRetryable,
  qualifyRetryable,
  retryable,
  rpcError,
} from "./errors.js";
// HandlerError is now a tagged union type (RetryableError | NonRetryableError),
// not a class — re-export it as a type.
export type { HandlerError } from "./errors.js";
export { defineHandler, defineHandlers } from "./handlers.js";
export { composeMiddleware, defineMiddleware } from "./middleware.js";
export type {
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
  WorkerInferRpcConsumedMessage,
  WorkerInferRpcErrors,
  WorkerInferRpcHandler,
  WorkerInferRpcHandlerEntry,
  WorkerInferRpcHeaders,
  WorkerInferRpcRequest,
  WorkerInferRpcResponse,
} from "./types.js";
