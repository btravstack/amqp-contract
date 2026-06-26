export { TypedAmqpWorker } from "./worker.js";
export type { CreateWorkerOptions, ConsumerOptions } from "./worker.js";
export {
  // Error classes
  MessageValidationError,
  NonRetryableError,
  RetryableError,
  // Type guards
  isHandlerError,
  isNonRetryableError,
  isRetryableError,
  // Factory functions
  nonRetryable,
  retryable,
} from "./errors.js";
// HandlerError is now a tagged union type (RetryableError | NonRetryableError),
// not a class — re-export it as a type.
export type { HandlerError } from "./errors.js";
export { defineHandler, defineHandlers } from "./handlers.js";
export type {
  WorkerConsumedMessage,
  WorkerInferConsumedMessage,
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlerEntry,
  WorkerInferConsumerHeaders,
  WorkerInferHandlers,
  WorkerInferRpcConsumedMessage,
  WorkerInferRpcHandler,
  WorkerInferRpcHandlerEntry,
  WorkerInferRpcHeaders,
  WorkerInferRpcRequest,
  WorkerInferRpcResponse,
} from "./types.js";
