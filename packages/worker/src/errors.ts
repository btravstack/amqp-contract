import { TaggedError } from "unthrown";

export { MessageValidationError } from "@amqp-contract/core";

/**
 * Retryable errors - transient failures that may succeed on retry
 * Examples: network timeouts, rate limiting, temporary service unavailability
 *
 * Use this error type when the operation might succeed if retried.
 * The worker will apply exponential backoff and retry the message.
 *
 * Built on unthrown's {@link TaggedError}, so it carries a `_tag` of
 * `"RetryableError"` (its `name` is the same) for exhaustive dispatch via
 * `matchTags`.
 */
export class RetryableError extends TaggedError("RetryableError")<{
  message: string;
  cause?: unknown;
}> {
  constructor(message: string, cause?: unknown) {
    super({ message, cause });
  }
}

/**
 * Non-retryable errors - permanent failures that should not be retried
 * Examples: invalid data, business rule violations, permanent external failures
 *
 * Use this error type when retrying would not help - the message will be
 * immediately sent to the dead letter queue (DLQ) if configured. Carries a
 * `_tag` of `"NonRetryableError"`.
 */
export class NonRetryableError extends TaggedError("NonRetryableError")<{
  message: string;
  cause?: unknown;
}> {
  constructor(message: string, cause?: unknown) {
    super({ message, cause });
  }
}

/**
 * Any handler-signalled error — the union a handler may put in the `Err`
 * channel of its `AsyncResult`. Discriminate on `_tag` (`"RetryableError"` /
 * `"NonRetryableError"`), e.g. with `matchTags`.
 *
 * Previously an abstract base class; now a tagged union, because unthrown's
 * `TaggedError` mints a distinct base class per tag. Use {@link isHandlerError}
 * for runtime narrowing instead of `instanceof HandlerError`.
 */
export type HandlerError = RetryableError | NonRetryableError;

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Type guard to check if an error is a RetryableError.
 *
 * Use this to check error types in catch blocks or error handlers.
 *
 * @param error - The error to check
 * @returns True if the error is a RetryableError
 *
 * @example
 * ```typescript
 * import { isRetryableError } from '@amqp-contract/worker';
 *
 * try {
 *   await processMessage();
 * } catch (error) {
 *   if (isRetryableError(error)) {
 *     console.log('Will retry:', error.message);
 *   } else {
 *     console.log('Permanent failure:', error);
 *   }
 * }
 * ```
 */
export function isRetryableError(error: unknown): error is RetryableError {
  return error instanceof RetryableError;
}

/**
 * Type guard to check if an error is a NonRetryableError.
 *
 * Use this to check error types in catch blocks or error handlers.
 *
 * @param error - The error to check
 * @returns True if the error is a NonRetryableError
 *
 * @example
 * ```typescript
 * import { isNonRetryableError } from '@amqp-contract/worker';
 *
 * try {
 *   await processMessage();
 * } catch (error) {
 *   if (isNonRetryableError(error)) {
 *     console.log('Will not retry:', error.message);
 *   }
 * }
 * ```
 */
export function isNonRetryableError(error: unknown): error is NonRetryableError {
  return error instanceof NonRetryableError;
}

/**
 * Type guard to check if an error is any HandlerError (RetryableError or NonRetryableError).
 *
 * @param error - The error to check
 * @returns True if the error is a HandlerError
 *
 * @example
 * ```typescript
 * import { isHandlerError } from '@amqp-contract/worker';
 *
 * function handleError(error: unknown) {
 *   if (isHandlerError(error)) {
 *     // error is RetryableError | NonRetryableError
 *     console.log('Handler error:', error.name, error.message);
 *   }
 * }
 * ```
 */
export function isHandlerError(error: unknown): error is HandlerError {
  return error instanceof RetryableError || error instanceof NonRetryableError;
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a RetryableError with less verbosity.
 *
 * This is a shorthand factory function for creating RetryableError instances.
 * Use it for cleaner error creation in handlers.
 *
 * @param message - Error message describing the failure
 * @param cause - Optional underlying error that caused this failure
 * @returns A new RetryableError instance
 *
 * @example
 * ```typescript
 * import { retryable } from '@amqp-contract/worker';
 * import { fromPromise } from 'unthrown';
 *
 * const handler = ({ payload }) =>
 *   fromPromise(
 *     processPayment(payload),
 *     (e) => retryable('Payment service unavailable', e),
 *   ).map(() => undefined);
 *
 * // Equivalent to:
 * // fromPromise(processPayment(payload), (e) => new RetryableError('...', e))
 * ```
 */
export function retryable(message: string, cause?: unknown): RetryableError {
  return new RetryableError(message, cause);
}

/**
 * Create a NonRetryableError with less verbosity.
 *
 * This is a shorthand factory function for creating NonRetryableError instances.
 * Use it for cleaner error creation in handlers.
 *
 * @param message - Error message describing the failure
 * @param cause - Optional underlying error that caused this failure
 * @returns A new NonRetryableError instance
 *
 * @example
 * ```typescript
 * import { nonRetryable } from '@amqp-contract/worker';
 * import { err, ok } from 'unthrown';
 *
 * const handler = ({ payload }) => {
 *   if (!isValidPayload(payload)) {
 *     return err(nonRetryable('Invalid payload format')).toAsync();
 *   }
 *   return ok(undefined).toAsync();
 * };
 *
 * // Equivalent to:
 * // return err(new NonRetryableError('Invalid payload format')).toAsync();
 * ```
 */
export function nonRetryable(message: string, cause?: unknown): NonRetryableError {
  return new NonRetryableError(message, cause);
}
