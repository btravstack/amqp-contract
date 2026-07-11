import type { MessageValidationError, RpcError, TechnicalError } from "@amqp-contract/core";
import type { AsyncResult } from "unthrown";
import type { CallOptions, PublishOptions } from "./client.js";
import type { RpcCancelledError, RpcTimeoutError } from "./errors.js";

/**
 * Error union a publish interceptor chain resolves with — identical to the
 * error channel of `client.publish(...)`.
 */
export type PublishError = TechnicalError | MessageValidationError;

/**
 * Error union a call interceptor chain resolves with. The `RpcError` member
 * is the widened (untyped) form; the public `client.call(...)` signature
 * narrows it to the RPC's declared error union.
 */
export type CallError =
  | TechnicalError
  | MessageValidationError
  | RpcTimeoutError
  | RpcCancelledError
  | RpcError;

/**
 * Arguments a publish interceptor observes. `message` is the pre-validation
 * payload — a patched message goes through schema validation exactly like the
 * caller's original.
 */
export type PublishInterceptorArgs = {
  /** The contract `publishers` key being published. */
  publisherName: string;
  /** The (not yet validated) message payload. */
  message: unknown;
  /** Per-call publish options (defaults are merged later, inside the client). */
  options: PublishOptions;
};

/**
 * Continuation a publish interceptor calls to run the rest of the chain.
 * The optional patch replaces `message` and/or `options` for everything
 * downstream (inner interceptors, validation, and the publish itself).
 */
export type PublishInterceptorNext = (patch?: {
  message?: unknown;
  options?: PublishOptions;
}) => AsyncResult<void, PublishError>;

/**
 * Intercepts `client.publish(...)`: runs outside validation and publishing,
 * so it can stamp headers (e.g. trace context), transform the payload, retry
 * by calling `next` again, or short-circuit by returning without calling
 * `next`. The first interceptor in the array is the outermost.
 *
 * @example Stamp a correlation header on every outgoing message
 * ```typescript
 * const stampTenant: PublishInterceptor = (args, next) =>
 *   next({
 *     options: {
 *       ...args.options,
 *       headers: { ...args.options.headers, 'x-tenant-id': currentTenant() },
 *     },
 *   });
 * ```
 */
export type PublishInterceptor = (
  args: PublishInterceptorArgs,
  next: PublishInterceptorNext,
) => AsyncResult<void, PublishError>;

/**
 * Arguments a call interceptor observes. `request` is the pre-validation
 * request payload.
 */
export type CallInterceptorArgs = {
  /** The contract `rpcs` key being called. */
  rpcName: string;
  /** The (not yet validated) request payload. */
  request: unknown;
  /** Per-call options (`timeoutMs`, `publishOptions`). */
  options: CallOptions;
};

/**
 * Continuation a call interceptor calls to run the rest of the chain. The
 * resolved value is the RPC response (typed at the public `call()` boundary).
 */
export type CallInterceptorNext = (patch?: {
  request?: unknown;
  options?: CallOptions;
}) => AsyncResult<unknown, CallError>;

/**
 * Intercepts `client.call(...)`: wraps the full request/reply round trip, so
 * it can adjust timeouts, stamp request headers, observe replies and typed
 * errors, or retry by calling `next` again. The first interceptor in the
 * array is the outermost.
 *
 * @example Retry timed-out calls once
 * ```typescript
 * const retryOnce: CallInterceptor = (args, next) =>
 *   next().orElse((error) =>
 *     error instanceof RpcTimeoutError ? next() : Err(error).toAsync(),
 *   );
 * ```
 */
export type CallInterceptor = (
  args: CallInterceptorArgs,
  next: CallInterceptorNext,
) => AsyncResult<unknown, CallError>;

/**
 * Run an interceptor chain: interceptors execute left-to-right (first =
 * outermost), each receiving the args as patched by everything outside it;
 * `terminal` receives the final args and performs the real operation.
 *
 * @internal Shared by `publish()` and `call()`; not part of the public API.
 */
export function chainInterceptors<TArgs extends object, TPatch, TValue, TError>(
  interceptors: readonly ((
    args: TArgs,
    next: (patch?: TPatch) => AsyncResult<TValue, TError>,
  ) => AsyncResult<TValue, TError>)[],
  args: TArgs,
  terminal: (args: TArgs) => AsyncResult<TValue, TError>,
): AsyncResult<TValue, TError> {
  const run = (index: number, current: TArgs): AsyncResult<TValue, TError> =>
    index >= interceptors.length
      ? terminal(current)
      : interceptors[index]!(current, (patch) => run(index + 1, { ...current, ...patch }));
  return run(0, args);
}
