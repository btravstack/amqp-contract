import { TaggedError } from "unthrown";

/**
 * Error for technical/runtime failures that cannot be prevented by TypeScript.
 *
 * This includes AMQP connection failures, channel issues, validation failures,
 * and other runtime errors. This error is shared across core, worker, and client packages.
 *
 * Built on unthrown's {@link TaggedError}, so it carries a `_tag` of
 * `"@amqp-contract/TechnicalError"` for exhaustive dispatch via `matchTags`. The
 * tag is namespaced to avoid colliding with other libraries' tags in a shared
 * `matchTags`; the human-facing `Error.name` is kept bare (`"TechnicalError"`).
 * Remains a real `Error` (and a *modeled* error — it lives in the `E` channel of
 * a `Result`, never the `Defect` channel).
 */
export class TechnicalError extends TaggedError("@amqp-contract/TechnicalError", {
  name: "TechnicalError",
})<{
  cause?: unknown;
}> {
  constructor(message: string, cause?: unknown) {
    super({ cause });
    this.message = message;
  }
}

/**
 * Error thrown when message validation fails (payload or headers).
 *
 * Used by both the client (publish-time payload validation) and the worker
 * (consume-time payload and headers validation). Carries a `_tag` of
 * `"@amqp-contract/MessageValidationError"` (namespaced to avoid collisions);
 * the `Error.name` is kept bare (`"MessageValidationError"`).
 *
 * @param source - The name of the publisher or consumer that triggered the validation
 * @param issues - The validation issues from the Standard Schema validation
 */
export class MessageValidationError extends TaggedError("@amqp-contract/MessageValidationError", {
  name: "MessageValidationError",
})<{
  source: string;
  issues: unknown;
}> {
  constructor(source: string, issues: unknown) {
    super({ source, issues });
    this.message = `Message validation failed for "${source}"`;
  }
}

/**
 * AMQP message header carrying the error code of a typed RPC error reply.
 *
 * A reply message with this header is an error reply: its body is
 * `{ message, data }` where `data` conforms to the error's declared schema in
 * the RPC's `errors` map. A reply without it is a regular success reply whose
 * body is the response payload — so success replies are wire-compatible with
 * contracts that declare no errors.
 */
export const RPC_ERROR_CODE_HEADER = "x-amqp-contract-error-code";

/**
 * A typed, contract-declared RPC error — the business-failure channel of an
 * RPC, as opposed to the transport failures modeled by {@link TechnicalError}.
 *
 * Declared per-RPC via `defineRpc(queue, { request, response, errors })`,
 * where each error code maps to a message definition validating the error's
 * `data` payload. A worker handler surfaces one by returning
 * `Err(rpcError(code, data))`; the worker validates `data` against the
 * declared schema, publishes an error reply, and acks the request (business
 * errors are not retried). The caller's `client.call(...)` resolves to
 * `Err(RpcError<code, data>)` with `data` re-validated on arrival.
 *
 * Carries a `_tag` of `"@amqp-contract/RpcError"` for exhaustive dispatch via
 * `matchTags`; the `Error.name` is kept bare (`"RpcError"`). Discriminate
 * between codes on the `code` property.
 */
export class RpcError<TCode extends string = string, TData = unknown> extends TaggedError(
  "@amqp-contract/RpcError",
  { name: "RpcError" },
)<{
  code: string;
  data: unknown;
}> {
  declare readonly code: TCode;
  declare readonly data: TData;

  constructor(code: TCode, data: TData, message?: string) {
    super({ code, data });
    this.message = message ?? `RPC failed with error "${code}"`;
  }
}

/**
 * Type guard to check if an error is an {@link RpcError}.
 *
 * Narrowing to a specific code (and thus a typed `data`) is done on the
 * `code` property after the guard, or via `matchTags` on the `_tag`.
 */
export function isRpcError(error: unknown): error is RpcError {
  return error instanceof RpcError;
}

/**
 * Create an {@link RpcError} with less verbosity.
 *
 * The code/data pair must match one of the entries declared in the RPC's
 * `errors` map — the handler's return type enforces this at compile time, and
 * the worker validates `data` against the declared schema at runtime before
 * replying.
 *
 * @param code - The error code, as declared in the RPC's `errors` map
 * @param data - The error data, validated against the declared schema
 * @param message - Optional human-readable message (defaults to a generic one)
 *
 * @example
 * ```typescript
 * import { rpcError } from '@amqp-contract/worker';
 * import { Err } from 'unthrown';
 *
 * const handler = ({ payload }) => {
 *   if (!orders.has(payload.orderId)) {
 *     return ErrAsync(rpcError('ORDER_NOT_FOUND', { orderId: payload.orderId }));
 *   }
 *   // ...
 * };
 * ```
 */
export function rpcError<TCode extends string, TData>(
  code: TCode,
  data: TData,
  message?: string,
): RpcError<TCode, TData> {
  return new RpcError(code, data, message);
}
