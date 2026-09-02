import { summarizeIssues } from "@amqp-contract/contract";
import { TaggedError } from "unthrown";

/**
 * Error for technical/runtime failures that cannot be prevented by TypeScript.
 *
 * This includes channel issues, compression/parse faults, and other unexpected
 * runtime errors. Shared across core, worker, and client packages. Dialing the
 * broker is NOT one of them any more — that failure is anticipated, and it is
 * modeled as {@link ConnectionError}.
 *
 * These failures are **unexpected**, so `@amqp-contract` surfaces them through
 * unthrown's **defect** channel, not the modeled `E` channel: a `TechnicalError`
 * instance is carried as the `cause` of a `Defect` (so its message/cause survive
 * for logging), and is handled in the `defect` arm of `result.match({ ok,
 * errCases, defect })` — or via `recoverDefect` / `tapDefect` — never matched in
 * `errCases`. It is deliberately absent from every operation's `E` (only
 * anticipated domain failures live there).
 *
 * Built on unthrown's {@link TaggedError}, so it carries a `_tag` of
 * `"@amqp-contract/TechnicalError"` (namespaced to avoid colliding with other
 * libraries' tags); the human-facing `Error.name` is kept bare
 * (`"TechnicalError"`). Remains a real `Error`.
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
 * The broker could not be reached: refused, unresolvable, unauthorized, or
 * still not ready when `connectTimeoutMs` elapsed.
 *
 * **Modeled, not a defect** — unlike {@link TechnicalError}. An unreachable
 * broker is the anticipated failure of dialing one: it is what a wrong URL, a
 * rotated credential or a cluster that has not come up yet look like, every
 * one of them an operator's business rather than a bug in the caller. So
 * `TypedAmqpWorker.create` and `TypedAmqpClient.create` report it on the `E`
 * channel, where a start-up path can triage it by tag and turn it into an exit
 * code, a retry, or a health probe — and the defect channel keeps its meaning:
 * the failures nobody anticipated.
 *
 * Carries a `_tag` of `"@amqp-contract/ConnectionError"`; the human-facing
 * `Error.name` is kept bare (`"ConnectionError"`). The underlying amqplib
 * rejection is on `cause`.
 */
export class ConnectionError extends TaggedError("@amqp-contract/ConnectionError", {
  name: "ConnectionError",
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
    // Render the issues into the message via the shared formatter when they
    // look like Standard Schema issues; keep the plain message otherwise
    // (issues is typed unknown — defensive against foreign shapes).
    const summary = Array.isArray(issues)
      ? ((): string | undefined => {
          try {
            return summarizeIssues(issues as Parameters<typeof summarizeIssues>[0]);
          } catch {
            return undefined;
          }
        })()
      : undefined;
    this.message = summary
      ? `Message validation failed for "${source}": ${summary}`
      : `Message validation failed for "${source}"`;
  }
}

/**
 * Type guard to check if an error is a {@link TechnicalError} — the cause
 * carried by every infrastructure `Defect` this library produces.
 */
export function isTechnicalError(error: unknown): error is TechnicalError {
  return error instanceof TechnicalError;
}

/**
 * Type guard to check if an error is a {@link ConnectionError} — the modeled
 * failure of dialing the broker.
 */
export function isConnectionError(error: unknown): error is ConnectionError {
  return error instanceof ConnectionError;
}

/**
 * Type guard to check if an error is a {@link MessageValidationError}.
 */
export function isMessageValidationError(error: unknown): error is MessageValidationError {
  return error instanceof MessageValidationError;
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
 * RPC, as opposed to transport failures (which surface as a `Defect` with a
 * {@link TechnicalError} cause).
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
 * the error matcher (`result.match({ ok, defect, errCases: (matcher) =>
 * matcher.with(P.tag("@amqp-contract/RpcError"), …) })`); the `Error.name` is kept
 * bare (`"RpcError"`). Discriminate between codes on the `code` property.
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
 * `code` property after the guard, or via the error matcher on the `_tag`
 * (`matcher.with(P.tag("@amqp-contract/RpcError"), …)`).
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
 * import { ErrAsync } from 'unthrown';
 *
 * const handler = ({ input: { payload } }) => {
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
