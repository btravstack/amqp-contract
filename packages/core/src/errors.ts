import { summarizeIssues } from "@amqp-contract/contract";
import { TaggedError } from "unthrown";

/**
 * Re-capture an error's stack once its `name` and `message` are final.
 *
 * unthrown's `TaggedError` calls `super()` with no message, so V8 renders the
 * stack header before `name`/`message` exist — every amqp-contract error used
 * to print as a bare `Error` at the top of its stack. Called last in each
 * error constructor; `this.constructor` trims the constructor frames.
 *
 * @internal
 */
export function recaptureStack(error: Error): void {
  Error.captureStackTrace?.(error, error.constructor);
}

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
  /** The `_tag`, for `P.tag(TechnicalError.tag)` without a raw string. */
  static readonly tag = "@amqp-contract/TechnicalError";

  constructor(message: string, cause?: unknown) {
    super({ cause });
    this.message = message;
    recaptureStack(this);
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
  /** The `_tag`, for `P.tag(ConnectionError.tag)` without a raw string. */
  static readonly tag = "@amqp-contract/ConnectionError";

  constructor(message: string, cause?: unknown) {
    super({ cause });
    this.message = message;
    recaptureStack(this);
  }
}

/**
 * Why the broker side of a publish failed — each one something core can
 * actually observe on amqp-connection-manager's confirm channel:
 *
 * - `"timeout"` — the message sat buffered past `publishTimeoutMs` (the broker
 *   was unreachable for that long).
 * - `"nacked"` — the broker refused the message (`basic.nack`).
 * - `"channel-closed"` — the channel closed before the message was confirmed.
 */
export type PublishFailureReason = "timeout" | "nacked" | "channel-closed";

const PUBLISH_FAILURE_DESCRIPTIONS: Record<PublishFailureReason, string> = {
  timeout: "timed out waiting for the broker (publishTimeoutMs)",
  nacked: "the broker rejected (nacked) the message",
  "channel-closed": "the channel closed before the message was confirmed",
};

/**
 * The broker side of a publish failed: timed out, nacked, or the channel
 * closed under it. (A full write buffer is NOT one: on the confirm channel the
 * wrapper only reports it after the broker confirmed the message.)
 *
 * **Modeled, not a defect** — a broker that is down, overloaded or refusing a
 * message is an operational condition a publisher is expected to handle
 * (buffer, retry, shed load, surface a 503), not a bug. Returned on the `E`
 * channel of `AmqpClient.publish`, `TypedAmqpClient.publish` and
 * `TypedAmqpClient.call`; switch on {@link PublishError.reason}. A failure
 * core cannot classify (an unencodable payload, an unknown rejection) stays a
 * `Defect` with a {@link TechnicalError} cause.
 *
 * Carries a `_tag` of `"@amqp-contract/PublishError"`; the `Error.name` is kept
 * bare (`"PublishError"`). The underlying rejection, if any, is on `cause`.
 */
export class PublishError extends TaggedError("@amqp-contract/PublishError", {
  name: "PublishError",
})<{
  reason: PublishFailureReason;
  /** Where the message was going, e.g. `exchange "orders" (routing key "order.created")`. */
  target: string;
  cause?: unknown;
}> {
  /** The `_tag`, for `P.tag(PublishError.tag)` without a raw string. */
  static readonly tag = "@amqp-contract/PublishError";

  constructor(props: { reason: PublishFailureReason; target: string; cause?: unknown }) {
    super(props);
    this.message = `Failed to publish message to ${props.target}: ${PUBLISH_FAILURE_DESCRIPTIONS[props.reason]}`;
    recaptureStack(this);
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
  /** The `_tag`, for `P.tag(MessageValidationError.tag)` without a raw string. */
  static readonly tag = "@amqp-contract/MessageValidationError";

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
    recaptureStack(this);
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
  /** The `_tag`, for `P.tag(RpcError.tag)` without a raw string. */
  static readonly tag = "@amqp-contract/RpcError";

  declare readonly code: TCode;
  declare readonly data: TData;

  constructor(code: TCode, data: TData, message?: string) {
    super({ code, data });
    this.message = message ?? `RPC failed with error "${code}"`;
    recaptureStack(this);
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
