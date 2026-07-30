import { fromThrowable, type NotThenable, type Result } from "unthrown";

/**
 * The `defect` helper injected into a qualify callback — the second argument
 * of unthrown's `fromThrowable` qualify. Extracted structurally because the
 * branded `Defect` marker type is internal to unthrown.
 */
type DefectFn = Parameters<Parameters<typeof fromThrowable>[1]>[1];

/** The opaque marker {@link DefectFn} returns. */
type DefectMark = ReturnType<DefectFn>;

/**
 * Parse a `Buffer` as JSON, triaging any `JSON.parse` exception through the
 * caller-supplied qualify callback.
 *
 * Use this in consume / reply paths where a parse failure is a typed value,
 * not a thrown exception — the caller decides how to translate the raw error
 * into a domain-level error (e.g. {@link TechnicalError}), or routes it to
 * the defect channel via the injected `defect` helper (the full unthrown
 * qualify signature, so no model-then-defect round-trip is ever needed).
 *
 * @typeParam R - What the qualify callback produces: a modeled error type,
 *   the defect marker, or a union of both. The defect marker is subtracted
 *   from the resulting error channel, exactly like `fromThrowable`.
 * @param buffer - The raw message body to parse.
 * @param qualify - Callback invoked with the underlying `JSON.parse` error and
 *   the injected `defect` helper; returns the modeled error or `defect(cause)`.
 * @returns A `Result` containing the parsed `unknown` value or the mapped error.
 *
 * @example Modeled error
 * ```typescript
 * const parsed = safeJsonParse(
 *   msg.content,
 *   (error) => new TechnicalError("Failed to parse JSON", error),
 * );
 * ```
 *
 * @example Defect-channel routing
 * ```typescript
 * const parsed = safeJsonParse(
 *   msg.content,
 *   (error, defect) => defect(new TechnicalError("Failed to parse JSON", error)),
 * ); // Result<unknown, never>
 * ```
 */
export function safeJsonParse<R>(
  buffer: Buffer,
  // The qualify's return is constrained `NotThenable` at THIS boundary — the
  // same requirement unthrown v5's `fromThrowable` qualify carries — so it
  // flows through with no cast, and a thenable error type is rejected here at
  // the call site instead of being silently accepted. For every real domain
  // error (extends `Error`), `NotThenable<R>` is `unknown`, so callers just
  // see their own error type.
  qualify: (raw: unknown, defect: DefectFn) => R & NotThenable<R>,
): Result<unknown, Exclude<R, DefectMark>> {
  return fromThrowable(() => JSON.parse(buffer.toString()) as unknown, qualify)();
}
