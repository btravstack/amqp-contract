import { fromThrowable, type Result } from "unthrown";

/**
 * Parse a `Buffer` as JSON, mapping any `JSON.parse` exception to the
 * caller-supplied error type.
 *
 * Use this in consume / reply paths where a parse failure is a typed value,
 * not a thrown exception — the caller decides how to translate the raw error
 * into a domain-level error (e.g. {@link TechnicalError}).
 *
 * @typeParam E - The error type produced by `errorFn`.
 * @param buffer - The raw message body to parse.
 * @param errorFn - Callback invoked with the underlying `JSON.parse` error.
 * @returns A `Result` containing the parsed `unknown` value or the mapped error.
 *
 * @example
 * ```typescript
 * const parsed = safeJsonParse(
 *   msg.content,
 *   (error) => new TechnicalError("Failed to parse JSON", error),
 * );
 * ```
 */
export function safeJsonParse<E>(buffer: Buffer, errorFn: (raw: unknown) => E): Result<unknown, E> {
  return fromThrowable(() => JSON.parse(buffer.toString()) as unknown, errorFn)();
}
