import { fromSafeThrowable, type Result } from "unthrown";

import type { TechnicalError } from "./errors.js";

/**
 * Mint a `Defect`-carrying `Result` from a {@link TechnicalError}, for the
 * imperative sites (outside a combinator callback) that must surface an
 * unexpected infrastructure failure through the defect channel. Uses the
 * `fromSafeThrowable` boundary — the sanctioned way to route a throw to a
 * `Defect` without a public defect constructor.
 *
 * The shape is the sync `Result<never, never>`: it fits every channel (both
 * `T` and `E` are `never`), so it can seed any pipeline. Call `.toAsync()`
 * where an `AsyncResult` is expected.
 *
 * Shared by `@amqp-contract/client` and `@amqp-contract/worker` (each used to
 * hand-roll its own copy); exported so any layer can mint a defect the same
 * way instead of re-deriving the boundary trick.
 */
export function technicalDefect(error: TechnicalError): Result<never, never> {
  return fromSafeThrowable((): never => {
    // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
    throw error;
  })();
}
