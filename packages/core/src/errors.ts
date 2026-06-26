import { TaggedError } from "unthrown";

/**
 * Error for technical/runtime failures that cannot be prevented by TypeScript.
 *
 * This includes AMQP connection failures, channel issues, validation failures,
 * and other runtime errors. This error is shared across core, worker, and client packages.
 *
 * Built on unthrown's {@link TaggedError}, so it carries a `_tag` of
 * `"TechnicalError"` for exhaustive dispatch via `matchTags`, and remains a real
 * `Error` (and a *modeled* error — it lives in the `E` channel of a `Result`,
 * never the `Defect` channel).
 */
export class TechnicalError extends TaggedError("TechnicalError")<{
  message: string;
  cause?: unknown;
}> {
  constructor(message: string, cause?: unknown) {
    super({ message, cause });
  }
}

/**
 * Error thrown when message validation fails (payload or headers).
 *
 * Used by both the client (publish-time payload validation) and the worker
 * (consume-time payload and headers validation). Carries a `_tag` of
 * `"MessageValidationError"`.
 *
 * @param source - The name of the publisher or consumer that triggered the validation
 * @param issues - The validation issues from the Standard Schema validation
 */
export class MessageValidationError extends TaggedError("MessageValidationError")<{
  message: string;
  source: string;
  issues: unknown;
}> {
  constructor(source: string, issues: unknown) {
    super({ message: `Message validation failed for "${source}"`, source, issues });
  }
}
