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
