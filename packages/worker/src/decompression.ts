import { promisify } from "node:util";
import { gunzip, inflate } from "node:zlib";

import { TechnicalError } from "@amqp-contract/core";
import { fromPromise, fromSafeThrowable, OkAsync, type AsyncResult } from "unthrown";

const gunzipAsync = promisify(gunzip);
const inflateAsync = promisify(inflate);

/**
 * Supported content encodings for message decompression.
 */
const SUPPORTED_ENCODINGS = ["gzip", "deflate"] as const;

/**
 * Type for supported content encodings.
 */
type SupportedEncoding = (typeof SUPPORTED_ENCODINGS)[number];

/**
 * Type guard to check if a string is a supported encoding.
 */
function isSupportedEncoding(encoding: string): encoding is SupportedEncoding {
  return SUPPORTED_ENCODINGS.includes(encoding.toLowerCase() as SupportedEncoding);
}

/**
 * Decompress a buffer based on the content-encoding header.
 *
 * @param buffer - The buffer to decompress
 * @param contentEncoding - The content-encoding header value (e.g., 'gzip', 'deflate')
 * @returns An AsyncResult resolving to the decompressed buffer. Decompression
 *   failures are infrastructure faults, so they surface through the `Defect`
 *   channel (with a {@link TechnicalError} cause), never a modeled `Err`.
 *
 * @internal
 */
export function decompressBuffer(
  buffer: Buffer,
  contentEncoding: string | undefined,
): AsyncResult<Buffer, never> {
  if (!contentEncoding) {
    return OkAsync(buffer);
  }

  const normalizedEncoding = contentEncoding.toLowerCase();

  if (!isSupportedEncoding(normalizedEncoding)) {
    return fromSafeThrowable((): Buffer => {
      // oxlint-disable-next-line unthrown/no-throw -- deliberate defect-channel routing inside the fromSafeThrowable thunk
      throw new TechnicalError(
        `Unsupported content-encoding: "${contentEncoding}". ` +
          `Supported encodings are: ${SUPPORTED_ENCODINGS.join(", ")}. ` +
          `Please check your publisher configuration.`,
      );
    })().toAsync();
  }

  switch (normalizedEncoding) {
    case "gzip":
      return fromPromise(gunzipAsync(buffer), (error, defect) =>
        defect(new TechnicalError("Failed to decompress gzip", error)),
      );
    case "deflate":
      return fromPromise(inflateAsync(buffer), (error, defect) =>
        defect(new TechnicalError("Failed to decompress deflate", error)),
      );
  }
}
